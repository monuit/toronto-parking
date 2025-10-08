"""Upload pre-aggregated tickets GeoJSON into Redis cache."""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import os
import sys
import time
from pathlib import Path

import dotenv
import redis


DEFAULT_NAMESPACE = "toronto:map-data"
DEFAULT_REDIS_TTL = 86_400  # 24 hours
RELATIVE_DATA_PATH = Path("map-app/public/data/tickets_aggregated.geojson")
SUMMARY_RELATIVE_PATH = Path("map-app/public/data/tickets_summary.json")
STREET_STATS_RELATIVE_PATH = Path("map-app/public/data/street_stats.json")
NEIGHBOURHOOD_STATS_RELATIVE_PATH = Path("map-app/public/data/neighbourhood_stats.json")

CHUNK_MANIFEST_KEY = "chunks"
SUMMARY_REDIS_KEY = "summary"
STREET_STATS_REDIS_KEY = "street-stats"
NEIGHBOURHOOD_STATS_REDIS_KEY = "neighbourhood-stats"


def _load_env(repo_root: Path) -> None:
    dotenv.load_dotenv(repo_root / ".env")


def _resolve_redis_url() -> str:
    candidates = (
        os.getenv("REDIS_PUBLIC_URL"),
        os.getenv("REDIS_URL"),
        os.getenv("REDIS_CONNECTION"),
    )
    for value in candidates:
        if value:
            return value
    raise RuntimeError("No Redis URL found in environment (REDIS_URL/REDIS_PUBLIC_URL/REDIS_CONNECTION).")


def _resolve_namespace() -> str:
    return os.getenv("MAP_DATA_REDIS_NAMESPACE", DEFAULT_NAMESPACE)


def _resolve_ttl() -> int | None:
    raw = os.getenv("MAP_DATA_REDIS_TTL")
    if not raw:
        return DEFAULT_REDIS_TTL
    try:
        ttl = int(raw)
    except ValueError as exc:  # pragma: no cover - configuration error
        raise RuntimeError(f"Invalid MAP_DATA_REDIS_TTL value: {raw}") from exc
    return ttl if ttl > 0 else None


def _read_geojson(repo_root: Path, override_path: str | None = None) -> tuple[str, Path]:
    if override_path:
        data_path = Path(override_path)
    else:
        data_path = (repo_root / RELATIVE_DATA_PATH).resolve()
    if not data_path.exists():
        raise FileNotFoundError(f"GeoJSON file not found at {data_path}")
    raw = data_path.read_text(encoding="utf-8")
    return raw, data_path


def _slugify_neighbourhood(name: str) -> str:
    if not name:
        return "unknown"
    text = (
        str(name)
        .strip()
        .lower()
        .replace("'", "")
        .replace("\"", "")
        .replace("/", "-")
        .replace(" ", "-")
    )
    return "".join(ch for ch in text if ch.isalnum() or ch in {"-", "_"}) or "unknown"


def _prepare_neighbourhood_payload(
    neighbourhood: str, features: list[dict], namespace: str
) -> tuple[str, dict]:
    slug = _slugify_neighbourhood(neighbourhood)
    payload_geojson = {
        "type": "FeatureCollection",
        "features": features,
    }
    raw = json.dumps(payload_geojson, ensure_ascii=False)
    key = f"{namespace}:tickets:aggregated:v1:neighbourhood:{slug}"
    payload = {
        "version": int(time.time() * 1000),
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "raw": base64.b64encode(gzip.compress(raw.encode("utf-8"))).decode("ascii"),
        "featureCount": len(features),
        "neighbourhood": neighbourhood,
        "slug": slug,
    }
    return key, payload


def _store_manifest(client, namespace: str, manifest: list[dict], ttl: int | None) -> None:
    key = f"{namespace}:tickets:aggregated:v1:{CHUNK_MANIFEST_KEY}"
    payload = json.dumps({
        "chunks": manifest,
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    client.set(key, payload, ex=ttl or None)


def _encode_payload(raw: str, namespace: str) -> tuple[str, dict[str, str | int]]:
    compressed = gzip.compress(raw.encode("utf-8"))
    encoded = base64.b64encode(compressed).decode("ascii")
    payload = {
        "version": int(time.time() * 1000),
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "raw": encoded,
    }
    key = f"{namespace}:tickets:aggregated:v1"
    return key, payload


def _group_features_by_neighbourhood(raw: str) -> dict[str, list[dict]]:
    data = json.loads(raw)
    features = data.get("features", []) if isinstance(data, dict) else []
    buckets: dict[str, list[dict]] = {}
    for feature in features:
        props = feature.get("properties") or {}
        neighbourhood = props.get("neighbourhood") or "Unknown"
        buckets.setdefault(neighbourhood, []).append(feature)
    return buckets


def _load_json_file(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:  # pragma: no cover - corrupted artefact
        raise RuntimeError(f"Invalid JSON payload in {path}") from exc


def _store_json_blob(client, key: str, payload: dict | list, ttl: int | None) -> None:
    wrapper = {
        "version": int(time.time() * 1000),
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data": payload,
    }
    client.set(key, json.dumps(wrapper), ex=ttl or None)


def push_to_redis(raw: str, data_path: Path, redis_url: str, namespace: str, ttl: int | None) -> None:
    key, payload = _encode_payload(raw, namespace)
    client = redis.from_url(redis_url)
    manifest_entries: list[dict] = []
    data_dir = data_path.parent
    try:
        client.set(key, json.dumps(payload), ex=ttl or None)

        neighbourhood_buckets = _group_features_by_neighbourhood(raw)
        for name, features in neighbourhood_buckets.items():
            if not features:
                continue
            chunk_key, chunk_payload = _prepare_neighbourhood_payload(name, features, namespace)
            client.set(chunk_key, json.dumps(chunk_payload), ex=ttl or None)
            manifest_entries.append(
                {
                    "key": chunk_key,
                    "featureCount": chunk_payload.get("featureCount", 0),
                    "neighbourhood": name,
                    "slug": chunk_payload.get("slug"),
                }
            )

        if manifest_entries:
            _store_manifest(client, namespace, manifest_entries, ttl)

        summary_path = data_dir / SUMMARY_RELATIVE_PATH.name
        street_stats_path = data_dir / STREET_STATS_RELATIVE_PATH.name
        neighbourhood_stats_path = data_dir / NEIGHBOURHOOD_STATS_RELATIVE_PATH.name

        summary_payload = _load_json_file(summary_path)
        if summary_payload is not None:
            summary_key = f"{namespace}:tickets:{SUMMARY_REDIS_KEY}:v1"
            _store_json_blob(client, summary_key, summary_payload, ttl)

        street_stats_payload = _load_json_file(street_stats_path)
        if street_stats_payload is not None:
            street_key = f"{namespace}:tickets:{STREET_STATS_REDIS_KEY}:v1"
            _store_json_blob(client, street_key, street_stats_payload, ttl)

        neighbourhood_stats_payload = _load_json_file(neighbourhood_stats_path)
        if neighbourhood_stats_payload is not None:
            neighbourhood_key = f"{namespace}:tickets:{NEIGHBOURHOOD_STATS_REDIS_KEY}:v1"
            _store_json_blob(client, neighbourhood_key, neighbourhood_stats_payload, ttl)
    finally:
        client.close()
    print(f"Stored tickets aggregate in Redis key '{key}' (TTL={ttl or 'none'}).")
    if manifest_entries:
        print(
            f"Stored {len(manifest_entries)} chunk payload(s) with manifest under namespace '{namespace}'."
        )


def main(argv: list[str] | None = None) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.append(str(repo_root))

    parser = argparse.ArgumentParser(description="Push tickets_aggregated.geojson into Redis cache.")
    parser.add_argument(
        "--data-file",
        help="Optional path to tickets_aggregated.geojson (defaults to map-app/public/data/tickets_aggregated.geojson).",
    )
    args = parser.parse_args(argv)

    _load_env(repo_root)
    redis_url = _resolve_redis_url()
    namespace = _resolve_namespace()
    ttl = _resolve_ttl()

    raw, data_path = _read_geojson(repo_root, args.data_file)
    push_to_redis(raw, data_path, redis_url, namespace, ttl)
    print(f"Source file: {data_path}")


if __name__ == "__main__":  # pragma: no cover
    main()
