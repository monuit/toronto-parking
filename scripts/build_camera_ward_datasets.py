"""Build ward-level aggregates for red light and ASE datasets."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import dotenv
import psycopg
import requests
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from preprocessing.build_camera_datasets import (  # noqa: E402
    _locate_latest_ase_charges,
    _locate_latest_red_light_charges,
    _normalise_ward_name,
    _safe_number,
)
from src.etl.datasets.ase_locations import _load_charges_summary as load_ase_charges_summary  # noqa: E402
from src.etl.datasets.red_light_locations import _load_charges_summary as load_rlc_charges_summary  # noqa: E402


WARD_GEOJSON_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/5e7a8234-f805-43ac-820f-03d7c360b588/resource/"
    "737b29e0-8329-4260-b6af-21555ab24f28/download/city-wards-data-4326.geojson"
)

OUTPUT_DIR = REPO_ROOT / "map-app" / "public" / "data"
WARD_CACHE_PATH = REPO_ROOT / "output" / "etl" / "static" / "city_wards.geojson"
STATE_FILE = REPO_ROOT / "output" / "etl" / "camera_ward_state.json"
STATE_VERSION = 2

SUMMARY_PATHS = {
    "ase_locations": OUTPUT_DIR / "ase_ward_summary.json",
    "red_light_locations": OUTPUT_DIR / "red_light_ward_summary.json",
    "cameras_combined": OUTPUT_DIR / "cameras_combined_ward_summary.json",
}

GEOJSON_PATHS = {
    "ase_locations": OUTPUT_DIR / "ase_ward_choropleth.geojson",
    "red_light_locations": OUTPUT_DIR / "red_light_ward_choropleth.geojson",
    "cameras_combined": OUTPUT_DIR / "cameras_combined_ward_choropleth.geojson",
}

REDIS_KEYS = {
    "ase_locations": {
        "geojson": "toronto:map-data:ase:wards:geojson:v1",
        "summary": "toronto:map-data:ase:wards:summary:v1",
    },
    "red_light_locations": {
        "geojson": "toronto:map-data:red_light:wards:geojson:v1",
        "summary": "toronto:map-data:red_light:wards:summary:v1",
    },
    "cameras_combined": {
        "geojson": "toronto:map-data:cameras:wards:geojson:v1",
        "summary": "toronto:map-data:cameras:wards:summary:v1",
    },
}


def _load_env() -> None:
    dotenv.load_dotenv(REPO_ROOT / ".env")


def _resolve_dsn(preferred: Optional[str]) -> str:
    if preferred:
        return preferred
    for key in ("POSTGIS_DATABASE_URL", "DATABASE_URL", "POSTGRES_URL"):
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError("Database URL not provided; set POSTGIS_DATABASE_URL or DATABASE_URL")


def _build_precomputed_tiles(datasets: Iterable[str]) -> None:
    script_path = REPO_ROOT / "map-app" / "scripts" / "build-ward-tiles.mjs"
    dataset_list = list(datasets)
    if not dataset_list or not script_path.exists():
        return

    command = ["node", str(script_path)]
    for dataset in dataset_list:
        command.extend(["--dataset", dataset])

    try:
        subprocess.run(command, check=True)
    except FileNotFoundError:
        print("Node executable not found; skipping prebuilt ward tiles.")
    except subprocess.CalledProcessError as error:
        print(f"Failed to generate prebuilt ward tiles: {error}")
        raise


def _resolve_redis_url() -> Optional[str]:
    for key in ("REDIS_PUBLIC_URL", "REDIS_URL", "REDIS_CONNECTION"):
        value = os.getenv(key)
        if value:
            return value
    return None


def _compute_checksum(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _artifact_invalid(path: Path, expect_features: bool = False) -> bool:
    if not path.exists():
        return True
    try:
        raw = path.read_text(encoding="utf-8")
        if not raw.strip():
            return True
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return True
    if expect_features:
        features = payload.get("features") if isinstance(payload, dict) else None
        if not isinstance(features, list) or not features:
            return True
        if not any(
            isinstance(feature, dict)
            and isinstance(feature.get("properties"), dict)
            and feature["properties"].get("ticketCount", 0)
        for feature in features
        ):
            return True
    return False


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_state(payload: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _checksum_to_version(value: Optional[str]) -> int:
    if not value:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        return int(value[:12], 16)
    except ValueError:
        return int(datetime.now(timezone.utc).timestamp() * 1000)


def _load_totals_from_summary(dataset: str) -> tuple[Optional[dict], Optional[Dict[int, Dict[str, float]]]]:
    summary_path = SUMMARY_PATHS.get(dataset)
    if not summary_path or not summary_path.exists():
        return None, None
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, None

    totals: Dict[int, Dict[str, float]] = {}
    for ward in summary.get("wards", []):
        ward_code = _normalise_ward_code(ward.get("wardCode"))
        if ward_code is None:
            continue
        totals[ward_code] = {
            "ward_name": ward.get("wardName") or f"Ward {ward_code}",
            "ticket_count": int(ward.get("ticketCount", 0)),
            "location_count": int(ward.get("locationCount", 0)),
            "total_revenue": round(_safe_number(ward.get("totalRevenue")), 2),
        }
    return summary, totals if totals else None


def download_ward_geojson(force: bool = False) -> dict:
    WARD_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if WARD_CACHE_PATH.exists() and not force:
        return json.loads(WARD_CACHE_PATH.read_text(encoding="utf-8"))

    response = requests.get(WARD_GEOJSON_URL, timeout=60)
    response.raise_for_status()
    payload = response.json()
    WARD_CACHE_PATH.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def ensure_tables(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS city_wards (
                ward_code INTEGER PRIMARY KEY,
                ward_name TEXT NOT NULL,
                ward_short_code TEXT,
                geom geometry(MULTIPOLYGON, 4326) NOT NULL,
                properties JSONB DEFAULT '{}'::JSONB,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS camera_ward_totals (
                dataset TEXT NOT NULL,
                ward_code INTEGER NOT NULL,
                ward_name TEXT NOT NULL,
                ticket_count BIGINT NOT NULL,
                location_count INTEGER NOT NULL,
                total_revenue NUMERIC(18, 2) NOT NULL,
                metadata JSONB DEFAULT '{}'::JSONB,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (dataset, ward_code)
            )
            """
        )
    conn.commit()


def upsert_wards(conn: psycopg.Connection, geojson: dict) -> None:
    records = []
    for feature in geojson.get("features", []):
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry")
        if not geometry:
            continue
        try:
            ward_code = int(str(properties.get("AREA_LONG_CODE") or properties.get("AREA_SHORT_CODE")))
        except (TypeError, ValueError):
            continue
        ward_name = str(properties.get("AREA_NAME") or f"Ward {ward_code}")
        ward_short = properties.get("AREA_SHORT_CODE")
        records.append((ward_code, ward_name, ward_short, json.dumps(geometry), json.dumps(properties)))

    with conn.cursor() as cur:
        for ward_code, ward_name, ward_short, geom_json, props_json in records:
            cur.execute(
                """
                INSERT INTO city_wards (ward_code, ward_name, ward_short_code, geom, properties, updated_at)
                VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), %s::JSONB, NOW())
                ON CONFLICT (ward_code) DO UPDATE SET
                    ward_name = EXCLUDED.ward_name,
                    ward_short_code = EXCLUDED.ward_short_code,
                    geom = EXCLUDED.geom,
                    properties = EXCLUDED.properties,
                    updated_at = NOW()
                """,
                (ward_code, ward_name, ward_short, geom_json, props_json),
            )
    conn.commit()


WARD_CODE_PATTERN = re.compile(r"(\d+(?:\.\d+)?)")


def _normalise_ward_code(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        number = int(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        match = WARD_CODE_PATTERN.search(text)
        if not match:
            return None
        token = match.group(1)
        try:
            number = int(token)
        except ValueError:
            try:
                number = int(float(token))
            except ValueError:
                return None
    while number > 25 and number % 10 == 0:
        number //= 10
    return number if number > 0 else None


def _extract_ward_code(raw: Optional[object]) -> Optional[int]:
    return _normalise_ward_code(raw)


def aggregate_charges(charges_lookup: Dict[str, Dict], dataset: str) -> Dict[int, Dict[str, float]]:
    totals: Dict[int, Dict[str, float]] = defaultdict(lambda: {
        "ticket_count": 0,
        "total_revenue": 0.0,
        "locations": set(),
        "ward_names": set(),
    })

    for location_code, metrics in charges_lookup.items():
        ward_raw = metrics.get("ward") or metrics.get("Ward")
        ward_code = _extract_ward_code(ward_raw)
        if ward_code is None:
            continue
        ward_name = _normalise_ward_name(ward_raw)
        ticket_count = int(metrics.get("ticket_count") or 0)
        total_revenue = _safe_number(metrics.get("total_fine_amount"))

        bucket = totals[ward_code]
        bucket["ticket_count"] += ticket_count
        bucket["total_revenue"] += total_revenue
        bucket["locations"].add(location_code)
        bucket["ward_names"].add(ward_name)

    # Convert location sets to counts and set canonical ward name
    for ward_code, bucket in totals.items():
        bucket["location_count"] = len(bucket["locations"])
        bucket["ward_name"] = sorted(bucket["ward_names"])[0]
        bucket["total_revenue"] = round(bucket["total_revenue"], 2)
        bucket.pop("locations", None)
        bucket.pop("ward_names", None)

    return totals


def merge_ward_totals(
    ase_totals: Dict[int, Dict[str, float]],
    rlc_totals: Dict[int, Dict[str, float]],
) -> Dict[int, Dict[str, float]]:
    combined: Dict[int, Dict[str, float]] = {}
    for ward_code in set(ase_totals.keys()) | set(rlc_totals.keys()):
        ase = ase_totals.get(ward_code, {})
        rlc = rlc_totals.get(ward_code, {})
        ward_name = next(
            (name for name in [ase.get("ward_name"), rlc.get("ward_name")] if name),
            f"Ward {ward_code}",
        )
        combined[ward_code] = {
            "ward_name": ward_name,
            "ticket_count": int(ase.get("ticket_count", 0)) + int(rlc.get("ticket_count", 0)),
            "total_revenue": round(_safe_number(ase.get("total_revenue")) + _safe_number(rlc.get("total_revenue")), 2),
            "location_count": int(ase.get("location_count", 0)) + int(rlc.get("location_count", 0)),
            "ase_ticket_count": int(ase.get("ticket_count", 0)),
            "rlc_ticket_count": int(rlc.get("ticket_count", 0)),
        }
    return combined


def upsert_ward_totals(
    conn: psycopg.Connection,
    dataset: str,
    totals: Dict[int, Dict[str, float]],
) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM camera_ward_totals WHERE dataset = %s", (dataset,))
        for ward_code, bucket in totals.items():
            cur.execute(
                """
                INSERT INTO camera_ward_totals (
                    dataset, ward_code, ward_name, ticket_count, location_count, total_revenue, metadata, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::JSONB, NOW())
                """,
                (
                    dataset,
                    ward_code,
                    bucket.get("ward_name") or f"Ward {ward_code}",
                    int(bucket.get("ticket_count", 0)),
                    int(bucket.get("location_count", 0)),
                    float(bucket.get("total_revenue", 0.0)),
                    json.dumps({k: v for k, v in bucket.items() if k not in {
                        "ward_name", "ticket_count", "location_count", "total_revenue"
                    }}),
                ),
            )
    conn.commit()


def build_geojson(
    base_geojson: dict,
    totals: Dict[int, Dict[str, float]],
    extra_fields: Optional[Dict[str, str]] = None,
) -> dict:
    features = []
    lookup = totals
    for feature in base_geojson.get("features", []):
        properties = dict(feature.get("properties") or {})
        geometry = feature.get("geometry")
        if not geometry:
            continue
        try:
            ward_code = int(str(properties.get("AREA_LONG_CODE") or properties.get("AREA_SHORT_CODE")))
        except (TypeError, ValueError):
            continue
        stats = lookup.get(ward_code, {})
        payload = {
            "wardCode": ward_code,
            "wardName": stats.get("ward_name") or properties.get("AREA_NAME") or f"Ward {ward_code}",
            "ticketCount": int(stats.get("ticket_count", 0)),
            "locationCount": int(stats.get("location_count", 0)),
            "totalRevenue": round(_safe_number(stats.get("total_revenue")), 2),
        }
        if extra_fields:
            for key, source in extra_fields.items():
                payload[key] = stats.get(source, 0)
        feature_payload = {
            "type": "Feature",
            "geometry": geometry,
            "properties": payload,
        }
        features.append(feature_payload)
    return {"type": "FeatureCollection", "features": features}


def build_summary(totals: Dict[int, Dict[str, float]]) -> dict:
    ordered = sorted(
        (
            {
                "wardCode": ward_code,
                "wardName": bucket.get("ward_name") or f"Ward {ward_code}",
                "ticketCount": int(bucket.get("ticket_count", 0)),
                "locationCount": int(bucket.get("location_count", 0)),
                "totalRevenue": round(_safe_number(bucket.get("total_revenue")), 2),
            }
            for ward_code, bucket in totals.items()
        ),
        key=lambda item: (item["ticketCount"], item["totalRevenue"]),
        reverse=True,
    )
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "ticketCount": sum(item["ticketCount"] for item in ordered),
            "locationCount": sum(item["locationCount"] for item in ordered),
            "totalRevenue": round(sum(item["totalRevenue"] for item in ordered), 2),
        },
        "topWards": ordered[:10],
        "wards": ordered,
    }


def _gzip_payload(raw: str) -> str:
    return base64.b64encode(gzip.compress(raw.encode("utf-8"))).decode("ascii")


def push_to_redis(
    redis_url: str,
    key: str,
    payload: dict,
    *,
    compress: bool = False,
    version: Optional[int] = None,
    etag: Optional[str] = None,
) -> None:
    import redis  # Imported lazily to avoid dependency when unused

    client = redis.from_url(redis_url)
    resolved_version = version or int(datetime.now(timezone.utc).timestamp() * 1000)
    body = json.dumps(
        {
            "version": resolved_version,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
            "data": _gzip_payload(json.dumps(payload)) if compress else payload,
            "encoding": "gzip+base64" if compress else None,
            "etag": etag,
        }
    )
    client.set(key, body)


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def build_and_store(
    conn: psycopg.Connection,
    redis_url: Optional[str],
    force_download: bool,
) -> None:
    ward_geojson = download_ward_geojson(force=force_download)
    upsert_wards(conn, ward_geojson)

    ase_path = _locate_latest_ase_charges()
    rlc_path = _locate_latest_red_light_charges()
    if not ase_path or not rlc_path:
        raise RuntimeError("Missing ASE or Red Light charges source files")

    state = _load_state()
    schema_version_mismatch = state.get("version") != STATE_VERSION
    now_iso = datetime.now(timezone.utc).isoformat()

    ase_checksum = _compute_checksum(ase_path)
    rlc_checksum = _compute_checksum(rlc_path)

    ase_state = state.get("ase_locations", {})
    rlc_state = state.get("red_light_locations", {})

    ase_outputs_missing = (
        _artifact_invalid(SUMMARY_PATHS["ase_locations"]) or
        _artifact_invalid(GEOJSON_PATHS["ase_locations"], expect_features=True)
    )
    rlc_outputs_missing = (
        _artifact_invalid(SUMMARY_PATHS["red_light_locations"]) or
        _artifact_invalid(GEOJSON_PATHS["red_light_locations"], expect_features=True)
    )
    combined_outputs_missing = (
        _artifact_invalid(SUMMARY_PATHS["cameras_combined"]) or
        _artifact_invalid(GEOJSON_PATHS["cameras_combined"], expect_features=True)
    )

    ase_changed = (
        force_download
        or ase_outputs_missing
        or ase_state.get("checksum") != ase_checksum
        or schema_version_mismatch
    )
    rlc_changed = (
        force_download
        or rlc_outputs_missing
        or rlc_state.get("checksum") != rlc_checksum
        or schema_version_mismatch
    )

    ase_summary, ase_totals = _load_totals_from_summary("ase_locations") if not ase_changed else (None, None)
    if ase_totals is None:
        ase_lookup = load_ase_charges_summary(ase_path)
        ase_totals = aggregate_charges(ase_lookup, "ase_locations")
        ase_summary = build_summary(ase_totals)
        ase_changed = True

    rlc_summary, rlc_totals = _load_totals_from_summary("red_light_locations") if not rlc_changed else (None, None)
    if rlc_totals is None:
        rlc_lookup = load_rlc_charges_summary(rlc_path)
        rlc_totals = aggregate_charges(rlc_lookup, "red_light_locations")
        rlc_summary = build_summary(rlc_totals)
        rlc_changed = True

    combined_summary_existing, combined_totals_existing = _load_totals_from_summary("cameras_combined")
    combined_totals = merge_ward_totals(ase_totals, rlc_totals)
    combined_checksum = hashlib.sha256(f"{ase_checksum}:{rlc_checksum}".encode("utf-8")).hexdigest()
    combined_changed = (
        force_download
        or combined_outputs_missing
        or ase_changed
        or rlc_changed
        or state.get("cameras_combined", {}).get("checksum") != combined_checksum
        or schema_version_mismatch
    )
    if combined_changed:
        combined_summary = build_summary(combined_totals)
    else:
        combined_summary = combined_summary_existing or build_summary(combined_totals)

    if ase_changed:
        upsert_ward_totals(conn, "ase_locations", ase_totals)
    if rlc_changed:
        upsert_ward_totals(conn, "red_light_locations", rlc_totals)
    if combined_changed:
        upsert_ward_totals(conn, "cameras_combined", combined_totals)

    ase_version = _checksum_to_version(ase_checksum)
    rlc_version = _checksum_to_version(rlc_checksum)
    combined_version = _checksum_to_version(combined_checksum)

    ase_etag = f'W/"ase:{ase_checksum}"'
    rlc_etag = f'W/"rlc:{rlc_checksum}"'
    combined_etag = f'W/"cameras_combined:{combined_checksum}"'

    datasets_to_tile: list[str] = []

    if ase_changed:
        ase_geojson = build_geojson(ward_geojson, ase_totals)
        save_json(GEOJSON_PATHS["ase_locations"], ase_geojson)
        save_json(SUMMARY_PATHS["ase_locations"], ase_summary)
        datasets_to_tile.append("ase_locations")
        if redis_url:
            push_to_redis(
                redis_url,
                REDIS_KEYS["ase_locations"]["geojson"],
                ase_geojson,
                compress=True,
                version=ase_version,
                etag=ase_etag,
            )
            push_to_redis(
                redis_url,
                REDIS_KEYS["ase_locations"]["summary"],
                ase_summary,
                compress=False,
                version=ase_version,
                etag=ase_etag,
            )

    if rlc_changed:
        rlc_geojson = build_geojson(ward_geojson, rlc_totals)
        save_json(GEOJSON_PATHS["red_light_locations"], rlc_geojson)
        save_json(SUMMARY_PATHS["red_light_locations"], rlc_summary)
        datasets_to_tile.append("red_light_locations")
        if redis_url:
            push_to_redis(
                redis_url,
                REDIS_KEYS["red_light_locations"]["geojson"],
                rlc_geojson,
                compress=True,
                version=rlc_version,
                etag=rlc_etag,
            )
            push_to_redis(
                redis_url,
                REDIS_KEYS["red_light_locations"]["summary"],
                rlc_summary,
                compress=False,
                version=rlc_version,
                etag=rlc_etag,
            )

    if combined_changed:
        combined_geojson = build_geojson(
            ward_geojson,
            combined_totals,
            extra_fields={
                "aseTicketCount": "ase_ticket_count",
                "rlcTicketCount": "rlc_ticket_count",
            },
        )
        save_json(GEOJSON_PATHS["cameras_combined"], combined_geojson)
        save_json(SUMMARY_PATHS["cameras_combined"], combined_summary)
        datasets_to_tile.append("cameras_combined")
        if redis_url:
            push_to_redis(
                redis_url,
                REDIS_KEYS["cameras_combined"]["geojson"],
                combined_geojson,
                compress=True,
                version=combined_version,
                etag=combined_etag,
            )
            push_to_redis(
                redis_url,
                REDIS_KEYS["cameras_combined"]["summary"],
                combined_summary,
                compress=False,
                version=combined_version,
                etag=combined_etag,
            )

    state.update(
        {
            "ase_locations": {
                "checksum": ase_checksum,
                "source": str(ase_path),
                "updated_at": now_iso,
            },
            "red_light_locations": {
                "checksum": rlc_checksum,
                "source": str(rlc_path),
                "updated_at": now_iso,
            },
            "cameras_combined": {
                "checksum": combined_checksum,
                "updated_at": now_iso,
            },
        }
    )
    state["version"] = STATE_VERSION
    _save_state(state)
    if datasets_to_tile:
        _build_precomputed_tiles(datasets_to_tile)

    if not any((ase_changed, rlc_changed, combined_changed)):
        print("Ward datasets already up to date; no changes detected.")
    else:
        print("Ward datasets refreshed.")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ward-level camera datasets")
    parser.add_argument("--database-url", help="Override database URL")
    parser.add_argument("--redis-url", help="Override Redis URL")
    parser.add_argument("--force-download", action="store_true", help="Re-download ward GeoJSON")
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    _load_env()

    dsn = _resolve_dsn(args.database_url)
    redis_url = args.redis_url or _resolve_redis_url()

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        ensure_tables(conn)
        build_and_store(conn, redis_url, force_download=args.force_download)

    print("Ward datasets built successfully.")


if __name__ == "__main__":
    main()
