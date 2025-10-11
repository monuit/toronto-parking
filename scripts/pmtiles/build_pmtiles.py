"""Build PMTiles archives for heavy map layers.

This script connects directly to PostGIS, streams vector tiles from the source
tables, and generates sharded PMTiles files that can be pushed to the
MinIO-backed edge bucket.  Each shard enumerates the relevant z/x/y tiles,
renders the vector tile payload using dataset-specific SQL, and writes the
tiles into a compressed PMTiles archive.

Usage
-----

    python scripts/pmtiles/build_pmtiles.py --output-dir pmtiles

Environment
-----------

The script reads the same database connection variables as the rest of the
pipeline (``DATABASE_URL`` / ``DATABASE_PRIVATE_URL`` / ``POSTGRES_URL``).
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import time
from urllib.parse import quote_plus
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Sequence

import psycopg
from dotenv import load_dotenv
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import write as pmtiles_write
import boto3

# Ensure the project root is on the Python path so we can import ``src`` modules.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DOTENV_PATH = PROJECT_ROOT / '.env'
if DOTENV_PATH.exists():
    load_dotenv(DOTENV_PATH)

from src.etl.postgres import PostgresClient  # noqa: E402
from src.tiles.schema import TileSchemaManager  # noqa: E402


@dataclass(frozen=True)
class ShardDefinition:
    dataset: str
    shard_id: str
    filename: str
    bounds: Tuple[float, float, float, float]  # (west, south, east, north)
    min_zoom: int
    max_zoom: int
    name: str
    description: str

    @property
    def center(self) -> Tuple[float, float]:
        west, south, east, north = self.bounds
        return ((west + east) / 2.0, (south + north) / 2.0)


DATASET_SOURCES: Dict[str, Dict[str, str]] = {
    "parking_tickets": {
        "table": "parking_tickets",
        "geom_column": "geom_3857",
    },
    "red_light_locations": {
        "table": "red_light_camera_locations",
        "geom_column": "geom_3857",
    },
    "ase_locations": {
        "table": "ase_camera_locations",
        "geom_column": "geom_3857",
    },
}

SHARD_CONFIG_PATH = PROJECT_ROOT / "shared" / "pmtiles" / "shards.json"
with open(SHARD_CONFIG_PATH, "r", encoding="utf-8") as shard_file:
    SHARD_CONFIG = json.load(shard_file)

DATASET_CONFIG: Dict[str, Dict[str, object]] = SHARD_CONFIG.get("datasets", {})
WARD_DATASET_CONFIG: Dict[str, Dict[str, object]] = SHARD_CONFIG.get("wardDatasets", {})


def _load_shard_definitions() -> Tuple[ShardDefinition, ...]:
    shards: List[ShardDefinition] = []
    for dataset, config in DATASET_CONFIG.items():
        for shard in config.get("shards", []):
            shards.append(
                ShardDefinition(
                    dataset=dataset,
                    shard_id=shard["id"],
                    filename=shard["filename"],
                    bounds=tuple(shard["bounds"]),
                    min_zoom=int(shard["minZoom"]),
                    max_zoom=int(shard["maxZoom"]),
                    name=shard.get("name", f"{dataset} – {shard['id']}").strip(),
                    description=shard.get("description", ""),
                )
            )
    return tuple(shards)


SHARDS: Tuple[ShardDefinition, ...] = _load_shard_definitions()


def _can_connect(dsn: str) -> bool:
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            conn.cursor().execute("SELECT 1")
        return True
    except Exception:  # pragma: no cover - diagnostic path
        return False


def resolve_database_dsn() -> str:
    """Find the appropriate Postgres DSN from environment variables."""

    candidates = (
        os.getenv("DATABASE_PRIVATE_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("POSTGRES_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
    )
    for candidate in candidates:
        if candidate and _can_connect(candidate):
            return candidate

    host = os.getenv("POSTGRES_HOST") or os.getenv("PGHOST")
    user = os.getenv("POSTGRES_USER") or os.getenv("PGUSER")
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD")
    database = (
        os.getenv("POSTGRES_DB")
        or os.getenv("POSTGRES_DATABASE")
        or os.getenv("PGDATABASE")
        or "postgres"
    )
    port = os.getenv("POSTGRES_PORT") or os.getenv("PGPORT") or "5432"

    if host and user:
        password_part = f":{quote_plus(password)}" if password else ""
        dsn = f"postgresql://{quote_plus(user)}{password_part}@{host}:{port}/{database}"
        if _can_connect(dsn):
            return dsn

    raise RuntimeError(
        "Unable to resolve Postgres DSN. Set DATABASE_PRIVATE_URL or POSTGRES_HOST/USER/PASSWORD variables."
    )


def quadkey_to_zxy(quadkey: str) -> Tuple[int, int, int]:
    if not quadkey:
        raise ValueError("Quadkey must be non-empty")
    z = len(quadkey)
    x = 0
    y = 0
    for i, char in enumerate(quadkey):
        digit = int(char)
        mask = 1 << (z - i - 1)
        if digit & 1:
            x |= mask
        if digit & 2:
            y |= mask
    return z, x, y


def collect_tiles_for_shard(pg: PostgresClient, shard: ShardDefinition) -> List[Tuple[int, int, int]]:
    source = DATASET_SOURCES.get(shard.dataset)
    if not source:
        raise ValueError(f"Dataset '{shard.dataset}' is not configured for PMTiles export")

    table = source["table"]
    geom_column = source["geom_column"]
    west, south, east, north = shard.bounds

    sql = f"""
        WITH bounds AS (
            SELECT ST_Transform(ST_SetSRID(ST_MakeEnvelope(%s, %s, %s, %s), 4326), 3857) AS geom
        ), features AS (
            SELECT {geom_column} AS geom_3857
            FROM {table}
            CROSS JOIN bounds
            WHERE {geom_column} IS NOT NULL
              AND {geom_column} && bounds.geom
        ), series AS (
            SELECT DISTINCT zoom, mercator_quadkey_prefix(geom_3857, zoom, zoom) AS quadkey
            FROM features, generate_series(%s::int, %s::int) AS zoom
        )
        SELECT zoom, quadkey
        FROM series
        WHERE quadkey IS NOT NULL AND quadkey <> ''
    """

    tiles_set: set[Tuple[int, int, int]] = set()
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (west, south, east, north, shard.min_zoom, shard.max_zoom))
            for zoom, quadkey in cur.fetchall():
                if not quadkey:
                    continue
                z, x, y = quadkey_to_zxy(str(quadkey))
                tiles_set.add((z, x, y))

    return sorted(tiles_set)


def _build_parking_tile_query(z: int, x: int, y: int) -> Tuple[str, Tuple[object, ...]]:
    sql = """
        WITH bounds AS (
            SELECT ST_SetSRID(ST_TileEnvelope(%s::integer, %s::integer, %s::integer), 3857) AS geom
        ), ranked AS (
            SELECT
                COALESCE(centreline_id::text, street_normalized, location1, ticket_hash) AS feature_id,
                geom_3857,
                street_normalized,
                centreline_id,
                COUNT(*) OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)) AS ticket_count,
                SUM(COALESCE(set_fine_amount, 0)) OVER (PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)) AS total_fine_amount,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(centreline_id::text, street_normalized, location1, ticket_hash)
                    ORDER BY date_of_infraction DESC NULLS LAST, time_of_infraction DESC NULLS LAST
                ) AS rn
            FROM parking_tickets
            CROSS JOIN bounds
            WHERE geom_3857 IS NOT NULL
              AND geom_3857 && bounds.geom
        ), aggregated AS (
            SELECT
                feature_id,
                ticket_count,
                total_fine_amount,
                geom_3857,
                street_normalized,
                centreline_id
            FROM ranked
            WHERE rn = 1
        ), variants AS (
            SELECT
                agg.feature_id,
                agg.ticket_count,
                agg.total_fine_amount,
                agg.street_normalized,
                agg.centreline_id,
                variant.min_zoom,
                variant.max_zoom,
                (ST_Dump(variant.geom_variant)).geom AS geom
            FROM aggregated AS agg
            CROSS JOIN LATERAL (
                SELECT 0 AS min_zoom, 10 AS max_zoom, subdivided.geom AS geom_variant
                FROM ST_Subdivide(ST_SimplifyPreserveTopology(agg.geom_3857, 25), 32) AS subdivided(geom)
                UNION ALL
                SELECT 11 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
                FROM ST_Subdivide(agg.geom_3857, 4) AS subdivided(geom)
            ) AS variant
        ), features AS (
            SELECT
                ST_AsMVTGeom(
                    variants.geom,
                    bounds.geom,
                    4096,
                    64,
                    true
                ) AS geom,
                'parking_tickets' AS dataset,
                variants.feature_id,
                variants.ticket_count,
                variants.total_fine_amount,
                variants.street_normalized,
                variants.centreline_id::BIGINT AS centreline_id
            FROM variants
            CROSS JOIN bounds
            WHERE variants.geom && bounds.geom
              AND %s BETWEEN variants.min_zoom AND variants.max_zoom
        )
        SELECT ST_AsMVT(features, %s, 4096, 'geom') FROM features;
    """
    params = (z, x, y, z, 'parking_tickets')
    return sql, params


def _build_red_light_tile_query(z: int, x: int, y: int) -> Tuple[str, Tuple[object, ...]]:
    sql = """
        WITH bounds AS (
            SELECT ST_SetSRID(ST_TileEnvelope(%s::integer, %s::integer, %s::integer), 3857) AS geom
        ), base AS (
            SELECT
                intersection_id,
                location_name,
                ticket_count,
                total_fine_amount,
                ward_1,
                geom_3857
            FROM red_light_camera_locations
            CROSS JOIN bounds
            WHERE geom_3857 IS NOT NULL
              AND geom_3857 && bounds.geom
        ), variants AS (
            SELECT
                base.intersection_id,
                base.location_name,
                base.ticket_count,
                base.total_fine_amount,
                base.ward_1,
                variant.min_zoom,
                variant.max_zoom,
                (ST_Dump(variant.geom_variant)).geom AS geom
            FROM base
            CROSS JOIN LATERAL (
                SELECT 0 AS min_zoom, 11 AS max_zoom, subdivided.geom AS geom_variant
                FROM ST_Subdivide(ST_SimplifyPreserveTopology(base.geom_3857, 30), 32) AS subdivided(geom)
                UNION ALL
                SELECT 12 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
                FROM ST_Subdivide(base.geom_3857, 4) AS subdivided(geom)
            ) AS variant
        ), features AS (
            SELECT
                ST_AsMVTGeom(
                    variants.geom,
                    bounds.geom,
                    4096,
                    64,
                    true
                ) AS geom,
                'red_light_locations' AS dataset,
                variants.intersection_id::TEXT AS feature_id,
                variants.ticket_count,
                variants.total_fine_amount,
                variants.location_name,
                variants.ward_1 AS ward
            FROM variants
            CROSS JOIN bounds
            WHERE variants.geom && bounds.geom
              AND %s BETWEEN variants.min_zoom AND variants.max_zoom
        )
        SELECT ST_AsMVT(features, %s, 4096, 'geom') FROM features;
    """
    params = (z, x, y, z, 'red_light_locations')
    return sql, params


def _build_ase_tile_query(z: int, x: int, y: int) -> Tuple[str, Tuple[object, ...]]:
    sql = """
        WITH bounds AS (
            SELECT ST_SetSRID(ST_TileEnvelope(%s::integer, %s::integer, %s::integer), 3857) AS geom
        ), base AS (
            SELECT
                location_code,
                location,
                status,
                ward,
                ticket_count,
                total_fine_amount,
                geom_3857
            FROM ase_camera_locations
            CROSS JOIN bounds
            WHERE geom_3857 IS NOT NULL
              AND geom_3857 && bounds.geom
        ), variants AS (
            SELECT
                base.location_code,
                base.location,
                base.status,
                base.ward,
                base.ticket_count,
                base.total_fine_amount,
                variant.min_zoom,
                variant.max_zoom,
                (ST_Dump(variant.geom_variant)).geom AS geom
            FROM base
            CROSS JOIN LATERAL (
                SELECT 0 AS min_zoom, 11 AS max_zoom, subdivided.geom AS geom_variant
                FROM ST_Subdivide(ST_SimplifyPreserveTopology(base.geom_3857, 30), 32) AS subdivided(geom)
                UNION ALL
                SELECT 12 AS min_zoom, 16 AS max_zoom, subdivided.geom AS geom_variant
                FROM ST_Subdivide(base.geom_3857, 4) AS subdivided(geom)
            ) AS variant
        ), features AS (
            SELECT
                ST_AsMVTGeom(
                    variants.geom,
                    bounds.geom,
                    4096,
                    64,
                    true
                ) AS geom,
                'ase_locations' AS dataset,
                variants.location_code::TEXT AS feature_id,
                variants.ticket_count,
                variants.total_fine_amount,
                variants.location,
                variants.status,
                variants.ward
            FROM variants
            CROSS JOIN bounds
            WHERE variants.geom && bounds.geom
              AND %s BETWEEN variants.min_zoom AND variants.max_zoom
        )
        SELECT ST_AsMVT(features, %s, 4096, 'geom') FROM features;
    """
    params = (z, x, y, z, 'ase_locations')
    return sql, params


def _build_tile_query(dataset: str, z: int, x: int, y: int) -> Tuple[str, Tuple[object, ...]]:
    if dataset == 'parking_tickets':
        return _build_parking_tile_query(z, x, y)
    if dataset == 'red_light_locations':
        return _build_red_light_tile_query(z, x, y)
    if dataset == 'ase_locations':
        return _build_ase_tile_query(z, x, y)
    raise ValueError(f"Unsupported dataset '{dataset}' for PMTiles export")


def fetch_tile(
    pg: PostgresClient,
    dataset: str,
    z: int,
    x: int,
    y: int,
) -> Optional[bytes]:
    sql, params = _build_tile_query(dataset, z, x, y)
    with pg.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return bytes(row[0])


def build_vector_layers(dataset: str) -> List[Dict[str, object]]:
    # Minimal field typing to satisfy MapLibre metadata expectations.
    field_types = {
        "ticket_count": "Number",
        "total_fine_amount": "Number",
        "street_normalized": "String",
        "centreline_id": "Number",
        "location_name": "String",
        "location": "String",
        "status": "String",
        "ward": "String",
    }
    dataset_config = DATASET_CONFIG.get(dataset, {})
    vector_layer = dataset_config.get("vectorLayer", dataset)
    label = dataset_config.get("label", dataset.replace("_", " ").title())
    return [
        {
            "id": vector_layer,
            "description": label,
            "fields": field_types,
        }
    ]


def build_metadata(shard: ShardDefinition, min_zoom: int, max_zoom: int) -> Dict[str, object]:
    bounds = ",".join(str(round(value, 6)) for value in shard.bounds)
    center_lng, center_lat = shard.center
    return {
        "name": shard.name,
        "description": shard.description,
        "version": "1.0.0",
        "type": "overlay",
        "format": "pbf",
        "attribution": "City of Toronto open data",
        "minzoom": min_zoom,
        "maxzoom": max_zoom,
        "bounds": bounds,
        "center": [round(center_lng, 6), round(center_lat, 6), int((min_zoom + max_zoom) / 2)],
        "vector_layers": build_vector_layers(shard.dataset),
        "generated": datetime.now(timezone.utc).isoformat(),
    }


def build_header(shard: ShardDefinition, min_zoom: int, max_zoom: int) -> Dict[str, object]:
    west, south, east, north = shard.bounds
    return {
        "clustered": False,
        "internal_compression": Compression.GZIP,
        "tile_compression": Compression.GZIP,
        "tile_type": TileType.MVT,
        "root_offset": 0,
        "root_length": 0,
        "metadata_offset": 0,
        "metadata_length": 0,
        "leaf_directory_offset": 0,
        "leaf_directory_length": 0,
        "tile_data_offset": 0,
        "tile_data_length": 0,
        "min_zoom": min_zoom,
        "max_zoom": max_zoom,
        "min_lon_e7": int(west * 10_000_000),
        "min_lat_e7": int(south * 10_000_000),
        "max_lon_e7": int(east * 10_000_000),
        "max_lat_e7": int(north * 10_000_000),
        "center_zoom": int((min_zoom + max_zoom) / 2),
        "center_lon_e7": int(((west + east) / 2) * 10_000_000),
        "center_lat_e7": int(((south + north) / 2) * 10_000_000),
    }


def generate_pmtiles_for_shard(
    shard: ShardDefinition,
    pg: PostgresClient,
    output_dir: Path,
    tiles: List[Tuple[int, int, int]],
) -> Dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / shard.filename

    total_tiles_target = len(tiles)
    written_tiles = 0
    progress_step = max(1, int(os.getenv("PMTILES_PROGRESS_STEP", "500")))
    if os.getenv("PMTILES_PROGRESS_PERCENT"):
        try:
            percent_step = max(1, min(100, int(os.getenv("PMTILES_PROGRESS_PERCENT"))))
            progress_step = max(1, max(1, total_tiles_target * percent_step // 100))
        except ValueError:
            pass

    if total_tiles_target == 0:
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        return {
            "dataset": shard.dataset,
            "shard": shard.shard_id,
            "filename": shard.filename,
            "path": str(output_path),
            "minZoom": shard.min_zoom,
            "maxZoom": shard.max_zoom,
            "bounds": shard.bounds,
            "totalTiles": 0,
            "writtenTiles": 0,
        }

    actual_min_zoom = min(z for z, _, _ in tiles)
    actual_max_zoom = max(z for z, _, _ in tiles)

    with pmtiles_write(output_path) as writer:
        for index, (z, tile_x, tile_y) in enumerate(tiles, start=1):
            tile_bytes = fetch_tile(pg, shard.dataset, z, tile_x, tile_y)
            if not tile_bytes:
                continue
            compressed = gzip.compress(tile_bytes, compresslevel=6)
            writer.write_tile(zxy_to_tileid(z, tile_x, tile_y), compressed)
            written_tiles += 1
            if total_tiles_target and written_tiles % progress_step == 0:
                percent = (written_tiles / total_tiles_target) * 100
                print(
                    f"  [{shard.dataset}:{shard.shard_id}] {written_tiles}/{total_tiles_target} tiles ({percent:.1f}%)",
                    flush=True,
                )

        header = build_header(shard, actual_min_zoom, actual_max_zoom)
        metadata = build_metadata(shard, actual_min_zoom, actual_max_zoom)
        writer.finalize(header, metadata)

    return {
        "dataset": shard.dataset,
        "shard": shard.shard_id,
        "filename": shard.filename,
        "path": str(output_path),
        "minZoom": actual_min_zoom,
        "maxZoom": actual_max_zoom,
        "bounds": shard.bounds,
        "totalTiles": total_tiles_target,
        "writtenTiles": written_tiles,
    }


def run(
    output_dir: Path,
    upload: bool,
    upload_prefix: str | None,
    force_schema_refresh: bool,
    dataset_filter: Optional[set[str]] = None,
    shard_filter: Optional[set[str]] = None,
) -> List[Dict[str, object]]:
    dsn = resolve_database_dsn()
    pg_client = PostgresClient(dsn=dsn, application_name="pmtiles-export", statement_timeout_ms=None)
    schema_client = PostgresClient(
        dsn=dsn,
        application_name="pmtiles-export-schema",
        statement_timeout_ms=None,
    )

    def schema_log(message: str) -> None:
        print(f"[schema] {message}", flush=True)

    if force_schema_refresh:
        print("Forcing tile schema refresh", flush=True)
        schema_manager = TileSchemaManager(schema_client, logger=schema_log)
        started = time.monotonic()
        schema_manager.ensure(include_tile_tables=False)
        print(f"[schema] Refresh completed in {time.monotonic() - started:.1f}s", flush=True)
    else:
        schema_manager = TileSchemaManager(schema_client, logger=schema_log)
        schema_manager.ensure_helpers()

    filtered_shards = []
    for shard in SHARDS:
        if dataset_filter and shard.dataset not in dataset_filter:
            continue
        shard_key = f"{shard.dataset}:{shard.shard_id}"
        if shard_filter and shard_key not in shard_filter:
            continue
        filtered_shards.append(shard)

    if not filtered_shards:
        raise RuntimeError("No shards selected for PMTiles generation")

    manifest: List[Dict[str, object]] = []
    for shard in filtered_shards:
        print(f"Building shard {shard.dataset}:{shard.shard_id} -> {shard.filename}", flush=True)
        shard_started = time.monotonic()
        tiles = collect_tiles_for_shard(pg_client, shard)
        print(f"  tile candidates={len(tiles)}", flush=True)
        if not tiles:
            print("  no tiles discovered for shard; consider rerunning with --refresh-schema", flush=True)
        summary = generate_pmtiles_for_shard(shard, pg_client, output_dir, tiles)
        shard_elapsed = time.monotonic() - shard_started
        print(
            f"  tiles considered={summary['totalTiles']} written={summary['writtenTiles']} path={summary['path']} time={shard_elapsed:.1f}s",
            flush=True,
        )
        manifest.append(summary)

    manifest_path = output_dir / "pmtiles-manifest.json"
    manifest_payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "shards": manifest,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2))
    print(f"Wrote manifest -> {manifest_path}")
    if upload:
        upload_manifest(manifest, output_dir, upload_prefix)
    return manifest


def upload_manifest(manifest: List[Dict[str, object]], output_dir: Path, prefix: str | None) -> None:
    bucket = os.getenv("PMTILES_BUCKET", "pmtiles")
    endpoint = os.getenv("MINIO_PUBLIC_ENDPOINT") or os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER") or os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("MINIO_REGION", "us-east-1")

    if not endpoint or not access_key or not secret_key:
        raise RuntimeError("MINIO_PUBLIC_ENDPOINT, MINIO_ROOT_USER, and MINIO_ROOT_PASSWORD must be set for upload")

    normalized_prefix = prefix
    if normalized_prefix is None:
        normalized_prefix = os.getenv("PMTILES_PREFIX", "pmtiles/")
    if normalized_prefix and not normalized_prefix.endswith('/'):
        normalized_prefix = f"{normalized_prefix}/"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    for shard in manifest:
        file_path = Path(shard["path"])
        if not file_path.exists():
            print(f"[upload] Skipping missing file {file_path}")
            continue
        key = f"{normalized_prefix or ''}{file_path.name}" if normalized_prefix else file_path.name
        print(f"[upload] {file_path.name} → s3://{bucket}/{key}")
        extra_args = {
            "ContentType": "application/octet-stream",
            "CacheControl": "public, immutable, max-age=31536000",
        }
        client.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args)
        print(f"[upload] Uploaded {file_path.name}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build PMTiles archives for heavy datasets")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "pmtiles"),
        help="Directory to write PMTiles files into (default: ./pmtiles)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload generated PMTiles to MinIO/S3 using environment credentials",
    )
    parser.add_argument(
        "--upload-prefix",
        default=None,
        help="Object key prefix when uploading (overrides PMTILES_PREFIX)",
    )
    parser.add_argument(
        "--refresh-schema",
        action="store_true",
        help="Force TileSchemaManager.ensure() before building PMTiles",
    )
    parser.add_argument(
        "--datasets",
        default=None,
        help="Comma-separated dataset identifiers to build (default: all)",
    )
    parser.add_argument(
        "--shards",
        default=None,
        help="Comma-separated shard identifiers in dataset:shard form (default: all)",
    )
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir).resolve()
    dataset_filter = None
    if args.datasets:
        dataset_filter = {entry.strip() for entry in args.datasets.split(",") if entry.strip()}
    shard_filter = None
    if args.shards:
        shard_filter = {entry.strip() for entry in args.shards.split(",") if entry.strip()}

    run(
        output_dir,
        args.upload,
        args.upload_prefix,
        args.refresh_schema,
        dataset_filter,
        shard_filter,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
