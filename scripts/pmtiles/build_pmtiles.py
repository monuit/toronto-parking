"""Build PMTiles archives for heavy map layers.

This script connects directly to PostGIS using the existing tile tables and
generates sharded PMTiles files that can be pushed to the MinIO-backed edge
bucket.  Each shard enumerates the relevant z/x/y tiles, renders the vector
tile payload using the same SQL as the live tile service, and writes the tiles
into a compressed PMTiles archive.

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
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import psycopg
from dotenv import load_dotenv
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import write as pmtiles_write

# Ensure the project root is on the Python path so we can import ``src`` modules.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DOTENV_PATH = PROJECT_ROOT / '.env'
if DOTENV_PATH.exists():
    load_dotenv(DOTENV_PATH)

from src.etl.postgres import PostgresClient  # noqa: E402
from src.tiles.service import TILE_DATASET_DEFINITIONS  # noqa: E402
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


# Geographic sharding configuration.  Ontario/GTA tiles are stored in a shard
# separate from the "rest of Canada" fallback so that the initial viewport hits
# a short-range PMTiles file and avoids cross-country fetches.
SHARDS: Tuple[ShardDefinition, ...] = (
    ShardDefinition(
        dataset="parking_tickets",
        shard_id="ontario",
        filename="parking_tickets-ontario.pmtiles",
        bounds=(-81.0, 42.0, -78.0, 44.4),
        min_zoom=8,
        max_zoom=16,
        name="Parking tickets – Ontario",
        description="Aggregated parking ticket clusters for Southern Ontario",
    ),
    ShardDefinition(
        dataset="parking_tickets",
        shard_id="canada",
        filename="parking_tickets-canada.pmtiles",
        bounds=(-142.0, 41.0, -52.0, 70.0),
        min_zoom=6,
        max_zoom=12,
        name="Parking tickets – Canada fallback",
        description="Low-zoom overview tiles covering the rest of Canada",
    ),
    ShardDefinition(
        dataset="red_light_locations",
        shard_id="ontario",
        filename="red-light-ontario.pmtiles",
        bounds=(-81.0, 42.0, -78.0, 44.4),
        min_zoom=8,
        max_zoom=16,
        name="Red light cameras – Ontario",
        description="Red light camera enforcement coverage across Ontario",
    ),
    ShardDefinition(
        dataset="red_light_locations",
        shard_id="canada",
        filename="red-light-canada.pmtiles",
        bounds=(-142.0, 41.0, -52.0, 70.0),
        min_zoom=6,
        max_zoom=12,
        name="Red light cameras – Canada fallback",
        description="Low-zoom red light camera coverage for the rest of Canada",
    ),
    ShardDefinition(
        dataset="ase_locations",
        shard_id="ontario",
        filename="ase-ontario.pmtiles",
        bounds=(-81.0, 42.0, -78.0, 44.4),
        min_zoom=8,
        max_zoom=16,
        name="ASE cameras – Ontario",
        description="Automated speed enforcement cameras in Ontario",
    ),
    ShardDefinition(
        dataset="ase_locations",
        shard_id="canada",
        filename="ase-canada.pmtiles",
        bounds=(-142.0, 41.0, -52.0, 70.0),
        min_zoom=6,
        max_zoom=12,
        name="ASE cameras – Canada fallback",
        description="Low-zoom automated speed enforcement coverage for Canada",
    ),
)


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
    raise RuntimeError(
        "DATABASE_URL / DATABASE_PRIVATE_URL environment variable is required"
    )


def table_exists(pg: PostgresClient, table_name: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s
        LIMIT 1
    """
    return pg.fetch_one(sql, (table_name,)) is not None


def clamp_lat(lat: float) -> float:
    return max(min(lat, 85.05112878), -85.05112878)


def lng_to_tile_x(lng: float, zoom: int) -> int:
    scale = 2 ** zoom
    return int((lng + 180.0) / 360.0 * scale)


def lat_to_tile_y(lat: float, zoom: int) -> int:
    lat_rad = math.radians(clamp_lat(lat))
    scale = 2 ** zoom
    return int(
        ((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0)
        * scale
    )


def iter_tiles_for_bounds(
    bounds: Tuple[float, float, float, float], min_zoom: int, max_zoom: int
) -> Iterator[Tuple[int, int, int]]:
    west, south, east, north = bounds
    for zoom in range(min_zoom, max_zoom + 1):
        min_x = max(lng_to_tile_x(west, zoom), 0)
        max_x = min(lng_to_tile_x(east, zoom), (2 ** zoom) - 1)
        min_y = max(lat_to_tile_y(north, zoom), 0)
        max_y = min(lat_to_tile_y(south, zoom), (2 ** zoom) - 1)
        for x in range(min_x, max_x + 1):
            for y in range(min_y, max_y + 1):
                yield zoom, x, y


def count_tiles_for_bounds(
    bounds: Tuple[float, float, float, float], min_zoom: int, max_zoom: int
) -> int:
    total = 0
    west, south, east, north = bounds
    for zoom in range(min_zoom, max_zoom + 1):
        min_x = max(lng_to_tile_x(west, zoom), 0)
        max_x = min(lng_to_tile_x(east, zoom), (2 ** zoom) - 1)
        min_y = max(lat_to_tile_y(north, zoom), 0)
        max_y = min(lat_to_tile_y(south, zoom), (2 ** zoom) - 1)
        if max_x < min_x or max_y < min_y:
            continue
        total += (max_x - min_x + 1) * (max_y - min_y + 1)
    return total


def quadkey_prefix(z: int, x: int, y: int, prefix_length: int = 6) -> str:
    if z <= 0:
        return ""
    chars: List[str] = []
    for i in range(z, 0, -1):
        mask = 1 << (i - 1)
        digit = 0
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        chars.append(str(digit))
    return "".join(chars[:prefix_length])


def build_tile_sql(definition: Dict[str, object]) -> str:
    attributes = ", ".join(definition["attributes"])
    geom_column = definition["geom_column"]
    min_zoom_column = definition["min_zoom_column"]
    max_zoom_column = definition["max_zoom_column"]
    tile_prefix_column = definition["tile_prefix_column"]
    tile_group_column = definition["tile_group_column"]

    return f"""
        WITH bounds AS (
            SELECT ST_SetSRID(ST_TileEnvelope(%s::integer, %s::integer, %s::integer), 3857) AS geom
        ), features AS (
            SELECT
                ST_AsMVTGeom(
                    data.{geom_column},
                    bounds.geom,
                    4096,
                    64,
                    true
                ) AS geom,
                {attributes}
            FROM {definition['table']} AS data
            CROSS JOIN bounds
            WHERE data.{geom_column} && bounds.geom
              AND %s::integer BETWEEN data.{min_zoom_column} AND data.{max_zoom_column}
              AND (%s = '' OR data.{tile_group_column} = %s)
              AND data.{tile_prefix_column} LIKE %s
        )
        SELECT ST_AsMVT(features, %s, 4096, 'geom') FROM features;
    """


def fetch_tile(
    pg: PostgresClient,
    sql: str,
    dataset: str,
    z: int,
    x: int,
    y: int,
    quadkey: str,
) -> Optional[bytes]:
    group_value = quadkey[0] if quadkey else ""
    prefix_like = f"{quadkey}%" if quadkey else "%"
    params: Tuple[object, ...] = (
        z,
        x,
        y,
        z,
        group_value,
        group_value,
        prefix_like,
        dataset,
    )
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
    return [
        {
            "id": dataset,
            "description": dataset.replace("_", " ").title(),
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
    shard: ShardDefinition, pg: PostgresClient, output_dir: Path
) -> Dict[str, object]:
    definition = TILE_DATASET_DEFINITIONS.get(shard.dataset)
    if not definition:
        raise ValueError(f"Dataset '{shard.dataset}' is not configured for tiles")

    sql = build_tile_sql(definition)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / shard.filename

    total_tiles_target = count_tiles_for_bounds(shard.bounds, shard.min_zoom, shard.max_zoom)
    written_tiles = 0
    progress_step = max(1, int(os.getenv("PMTILES_PROGRESS_STEP", "500")))
    if os.getenv("PMTILES_PROGRESS_PERCENT"):
        try:
            percent_step = max(1, min(100, int(os.getenv("PMTILES_PROGRESS_PERCENT"))))
            progress_step = max(1, max(1, total_tiles_target * percent_step // 100))
        except ValueError:
            pass

    with pmtiles_write(output_path) as writer:
        for index, (z, x, y) in enumerate(
            iter_tiles_for_bounds(shard.bounds, shard.min_zoom, shard.max_zoom),
            start=1,
        ):
            quadkey = quadkey_prefix(z, x, y)
            tile_bytes = fetch_tile(pg, sql, shard.dataset, z, x, y, quadkey)
            if not tile_bytes:
                continue
            compressed = gzip.compress(tile_bytes, compresslevel=6)
            writer.write_tile(zxy_to_tileid(z, x, y), compressed)
            written_tiles += 1
            if total_tiles_target and written_tiles % progress_step == 0:
                percent = (written_tiles / total_tiles_target) * 100
                print(
                    f"  [{shard.dataset}:{shard.shard_id}] {written_tiles}/{total_tiles_target} tiles ({percent:.1f}%)",
                    flush=True,
                )

        header = build_header(shard, shard.min_zoom, shard.max_zoom)
        metadata = build_metadata(shard, shard.min_zoom, shard.max_zoom)
        writer.finalize(header, metadata)

    return {
        "dataset": shard.dataset,
        "shard": shard.shard_id,
        "filename": shard.filename,
        "path": str(output_path),
        "minZoom": shard.min_zoom,
        "maxZoom": shard.max_zoom,
        "bounds": shard.bounds,
        "totalTiles": total_tiles_target,
        "writtenTiles": written_tiles,
    }


def run(output_dir: Path) -> None:
    dsn = resolve_database_dsn()
    pg_client = PostgresClient(dsn=dsn, application_name="pmtiles-export", statement_timeout_ms=240_000)
    missing_tables = [
        definition["table"]
        for definition in TILE_DATASET_DEFINITIONS.values()
        if not table_exists(pg_client, definition["table"])
    ]
    if missing_tables:
        print(
            "Required tile tables missing (" + ", ".join(sorted(set(missing_tables))) + "); generating via TileSchemaManager",
            flush=True,
        )
        schema_client = PostgresClient(
            dsn=dsn,
            application_name="pmtiles-export-schema",
            statement_timeout_ms=None,
        )
        def schema_log(message: str) -> None:
            print(f"[schema] {message}", flush=True)

        schema_manager = TileSchemaManager(schema_client, logger=schema_log)
        started_at = time.monotonic()
        schema_manager.ensure()
        elapsed = time.monotonic() - started_at
        print(f"[schema] Completed in {elapsed:.1f}s", flush=True)
        missing_tables = [
            definition["table"]
            for definition in TILE_DATASET_DEFINITIONS.values()
            if not table_exists(pg_client, definition["table"])
        ]
        if missing_tables:
            raise RuntimeError(
                "Required tile tables are still missing after schema creation: "
                + ", ".join(sorted(set(missing_tables)))
            )

    manifest: List[Dict[str, object]] = []
    for shard in SHARDS:
        print(f"Building shard {shard.dataset}:{shard.shard_id} -> {shard.filename}", flush=True)
        shard_started = time.monotonic()
        summary = generate_pmtiles_for_shard(shard, pg_client, output_dir)
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
    print(f"Wrote manifest → {manifest_path}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build PMTiles archives for heavy datasets")
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "pmtiles"),
        help="Directory to write PMTiles files into (default: ./pmtiles)",
    )
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir).resolve()
    run(output_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
