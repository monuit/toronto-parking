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
import concurrent.futures
import gzip
import hashlib
import heapq
import json
import os
import sys
import time
from urllib.parse import quote_plus
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Sequence, Iterable, Set

import math
import psycopg
import threading
from dotenv import load_dotenv
from pmtiles.tile import Compression, TileType, zxy_to_tileid
from pmtiles.writer import write as pmtiles_write
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from multiprocessing import cpu_count, get_context

try:
    import zstandard as zstd  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    zstd = None

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

CPU_TOTAL = cpu_count() or (os.cpu_count() or 4) or 4
TILE_PREFIX_LENGTH = int(os.getenv("PMTILES_QUADKEY_PREFIX_LENGTH", "16"))
DEFAULT_BATCH_SIZE = int(os.getenv("PMTILES_BATCH_SIZE", "2048"))
DEFAULT_DB_WORKERS = max(1, int(os.getenv("PMTILES_DB_WORKERS", "3")))
_cpu_fallback = max(1, CPU_TOTAL - 1)
DEFAULT_COMPRESS_WORKERS = max(1, int(os.getenv("PMTILES_CPU_WORKERS", str(_cpu_fallback))))
DEFAULT_COMPRESSION = os.getenv("PMTILES_COMPRESSION", "gzip").strip().lower() or "gzip"
DEFAULT_GZIP_LEVEL = max(1, min(9, int(os.getenv("PMTILES_GZIP_LEVEL", "2"))))
DEFAULT_ZSTD_LEVEL = max(-5, min(21, int(os.getenv("PMTILES_ZSTD_LEVEL", "4"))))
DEFAULT_MAX_ZOOM_OVERRIDE = int(os.getenv("PMTILES_BUILD_MAX_ZOOM", "12"))
DEFAULT_PARKING_MAX_ZOOM = DEFAULT_MAX_ZOOM_OVERRIDE
PROCESS_MAX_TASKS = max(1, int(os.getenv("PMTILES_PROCESS_MAX_TASKS", "128")))
WRITE_BUFFER_BYTES = max(262_144, int(os.getenv("PMTILES_WRITE_BUFFER_BYTES", str(4 * 1024 * 1024))))
UPLOAD_MAX_CONCURRENCY = max(1, int(os.getenv("PMTILES_S3_MAX_CONCURRENCY", "8")))
UPLOAD_CHUNK_BYTES = max(5 * 1024 * 1024, int(os.getenv("PMTILES_S3_CHUNK_BYTES", str(8 * 1024 * 1024))))
UPLOAD_THREAD_WORKERS = max(1, int(os.getenv("PMTILES_S3_UPLOAD_WORKERS", "4")))

_affinity_raw = os.getenv("PMTILES_CPU_AFFINITY")
AFFINITY_CORES: Optional[List[int]] = None
if _affinity_raw:
    try:
        AFFINITY_CORES = sorted({int(part.strip()) for part in _affinity_raw.split(",") if part.strip()})
    except ValueError:
        AFFINITY_CORES = None
DATASET_MAX_ZOOM_CAPS: Dict[str, int] = {
    "parking_tickets": DEFAULT_PARKING_MAX_ZOOM,
    "red_light_locations": DEFAULT_MAX_ZOOM_OVERRIDE,
    "ase_locations": DEFAULT_MAX_ZOOM_OVERRIDE,
}

TILE_BATCH_FUNCTIONS: Dict[str, str] = {
    "parking_tickets": "public.get_parking_tiles",
    "red_light_locations": "public.get_red_light_tiles",
    "ase_locations": "public.get_ase_tiles",
}

SIMPLIFICATION_FUNCTION_SNIPPETS: Dict[str, Tuple[str, ...]] = {
    "public.get_parking_tiles(integer[],integer[],integer[])": (
        "ST_SnapToGrid",
        "parking_ticket_tiles",
    ),
    "public.get_red_light_tiles(integer[],integer[],integer[])": (
        "ST_SnapToGrid",
        "red_light_camera_tiles",
    ),
    "public.get_ase_tiles(integer[],integer[],integer[])": (
        "ST_SnapToGrid",
        "ase_camera_tiles",
    ),
}

SIMPLIFICATION_TABLE_EXPECTATIONS: Tuple[Tuple[str, str, int, int], ...] = (
    ("parking_ticket_tiles", "parking_tickets", 0, 16),
    ("red_light_camera_tiles", "red_light_locations", 0, 16),
    ("ase_camera_tiles", "ase_locations", 0, 16),
)

if DEFAULT_COMPRESSION not in {"gzip", "zstd"}:
    DEFAULT_COMPRESSION = "gzip"

CURRENT_TILE_COMPRESSION = Compression.ZSTD if DEFAULT_COMPRESSION == "zstd" else Compression.GZIP


class UploadManager:
    def __init__(self, prefix: Optional[str]) -> None:
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

        self.bucket = bucket
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.prefix = normalized_prefix or ""
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=UPLOAD_THREAD_WORKERS, thread_name_prefix="pmtiles-upload")
        self.futures: List[concurrent.futures.Future] = []
        self.transfer_kwargs = {
            "multipart_threshold": UPLOAD_CHUNK_BYTES,
            "multipart_chunksize": UPLOAD_CHUNK_BYTES,
            "max_concurrency": UPLOAD_MAX_CONCURRENCY,
            "use_threads": True,
        }

    def submit(self, file_path: Path) -> None:
        if not file_path.exists():
            return
        key = f"{self.prefix}{file_path.name}" if self.prefix else file_path.name
        future = self.executor.submit(
            _upload_file,
            file_path,
            self.bucket,
            key,
            self.endpoint,
            self.access_key,
            self.secret_key,
            self.region,
            self.transfer_kwargs,
        )
        self.futures.append(future)

    def wait(self) -> None:
        for future in concurrent.futures.as_completed(self.futures):
            future.result()
        self.executor.shutdown(wait=True)


def _compute_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with file_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _upload_file(
    file_path: Path,
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    region: str,
    transfer_kwargs: Dict[str, object],
) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    config = TransferConfig(**transfer_kwargs)
    checksum = _compute_sha256(file_path)
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        existing_hash = head.get("Metadata", {}).get("sha256")
        if existing_hash and existing_hash == checksum:
            print(f"[upload] Skipping {key}; checksum unchanged")
            return
    except ClientError as error:  # pragma: no cover - network path
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in (404, 400):
            print(f"[upload] HeadObject failed for {key}: {error}")
    extra_args = {
        "ContentType": "application/octet-stream",
        "CacheControl": "public, immutable, max-age=31536000",
        "Metadata": {"sha256": checksum},
    }
    client.upload_file(str(file_path), bucket, key, ExtraArgs=extra_args, Config=config)


def _validate_tile_tables(pg_client: PostgresClient) -> List[str]:
    issues: List[str] = []
    for table_name, dataset_name, expected_min, expected_max in SIMPLIFICATION_TABLE_EXPECTATIONS:
        exists_row = pg_client.fetch_one(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = 'public'
                AND table_name = %s
            )
            """,
            (table_name,),
        )
        exists = bool(exists_row[0]) if exists_row else False
        if not exists:
            issues.append(f"tile table '{table_name}' is missing (expected for dataset '{dataset_name}')")
            continue

        row_present = pg_client.fetch_one(
            f"SELECT 1 FROM {table_name} WHERE dataset = %s LIMIT 1",
            (dataset_name,),
        )
        if not row_present:
            issues.append(f"tile table '{table_name}' has no rows for dataset '{dataset_name}'")
            continue

        min_max = pg_client.fetch_one(
            f"SELECT MIN(min_zoom), MAX(max_zoom) FROM {table_name} WHERE dataset = %s",
            (dataset_name,),
        )
        if not min_max or min_max[0] is None or min_max[1] is None:
            issues.append(f"tile table '{table_name}' has incomplete zoom metadata for dataset '{dataset_name}'")
        else:
            min_zoom, max_zoom = min_max
            if min_zoom > expected_min:
                issues.append(
                    f"tile table '{table_name}' minimum zoom {min_zoom} exceeds expected {expected_min}"
                )
            if max_zoom < expected_max:
                issues.append(
                    f"tile table '{table_name}' maximum zoom {max_zoom} below expected {expected_max}"
                )

        groups = pg_client.fetch_one(
            f"SELECT COUNT(DISTINCT tile_qk_group) FROM {table_name} WHERE dataset = %s",
            (dataset_name,),
        )
        if not groups or int(groups[0] or 0) < 4:
            issues.append(f"tile table '{table_name}' is missing quadkey partitions for dataset '{dataset_name}'")

    return issues


def _validate_function_simplification(pg_client: PostgresClient) -> List[str]:
    issues: List[str] = []
    for signature, snippets in SIMPLIFICATION_FUNCTION_SNIPPETS.items():
        definition_row = pg_client.fetch_one(
            "SELECT pg_get_functiondef($1::regprocedure)",
            (signature,),
        )
        if not definition_row or definition_row[0] is None:
            issues.append(f"function {signature} is missing")
            continue
        definition_text = definition_row[0]
        missing = [fragment for fragment in snippets if fragment not in definition_text]
        if missing:
            issues.append(
                f"function {signature} is missing required simplification fragments: {', '.join(missing)}"
            )
    return issues


def validate_tile_simplification(pg_client: PostgresClient) -> None:
    issues = _validate_tile_tables(pg_client)
    issues.extend(_validate_function_simplification(pg_client))
    if issues:
        bullet_points = "\n  - ".join(issues)
        raise RuntimeError(f"PMTiles simplification validation failed:\n  - {bullet_points}")


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
def zxy_to_quadkey(z: int, x: int, y: int) -> str:
    if z <= 0:
        return ""
    digits: List[str] = []
    for i in range(z, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if x & mask:
            digit += 1
        if y & mask:
            digit += 2
        digits.append(str(digit))
    return "".join(digits)


def collect_tiles_for_shard(pg: PostgresClient, shard: ShardDefinition) -> List[Tuple[int, int, int]]:  # noqa: ARG001
    west, south, east, north = shard.bounds
    max_zoom = shard.max_zoom
    if shard.dataset in DATASET_MAX_ZOOM_CAPS:
        max_zoom = min(max_zoom, DATASET_MAX_ZOOM_CAPS[shard.dataset])

    def clamp_lat(value: float) -> float:
        return max(min(value, 85.0511287798), -85.0511287798)

    def lon_to_tile_x(lon: float, zoom: int) -> int:
        n = 2 ** zoom
        x = (lon + 180.0) / 360.0 * n
        return max(0, min(int(math.floor(x)), n - 1))

    def lat_to_tile_y(lat: float, zoom: int) -> int:
        lat = clamp_lat(lat)
        n = 2 ** zoom
        rad = math.radians(lat)
        value = (1.0 - math.log(math.tan(rad) + (1.0 / math.cos(rad))) / math.pi) / 2.0 * n
        return max(0, min(int(math.floor(value)), n - 1))

    def adjust(value: float, fallback: float, lower: bool) -> float:
        if lower:
            return value
        if value <= fallback:
            return value
        return math.nextafter(value, fallback)

    tiles: list[Tuple[int, int, int]] = []
    for zoom in range(shard.min_zoom, max_zoom + 1):
        west_x = lon_to_tile_x(west, zoom)
        east_x = lon_to_tile_x(adjust(east, west, False), zoom)
        south_y = lat_to_tile_y(south, zoom)
        north_y = lat_to_tile_y(adjust(north, south, False), zoom)

        x_start, x_end = sorted((west_x, east_x))
        y_start, y_end = sorted((north_y, south_y))

        for tile_x in range(x_start, x_end + 1):
            for tile_y in range(y_start, y_end + 1):
                tiles.append((zoom, tile_x, tile_y))

    tiles.sort()
    return tiles


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
        "tile_compression": CURRENT_TILE_COMPRESSION,
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

    order_strategy = os.getenv("PMTILES_ORDER", "prefix").lower()
    if order_strategy == "prefix":
        sorted_tiles = sorted(tiles, key=lambda entry: _tile_sort_key(*entry))
    else:
        sorted_tiles = list(tiles)
    tile_entries: List[Tuple[int, int, int, int]] = [
        (index, z, x, y)
        for index, (z, x, y) in enumerate(sorted_tiles, start=1)
    ]

    actual_min_zoom = min(z for _, z, _, _ in tile_entries)
    actual_max_zoom = max(z for _, z, _, _ in tile_entries)

    batch_size = max(1, DEFAULT_BATCH_SIZE)
    db_workers = max(1, DEFAULT_DB_WORKERS)
    compress_workers = max(1, DEFAULT_COMPRESS_WORKERS)

    compression = DEFAULT_COMPRESSION
    gzip_level = DEFAULT_GZIP_LEVEL
    zstd_level = DEFAULT_ZSTD_LEVEL

    processed_indices: Set[int] = set()
    result_heap: List[Tuple[int, int, int, int, bytes]] = []
    written_tiles = 0
    next_index = 1

    pending_tiles: List[Tuple[int, bytes]] = []
    pending_bytes = 0
    buffer_limit = WRITE_BUFFER_BYTES

    def flush_buffer(writer_obj) -> None:
        nonlocal pending_tiles, pending_bytes
        if not pending_tiles:
            return
        for tile_id, payload in pending_tiles:
            writer_obj.write_tile(tile_id, payload)
        pending_tiles = []
        pending_bytes = 0

    def queue_tile(writer_obj, z: int, x: int, y: int, payload: bytes) -> None:
        nonlocal pending_tiles, pending_bytes
        tile_id = zxy_to_tileid(z, x, y)
        pending_tiles.append((tile_id, payload))
        pending_bytes += len(payload)
        if pending_bytes >= buffer_limit:
            flush_buffer(writer_obj)

    def advance_pointer(writer_obj) -> None:
        nonlocal next_index, written_tiles
        progressed = True
        while progressed:
            progressed = False
            if next_index in processed_indices:
                if result_heap and result_heap[0][0] == next_index:
                    _, z, x, y, compressed_payload = heapq.heappop(result_heap)
                    queue_tile(writer_obj, z, x, y, compressed_payload)
                    written_tiles += 1
                    if total_tiles_target and written_tiles % progress_step == 0:
                        percent = (written_tiles / total_tiles_target) * 100
                        print(
                            f"  [{shard.dataset}:{shard.shard_id}] {written_tiles}/{total_tiles_target} tiles ({percent:.1f}%)",
                            flush=True,
                        )
                processed_indices.discard(next_index)
                next_index += 1
                progressed = True

    db_executor = concurrent.futures.ThreadPoolExecutor(max_workers=db_workers, thread_name_prefix="pmtiles-db")
    ctx = get_context("spawn")
    compress_executor = concurrent.futures.ProcessPoolExecutor(
        max_workers=compress_workers,
        mp_context=ctx,
        initializer=_compress_worker_init,
        initargs=(AFFINITY_CORES,),
        max_tasks_per_child=PROCESS_MAX_TASKS,
    )
    compress_futures: Set[concurrent.futures.Future] = set()

    try:
        future_to_batch: Dict[concurrent.futures.Future, List[Tuple[int, int, int, int]]] = {}
        for batch in _iter_batches(tile_entries, batch_size):
            future = db_executor.submit(_fetch_tiles_batch, pg, shard.dataset, batch)
            future_to_batch[future] = batch

        with pmtiles_write(output_path) as writer:
            for db_future in concurrent.futures.as_completed(future_to_batch):
                batch = future_to_batch.pop(db_future)
                rows, empty_indexes = db_future.result()
                for idx in empty_indexes:
                    processed_indices.add(idx)
                if rows:
                    comp_future = compress_executor.submit(
                        _compress_tiles,
                        rows,
                        compression,
                        gzip_level,
                        zstd_level,
                    )
                    compress_futures.add(comp_future)

                ready_compress = [f for f in list(compress_futures) if f.done()]
                for comp_future in ready_compress:
                    compress_futures.discard(comp_future)
                    compressed_rows = comp_future.result()
                    for entry in compressed_rows:
                        heapq.heappush(result_heap, entry)
                        processed_indices.add(entry[0])
                    advance_pointer(writer)

                advance_pointer(writer)

            if compress_futures:
                for comp_future in concurrent.futures.as_completed(compress_futures):
                    compressed_rows = comp_future.result()
                    for entry in compressed_rows:
                        heapq.heappush(result_heap, entry)
                        processed_indices.add(entry[0])
                    advance_pointer(writer)
            flush_buffer(writer)

            # Ensure no leftovers remain.
            advance_pointer(writer)
            flush_buffer(writer)

            header = build_header(shard, actual_min_zoom, actual_max_zoom)
            metadata = build_metadata(shard, actual_min_zoom, actual_max_zoom)
            writer.finalize(header, metadata)
    finally:
        db_executor.shutdown(wait=True)
        compress_executor.shutdown(wait=True)

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


_WORKER_LOCAL = threading.local()


def _get_worker_connection(pg_client: PostgresClient) -> psycopg.Connection:
    conn = getattr(_WORKER_LOCAL, "conn", None)
    if conn is None or conn.closed:
        conn = psycopg.connect(
            pg_client.dsn,
            autocommit=True,
            application_name=pg_client.application_name,
        )
        if pg_client.statement_timeout_ms is not None:
            conn.execute(f"SET statement_timeout = {int(pg_client.statement_timeout_ms)}")
        conn.execute("SET max_parallel_workers_per_gather = 4")
        conn.execute("SET work_mem = '128MB'")
        conn.execute("SET jit = off")
        _WORKER_LOCAL.conn = conn
    return conn


def _tile_sort_key(z: int, x: int, y: int) -> Tuple[str, int, int, int]:
    quadkey = zxy_to_quadkey(z, x, y)
    prefix = quadkey[:TILE_PREFIX_LENGTH]
    return (prefix, z, x, y)


def _iter_batches(entries: List[Tuple[int, int, int, int]], batch_size: int) -> Iterable[List[Tuple[int, int, int, int]]]:
    for index in range(0, len(entries), batch_size):
        yield entries[index : index + batch_size]


def _compress_tiles(
    entries: List[Tuple[int, int, int, int, bytes]],
    compression: str,
    gzip_level: int,
    zstd_level: int,
) -> List[Tuple[int, int, int, int, bytes]]:
    if not entries:
        return []
    if compression == "gzip":
        return [
            (index, z, x, y, gzip.compress(payload, compresslevel=gzip_level))
            for index, z, x, y, payload in entries
        ]
    if compression == "zstd":
        if zstd is None:
            raise RuntimeError("zstd compression requested but zstandard module is not installed")
        compressor = zstd.ZstdCompressor(level=zstd_level)
        return [
            (index, z, x, y, compressor.compress(payload))
            for index, z, x, y, payload in entries
        ]
    raise ValueError(f"Unsupported compression '{compression}'")


def _fetch_tiles_batch(
    pg_client: PostgresClient,
    dataset: str,
    batch: List[Tuple[int, int, int, int]],
) -> Tuple[List[Tuple[int, int, int, int, bytes]], List[int]]:
    if not batch:
        return [], []

    function_name = TILE_BATCH_FUNCTIONS.get(dataset)
    if not function_name:
        raise ValueError(f"No batch query function registered for dataset '{dataset}'")

    conn = _get_worker_connection(pg_client)
    index_lookup: Dict[Tuple[int, int, int], int] = {}
    zs: List[int] = []
    xs: List[int] = []
    ys: List[int] = []
    for index, z, x, y in batch:
        index_lookup[(z, x, y)] = index
        zs.append(z)
        xs.append(x)
        ys.append(y)

    rows: List[Tuple[int, int, int, object]]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT z, x, y, mvt FROM {function_name}(%s, %s, %s)",
            (zs, xs, ys),
        )
        rows = cur.fetchall()

    results: List[Tuple[int, int, int, int, bytes]] = []
    found_indexes: Set[int] = set()
    for z, x, y, mvt in rows:
        index = index_lookup.get((z, x, y))
        if index is None or mvt is None:
            continue
        results.append((index, z, x, y, bytes(mvt)))
        found_indexes.add(index)

    empty_indexes = [index for index in index_lookup.values() if index not in found_indexes]
    return results, empty_indexes


def _compress_worker_init(cores: Optional[List[int]] = None) -> None:  # pragma: no cover - platform specific
    if cores:
        try:
            os.sched_setaffinity(0, cores)
        except (AttributeError, NotImplementedError):
            return
        except OSError:
            return


def run(
    output_dir: Path,
    upload: bool,
    upload_prefix: str | None,
    force_schema_refresh: bool,
    refresh_tile_tables: bool,
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

    schema_manager = TileSchemaManager(schema_client, logger=schema_log)
    if force_schema_refresh or refresh_tile_tables:
        print("Forcing tile schema refresh", flush=True)
        started = time.monotonic()
        schema_manager.ensure(include_tile_tables=True)
        print(f"[schema] Refresh completed in {time.monotonic() - started:.1f}s", flush=True)
    else:
        schema_manager.ensure(include_tile_tables=False)

    try:
        validate_tile_simplification(schema_client)
    except RuntimeError as validation_error:
        if force_schema_refresh or refresh_tile_tables:
            raise
        print("[schema] Simplification validation failed; rebuilding tile tables", flush=True)
        schema_manager.ensure(include_tile_tables=True)
        validate_tile_simplification(schema_client)

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

    upload_manager: Optional[UploadManager] = UploadManager(upload_prefix) if upload else None

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
        if upload_manager:
            upload_manager.submit(Path(summary["path"]))

    manifest_path = output_dir / "pmtiles-manifest.json"
    manifest_payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "shards": manifest,
    }
    manifest_path.write_text(json.dumps(manifest_payload, indent=2))
    print(f"Wrote manifest -> {manifest_path}")
    if upload_manager:
        upload_manager.submit(manifest_path)
        upload_manager.wait()
    return manifest


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
        "--refresh-tile-tables",
        action="store_true",
        help="Rebuild precomputed tile tables before exporting",
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
        args.refresh_tile_tables,
        dataset_filter,
        shard_filter,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
