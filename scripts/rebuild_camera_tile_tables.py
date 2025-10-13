"""Rebuild precomputed camera tile partitions without touching PMTiles exports.

This script loads environment variables from the project ``.env`` files, resolves the
database connection string, and invokes ``TileSchemaManager.ensure`` with
``include_tile_tables=True`` so that the Postgres ``*_camera_tiles`` tables are
fully regenerated.  Rebuilds run in parallel worker threads to minimise wall-clock
time while keeping the database tuned for heavy writes.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def load_env_files(files: Iterable[str]) -> None:
    for relative in files:
        path = PROJECT_ROOT / relative
        if path.exists():
            load_dotenv(path, override=False)


load_env_files(
    (
        ".env",
        ".env.production",
        "map-app/.env",
        "map-app/.env.local",
        "map-app/.env.production",
    )
)

from src.etl.postgres import PostgresClient  # noqa: E402
from src.tiles.schema import TileSchemaManager  # noqa: E402


def resolve_dsn() -> str:
    candidates = (
        os.getenv("TILES_DB_URL"),
        os.getenv("DATABASE_PRIVATE_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("POSTGRES_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
    )
    for candidate in candidates:
        if candidate:
            return candidate.strip()

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
        return f"postgresql://{user}:{password or ''}@{host}:{port}/{database}".strip()

    raise RuntimeError(
        "Unable to resolve Postgres DSN. Set TILES_DB_URL or standard Postgres env vars."
    )


def main() -> None:
    dsn = resolve_dsn()
    redacted = dsn
    if "@" in dsn:
        redacted = f"***@{dsn.split('@', 1)[-1]}"
    print("Rebuilding camera tile tables using DSN:", redacted, flush=True)

    extra_pgoptions = [
        "-c work_mem=256MB",
        "-c maintenance_work_mem=512MB",
        "-c max_parallel_workers_per_gather=8",
        "-c max_parallel_workers=8",
        "-c jit=off",
    ]
    options = os.environ.get("PGOPTIONS", "")
    for option in extra_pgoptions:
        if option not in options:
            options = f"{options} {option}".strip()
    os.environ["PGOPTIONS"] = options

    worker_env = os.getenv("TILE_REBUILD_WORKERS")
    if worker_env is not None:
        try:
            workers = max(1, int(worker_env))
        except ValueError:
            workers = 1
    else:
        cpu_count = os.cpu_count() or 4
        workers = max(1, min(8, cpu_count))
    print(f"Using {workers} parallel worker(s) for tile rebuild", flush=True)

    client = PostgresClient(
        dsn=dsn,
        application_name="camera-tile-rebuild",
        statement_timeout_ms=None,
    )

    manager = TileSchemaManager(
        client,
        tile_rebuild_workers=workers,
        logger=lambda msg: print(f"[tiles] {msg}", flush=True),
    )
    started = time.monotonic()
    manager.ensure(include_tile_tables=True)
    duration = time.monotonic() - started
    print(f"Camera tile tables rebuilt in {duration:.1f}s", flush=True)


if __name__ == "__main__":
    main()
