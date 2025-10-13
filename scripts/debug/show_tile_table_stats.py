"""Print row and size metrics for tile tables."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
        password_part = f":{password}" if password else ""
        return f"postgresql://{user}{password_part}@{host}:{port}/{database}".strip()

    raise RuntimeError("Unable to resolve Postgres DSN from environment variables.")


def main() -> None:
    dsn = resolve_dsn()
    client = PostgresClient(dsn=dsn, application_name="tile-stats", statement_timeout_ms=300_000)

    size_rows = client.fetch_all(
        """
        SELECT
            c.relname AS table,
            pg_total_relation_size(c.oid) AS total_bytes,
            pg_indexes_size(c.oid) AS index_bytes,
            pg_table_size(c.oid) AS heap_bytes,
            c.reltuples::bigint AS est_rows
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n
          ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname IN ('parking_ticket_tiles', 'red_light_camera_tiles', 'ase_camera_tiles')
        ORDER BY c.relname;
        """
    )
    counts = client.fetch_all(
        """
        SELECT 'red_light_camera_tiles' AS table, COUNT(*)::bigint
        FROM red_light_camera_tiles
        UNION ALL
        SELECT 'ase_camera_tiles' AS table, COUNT(*)::bigint
        FROM ase_camera_tiles
        UNION ALL
        SELECT 'parking_ticket_tiles' AS table, COUNT(*)::bigint
        FROM parking_ticket_tiles;
        """
    )
    )

    actual_counts = {name: value for name, value in counts}

    print("Tile table stats:")
    for table, total_bytes, index_bytes, heap_bytes, est_rows in size_rows:
        actual = actual_counts.get(table)
        row_display = actual if actual is not None else est_rows
        print(
            f"  {table}: rows={row_display:,} (est={est_rows:,}), "
            f"heap={heap_bytes / 1_048_576:.1f} MiB, indexes={index_bytes / 1_048_576:.1f} MiB, "
            f"total={total_bytes / 1_048_576:.1f} MiB"
        )


if __name__ == "__main__":
    main()
