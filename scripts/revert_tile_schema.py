"""Rollback helper to remove tile-service columns/indexes from base tables.

This script drops the `geom_3857` and `tile_qk_prefix` columns (and their
indexes) from the core datasets so that Postgres storage returns to the
pre-schema state.  It should only be run when the tile tables are not needed.

Usage:
    python scripts/revert_tile_schema.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from urllib.parse import quote_plus

from dotenv import load_dotenv
from psycopg import connect
from psycopg.errors import OperationalError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"


def load_env() -> None:
    if DOTENV_PATH.exists():
        load_dotenv(DOTENV_PATH)


def _can_connect(candidate: str | None) -> bool:
    if not candidate:
        return False
    try:
        with connect(candidate, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except OperationalError:
        return False


def resolve_dsn() -> str:
    candidates = (
        os.getenv("DATABASE_PRIVATE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("POSTGRES_URL"),
    )
    for candidate in candidates:
        if _can_connect(candidate):
            return candidate  # type: ignore[return-value]

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
        user_enc = quote_plus(user)
        password_part = f":{quote_plus(password)}" if password else ""
        candidate = f"postgresql://{user_enc}{password_part}@{host}:{port}/{database}"
        if _can_connect(candidate):
            return candidate

    raise RuntimeError("Unable to resolve a working Postgres connection string from environment variables.")


def format_qualified(schema: str, table: str) -> str:
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def quote_identifier(value: str) -> str:
    return f'"{value.replace("\"", "\"\"")}"'


def list_candidate_tables(cursor) -> list[tuple[str, str]]:
    cursor.execute(
        """
        SELECT DISTINCT table_schema, table_name
        FROM information_schema.columns
        WHERE column_name IN ('geom_3857', 'tile_qk_prefix')
        ORDER BY table_schema, table_name
        """
    )
    return cursor.fetchall()


def drop_indexes(cursor, schema: str, table: str) -> None:
    cursor.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
          AND (indexdef ILIKE '%%geom_3857%%' OR indexdef ILIKE '%%tile_qk_prefix%%')
        """,
        (schema, table),
    )
    for (index_name,) in cursor.fetchall():
        cursor.execute(f"DROP INDEX IF EXISTS {format_qualified(schema, index_name)}")


def execute_statements(cursor, schema: str, table: str) -> None:
    qualified = format_qualified(schema, table)
    cursor.execute(f"ALTER TABLE {qualified} DROP COLUMN IF EXISTS geom_3857")
    cursor.execute(f"ALTER TABLE {qualified} DROP COLUMN IF EXISTS tile_qk_prefix")
    drop_indexes(cursor, schema, table)


def vacuum_table(cursor, schema: str, table: str) -> None:
    cursor.execute(f"VACUUM (FULL, ANALYZE) {format_qualified(schema, table)};")


def run() -> None:
    load_env()
    dsn = resolve_dsn()

    with connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            candidates = list_candidate_tables(cur)
            if not candidates:
                print("No tables contain geom_3857 or tile_qk_prefix columns; nothing to do.")
                return

            resolved: list[tuple[str, str]] = []
            for schema_name, table_name in candidates:
                print(f"Rolling back tile schema on '{schema_name}.{table_name}'...")
                execute_statements(cur, schema_name, table_name)
                resolved.append((schema_name, table_name))
            conn.commit()

    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            for schema_name, table_name in resolved:
                print(f"Vacuuming '{schema_name}.{table_name}' (FULL, ANALYZE)...")
                vacuum_table(cur, schema_name, table_name)

    print("Tile schema rollback complete.")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # pragma: no cover - diagnostic path
        print(f"Tile schema rollback failed: {exc}", file=sys.stderr)
        sys.exit(1)
