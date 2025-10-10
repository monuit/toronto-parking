"""Utility to list the largest Postgres/PostGIS tables by on-disk size."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg.rows import tuple_row


DEFAULT_LIMIT = 25
DEFAULT_TIMEOUT = 60  # Query timeout in seconds

_CANDIDATE_ENV_KEYS: Sequence[str] = (
    "DATABASE_PUBLIC_URL",
    "POSTGRES_PUBLIC_URL",
    "DATABASE_URL",
    "POSTGRES_URL",
    "POSTGIS_DATABASE_URL",
    "DATABASE_INTERNAL_URL",
    "DATABASE_PRIVATE_URL",
)


def _load_env() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)


def _classify_dsn(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
    except ValueError:
        return "unknown"
    host = (parsed.hostname or "").lower()
    if not host:
        return "unknown"
    if any(token in host for token in ("internal", "private")):
        return "internal"
    return "public"


def _iter_dsn_candidates(cli_dsn: str | None) -> Iterable[str]:
    if cli_dsn:
        yield cli_dsn
    seen: set[str] = set()
    public_dsn: list[str] = []
    internal_dsn: list[str] = []
    unknown_dsn: list[str] = []
    for key in _CANDIDATE_ENV_KEYS:
        value = os.getenv(key)
        if value and value not in seen:
            seen.add(value)
            classification = _classify_dsn(value)
            if classification == "internal":
                internal_dsn.append(value)
            elif classification == "public":
                public_dsn.append(value)
            else:
                unknown_dsn.append(value)
    yield from public_dsn
    yield from unknown_dsn
    yield from internal_dsn


def _resolve_connection(dsn_candidates: Iterable[str]) -> tuple[psycopg.Connection, str]:
    last_error: Exception | None = None
    for candidate in dsn_candidates:
        try:
            conn = psycopg.connect(candidate, connect_timeout=5, row_factory=tuple_row)
        except Exception as exc:
            last_error = exc
            print(f"Failed to connect using DSN '{candidate}': {exc}", file=sys.stderr)
            continue
        host_display = candidate.split("@")[-1].split("?")[0]
        print(f"Connected to {host_display}")
        return conn, candidate
    message = (
        f"Could not connect to Postgres. Last error: {last_error}" if last_error else "No DSN provided"
    )
    raise RuntimeError(message)


def _build_geometry_query(min_bytes: int | None) -> str:
    min_clause = ""
    if min_bytes is not None:
        min_clause = f"\nWHERE sizes.total_bytes >= {min_bytes}"
    return (
        "WITH targets AS (\n"
    "  SELECT DISTINCT c.table_schema AS schema, c.table_name AS table_name\n"
    "  FROM information_schema.columns c\n"
    "  JOIN information_schema.tables t\n"
    "    ON c.table_schema = t.table_schema\n"
    "   AND c.table_name = t.table_name\n"
    "  WHERE c.udt_name IN ('geometry', 'geography')\n"
    "    AND t.table_type IN ('BASE TABLE', 'PARTITIONED TABLE')\n"
    "    AND c.table_schema NOT IN ('pg_catalog', 'information_schema')\n"
        "),\n"
        "resolved AS (\n"
        "  SELECT\n"
        "    schema,\n"
        "    table_name,\n"
        "    to_regclass(format('%I.%I', schema, table_name)) AS regclass\n"
        "  FROM targets\n"
        "),\n"
        "sizes AS (\n"
        "  SELECT\n"
        "    schema,\n"
    "    table_name AS table_name,\n"
        "    pg_total_relation_size(regclass) AS total_bytes,\n"
        "    pg_relation_size(regclass) AS table_bytes,\n"
        "    pg_indexes_size(regclass) AS index_bytes\n"
        "  FROM resolved\n"
        "  WHERE regclass IS NOT NULL\n"
        ")\n"
    "SELECT schema, table_name, total_bytes, table_bytes, index_bytes\n"
        "FROM sizes"
        f"{min_clause}\n"
        "ORDER BY total_bytes DESC\n"
    )


def _build_raster_query(min_bytes: int | None) -> str:
    min_clause = ""
    if min_bytes is not None:
        min_clause = f"\nWHERE sizes.total_bytes >= {min_bytes}"
    return (
        "WITH targets AS (\n"
    "  SELECT DISTINCT c.table_schema AS schema, c.table_name AS table_name\n"
    "  FROM information_schema.columns c\n"
    "  JOIN information_schema.tables t\n"
    "    ON c.table_schema = t.table_schema\n"
    "   AND c.table_name = t.table_name\n"
    "  WHERE c.udt_name = 'raster'\n"
    "    AND t.table_type IN ('BASE TABLE', 'PARTITIONED TABLE')\n"
    "    AND c.table_schema NOT IN ('pg_catalog', 'information_schema')\n"
        "),\n"
        "resolved AS (\n"
        "  SELECT\n"
        "    schema,\n"
        "    table_name,\n"
        "    to_regclass(format('%I.%I', schema, table_name)) AS regclass\n"
        "  FROM targets\n"
        "),\n"
        "sizes AS (\n"
        "  SELECT\n"
        "    schema,\n"
    "    table_name AS table_name,\n"
        "    pg_total_relation_size(regclass) AS total_bytes,\n"
        "    pg_relation_size(regclass) AS table_bytes,\n"
        "    pg_indexes_size(regclass) AS index_bytes\n"
        "  FROM resolved\n"
        "  WHERE regclass IS NOT NULL\n"
        ")\n"
    "SELECT schema, table_name, total_bytes, table_bytes, index_bytes\n"
        "FROM sizes"
        f"{min_clause}\n"
        "ORDER BY total_bytes DESC\n"
    )


def _build_postgis_query(min_bytes: int | None) -> str:
    combined_filter = ""
    if min_bytes is not None:
        combined_filter = f"\nWHERE total_bytes >= {min_bytes}"
    return (
        "WITH geometry_sizes AS (\n"
        "  WITH targets AS (\n"
        "    SELECT DISTINCT c.table_schema AS schema, c.table_name AS table_name\n"
        "    FROM information_schema.columns c\n"
        "    JOIN information_schema.tables t\n"
        "      ON c.table_schema = t.table_schema\n"
        "     AND c.table_name = t.table_name\n"
        "    WHERE c.udt_name IN ('geometry', 'geography')\n"
        "      AND t.table_type IN ('BASE TABLE', 'PARTITIONED TABLE')\n"
        "      AND c.table_schema NOT IN ('pg_catalog', 'information_schema')\n"
        "  ),\n"
        "  resolved AS (\n"
        "    SELECT\n"
        "      schema,\n"
        "      table_name,\n"
        "      to_regclass(format('%I.%I', schema, table_name)) AS regclass\n"
        "    FROM targets\n"
        "  )\n"
        "  SELECT\n"
        "    schema,\n"
        "    table_name,\n"
        "    pg_total_relation_size(regclass) AS total_bytes,\n"
        "    pg_relation_size(regclass) AS table_bytes,\n"
        "    pg_indexes_size(regclass) AS index_bytes\n"
        "  FROM resolved\n"
        "  WHERE regclass IS NOT NULL\n"
        "),\n"
        "raster_sizes AS (\n"
        "  WITH targets AS (\n"
        "    SELECT DISTINCT c.table_schema AS schema, c.table_name AS table_name\n"
        "    FROM information_schema.columns c\n"
        "    JOIN information_schema.tables t\n"
        "      ON c.table_schema = t.table_schema\n"
        "     AND c.table_name = t.table_name\n"
        "    WHERE c.udt_name = 'raster'\n"
        "      AND t.table_type IN ('BASE TABLE', 'PARTITIONED TABLE')\n"
        "      AND c.table_schema NOT IN ('pg_catalog', 'information_schema')\n"
        "  ),\n"
        "  resolved AS (\n"
        "    SELECT\n"
        "      schema,\n"
        "      table_name,\n"
        "      to_regclass(format('%I.%I', schema, table_name)) AS regclass\n"
        "    FROM targets\n"
        "  )\n"
        "  SELECT\n"
        "    schema,\n"
        "    table_name,\n"
        "    pg_total_relation_size(regclass) AS total_bytes,\n"
        "    pg_relation_size(regclass) AS table_bytes,\n"
        "    pg_indexes_size(regclass) AS index_bytes\n"
        "  FROM resolved\n"
        "  WHERE regclass IS NOT NULL\n"
        "),\n"
        "combined AS (\n"
        "  SELECT * FROM geometry_sizes\n"
        "  UNION ALL\n"
        "  SELECT * FROM raster_sizes\n"
        ")\n"
        "SELECT * FROM combined"
        f"{combined_filter}\n"
        "ORDER BY total_bytes DESC\n"
    )


def _build_query(min_bytes: int | None, catalog: str) -> str:
    if catalog == "geometry":
        return _build_geometry_query(min_bytes)
    if catalog == "raster":
        return _build_raster_query(min_bytes)
    if catalog == "postgis":
        return _build_postgis_query(min_bytes)
    filter_clause = ""
    if min_bytes is not None:
        filter_clause = f"\n  AND pg_total_relation_size(c.oid) >= {min_bytes}"
    return (
        "SELECT\n"
        "  n.nspname AS schema,\n"
    "  c.relname AS table_name,\n"
        "  pg_total_relation_size(c.oid) AS total_bytes,\n"
        "  pg_relation_size(c.oid) AS table_bytes,\n"
        "  pg_indexes_size(c.oid) AS index_bytes\n"
        "FROM pg_class c\n"
        "JOIN pg_namespace n ON n.oid = c.relnamespace\n"
    "WHERE c.relkind IN ('r', 'p')\n"
        "  AND n.nspname NOT IN ('pg_catalog', 'information_schema')"
        f"{filter_clause}\n"
        "ORDER BY total_bytes DESC\n"
    )


def list_table_sizes(
    conn: psycopg.Connection,
    *,
    limit: int,
    min_bytes: int | None,
    timeout: int = 60,
    catalog: str = "all",
) -> list[tuple[str, str, int, int, int]]:
    """
    List table sizes with configurable timeout.

    Args:
        conn: Database connection
        limit: Maximum number of rows to return
        min_bytes: Minimum size filter in bytes
        timeout: Query timeout in seconds (default: 60)
        catalog: Which catalog to inspect (all, geometry, raster, postgis)
    """
    query = _build_query(min_bytes, catalog)
    if limit > 0:
        query += f"LIMIT {limit}"

    with conn.cursor() as cur:
        # Set statement timeout for this query
        print(f"Setting query timeout to {timeout} seconds...")
        cur.execute(f"SET statement_timeout = '{timeout}s'")

        print(f"Executing table size query (this may take a while)...")
        try:
            cur.execute(query)
            rows = cur.fetchall()
            print(f"Query completed successfully. Found {len(rows)} tables.")
        except psycopg.errors.QueryCanceled:
            print(f"\n⚠️  Query exceeded {timeout}s timeout. Try:")
            print("  1. Reducing --limit (e.g., --limit 10)")
            print("  2. Adding --min-bytes filter (e.g., --min-bytes 1000000)")
            print("  3. Increasing timeout by editing the script")
            raise
        finally:
            # Reset timeout
            cur.execute("RESET statement_timeout")

    return [
        (row[0], row[1], int(row[2]), int(row[3]), int(row[4]))
        for row in rows
    ]


def format_size(num_bytes: int) -> str:
    step = 1024.0
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)
    for unit in units:
        if value < step:
            return f"{value:.1f}{unit}"
        value /= step
    return f"{value:.1f}EB"


def print_table(rows: Sequence[tuple[str, str, int, int, int]]) -> None:
    header = f"{'schema':<20} {'table':<40} {'total':>12} {'table':>12} {'indexes':>12}"
    print(header)
    print("-" * len(header))
    for schema, table, total_bytes, table_bytes, index_bytes in rows:
        print(
            f"{schema:<20} {table:<40} "
            f"{format_size(total_bytes):>12} {format_size(table_bytes):>12} {format_size(index_bytes):>12}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List largest Postgres/PostGIS tables by size.")
    parser.add_argument(
        "--dsn",
        help="Explicit Postgres connection string (overrides environment variables)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Number of rows to return (default: {DEFAULT_LIMIT}; 0 for no limit)",
    )
    parser.add_argument(
        "--min-bytes",
        type=int,
        default=None,
        help="Only include tables whose total size is at least this many bytes",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Query timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--catalog",
        choices=["all", "geometry", "raster", "postgis"],
        default="all",
        help="Choose which catalog to inspect: all relations, geometry tables, raster tables, or both PostGIS catalogs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _load_env()
    conn, _dsn = _resolve_connection(_iter_dsn_candidates(args.dsn))
    with conn:
        rows = list_table_sizes(
            conn,
            limit=args.limit,
            min_bytes=args.min_bytes,
            timeout=args.timeout,
            catalog=args.catalog,
        )
    if not rows:
        print("No tables matched the criteria.")
        return
    print_table(rows)


if __name__ == "__main__":
    main()
