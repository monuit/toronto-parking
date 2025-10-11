"""Report Postgres table sizes and row counts.

Connects using the same environment variables as the rest of the project and
prints a table of relations ordered by total on-disk size.  This helps
identify which tables are responsible for large storage or memory usage.

Usage:
    python scripts/list_table_usage.py [--limit 30]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, Tuple

from urllib.parse import quote_plus, urlparse, urlunparse, parse_qsl, urlencode

from dotenv import load_dotenv
from psycopg import connect
from psycopg.errors import OperationalError


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = PROJECT_ROOT / ".env"
OUTPUT_PATH = PROJECT_ROOT / "postgis_table_usage.txt"


def load_env() -> None:
    if DOTENV_PATH.exists():
        load_dotenv(DOTENV_PATH)


def _normalize_dsn(candidate: str | None) -> str | None:
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme not in {"postgres", "postgresql"}:
        return candidate
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if "sslmode" not in query:
        query["sslmode"] = "require"
        parsed = parsed._replace(query=urlencode(query))
    return urlunparse(parsed)


def _can_connect(candidate: str | None) -> bool:
    if not candidate:
        return False
    try:
        with connect(candidate, connect_timeout=8) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except OperationalError:
        return False


def resolve_dsn() -> str:
    def candidate_priority(dsn: str) -> int:
        host = (urlparse(dsn).hostname or "").lower()
        if "postgis" in host:
            return 3
        if "interchange" in host:
            return 2
        return 1

    candidates: Iterable[str | None] = (
        os.getenv("POSTGIS_PRIVATE_URL"),
        os.getenv("POSTGIS_PUBLIC_URL"),
        os.getenv("POSTGIS_URL"),
        os.getenv("DATABASE_PRIVATE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("POSTGRES_URL"),
    )
    normalized_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = _normalize_dsn(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_candidates.append(normalized)

    normalized_candidates.sort(key=candidate_priority, reverse=True)

    for candidate in normalized_candidates:
        if _can_connect(candidate):
            return candidate

    host = os.getenv("POSTGIS_HOST") or os.getenv("POSTGRES_HOST") or os.getenv("PGHOST")
    user = os.getenv("POSTGIS_USER") or os.getenv("POSTGRES_USER") or os.getenv("PGUSER")
    password = os.getenv("POSTGIS_PASSWORD") or os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD")
    database = (
        os.getenv("POSTGIS_DB")
        or os.getenv("POSTGRES_DB")
        or os.getenv("POSTGRES_DATABASE")
        or os.getenv("PGDATABASE")
        or "postgres"
    )
    port = (
        os.getenv("POSTGIS_PORT")
        or os.getenv("POSTGRES_PORT")
        or os.getenv("PGPORT")
        or "5432"
    )

    if host and user:
        user_enc = quote_plus(user)
        password_part = f":{quote_plus(password)}" if password else ""
        candidate = f"postgresql://{user_enc}{password_part}@{host}:{port}/{database}"
        normalized_candidate = _normalize_dsn(candidate)
        if normalized_candidate and _can_connect(normalized_candidate):
            return normalized_candidate

    raise RuntimeError("Unable to resolve a working Postgres connection string from environment variables.")


def fetch_table_usage(limit: int, explicit_dsn: str | None = None) -> Tuple[Tuple[str, str, str, str, str, int, int], str, str]:
    dsn = _normalize_dsn(explicit_dsn) if explicit_dsn else resolve_dsn()
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database();")
            database_name = cur.fetchone()[0]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    concat_ws('.', n.nspname, c.relname) AS table_name,
                    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                    pg_size_pretty(pg_relation_size(c.oid)) AS heap_size,
                    pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
                    pg_size_pretty(CASE WHEN c.reltoastrelid <> 0 THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END) AS toast_size,
                    COALESCE(pg_stat_get_live_tuples(c.oid), 0)::bigint AS live_rows,
                    COALESCE(pg_stat_get_dead_tuples(c.oid), 0)::bigint AS dead_rows
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind IN ('r', 'p')
                ORDER BY pg_total_relation_size(c.oid) DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return rows, database_name, dsn


def _mask_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    netloc = parsed.netloc
    if "@" in netloc:
        creds, host = netloc.split("@", 1)
        if ":" in creds:
            user, _ = creds.split(":", 1)
        else:
            user = creds
        masked = f"{user}@{host}"
    else:
        masked = netloc
    return parsed._replace(netloc=masked).geturl()


def render_report(rows: Tuple[Tuple[str, str, str, str, str, int, int], ...], database_name: str, dsn: str) -> str:
    if not rows:
        return "No tables found."

    header = (
        "Table",
        "Total Size",
        "Heap",
        "Indexes",
        "TOAST",
        "Live Rows",
        "Dead Rows",
    )
    widths = [len(h) for h in header]

    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))

    def format_row(values):
        return "  ".join(str(value).ljust(widths[idx]) for idx, value in enumerate(values))

    lines = [
        f"Database: {database_name}",
        f"DSN: {_mask_dsn(dsn)}",
        format_row(header),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(format_row(row) for row in rows)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="List Postgres tables by storage usage")
    parser.add_argument("--limit", type=int, default=25, help="Maximum number of tables to display (default: 25)")
    parser.add_argument("--dsn", default=None, help="Optional Postgres DSN override")
    args = parser.parse_args(argv)

    load_env()
    rows, db_name, dsn = fetch_table_usage(max(1, args.limit), args.dsn)
    report = render_report(rows, db_name, dsn)
    print(report)
    try:
        OUTPUT_PATH.write_text(report + "\n", encoding="utf-8")
        print(f"\nWrote report to {OUTPUT_PATH}")
    except OSError as exc:
        print(f"Failed to write report to {OUTPUT_PATH}: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Table usage report failed: {exc}", file=sys.stderr)
        sys.exit(1)
