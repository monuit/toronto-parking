"""PostgreSQL/PostGIS utilities for the ETL pipeline."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable, Iterator, Sequence

import psycopg


@dataclass
class PostgresClient:
    """Thin wrapper around psycopg connections with sensible defaults."""

    dsn: str
    application_name: str = "toronto-parking-etl"
    connect_timeout: int = 10
    statement_timeout_ms: int | None = 600_000

    @contextmanager
    def connect(self, *, autocommit: bool = False) -> Iterator[psycopg.Connection]:
        with psycopg.connect(
            self.dsn,
            autocommit=autocommit,
            connect_timeout=self.connect_timeout,
            application_name=self.application_name,
        ) as conn:
            if self.statement_timeout_ms is not None:
                conn.execute(f"SET statement_timeout = {int(self.statement_timeout_ms)}")
            yield conn

    def execute(self, sql: str, params: Sequence[object] | None = None) -> int | None:
        with self.connect(autocommit=True) as conn:
            cursor = conn.execute(sql, params or ())
            try:
                return cursor.rowcount
            except AttributeError:  # pragma: no cover - psycopg < 3 compatibility path
                return None

    def fetch_one(self, sql: str, params: Sequence[object] | None = None) -> tuple | None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return cur.fetchone()

    def fetch_all(self, sql: str, params: Sequence[object] | None = None) -> list[tuple]:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                return cur.fetchall()

    def copy_rows(self, table: str, columns: Sequence[str], rows: Iterable[Sequence[object]]) -> int:
        """Bulk load ``rows`` into ``table`` using ``COPY``."""

        column_list = ",".join(columns)
        copy_sql = f"COPY {table} ({column_list}) FROM STDIN WITH (FORMAT text)"
        row_count = 0
        with self.connect(autocommit=True) as conn:
            with conn.cursor().copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(row)
                    row_count += 1
        return row_count

    def ensure_extensions(self) -> None:
        self.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        self.execute("CREATE EXTENSION IF NOT EXISTS postgis_raster")


__all__ = ["PostgresClient"]
