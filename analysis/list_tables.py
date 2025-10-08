from __future__ import annotations

import os
from dotenv import load_dotenv
import psycopg


def _resolve_dsn() -> str:
    load_dotenv()
    dsn = (
        os.environ.get("POSTGIS_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_URL")
    )
    if not dsn:
        raise RuntimeError(
            "POSTGIS_DATABASE_URL (or DATABASE_URL/POSTGRES_URL) must be set in the environment"
        )
    return dsn


def main() -> None:
    dsn = _resolve_dsn()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_name = 'centreline_segments'
                """
            )
            print(cur.fetchall())


if __name__ == "__main__":
    main()
