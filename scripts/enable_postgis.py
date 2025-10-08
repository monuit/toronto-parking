"""Enable PostGIS extensions on the configured PostgreSQL database.

Loads credentials from the project `.env` file (falling back to environment
variables), connects via psycopg, and runs the necessary `CREATE EXTENSION`
statements. Intended for one-off/manual execution when provisioning a new
database.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import tuple_row


def _normalise_scheme(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


def _load_database_url() -> str:
    project_root = Path(__file__).resolve().parents[1]
    env_path = project_root / ".env"
    load_dotenv(env_path, override=False)

    candidates = [
        os.getenv("POSTGIS_DATABASE_URL"),
        os.getenv("DATABASE_URL"),
        os.getenv("DATABASE_PUBLIC_URL"),
        os.getenv("DATABASE_PRIVATE_URL"),
    ]

    for candidate in candidates:
        if candidate:
            return _normalise_scheme(candidate)

    raise RuntimeError(
        "Set DATABASE_URL (or POSTGIS_DATABASE_URL / DATABASE_PUBLIC_URL / DATABASE_PRIVATE_URL) before running this script."
    )


def enable_postgis(dsn: str) -> None:
    with connect(dsn, autocommit=True, row_factory=tuple_row) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cur.execute("CREATE EXTENSION IF NOT EXISTS postgis_raster")
            cur.execute("SELECT PostGIS_Version()")
            version = cur.fetchone()[0]
    print(f"PostGIS is enabled (version: {version})")


def main() -> int:
    try:
        dsn = _load_database_url()
        enable_postgis(dsn)
    except Exception as exc:  # noqa: BLE001 - surface errors for operator
        print(f"Failed to enable PostGIS: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
