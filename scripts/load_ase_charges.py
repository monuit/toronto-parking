"""Download and load Automated Speed Enforcement (ASE) charge totals into Postgres.

This script fetches the ASE charges Excel package from Toronto's CKAN API, parses
monthly ticket counts per site, and updates the ``ase_camera_locations`` table so the
map UI can surface real ticket counts for each camera.

Usage:
    python scripts/load_ase_charges.py [--database-url postgresql://...]

The script automatically loads ``.env`` if no database URL is present in the
environment, matching the behaviour of the other preprocessing utilities.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable

import psycopg
import requests
import dotenv
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.etl.datasets.ase_locations import _format_pg_array, _load_charges_summary  # noqa: E402


BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
PACKAGE_ID = "automated-speed-enforcement-ase-charges"


@dataclass
class DownloadedResource:
    resource_id: str
    filename: str
    content: bytes


def _load_env_if_needed() -> None:
    if os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL") or os.getenv("POSTGIS_DATABASE_URL"):
        return
    repo_root = Path(__file__).resolve().parents[1]
    dotenv.load_dotenv(repo_root / ".env")


def _resolve_dsn(cli_dsn: str | None) -> str:
    if cli_dsn:
        return cli_dsn
    _load_env_if_needed()
    for key in ("DATABASE_URL", "POSTGRES_URL", "POSTGIS_DATABASE_URL"):
        value = os.getenv(key)
        if value:
            return value
    raise RuntimeError(
        "Database URL is required via --database-url or one of DATABASE_URL/POSTGRES_URL/POSTGIS_DATABASE_URL"
    )


def _download_ase_charges() -> DownloadedResource:
    package_url = f"{BASE_URL}/api/3/action/package_show"
    package = requests.get(package_url, params={"id": PACKAGE_ID}, timeout=60)
    package.raise_for_status()
    payload = package.json()
    if not payload.get("success"):
        raise RuntimeError(f"Failed to fetch package metadata: {payload}")

    resources = payload["result"].get("resources", [])
    chosen = None
    for resource in resources:
        if (resource.get("format") or "").lower() == "xlsx":
            chosen = resource
            break
    if not chosen:
        raise RuntimeError("Unable to locate XLSX resource in ASE charges package metadata")

    resource_id = chosen["id"]
    if chosen.get("datastore_active"):
        download_url = chosen.get("url")
    else:
        resource_meta_url = f"{BASE_URL}/api/3/action/resource_show"
        resource_resp = requests.get(resource_meta_url, params={"id": resource_id}, timeout=60)
        resource_resp.raise_for_status()
        resource_payload = resource_resp.json()
        if not resource_payload.get("success"):
            raise RuntimeError(f"Failed to fetch resource metadata: {resource_payload}")
        download_url = resource_payload["result"].get("url")

    if not download_url:
        raise RuntimeError(f"CKAN resource {resource_id} does not expose a download URL")

    download = requests.get(download_url, timeout=120)
    download.raise_for_status()
    filename = Path(download_url).name or f"{resource_id}.xlsx"
    return DownloadedResource(resource_id=resource_id, filename=filename, content=download.content)


def _persist_resource(resource: DownloadedResource, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / resource.filename
    target.write_bytes(resource.content)
    return target


def _prepare_records(summary: Dict[str, Dict[str, object]]) -> Iterable[tuple]:
    for location_code, metrics in summary.items():
        monthly_counts = metrics.get("monthly_counts") or {}
        yield (
            location_code,
            int(metrics.get("ticket_count", 0) or 0),
            str(metrics.get("total_fine_amount")) if metrics.get("total_fine_amount") else None,
            _format_pg_array(metrics.get("years", [])),
            _format_pg_array(metrics.get("months", [])),
            json.dumps(monthly_counts) if monthly_counts else None,
        )


def _update_database(dsn: str, records: Iterable[tuple]) -> dict[str, int]:
    with psycopg.connect(dsn) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ase_camera_locations (
                location_code TEXT PRIMARY KEY,
                ward TEXT,
                status TEXT,
                location TEXT,
                ticket_count INTEGER DEFAULT 0,
                total_fine_amount NUMERIC(18, 2),
                years INTEGER[],
                months INTEGER[],
                monthly_counts JSONB,
                geom geometry(POINT, 4326)
            )
            """
        )

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_ase_charge_totals (
                    location_code TEXT,
                    ticket_count INTEGER,
                    total_fine_amount TEXT,
                    years TEXT,
                    months TEXT,
                    monthly_counts_json TEXT
                ) ON COMMIT DROP
            """)
            with cur.copy("COPY tmp_ase_charge_totals FROM STDIN WITH (FORMAT text)") as copy:
                for row in records:
                    copy.write_row(row)
            cur.execute(
                """
                UPDATE ase_camera_locations AS target
                SET
                    ticket_count = COALESCE(src.ticket_count, target.ticket_count),
                    total_fine_amount = COALESCE(NULLIF(src.total_fine_amount, '')::NUMERIC, target.total_fine_amount),
                    years = CASE
                        WHEN src.years IS NULL OR src.years = '' THEN target.years
                        ELSE src.years::INT[]
                    END,
                    months = CASE
                        WHEN src.months IS NULL OR src.months = '' THEN target.months
                        ELSE src.months::INT[]
                    END,
                    monthly_counts = CASE
                        WHEN src.monthly_counts_json IS NULL OR src.monthly_counts_json = '' THEN target.monthly_counts
                        ELSE src.monthly_counts_json::JSONB
                    END
                FROM tmp_ase_charge_totals AS src
                WHERE src.location_code = target.location_code
            """
            )
            cur.execute(
                """
                SELECT
                    COUNT(*) AS location_count,
                    COUNT(*) FILTER (WHERE ticket_count > 0) AS with_tickets,
                    COALESCE(SUM(ticket_count), 0) AS total_tickets
                FROM ase_camera_locations
            """
            )
            result = cur.fetchone()
            conn.commit()
            return {
                "locations": int(result[0]),
                "locations_with_tickets": int(result[1]),
                "ticket_total": int(result[2]),
            }


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ASE charge totals into Postgres")
    parser.add_argument("--database-url", dest="database_url", help="PostgreSQL DSN", default=None)
    parser.add_argument("--output-dir", dest="output_dir", help="Directory to persist downloaded Excel", default=str(Path("output") / "etl" / "raw" / "ase_locations"))
    args = parser.parse_args()

    dsn = _resolve_dsn(args.database_url)
    resource = _download_ase_charges()
    output_dir = Path(args.output_dir)
    persisted_path = _persist_resource(resource, output_dir)
    summary = _load_charges_summary(persisted_path)
    if not summary:
        raise RuntimeError("Parsed ASE charges Excel but found no rows")

    stats = _update_database(dsn, _prepare_records(summary))
    print(
        "Updated ASE camera locations with charges: "
        f"{stats['locations_with_tickets']} of {stats['locations']} locations now have totals "
        f"({stats['ticket_total']} tickets)."
    )


if __name__ == "__main__":
    main()
