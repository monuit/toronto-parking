"""Load pre-downloaded parking ticket CSVs into PostGIS using cached geocodes."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import zipfile
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.etl.datasets.parking_tickets import (  # noqa: E402
    STAGING_COLUMNS,
    _normalise_date,
    _normalise_time,
    _safe_decimal,
)
from src.etl.postgres import PostgresClient  # noqa: E402


GeocodeRecord = Tuple[Optional[str], Optional[int], Optional[float], Optional[float]]


def load_geocode_cache(path: Path) -> Dict[str, GeocodeRecord]:
    if not path.exists():
        raise FileNotFoundError(f"Geocode cache not found: {path}")
    cache: Dict[str, GeocodeRecord] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            data = json.loads(line)
            key = data.get("address")
            if not key:
                continue
            cache[key.upper()] = (
                data.get("street_normalized"),
                data.get("centreline_id"),
                data.get("latitude"),
                data.get("longitude"),
            )
    return cache


def ensure_tables(pg: PostgresClient) -> None:
    pg.execute(
        """
        CREATE TABLE IF NOT EXISTS parking_tickets (
            ticket_number TEXT PRIMARY KEY,
            date_of_infraction DATE,
            time_of_infraction TEXT,
            infraction_code TEXT,
            infraction_description TEXT,
            set_fine_amount NUMERIC,
            location1 TEXT,
            location2 TEXT,
            location3 TEXT,
            location4 TEXT,
            street_normalized TEXT,
            centreline_id BIGINT,
            geom geometry(POINT, 4326),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    pg.execute(
        """
        CREATE TABLE IF NOT EXISTS parking_tickets_staging (
            ticket_number TEXT,
            date_of_infraction TEXT,
            time_of_infraction TEXT,
            infraction_code TEXT,
            infraction_description TEXT,
            set_fine_amount TEXT,
            location1 TEXT,
            location2 TEXT,
            location3 TEXT,
            location4 TEXT,
            street_normalized TEXT,
            centreline_id BIGINT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        )
        """
    )


def load_batch(pg: PostgresClient, rows: Iterable[Tuple]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    pg.execute("TRUNCATE parking_tickets_staging")
    pg.copy_rows("parking_tickets_staging", STAGING_COLUMNS, rows_list)
    pg.execute(
        """
        INSERT INTO parking_tickets AS target (
            ticket_number,
            date_of_infraction,
            time_of_infraction,
            infraction_code,
            infraction_description,
            set_fine_amount,
            location1,
            location2,
            location3,
            location4,
            street_normalized,
            centreline_id,
            geom
        )
        SELECT DISTINCT ON (ticket_number)
            ticket_number,
            NULLIF(date_of_infraction, '')::DATE,
            time_of_infraction,
            infraction_code,
            infraction_description,
            NULLIF(set_fine_amount, '')::NUMERIC,
            location1,
            location2,
            location3,
            location4,
            street_normalized,
            centreline_id,
            CASE
                WHEN longitude IS NULL OR latitude IS NULL THEN NULL
                ELSE ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            END
        FROM parking_tickets_staging
        ORDER BY ticket_number, date_of_infraction DESC
        ON CONFLICT (ticket_number) DO UPDATE SET
            date_of_infraction = EXCLUDED.date_of_infraction,
            time_of_infraction = EXCLUDED.time_of_infraction,
            infraction_code = EXCLUDED.infraction_code,
            infraction_description = EXCLUDED.infraction_description,
            set_fine_amount = EXCLUDED.set_fine_amount,
            location1 = EXCLUDED.location1,
            location2 = EXCLUDED.location2,
            location3 = EXCLUDED.location3,
            location4 = EXCLUDED.location4,
            street_normalized = COALESCE(EXCLUDED.street_normalized, target.street_normalized),
            centreline_id = COALESCE(EXCLUDED.centreline_id, target.centreline_id),
            geom = COALESCE(EXCLUDED.geom, target.geom),
            updated_at = NOW()
        """
    )


def _prepare_ticket_row(record: Dict[str, Optional[str]], geocodes: Dict[str, GeocodeRecord]) -> Optional[Tuple]:
    ticket_number = (record.get("tag_number_masked") or record.get("ticket_number") or "").strip()
    if not ticket_number:
        return None
    parsed_date = _normalise_date(record.get("date_of_infraction") or record.get("dateofinfraction"))
    if not parsed_date:
        return None
    time_value = _normalise_time(record.get("time_of_infraction") or record.get("timeofinfraction"))

    location1 = (record.get("location1") or "").strip()
    location2 = (record.get("location2") or "").strip()
    location3 = (record.get("location3") or "").strip()
    location4 = (record.get("location4") or "").strip()

    key = None
    if location2:
        main = location2.upper()
        if location4:
            key = f"{main} AND {location4.upper()}"
        else:
            key = main

    street_normalized: Optional[str] = None
    centreline_id: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    if key is not None:
        match = geocodes.get(key)
        if match:
            street_normalized, centreline_id, latitude, longitude = match

    return (
        ticket_number,
        parsed_date,
        time_value,
        (record.get("infraction_code") or record.get("infractioncode") or "").strip() or None,
        (record.get("infraction_description") or record.get("infractiondescription") or "").strip() or None,
        _safe_decimal(record.get("set_fine_amount") or record.get("setfineamount")),
        location1 or None,
        location2 or None,
        location3 or None,
        location4 or None,
        street_normalized,
        centreline_id,
        latitude,
        longitude,
    )


def iter_ticket_rows(csv_path: Path, geocodes: Dict[str, GeocodeRecord]) -> Iterator[Tuple]:
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for record in reader:
            prepared = _prepare_ticket_row(record, geocodes)
            if prepared is not None:
                yield prepared


def iter_ticket_rows_from_zip(zip_path: Path, geocodes: Dict[str, GeocodeRecord]) -> Iterator[Tuple]:
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
    with zipfile.ZipFile(zip_path) as archive:
        members = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
        if not members:
            raise FileNotFoundError(f"No CSV members found in archive {zip_path}")
        for member in members:
            print(f"  -> {member}")
            with archive.open(member) as handle:
                reader = csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8", errors="ignore"))
                for record in reader:
                    prepared = _prepare_ticket_row(record, geocodes)
                    if prepared is not None:
                        yield prepared


def load_year(pg: PostgresClient, csv_paths: List[Path], year: int, geocodes: Dict[str, GeocodeRecord]) -> int:
    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    pg.execute(
        "DELETE FROM parking_tickets WHERE date_of_infraction >= %s AND date_of_infraction < %s",
        (start.isoformat(), end.isoformat()),
    )

    written = 0
    for csv_path in csv_paths:
        print(f"Processing {csv_path} ...")
        if csv_path.suffix.lower() == ".zip":
            rows = list(iter_ticket_rows_from_zip(csv_path, geocodes))
        else:
            rows = list(iter_ticket_rows(csv_path, geocodes))
        if rows:
            load_batch(pg, rows)
            written += len(rows)
    return written


def find_csvs(base_dir: Path, year: int) -> List[Path]:
    year_dir = base_dir / str(year)
    if not year_dir.exists():
        raise FileNotFoundError(f"Year directory not found: {year_dir}")
    csv_like = sorted(
        path
        for path in year_dir.iterdir()
        if path.is_file() and _is_csv_like(path)
    )
    if csv_like:
        return csv_like
    zip_files = sorted(year_dir.glob("*.zip"))
    if zip_files:
        return zip_files
    raise FileNotFoundError(f"No data files found for year {year} in {year_dir}")


def _is_csv_like(path: Path) -> bool:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".txt"}:
        return True
    numeric_suffix = suffix.lstrip(".")
    if numeric_suffix.isdigit():
        return True
    return False


def parse_years(spec: str | None) -> List[int]:
    if not spec:
        return list(range(2008, 2025))
    result: Counter[int] = Counter()
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            start_str, end_str = token.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            if end < start:
                start, end = end, start
            for year in range(start, end + 1):
                result[year] += 1
        else:
            result[int(token)] += 1
    return sorted(result.keys())


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load parking ticket CSVs already on disk into PostGIS.")
    parser.add_argument("--data-root", default="parking_data/extracted", help="Root directory containing year folders with CSVs.")
    parser.add_argument("--geocode-cache", default="output/geocoded_addresses_combined.jsonl", help="JSONL file with cached geocode results.")
    parser.add_argument("--years", help="Comma-separated years or ranges (default 2008-2024).")
    args = parser.parse_args(argv)

    dotenv.load_dotenv(REPO_ROOT / ".env")

    data_root = (REPO_ROOT / args.data_root).resolve()
    geocode_path = (REPO_ROOT / args.geocode_cache).resolve()

    years = parse_years(args.years)
    geocodes = load_geocode_cache(geocode_path)
    print(f"Loaded {len(geocodes):,} geocoded addresses from {geocode_path}")

    dsn = os.getenv("POSTGIS_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("POSTGIS_DATABASE_URL or DATABASE_URL must be set in the environment")

    pg = PostgresClient(dsn=dsn)
    pg.ensure_extensions()
    ensure_tables(pg)

    total = 0
    completed_years: List[int] = []
    for year in years:
        csv_paths = find_csvs(data_root, year)
        print(f"\n=== Loading {year} ({len(csv_paths)} file(s)) ===")
        written = load_year(pg, csv_paths, year, geocodes)
        total += written
        print(f"Loaded {written:,} tickets for {year}")
        completed_years.append(year)

    print(f"\nCompleted. Total tickets loaded: {total:,}")
    if completed_years:
        years_str = ", ".join(str(year) for year in completed_years)
        print(f"Years processed: {years_str}")


if __name__ == "__main__":
    main()
