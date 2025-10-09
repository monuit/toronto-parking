"""Generate yearly aggregate tables for parking tickets and camera datasets."""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional, Tuple
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from src.etl.datasets.ase_locations import _load_charges_summary as load_ase_charges_summary

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None  # type: ignore


DATA_DIR = PROJECT_ROOT / "map-app" / "public" / "data"
AGGREGATED_PATH = DATA_DIR / "tickets_aggregated.geojson"


def _load_dotenv() -> None:
    if load_dotenv is None:
        return
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        load_dotenv(env_path)


def _round_currency(value: Decimal | float | int) -> Decimal:
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _decimal_from(value: Optional[str | float | int | Decimal]) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _normalize_address(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.lower() == "nan":
        return None
    return text.upper()


STREET_DIRECTION_RE = __import__("re").compile(r"\b(NB|SB|EB|WB)\b")


def _normalize_street_label(location: Optional[str]) -> Optional[str]:
    if not location:
        return None
    text = str(location).upper()
    text = STREET_DIRECTION_RE.sub("", text)
    text = " ".join(text.split())
    text = text.lstrip("0123456789- ")
    return text or None


def _normalise_ward_name(value: Optional[str]) -> str:
    if value is None:
        return "Unknown"
    text = str(value).strip()
    if not text:
        return "Unknown"
    try:
        number = float(text)
        if number.is_integer():
            return f"Ward {int(number)}"
    except ValueError:
        pass
    return text


@dataclass
class LocationMeta:
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    neighbourhood: Optional[str] = None


def load_location_lookup(path: Path) -> Dict[str, LocationMeta]:
    if not path.exists():
        raise FileNotFoundError(f"Aggregated GeoJSON not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    lookup: Dict[str, LocationMeta] = {}
    features: Iterable[dict] = payload.get("features", [])

    for feature in features:
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates")
        properties = feature.get("properties") or {}
        location = properties.get("location")
        normalized = _normalize_address(location)
        if not normalized:
            continue

        latitude = None
        longitude = None
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            longitude = float(coords[0]) if coords[0] is not None else None
            latitude = float(coords[1]) if coords[1] is not None else None

        lookup[normalized] = LocationMeta(
            latitude=latitude,
            longitude=longitude,
            neighbourhood=properties.get("neighbourhood"),
        )

    return lookup


def stream_parking_location_years(conn: psycopg.Connection) -> Iterator[dict]:
    query = sql.SQL(
        """
        WITH normalized AS (
            SELECT
                DATE_PART('year', date_of_infraction)::INT AS year,
                NULLIF(UPPER(TRIM(location2)), '') AS location,
                infraction_code,
                set_fine_amount
            FROM parking_tickets
            WHERE location2 IS NOT NULL
                AND TRIM(location2) <> ''
                AND LOWER(TRIM(location2)) <> 'nan'
                AND date_of_infraction IS NOT NULL
        ),
        location_totals AS (
            SELECT
                year,
                location,
                COUNT(*)::BIGINT AS ticket_count,
                COALESCE(SUM(set_fine_amount), 0)::NUMERIC AS total_revenue
            FROM normalized
            GROUP BY year, location
        ),
        infraction_rank AS (
            SELECT
                year,
                location,
                infraction_code,
                COUNT(*) AS infraction_count,
                ROW_NUMBER() OVER (
                    PARTITION BY year, location
                    ORDER BY COUNT(*) DESC, infraction_code
                ) AS rn
            FROM normalized
            GROUP BY year, location, infraction_code
        )
        SELECT
            totals.year,
            totals.location,
            totals.ticket_count,
            totals.total_revenue,
            ranked.infraction_code AS top_infraction
        FROM location_totals AS totals
        LEFT JOIN infraction_rank AS ranked
            ON ranked.year = totals.year
           AND ranked.location = totals.location
           AND ranked.rn = 1
        ORDER BY totals.year, totals.location
        """
    )

    with conn.cursor(name="parking_yearly_locations", row_factory=dict_row) as cursor:
        cursor.itersize = 5_000
        cursor.execute(query)
        for row in cursor:
            yield row


def ensure_tables(conn: psycopg.Connection) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS parking_ticket_yearly_locations (
            location_key TEXT NOT NULL,
            location TEXT NOT NULL,
            year INTEGER NOT NULL,
            ticket_count BIGINT NOT NULL,
            total_revenue NUMERIC(18, 2) NOT NULL,
            top_infraction TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            neighbourhood TEXT,
            PRIMARY KEY (location_key, year)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS parking_ticket_yearly_streets (
            street_key TEXT NOT NULL,
            street TEXT NOT NULL,
            year INTEGER NOT NULL,
            ticket_count BIGINT NOT NULL,
            total_revenue NUMERIC(18, 2) NOT NULL,
            PRIMARY KEY (street_key, year)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS parking_ticket_yearly_neighbourhoods (
            neighbourhood_key TEXT NOT NULL,
            neighbourhood TEXT NOT NULL,
            year INTEGER NOT NULL,
            ticket_count BIGINT NOT NULL,
            total_revenue NUMERIC(18, 2) NOT NULL,
            PRIMARY KEY (neighbourhood_key, year)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS red_light_yearly_locations (
            location_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            ticket_count BIGINT NOT NULL,
            total_revenue NUMERIC(18, 2) NOT NULL,
            location_name TEXT,
            ward TEXT,
            police_division TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            PRIMARY KEY (location_code, year)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ase_yearly_locations (
            location_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            ticket_count BIGINT NOT NULL,
            total_revenue NUMERIC(18, 2) NOT NULL,
            location_name TEXT,
            ward TEXT,
            status TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            PRIMARY KEY (location_code, year)
        )
        """,
    ]

    with conn.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
        # Ensure schema upgrades for existing installations
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_locations ADD COLUMN IF NOT EXISTS location_key TEXT"
        )
        cursor.execute(
            "UPDATE parking_ticket_yearly_locations SET location_key = md5(location) WHERE location_key IS NULL"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_locations ALTER COLUMN location_key SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_locations DROP CONSTRAINT IF EXISTS parking_ticket_yearly_locations_pkey"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_locations ADD PRIMARY KEY (location_key, year)"
        )
        cursor.execute(
            "DROP INDEX IF EXISTS idx_parking_ticket_yearly_locations_location"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_parking_ticket_yearly_locations_key ON parking_ticket_yearly_locations(location_key)"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_streets ADD COLUMN IF NOT EXISTS street_key TEXT"
        )
        cursor.execute(
            "UPDATE parking_ticket_yearly_streets SET street_key = md5(street) WHERE street_key IS NULL"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_streets ALTER COLUMN street_key SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_streets DROP CONSTRAINT IF EXISTS parking_ticket_yearly_streets_pkey"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_streets ADD PRIMARY KEY (street_key, year)"
        )
        cursor.execute(
            "DROP INDEX IF EXISTS idx_parking_ticket_yearly_streets_street"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_parking_ticket_yearly_streets_key ON parking_ticket_yearly_streets(street_key)"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_neighbourhoods ADD COLUMN IF NOT EXISTS neighbourhood_key TEXT"
        )
        cursor.execute(
            "UPDATE parking_ticket_yearly_neighbourhoods SET neighbourhood_key = md5(neighbourhood) WHERE neighbourhood_key IS NULL"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_neighbourhoods ALTER COLUMN neighbourhood_key SET NOT NULL"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_neighbourhoods DROP CONSTRAINT IF EXISTS parking_ticket_yearly_neighbourhoods_pkey"
        )
        cursor.execute(
            "ALTER TABLE parking_ticket_yearly_neighbourhoods ADD PRIMARY KEY (neighbourhood_key, year)"
        )
        cursor.execute(
            "DROP INDEX IF EXISTS idx_parking_ticket_yearly_neighbourhoods_neighbourhood"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_parking_ticket_yearly_neighbourhoods_key ON parking_ticket_yearly_neighbourhoods(neighbourhood_key)"
        )
    conn.commit()


def upsert_rows(
    conn: psycopg.Connection,
    table: str,
    columns: Tuple[str, ...],
    rows: Iterable[Tuple],
) -> None:
    if not rows:
        return

    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("TRUNCATE {}".format(sql.Identifier(table).as_string(conn))))

    column_identifiers = [sql.Identifier(col) for col in columns]
    copy_columns = sql.SQL(", ").join(column_identifiers)
    copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(table),
        copy_columns,
    )

    with conn.cursor() as cursor:
        with cursor.copy(copy_stmt) as copy:
            for row in rows:
                copy.write_row(row)

    conn.commit()


def build_parking_tables(conn: psycopg.Connection, location_lookup: Dict[str, LocationMeta]) -> None:
    location_rows: list[Tuple] = []
    street_totals: Dict[Tuple[str, int], Dict[str, Decimal | int]] = defaultdict(
        lambda: {"ticket_count": 0, "total_revenue": Decimal("0")}
    )
    neighbourhood_totals: Dict[Tuple[str, int], Dict[str, Decimal | int]] = defaultdict(
        lambda: {"ticket_count": 0, "total_revenue": Decimal("0")}
    )

    for row in stream_parking_location_years(conn):
        location = _normalize_address(row["location"])
        if not location:
            continue

        location = location.replace("\t", " ").replace("\n", " ").strip()
        if not location:
            continue

        year = int(row["year"])
        ticket_count = int(row["ticket_count"])
        total_revenue = _round_currency(row["total_revenue"])
        top_infraction = row.get("top_infraction")

        meta = location_lookup.get(location, LocationMeta())
        neighbourhood = meta.neighbourhood or "Unknown"

        location_key = hashlib.md5(location.encode('utf-8', 'ignore')).hexdigest()

        location_rows.append(
            (
                location_key,
                location,
                year,
                ticket_count,
                total_revenue,
                top_infraction,
                meta.latitude,
                meta.longitude,
                neighbourhood,
            )
        )

        street_label = _normalize_street_label(location)
        if street_label:
            key = (street_label, year)
            street_totals[key]["ticket_count"] += ticket_count
            street_totals[key]["total_revenue"] += total_revenue

        hood_key = (neighbourhood or "Unknown", year)
        neighbourhood_totals[hood_key]["ticket_count"] += ticket_count
        neighbourhood_totals[hood_key]["total_revenue"] += total_revenue

    street_rows = []
    for (street, year), values in street_totals.items():
        if not street:
            continue
        street_clean = street.replace("\t", " ").replace("\n", " ").strip()
        if not street_clean:
            continue
        street_rows.append(
            (
                hashlib.md5(street_clean.encode('utf-8', 'ignore')).hexdigest(),
                street_clean,
                year,
                int(values["ticket_count"]),
                _round_currency(values["total_revenue"]),
            )
        )

    neighbourhood_rows = []
    for (neighbourhood, year), values in neighbourhood_totals.items():
        if not neighbourhood:
            continue
        neighbourhood_clean = neighbourhood.replace("\t", " ").replace("\n", " ").strip()
        if not neighbourhood_clean:
            continue
        neighbourhood_rows.append(
            (
                hashlib.md5(neighbourhood_clean.encode('utf-8', 'ignore')).hexdigest(),
                neighbourhood_clean,
                year,
                int(values["ticket_count"]),
                _round_currency(values["total_revenue"]),
            )
        )

    upsert_rows(
        conn,
        "parking_ticket_yearly_locations",
        (
            "location_key",
            "location",
            "year",
            "ticket_count",
            "total_revenue",
            "top_infraction",
            "latitude",
            "longitude",
            "neighbourhood",
        ),
        location_rows,
    )

    upsert_rows(
        conn,
        "parking_ticket_yearly_streets",
        ("street_key", "street", "year", "ticket_count", "total_revenue"),
        street_rows,
    )

    upsert_rows(
        conn,
        "parking_ticket_yearly_neighbourhoods",
        ("neighbourhood_key", "neighbourhood", "year", "ticket_count", "total_revenue"),
        neighbourhood_rows,
    )


def build_red_light_table(conn: psycopg.Connection) -> None:
    query = """
        SELECT
            location_code,
            location_name,
            ward_1,
            police_division_1,
            ticket_count,
            total_fine_amount,
            ST_Y(geom) AS latitude,
            ST_X(geom) AS longitude,
            yearly_counts
        FROM red_light_camera_locations
    """

    rows: list[Tuple] = []
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query)
        for record in cursor:
            code = record.get("location_code")
            if not code:
                continue
            yearly_counts = record.get("yearly_counts") or {}
            if isinstance(yearly_counts, str):
                try:
                    yearly_counts = json.loads(yearly_counts)
                except json.JSONDecodeError:
                    yearly_counts = {}

            if not isinstance(yearly_counts, dict):
                continue

            total_tickets = sum(int(v) for v in yearly_counts.values()) or 0
            total_revenue = _decimal_from(record.get("total_fine_amount"))
            revenue_per_ticket = (
                (total_revenue / total_tickets) if total_tickets > 0 else Decimal("0")
            )

            for year_str, count in yearly_counts.items():
                try:
                    year = int(year_str)
                except (TypeError, ValueError):
                    continue
                ticket_count = int(count)
                if ticket_count <= 0:
                    continue
                revenue_value = revenue_per_ticket * Decimal(ticket_count)
                rows.append(
                    (
                        str(code),
                        year,
                        ticket_count,
                        _round_currency(revenue_value),
                        record.get("location_name"),
                        record.get("ward_1"),
                        record.get("police_division_1"),
                        float(record.get("latitude") or 0) if record.get("latitude") is not None else None,
                        float(record.get("longitude") or 0) if record.get("longitude") is not None else None,
                    )
                )

    upsert_rows(
        conn,
        "red_light_yearly_locations",
        (
            "location_code",
            "year",
            "ticket_count",
            "total_revenue",
            "location_name",
            "ward",
            "police_division",
            "latitude",
            "longitude",
        ),
        rows,
    )


def _locate_latest_ase_charges() -> Path | None:
    raw_root = PROJECT_ROOT / "output" / "etl" / "raw" / "ase_locations"
    if not raw_root.exists():
        return None
    candidates = sorted(raw_root.glob("*.xlsx"), key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _load_monthly_counts_from_record(payload: Optional[dict | str]) -> Dict[str, int]:
    if not payload:
        return {}
    if isinstance(payload, dict):
        result: Dict[str, int] = {}
        for key, value in payload.items():
            if value is None:
                continue
            try:
                result[str(key)] = int(value)
            except (TypeError, ValueError):
                continue
        return result
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return _load_monthly_counts_from_record(parsed)
    return {}


def _aggregate_year_totals(monthly_counts: Dict[str, int]) -> Dict[int, int]:
    year_totals: Dict[int, int] = defaultdict(int)
    for key, value in monthly_counts.items():
        if not value:
            continue
        try:
            year = int(str(key)[:4])
        except (TypeError, ValueError):
            continue
        year_totals[year] += int(value)
    return year_totals


def build_ase_table(conn: psycopg.Connection) -> None:
    charges_path = _locate_latest_ase_charges()
    charges_lookup = load_ase_charges_summary(charges_path) if charges_path else {}

    db_rows: Dict[str, dict] = {}
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT
                location_code,
                location,
                ward,
                status,
                ticket_count,
                total_fine_amount,
                months,
                monthly_counts,
                ST_Y(geom) AS latitude,
                ST_X(geom) AS longitude
            FROM ase_camera_locations
            """,
        )
        for record in cursor.fetchall():
            code = record.get("location_code")
            if not code:
                continue
            db_rows[str(code)] = record

    ase_fine_estimate = Decimal(os.getenv("ASE_FINE_AVG", "50"))
    rows: list[Tuple] = []
    processed_codes: set[str] = set()

    def append_rows_for_code(code: str, base_record: Optional[dict], metrics: Optional[dict]) -> None:
        monthly_counts = _load_monthly_counts_from_record(
            (metrics or {}).get("monthly_counts") or (base_record or {}).get("monthly_counts")
        )

        year_totals = _aggregate_year_totals(monthly_counts)

        if not year_totals:
            total_tickets = int((metrics or {}).get("ticket_count") or (base_record or {}).get("ticket_count") or 0)
            if total_tickets > 0:
                months = (base_record or {}).get("months") or (metrics or {}).get("months")
                activation_year = None
                if months:
                    if isinstance(months, str):
                        tokens = [token for token in months.strip("{} ").split(",") if token]
                    else:
                        tokens = list(months)
                    if tokens:
                        try:
                            activation_year = int(str(tokens[0])[:4])
                        except (TypeError, ValueError):
                            activation_year = None
                target_year = activation_year or datetime.now(timezone.utc).year
                year_totals[target_year] = total_tickets

        if not year_totals:
            return

        total_tickets = sum(year_totals.values())
        revenue_total = _decimal_from((metrics or {}).get("total_fine_amount"))
        if revenue_total == 0 and base_record:
            revenue_total = _decimal_from(base_record.get("total_fine_amount"))

        revenue_per_ticket = (
            (revenue_total / total_tickets) if total_tickets > 0 and revenue_total > 0 else ase_fine_estimate
        )

        name = (metrics or {}).get("location") or (base_record or {}).get("location")
        ward_raw = (metrics or {}).get("ward") or (base_record or {}).get("ward")
        ward = _normalise_ward_name(ward_raw)
        status = (base_record or {}).get("status")
        latitude = None
        longitude = None
        if base_record:
            latitude = float(base_record.get("latitude")) if base_record.get("latitude") is not None else None
            longitude = float(base_record.get("longitude")) if base_record.get("longitude") is not None else None

        for year, ticket_count in year_totals.items():
            if ticket_count <= 0:
                continue
            proportional_revenue = revenue_per_ticket * Decimal(ticket_count)
            rows.append(
                (
                    str(code),
                    int(year),
                    int(ticket_count),
                    _round_currency(proportional_revenue),
                    name,
                    ward,
                    status,
                    latitude,
                    longitude,
                )
            )

    for code, metrics in charges_lookup.items():
        base_record = db_rows.get(code)
        append_rows_for_code(code, base_record, metrics)
        processed_codes.add(code)

    for code, base_record in db_rows.items():
        if code in processed_codes:
            continue
        append_rows_for_code(code, base_record, None)

    upsert_rows(
        conn,
        "ase_yearly_locations",
        (
            "location_code",
            "year",
            "ticket_count",
            "total_revenue",
            "location_name",
            "ward",
            "status",
            "latitude",
            "longitude",
        ),
        rows,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build yearly aggregates for datasets")
    parser.add_argument(
        "--database-url",
        dest="database_url",
        default=os.getenv("DATABASE_URL"),
        help="Postgres connection string (defaults to DATABASE_URL)",
    )
    parser.add_argument(
        "--aggregated",
        type=Path,
        default=AGGREGATED_PATH,
        help="Path to tickets_aggregated.geojson used for metadata",
    )
    return parser.parse_args()


def main() -> None:
    _load_dotenv()
    args = parse_args()

    if not args.database_url:
        raise RuntimeError("DATABASE_URL not provided")

    location_lookup = load_location_lookup(args.aggregated)

    with psycopg.connect(args.database_url) as conn:
        ensure_tables(conn)
        build_parking_tables(conn, location_lookup)
        build_red_light_table(conn)
        build_ase_table(conn)


if __name__ == "__main__":
    main()
