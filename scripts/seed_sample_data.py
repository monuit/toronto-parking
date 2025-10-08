"""Seed minimal sample data into PostGIS tables for local UI testing."""

from __future__ import annotations

import os
from pathlib import Path
from psycopg.types.json import Json

from dotenv import load_dotenv
from psycopg import connect


SAMPLE_TICKETS = (
    (
        "TEST-001",
        "2024-09-01",
        "09:15",
        "101",
        "No Parking",
        60,
        "123 Queen St W",
        None,
        None,
        None,
        "QUEEN ST W",
        None,
        -79.3832,
        43.6535,
    ),
    (
        "TEST-002",
        "2024-09-02",
        "14:30",
        "365",
        "Expired Meter",
        30,
        "200 King St W",
        None,
        None,
        None,
        "KING ST W",
        None,
        -79.3837,
        43.6466,
    ),
    (
        "TEST-003",
        "2024-09-03",
        "18:45",
        "231",
        "Rush Hour Parking",
        100,
        "50 Bloor St W",
        None,
        None,
        None,
        "BLOOR ST W",
        None,
        -79.3889,
        43.6708,
    ),
)

SAMPLE_RED_LIGHT_LOCATIONS = (
    (
        "INT-001",
        "QUEEN ST W",
        "SPADINA AVE",
        "Queen St W & Spadina Ave",
        "Ward 10",
        "14 Division",
        "2015-06-01",
        2750,
        893750,
        [2023, 2024],
        [],
        {"2023": 1400, "2024": 1350},
        "POINT(-79.394 43.6501)",
    ),
)

SAMPLE_ASE_LOCATIONS = (
    (
        "ASE-001",
        "Ward 4",
        "Active",
        "Dufferin St near Lawrence Ave W",
        480,
        58000,
        [2023, 2024],
        [7, 8, 9],
        {
            "2023-07": 180,
            "2023-08": 160,
            "2024-07": 140,
        },
        "POINT(-79.452 43.708)",
    ),
)


def _dsn() -> str:
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env", override=False)
    url = (
        os.getenv("POSTGIS_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("DATABASE_PUBLIC_URL")
        or os.getenv("DATABASE_PRIVATE_URL")
    )
    if not url:
        raise RuntimeError("Database URL missing; check your .env")
    return url.replace("postgres://", "postgresql://", 1)


def _seed_parking_tickets(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
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
        cur.execute("TRUNCATE parking_tickets")
        cur.executemany(
            """
            INSERT INTO parking_tickets (
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
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            )
            ON CONFLICT (ticket_number) DO NOTHING
            """,
            SAMPLE_TICKETS,
        )


def _seed_red_light(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS red_light_camera_locations (
                intersection_id TEXT PRIMARY KEY,
                linear_name_full_1 TEXT,
                linear_name_full_2 TEXT,
                location_name TEXT,
                ward_1 TEXT,
                police_division_1 TEXT,
                activation_date DATE,
                ticket_count INTEGER DEFAULT 0,
                total_fine_amount NUMERIC(18, 2),
                years INTEGER[],
                months INTEGER[],
                yearly_counts JSONB,
                geom geometry(POINT, 4326)
            )
            """
        )
        cur.execute("TRUNCATE red_light_camera_locations")
        cur.executemany(
            """
            INSERT INTO red_light_camera_locations (
                intersection_id,
                linear_name_full_1,
                linear_name_full_2,
                location_name,
                ward_1,
                police_division_1,
                activation_date,
                ticket_count,
                total_fine_amount,
                years,
                months,
                yearly_counts,
                geom
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                ST_SetSRID(ST_GeomFromText(%s), 4326)
            )
            ON CONFLICT (intersection_id) DO NOTHING
            """,
            [
                (
                    intersection_id,
                    line1,
                    line2,
                    location_name,
                    ward,
                    division,
                    activation,
                    ticket_count,
                    total_fine,
                    years,
                    months,
                    Json(yearly_counts),
                    geom_wkt,
                )
                for (
                    intersection_id,
                    line1,
                    line2,
                    location_name,
                    ward,
                    division,
                    activation,
                    ticket_count,
                    total_fine,
                    years,
                    months,
                    yearly_counts,
                    geom_wkt,
                ) in SAMPLE_RED_LIGHT_LOCATIONS
            ],
        )


def _seed_ase(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
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
        cur.execute("TRUNCATE ase_camera_locations")
        cur.executemany(
            """
            INSERT INTO ase_camera_locations (
                location_code,
                ward,
                status,
                location,
                ticket_count,
                total_fine_amount,
                years,
                months,
                monthly_counts,
                geom
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                ST_SetSRID(ST_GeomFromText(%s), 4326)
            )
            ON CONFLICT (location_code) DO NOTHING
            """,
            [
                (
                    code,
                    ward,
                    status,
                    location,
                    ticket_count,
                    total_fine,
                    years,
                    months,
                    Json(monthly_counts),
                    geom_wkt,
                )
                for (
                    code,
                    ward,
                    status,
                    location,
                    ticket_count,
                    total_fine,
                    years,
                    months,
                    monthly_counts,
                    geom_wkt,
                ) in SAMPLE_ASE_LOCATIONS
            ],
        )


def main() -> int:
    dsn = _dsn()
    with connect(dsn, autocommit=True) as conn:
        _seed_parking_tickets(conn)
        _seed_red_light(conn)
        _seed_ase(conn)
    print("Seeded sample data for parking, red light, and ASE datasets")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
