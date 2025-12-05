#!/usr/bin/env python3
"""
Fix the remaining two tables with data type issues.
"""

import csv
import sys
import time
from pathlib import Path

import psycopg

# Connection string for Railway PostGIS
POSTGIS_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"
EXPORT_DIR = Path(r"F:\Coding\toronto-parking\data_export")


# Fixed DDLs with TEXT types for mixed data columns
TABLE_DDLS = {
    "parking_tickets": """
        CREATE TABLE IF NOT EXISTS public.parking_tickets (
            ticket_hash TEXT PRIMARY KEY,
            ticket_number TEXT,
            date_of_infraction DATE,
            time_of_infraction TIME,
            infraction_code INTEGER,
            infraction_description TEXT,
            set_fine_amount NUMERIC(10,2),
            location1 TEXT,
            location2 TEXT,
            location3 TEXT,
            location4 TEXT,
            street_normalized TEXT,
            centreline_id BIGINT,
            geom GEOMETRY(POINT, 4326),
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            geom_3857 GEOMETRY(POINT, 3857),
            tile_qk_prefix TEXT
        )
    """,

    "red_light_camera_locations": """
        CREATE TABLE IF NOT EXISTS public.red_light_camera_locations (
            intersection_id TEXT,
            location_code TEXT PRIMARY KEY,
            linear_name_full_1 TEXT,
            linear_name_full_2 TEXT,
            location_name TEXT,
            ward_1 TEXT,
            police_division_1 TEXT,
            activation_date DATE,
            ticket_count INTEGER DEFAULT 0,
            total_fine_amount NUMERIC(12,2) DEFAULT 0,
            years INTEGER[],
            months TEXT[],
            yearly_counts JSONB,
            geom GEOMETRY(POINT, 4326),
            geom_3857 GEOMETRY(POINT, 3857),
            tile_qk_prefix TEXT
        )
    """
}


def get_connection():
    """Get a fresh database connection."""
    return psycopg.connect(POSTGIS_URL, connect_timeout=30)


def execute_with_retry(sql: str, max_retries: int = 3) -> bool:
    """Execute SQL with connection retry logic."""
    for attempt in range(max_retries):
        try:
            conn = get_connection()
            conn.execute(sql)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"    Attempt {attempt + 1}/{max_retries}: {e}")
            try:
                conn.close()
            except:
                pass
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    return False


def get_csv_row_count(csv_file: Path) -> int:
    """Count rows in CSV file (excluding header)."""
    with open(csv_file, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def get_table_row_count(conn, table: str) -> int:
    """Get row count from table."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM public.{table}')
        return cur.fetchone()[0]


def import_table_with_retry(table: str, csv_file: Path, max_retries: int = 3) -> bool:
    """Import a table with connection retry logic."""
    if not csv_file.exists():
        print(f"    ⚠ CSV file not found")
        return False

    csv_rows = get_csv_row_count(csv_file)
    if csv_rows == 0:
        print(f"    ⚠ CSV empty, skipping")
        return True

    for attempt in range(max_retries):
        try:
            conn = get_connection()

            print(f"    Importing {csv_rows:,} rows...")

            # Read header to get columns
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                columns = next(reader)

            columns_str = ", ".join([f'"{c}"' for c in columns])
            query = f'COPY public.{table} ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'

            with conn.cursor() as cur:
                with open(csv_file, "rb") as f:
                    with cur.copy(query) as copy:
                        while data := f.read(65536):
                            copy.write(data)

            conn.commit()
            imported = get_table_row_count(conn, table)
            print(f"    ✓ Imported {imported:,} rows")
            conn.close()
            return True

        except Exception as e:
            print(f"    ✗ Attempt {attempt + 1}/{max_retries}: {e}")
            try:
                conn.close()
            except:
                pass
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)

    return False


def main():
    print("=" * 60)
    print("Railway PostGIS - Fix Remaining Tables")
    print("=" * 60)

    success = 0
    failed = 0

    for table, ddl in TABLE_DDLS.items():
        print(f"\n[{table}]")

        # Drop existing table
        print(f"  Dropping table...")
        if not execute_with_retry(f"DROP TABLE IF EXISTS public.{table} CASCADE"):
            print(f"  ✗ Failed to drop")
            failed += 1
            continue
        print(f"  ✓ Dropped")

        # Create with correct schema
        print(f"  Creating table...")
        if not execute_with_retry(ddl):
            print(f"  ✗ Failed to create")
            failed += 1
            continue
        print(f"  ✓ Created")

        # Import data
        csv_file = EXPORT_DIR / "public" / f"{table}.csv"
        if import_table_with_retry(table, csv_file):
            success += 1
        else:
            failed += 1

    print("\n" + "=" * 60)
    print("Fix Complete!")
    print(f"  ✓ Success: {success} tables")
    if failed > 0:
        print(f"  ✗ Failed: {failed} tables")
    print("=" * 60)


if __name__ == "__main__":
    main()
