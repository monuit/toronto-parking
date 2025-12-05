#!/usr/bin/env python3
"""
Import data from local CSV exports to Railway PostGIS database.

This script reads the exported CSV files from data_export/ and imports them
into the new Railway PostGIS instance.

Usage:
    python scripts/railway_import_data.py
"""

import psycopg
import os
import sys
from pathlib import Path
import csv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# New Railway PostGIS connection
POSTGIS_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"

# Export directory
EXPORT_DIR = PROJECT_ROOT / "data_export"

# Tables to import in order (respecting foreign key dependencies)
# Skip tiger/topology schemas - they're PostGIS extension tables
IMPORT_ORDER = [
    # Core reference tables first
    ("public", "spatial_ref_sys"),
    ("public", "city_wards"),
    ("public", "centreline_segments"),
    ("public", "schools"),

    # ETL state
    ("public", "etl_state"),

    # Main data tables
    ("public", "parking_tickets"),
    ("public", "parking_ticket_yearly_locations"),
    ("public", "parking_ticket_yearly_streets"),
    ("public", "parking_ticket_yearly_neighbourhoods"),

    # ASE camera data
    ("public", "ase_camera_locations"),
    ("public", "ase_yearly_locations"),
    ("public", "ase_charges"),

    # Red light camera data
    ("public", "red_light_camera_locations"),
    ("public", "red_light_yearly_locations"),
    ("public", "red_light_charges"),

    # Schools with cameras
    ("public", "schools_with_nearby_cameras"),

    # Visualization data
    ("public", "glow_lines"),

    # Tile tables (partitioned - we'll import parent tables, not partitions)
    ("public", "parking_ticket_tiles"),
    ("public", "ase_camera_tiles"),
    ("public", "red_light_camera_tiles"),

    # Ward totals
    ("public", "camera_ward_totals"),

    # Cache tables (skip - will be regenerated)
    # ("public", "tile_blob_cache"),
]


def get_connection():
    """Create database connection."""
    return psycopg.connect(POSTGIS_URL)


def check_postgis_extension(conn):
    """Ensure PostGIS extension is enabled."""
    print("Checking PostGIS extension...")
    with conn.cursor() as cur:
        cur.execute("SELECT PostGIS_Version();")
        version = cur.fetchone()[0]
        print(f"  PostGIS version: {version}")
    return True


def get_csv_columns(csv_file: Path) -> list[str]:
    """Get column names from CSV file."""
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        return next(reader)


def get_csv_row_count(csv_file: Path) -> int:
    """Count rows in CSV file (excluding header)."""
    with open(csv_file, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def table_exists(conn, schema: str, table: str) -> bool:
    """Check if table exists in database."""
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        );
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        return cur.fetchone()[0]


def get_table_row_count(conn, schema: str, table: str) -> int:
    """Get row count from table."""
    query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def import_table_from_csv(conn, schema: str, table: str, csv_file: Path) -> bool:
    """Import a single table from CSV."""
    if not csv_file.exists():
        print(f"  ⚠ CSV file not found: {csv_file}")
        return False

    csv_rows = get_csv_row_count(csv_file)
    if csv_rows == 0:
        print(f"  ⚠ CSV file is empty, skipping")
        return True

    print(f"  Importing {csv_rows:,} rows from {csv_file.name}...")

    # Check if table exists
    if not table_exists(conn, schema, table):
        print(f"  ⚠ Table {schema}.{table} does not exist, skipping")
        return False

    # Check if table already has data
    existing_rows = get_table_row_count(conn, schema, table)
    if existing_rows > 0:
        print(
            f"  ⚠ Table already has {existing_rows:,} rows, skipping (use --force to overwrite)")
        return True

    try:
        # Use COPY for efficient import
        columns = get_csv_columns(csv_file)
        columns_str = ", ".join([f'"{c}"' for c in columns])

        query = f'COPY "{schema}"."{table}" ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'

        with conn.cursor() as cur:
            with open(csv_file, "rb") as f:
                with cur.copy(query) as copy:
                    while data := f.read(65536):
                        copy.write(data)

        conn.commit()

        # Verify import
        imported_rows = get_table_row_count(conn, schema, table)
        print(f"    ✓ Imported {imported_rows:,} rows")
        return True

    except Exception as e:
        print(f"    ✗ Error: {e}")
        conn.rollback()
        return False


def create_schema_if_not_exists(conn, schema: str):
    """Create schema if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.commit()


def main():
    print("=" * 60)
    print("Railway PostGIS Data Import")
    print("=" * 60)

    if not EXPORT_DIR.exists():
        print(f"Error: Export directory not found: {EXPORT_DIR}")
        print("Run export_db_to_local.py first to export data.")
        sys.exit(1)

    print(f"\nSource: {EXPORT_DIR}")
    print(f"Target: {POSTGIS_URL.split('@')[1].split('?')[0]}")

    try:
        with get_connection() as conn:
            print("\n✓ Connected to Railway PostGIS\n")

            # Check PostGIS extension
            check_postgis_extension(conn)

            # Import tables in order
            print("\n" + "-" * 60)
            print("Starting import...\n")

            success_count = 0
            skip_count = 0
            fail_count = 0

            for schema, table in IMPORT_ORDER:
                csv_file = EXPORT_DIR / schema / f"{table}.csv"
                print(f"\n[{schema}.{table}]")

                # Create schema if needed
                create_schema_if_not_exists(conn, schema)

                result = import_table_from_csv(conn, schema, table, csv_file)
                if result:
                    success_count += 1
                else:
                    fail_count += 1

            print("\n" + "=" * 60)
            print(f"Import complete!")
            print(f"  ✓ Success: {success_count} tables")
            if skip_count > 0:
                print(f"  ⚠ Skipped: {skip_count} tables")
            if fail_count > 0:
                print(f"  ✗ Failed: {fail_count} tables")
            print("=" * 60)

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
