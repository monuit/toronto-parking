#!/usr/bin/env python3
"""
Full Railway PostGIS Setup: Create schema and import data.

This script:
1. Creates all required tables using bootstrap DDL
2. Creates tile schema (functions, indexes, partitioned tables)
3. Imports data from local CSV exports

Usage:
    python scripts/railway_full_setup.py
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


def log(msg: str):
    print(msg)


def get_connection():
    """Get a fresh database connection."""
    return psycopg.connect(POSTGIS_URL, connect_timeout=30)


def execute_with_retry(sql: str, max_retries: int = 3) -> bool:
    """Execute SQL with connection retry logic."""
    for attempt in range(max_retries):
        try:
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            if attempt < max_retries - 1:
                log(f"    Retry {attempt + 1}/{max_retries}...")
                import time
                time.sleep(2)
            else:
                log(f"    ✗ Failed after {max_retries} attempts: {e}")
                return False
    return False


class SimplePostgresClient:
    """Minimal Postgres client for schema setup."""

    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
        self.conn.commit()
        return cur.rowcount if hasattr(cur, 'rowcount') else 0

    def fetch_one(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def fetch_all(self, sql: str, params=None):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def ensure_extensions(self):
        log("  Creating PostGIS extension...")
        self.execute("CREATE EXTENSION IF NOT EXISTS postgis CASCADE;")
        self.execute(
            "CREATE EXTENSION IF NOT EXISTS postgis_topology CASCADE;")
        # Tiger geocoder is optional
        try:
            self.execute(
                "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch CASCADE;")
            self.execute(
                "CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder CASCADE;")
        except Exception as e:
            log(f"  Warning: Tiger geocoder not available: {e}")
            self.conn.rollback()


# ============================================================
# Schema DDL from bootstrap.py
# ============================================================

BASE_TABLE_DDLS = [
    """
    CREATE TABLE IF NOT EXISTS parking_tickets (
        ticket_hash TEXT PRIMARY KEY,
        ticket_number TEXT,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS parking_tickets_staging (
        ticket_hash TEXT,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_camera_locations (
        intersection_id TEXT PRIMARY KEY,
        location_code TEXT,
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
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_camera_locations_staging (
        intersection_id TEXT,
        location_code TEXT,
        linear_name_full_1 TEXT,
        linear_name_full_2 TEXT,
        location_name TEXT,
        ward_1 TEXT,
        police_division_1 TEXT,
        activation_date TEXT,
        ticket_count INTEGER,
        total_fine_amount TEXT,
        years TEXT,
        months TEXT,
        yearly_counts_json TEXT,
        geometry_geojson TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_charges (
        rlc_notice_number TEXT PRIMARY KEY,
        intersection_id TEXT,
        charge_date DATE,
        set_fine_amount NUMERIC,
        infraction_code TEXT,
        infraction_description TEXT,
        location TEXT,
        time_of_infraction TEXT,
        geom geometry(POINT, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_charges_staging (
        rlc_notice_number TEXT,
        intersection_id TEXT,
        charge_date TEXT,
        set_fine_amount TEXT,
        infraction_code TEXT,
        infraction_description TEXT,
        location TEXT,
        time_of_infraction TEXT
    )
    """,
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
        yearly_counts JSONB,
        monthly_counts JSONB,
        geom geometry(POINT, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_camera_locations_staging (
        location_code TEXT,
        ward TEXT,
        status TEXT,
        location TEXT,
        ticket_count INTEGER,
        total_fine_amount TEXT,
        years TEXT,
        months TEXT,
        yearly_counts_json TEXT,
        monthly_counts_json TEXT,
        geometry_geojson TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_charges (
        ticket_number TEXT PRIMARY KEY,
        location_code TEXT,
        infraction_date DATE,
        infraction_time TEXT,
        set_fine_amount NUMERIC,
        speed_over_limit NUMERIC,
        location TEXT,
        geom geometry(POINT, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_charges_staging (
        ticket_number TEXT,
        location_code TEXT,
        infraction_date TEXT,
        infraction_time TEXT,
        set_fine_amount TEXT,
        speed_over_limit TEXT,
        location TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS centreline_segments (
        centreline_id BIGINT PRIMARY KEY,
        linear_name TEXT,
        linear_name_type TEXT,
        linear_name_dir TEXT,
        linear_name_full TEXT,
        linear_name_label TEXT,
        parity_left TEXT,
        parity_right TEXT,
        low_num_even INTEGER,
        high_num_even INTEGER,
        low_num_odd INTEGER,
        high_num_odd INTEGER,
        feature_code INTEGER,
        feature_code_desc TEXT,
        jurisdiction TEXT,
        geom geometry(MULTILINESTRING, 4326)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS centreline_segments_staging (
        centreline_id BIGINT,
        linear_name TEXT,
        linear_name_type TEXT,
        linear_name_dir TEXT,
        linear_name_full TEXT,
        linear_name_label TEXT,
        parity_left TEXT,
        parity_right TEXT,
        low_num_even INTEGER,
        high_num_even INTEGER,
        low_num_odd INTEGER,
        high_num_odd INTEGER,
        feature_code INTEGER,
        feature_code_desc TEXT,
        jurisdiction TEXT,
        geometry_geojson TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS city_wards (
        ward_code INTEGER PRIMARY KEY,
        ward_name TEXT NOT NULL,
        ward_short_code TEXT,
        geom geometry(MULTIPOLYGON, 4326) NOT NULL,
        properties JSONB DEFAULT '{}'::JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS camera_ward_totals (
        dataset TEXT NOT NULL,
        ward_code INTEGER NOT NULL,
        ward_name TEXT NOT NULL,
        ticket_count BIGINT NOT NULL,
        location_count INTEGER NOT NULL,
        total_revenue NUMERIC(18, 2) NOT NULL,
        metadata JSONB DEFAULT '{}'::JSONB,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (dataset, ward_code)
    )
    """,
    # ETL state table
    """
    CREATE TABLE IF NOT EXISTS etl_state (
        key TEXT PRIMARY KEY,
        value JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # Schools table
    """
    CREATE TABLE IF NOT EXISTS schools (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        address TEXT,
        school_type TEXT,
        grade_range TEXT,
        board TEXT,
        ward TEXT,
        geom geometry(POINT, 4326),
        properties JSONB DEFAULT '{}'::JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    # Schools with nearby cameras
    """
    CREATE TABLE IF NOT EXISTS schools_with_nearby_cameras (
        school_id INTEGER PRIMARY KEY,
        school_name TEXT NOT NULL,
        ase_cameras_within_500m INTEGER DEFAULT 0,
        ase_cameras_within_1km INTEGER DEFAULT 0,
        red_light_cameras_within_500m INTEGER DEFAULT 0,
        red_light_cameras_within_1km INTEGER DEFAULT 0,
        nearest_ase_distance_m NUMERIC,
        nearest_red_light_distance_m NUMERIC,
        geom geometry(POINT, 4326),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    # Yearly location aggregates
    """
    CREATE TABLE IF NOT EXISTS parking_ticket_yearly_locations (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        location_key TEXT NOT NULL,
        street_normalized TEXT,
        centreline_id BIGINT,
        ticket_count INTEGER NOT NULL,
        total_fines NUMERIC(18, 2),
        geom geometry(POINT, 4326),
        UNIQUE(year, location_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS parking_ticket_yearly_streets (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        street_normalized TEXT NOT NULL,
        ticket_count INTEGER NOT NULL,
        total_fines NUMERIC(18, 2),
        UNIQUE(year, street_normalized)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS parking_ticket_yearly_neighbourhoods (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        neighbourhood TEXT NOT NULL,
        ticket_count INTEGER NOT NULL,
        total_fines NUMERIC(18, 2),
        UNIQUE(year, neighbourhood)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_yearly_locations (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        location_code TEXT NOT NULL,
        ticket_count INTEGER NOT NULL,
        total_fines NUMERIC(18, 2),
        geom geometry(POINT, 4326),
        UNIQUE(year, location_code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_yearly_locations (
        id SERIAL PRIMARY KEY,
        year INTEGER NOT NULL,
        intersection_id TEXT NOT NULL,
        ticket_count INTEGER NOT NULL,
        total_fines NUMERIC(18, 2),
        geom geometry(POINT, 4326),
        UNIQUE(year, intersection_id)
    )
    """,
    # Glow lines for visualization
    """
    CREATE TABLE IF NOT EXISTS glow_lines (
        dataset TEXT NOT NULL,
        centreline_id BIGINT NOT NULL,
        count INTEGER NOT NULL,
        years_mask INTEGER NOT NULL,
        months_mask INTEGER NOT NULL,
        geom geometry(MultiLineString, 4326) NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (dataset, centreline_id)
    )
    """,
    # Tile blob cache
    """
    CREATE TABLE IF NOT EXISTS tile_blob_cache (
        dataset TEXT NOT NULL,
        z INTEGER NOT NULL,
        x INTEGER NOT NULL,
        y INTEGER NOT NULL,
        mvt BYTEA NOT NULL,
        PRIMARY KEY (dataset, z, x, y)
    )
    """,
]

# Partitioned tile tables
TILE_TABLE_DDLS = [
    """
    CREATE TABLE IF NOT EXISTS parking_ticket_tiles (
        tile_id BIGSERIAL,
        dataset TEXT NOT NULL,
        feature_id TEXT NOT NULL,
        min_zoom INTEGER NOT NULL,
        max_zoom INTEGER NOT NULL,
        tile_qk_prefix TEXT NOT NULL,
        tile_qk_group TEXT NOT NULL,
        geom geometry(GEOMETRY, 3857) NOT NULL,
        ticket_count BIGINT,
        total_fine_amount NUMERIC,
        street_normalized TEXT,
        centreline_id BIGINT,
        location_name TEXT,
        location TEXT,
        status TEXT,
        ward TEXT,
        kind TEXT NOT NULL DEFAULT 'point',
        cluster_size INTEGER,
        grid_meters NUMERIC,
        PRIMARY KEY (tile_qk_group, tile_id)
    ) PARTITION BY LIST (tile_qk_group)
    """,
    """
    CREATE TABLE IF NOT EXISTS red_light_camera_tiles (
        tile_id BIGSERIAL,
        dataset TEXT NOT NULL,
        feature_id TEXT NOT NULL,
        min_zoom INTEGER NOT NULL,
        max_zoom INTEGER NOT NULL,
        tile_qk_prefix TEXT NOT NULL,
        tile_qk_group TEXT NOT NULL,
        geom geometry(GEOMETRY, 3857) NOT NULL,
        ticket_count BIGINT,
        total_fine_amount NUMERIC,
        street_normalized TEXT,
        centreline_id BIGINT,
        location_name TEXT,
        location TEXT,
        status TEXT,
        ward TEXT,
        kind TEXT NOT NULL DEFAULT 'point',
        cluster_size INTEGER,
        grid_meters NUMERIC,
        PRIMARY KEY (tile_qk_group, tile_id)
    ) PARTITION BY LIST (tile_qk_group)
    """,
    """
    CREATE TABLE IF NOT EXISTS ase_camera_tiles (
        tile_id BIGSERIAL,
        dataset TEXT NOT NULL,
        feature_id TEXT NOT NULL,
        min_zoom INTEGER NOT NULL,
        max_zoom INTEGER NOT NULL,
        tile_qk_prefix TEXT NOT NULL,
        tile_qk_group TEXT NOT NULL,
        geom geometry(GEOMETRY, 3857) NOT NULL,
        ticket_count BIGINT,
        total_fine_amount NUMERIC,
        street_normalized TEXT,
        centreline_id BIGINT,
        location_name TEXT,
        location TEXT,
        status TEXT,
        ward TEXT,
        kind TEXT NOT NULL DEFAULT 'point',
        cluster_size INTEGER,
        grid_meters NUMERIC,
        PRIMARY KEY (tile_qk_group, tile_id)
    ) PARTITION BY LIST (tile_qk_group)
    """,
]

PARTITION_DDLS = [
    # Partitions for parking_ticket_tiles
    "CREATE TABLE IF NOT EXISTS parking_ticket_tiles_p0 PARTITION OF parking_ticket_tiles FOR VALUES IN ('0')",
    "CREATE TABLE IF NOT EXISTS parking_ticket_tiles_p1 PARTITION OF parking_ticket_tiles FOR VALUES IN ('1')",
    "CREATE TABLE IF NOT EXISTS parking_ticket_tiles_p2 PARTITION OF parking_ticket_tiles FOR VALUES IN ('2')",
    "CREATE TABLE IF NOT EXISTS parking_ticket_tiles_p3 PARTITION OF parking_ticket_tiles FOR VALUES IN ('3')",
    # Partitions for red_light_camera_tiles
    "CREATE TABLE IF NOT EXISTS red_light_camera_tiles_p0 PARTITION OF red_light_camera_tiles FOR VALUES IN ('0')",
    "CREATE TABLE IF NOT EXISTS red_light_camera_tiles_p1 PARTITION OF red_light_camera_tiles FOR VALUES IN ('1')",
    "CREATE TABLE IF NOT EXISTS red_light_camera_tiles_p2 PARTITION OF red_light_camera_tiles FOR VALUES IN ('2')",
    "CREATE TABLE IF NOT EXISTS red_light_camera_tiles_p3 PARTITION OF red_light_camera_tiles FOR VALUES IN ('3')",
    # Partitions for ase_camera_tiles
    "CREATE TABLE IF NOT EXISTS ase_camera_tiles_p0 PARTITION OF ase_camera_tiles FOR VALUES IN ('0')",
    "CREATE TABLE IF NOT EXISTS ase_camera_tiles_p1 PARTITION OF ase_camera_tiles FOR VALUES IN ('1')",
    "CREATE TABLE IF NOT EXISTS ase_camera_tiles_p2 PARTITION OF ase_camera_tiles FOR VALUES IN ('2')",
    "CREATE TABLE IF NOT EXISTS ase_camera_tiles_p3 PARTITION OF ase_camera_tiles FOR VALUES IN ('3')",
]


# ============================================================
# Data Import
# ============================================================

# Tables to import (in dependency order)
IMPORT_TABLES = [
    # Core reference data
    ("public", "city_wards"),
    ("public", "centreline_segments"),
    ("public", "schools"),
    ("public", "etl_state"),

    # Main ticket data
    ("public", "parking_tickets"),

    # Yearly aggregates
    ("public", "parking_ticket_yearly_locations"),
    ("public", "parking_ticket_yearly_streets"),
    ("public", "parking_ticket_yearly_neighbourhoods"),

    # Camera locations
    ("public", "ase_camera_locations"),
    ("public", "red_light_camera_locations"),

    # Yearly camera data
    ("public", "ase_yearly_locations"),
    ("public", "red_light_yearly_locations"),

    # Schools with cameras
    ("public", "schools_with_nearby_cameras"),

    # Visualization
    ("public", "glow_lines"),

    # Tile data (partitioned - import to parent)
    ("public", "parking_ticket_tiles"),
    ("public", "ase_camera_tiles"),
    ("public", "red_light_camera_tiles"),
]


def get_csv_row_count(csv_file: Path) -> int:
    """Count rows in CSV file (excluding header)."""
    with open(csv_file, "r", encoding="utf-8") as f:
        return sum(1 for _ in f) - 1


def get_table_row_count(conn, schema: str, table: str) -> int:
    """Get row count from table."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return cur.fetchone()[0]


def import_table(conn, schema: str, table: str, csv_file: Path) -> bool:
    """Import a table from CSV using COPY."""
    if not csv_file.exists():
        log(f"    ⚠ CSV file not found")
        return False

    csv_rows = get_csv_row_count(csv_file)
    if csv_rows == 0:
        log(f"    ⚠ CSV empty, skipping")
        return True

    # Check existing rows
    try:
        existing = get_table_row_count(conn, schema, table)
        if existing > 0:
            log(f"    ⚠ Already has {existing:,} rows, skipping")
            return True
    except:
        pass

    log(f"    Importing {csv_rows:,} rows...")

    try:
        # Read header to get columns
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            columns = next(reader)

        columns_str = ", ".join([f'"{c}"' for c in columns])
        query = f'COPY "{schema}"."{table}" ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'

        with conn.cursor() as cur:
            with open(csv_file, "rb") as f:
                with cur.copy(query) as copy:
                    while data := f.read(65536):
                        copy.write(data)

        conn.commit()
        imported = get_table_row_count(conn, schema, table)
        log(f"    ✓ Imported {imported:,} rows")
        return True

    except Exception as e:
        log(f"    ✗ Error: {e}")
        conn.rollback()
        return False


def import_table_with_retry(schema: str, table: str, csv_file: Path, max_retries: int = 3) -> bool:
    """Import a table with connection retry logic."""
    import time

    if not csv_file.exists():
        log(f"    ⚠ CSV file not found")
        return False

    csv_rows = get_csv_row_count(csv_file)
    if csv_rows == 0:
        log(f"    ⚠ CSV empty, skipping")
        return True

    for attempt in range(max_retries):
        try:
            conn = get_connection()

            # Check existing rows
            try:
                existing = get_table_row_count(conn, schema, table)
                if existing > 0:
                    log(f"    ⚠ Already has {existing:,} rows, skipping")
                    conn.close()
                    return True
            except:
                pass

            log(f"    Importing {csv_rows:,} rows...")

            # Read header to get columns
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                columns = next(reader)

            columns_str = ", ".join([f'"{c}"' for c in columns])
            query = f'COPY "{schema}"."{table}" ({columns_str}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'

            with conn.cursor() as cur:
                with open(csv_file, "rb") as f:
                    with cur.copy(query) as copy:
                        while data := f.read(65536):
                            copy.write(data)

            conn.commit()
            imported = get_table_row_count(conn, schema, table)
            log(f"    ✓ Imported {imported:,} rows")
            conn.close()
            return True

        except Exception as e:
            log(f"    ✗ Attempt {attempt + 1}/{max_retries}: {e}")
            try:
                conn.close()
            except:
                pass
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

    return False


def main():
    print("=" * 60)
    print("Railway PostGIS Full Setup")
    print("=" * 60)

    if not EXPORT_DIR.exists():
        print(f"\nError: Export directory not found: {EXPORT_DIR}")
        print("Run export_db_to_local.py first.")
        sys.exit(1)

    print(f"\nTarget: centerbeam.proxy.rlwy.net:21753/railway")
    print(f"Source: {EXPORT_DIR}")

    # Step 1: Extensions
    print("\n" + "=" * 40)
    print("STEP 1: Creating Extensions")
    print("=" * 40)

    conn = get_connection()
    client = SimplePostgresClient(conn)
    client.ensure_extensions()
    conn.close()
    print("  ✓ Extensions ready\n")

    # Step 2: Base tables (one connection per table)
    print("=" * 40)
    print("STEP 2: Creating Base Tables")
    print("=" * 40)

    for ddl in BASE_TABLE_DDLS:
        try:
            table_name = ddl.split("EXISTS")[1].split("(")[0].strip()
            if execute_with_retry(ddl):
                print(f"  ✓ {table_name}")
            else:
                print(f"  ✗ {table_name}")
        except Exception as e:
            print(f"  ✗ Error: {e}")
    print()

    # Step 3: Partitioned tile tables
    print("=" * 40)
    print("STEP 3: Creating Tile Tables")
    print("=" * 40)

    for ddl in TILE_TABLE_DDLS:
        try:
            table_name = ddl.split("EXISTS")[1].split("(")[0].strip()
            if execute_with_retry(ddl):
                print(f"  ✓ {table_name}")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    for ddl in PARTITION_DDLS:
        execute_with_retry(ddl)
    print("  ✓ Partitions created\n")

    # Step 4: Import data
    print("=" * 40)
    print("STEP 4: Importing Data")
    print("=" * 40)

    success = 0
    failed = 0

    for schema, table in IMPORT_TABLES:
        csv_file = EXPORT_DIR / schema / f"{table}.csv"
        print(f"\n[{schema}.{table}]")

        if import_table_with_retry(schema, table, csv_file):
            success += 1
        else:
            failed += 1

    print("\n" + "=" * 60)
    print("Setup Complete!")
    print(f"  ✓ Imported: {success} tables")
    if failed > 0:
        print(f"  ✗ Failed: {failed} tables")
    print("=" * 60)


if __name__ == "__main__":
    main()
