#!/usr/bin/env python3
"""
Export all tables from the production PostGIS database to local CSV files.

This script connects to the production database and exports each table
to a separate CSV file in the data_export/ directory.

Usage:
    python scripts/export_db_to_local.py
"""

import psycopg
from dotenv import load_dotenv
import os
import sys
from pathlib import Path

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# Load production environment
env_file = PROJECT_ROOT / ".env.production"
if env_file.exists():
    load_dotenv(env_file)
    print(f"Loaded environment from {env_file}")
else:
    print(f"Warning: {env_file} not found, using environment variables")

# Output directory
EXPORT_DIR = PROJECT_ROOT / "data_export"


def get_connection_string() -> str:
    """Build connection string from environment variables."""
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        # Add sslmode=require if not present
        if "sslmode" not in database_url:
            separator = "&" if "?" in database_url else "?"
            database_url += f"{separator}sslmode=require"
        return database_url

    # Fallback to individual components
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "railway")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require"


def get_all_tables(conn) -> list[tuple[str, str, bool]]:
    """Get all user tables from the database (schema, table_name, is_partitioned)."""
    query = """
        SELECT
            n.nspname AS schema,
            c.relname AS table_name,
            c.relkind = 'p' AS is_partitioned
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'p')  -- regular tables and partitioned tables
        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY n.nspname, c.relname;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def get_table_row_count(conn, schema: str, table: str) -> int:
    """Get approximate row count for a table."""
    query = f'SELECT COUNT(*) FROM "{schema}"."{table}"'
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchone()[0]
    except Exception as e:
        print(f"  Warning: Could not get row count for {schema}.{table}: {e}")
        return -1


def export_table_to_csv(conn, schema: str, table: str, output_dir: Path, is_partitioned: bool = False) -> bool:
    """Export a single table to CSV."""
    # Create schema subdirectory
    schema_dir = output_dir / schema
    schema_dir.mkdir(parents=True, exist_ok=True)

    output_file = schema_dir / f"{table}.csv"

    # Get row count first
    row_count = get_table_row_count(conn, schema, table)
    part_label = " (partitioned)" if is_partitioned else ""
    print(f"  Exporting {schema}.{table}{part_label} ({row_count:,} rows)...")

    if row_count == 0:
        # Create empty file with just headers
        try:
            query = f'SELECT * FROM "{schema}"."{table}" LIMIT 0'
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(",".join(columns) + "\n")
            print(
                f"    ✓ Saved empty table to {output_file.relative_to(PROJECT_ROOT)}")
            return True
        except Exception as e:
            print(f"    ✗ Error exporting empty table {schema}.{table}: {e}")
            conn.rollback()
            return False

    try:
        # Use COPY (SELECT ...) for partitioned tables, regular COPY for others
        if is_partitioned:
            query = f'COPY (SELECT * FROM "{schema}"."{table}") TO STDOUT WITH (FORMAT CSV, HEADER TRUE)'
        else:
            query = f'COPY "{schema}"."{table}" TO STDOUT WITH (FORMAT CSV, HEADER TRUE)'

        with conn.cursor() as cur:
            with open(output_file, "wb") as f:  # Binary mode
                with cur.copy(query) as copy:
                    for data in copy:
                        # data is already bytes in psycopg3
                        if isinstance(data, memoryview):
                            f.write(bytes(data))
                        else:
                            f.write(data)

        file_size = output_file.stat().st_size
        print(
            f"    ✓ Saved to {output_file.relative_to(PROJECT_ROOT)} ({file_size:,} bytes)")
        return True

    except Exception as e:
        print(f"    ✗ Error exporting {schema}.{table}: {e}")
        conn.rollback()
        return False


def main():
    """Main entry point."""
    print("=" * 60)
    print("Database Export Script")
    print("=" * 60)

    # Create export directory
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\nExport directory: {EXPORT_DIR}")

    # Connect to database
    conn_string = get_connection_string()
    # Mask password in output
    masked_conn = conn_string.replace(os.getenv("PGPASSWORD", ""), "***")
    print(f"Connecting to: {masked_conn}")

    try:
        with psycopg.connect(conn_string) as conn:
            print("✓ Connected to database\n")

            # Get all tables
            tables = get_all_tables(conn)
            print(f"Found {len(tables)} tables:\n")

            for schema, table, is_partitioned in tables:
                part_label = " (partitioned)" if is_partitioned else ""
                print(f"  - {schema}.{table}{part_label}")

            print("\n" + "-" * 60)
            print("Starting export...\n")

            success_count = 0
            fail_count = 0

            for schema, table, is_partitioned in tables:
                if export_table_to_csv(conn, schema, table, EXPORT_DIR, is_partitioned):
                    success_count += 1
                else:
                    fail_count += 1

            print("\n" + "=" * 60)
            print(f"Export complete!")
            print(f"  ✓ Success: {success_count} tables")
            if fail_count > 0:
                print(f"  ✗ Failed: {fail_count} tables")
            print("=" * 60)

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
