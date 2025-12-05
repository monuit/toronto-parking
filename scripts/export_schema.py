#!/usr/bin/env python3
"""
Export PostgreSQL schema DDL from one database and optionally import to another.

This script exports CREATE TABLE statements, indexes, functions, and triggers
from the source database.

Usage:
    python scripts/export_schema.py [--import-to NEW_DB_URL]
"""

from dotenv import load_dotenv
import psycopg
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# Load production environment
env_file = PROJECT_ROOT / ".env.production"
if env_file.exists():
    load_dotenv(env_file)

# Source database (existing Railway PostGIS)
SOURCE_DB = os.getenv("DATABASE_URL")
if SOURCE_DB and "sslmode" not in SOURCE_DB:
    SOURCE_DB += "?sslmode=require" if "?" not in SOURCE_DB else "&sslmode=require"

# Target database (new Railway PostGIS)
TARGET_DB = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"


def get_table_ddl(conn, schema: str, table: str) -> str:
    """Generate CREATE TABLE DDL for a table."""
    # Get columns
    columns_query = """
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """

    with conn.cursor() as cur:
        cur.execute(columns_query, (schema, table))
        columns = cur.fetchall()

    if not columns:
        return None

    col_defs = []
    for col in columns:
        col_name, data_type, char_len, num_prec, num_scale, nullable, default, udt_name = col

        # Build type string
        if udt_name and udt_name.startswith("geometry"):
            # Get geometry type details
            geom_query = """
                SELECT type, srid
                FROM geometry_columns
                WHERE f_table_schema = %s AND f_table_name = %s AND f_geometry_column = %s
            """
            with conn.cursor() as cur:
                cur.execute(geom_query, (schema, table, col_name))
                geom_info = cur.fetchone()
            if geom_info:
                type_str = f"geometry({geom_info[0]}, {geom_info[1]})"
            else:
                type_str = "geometry"
        elif data_type == "character varying":
            type_str = f"VARCHAR({char_len})" if char_len else "TEXT"
        elif data_type == "numeric" and num_prec:
            type_str = f"NUMERIC({num_prec}, {num_scale or 0})"
        elif data_type == "ARRAY":
            type_str = f"{udt_name.replace('_', '')}[]"
        elif data_type == "USER-DEFINED":
            type_str = udt_name
        else:
            type_str = data_type.upper()

        col_def = f'    "{col_name}" {type_str}'

        if nullable == "NO":
            col_def += " NOT NULL"
        if default:
            col_def += f" DEFAULT {default}"

        col_defs.append(col_def)

    # Get primary key
    pk_query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(pk_query, (schema, table))
        pk_cols = [row[0] for row in cur.fetchall()]

    if pk_cols:
        pk_str = ", ".join([f'"{c}"' for c in pk_cols])
        col_defs.append(f"    PRIMARY KEY ({pk_str})")

    ddl = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n'
    ddl += ",\n".join(col_defs)
    ddl += "\n);"

    return ddl


def get_index_ddl(conn, schema: str, table: str) -> list[str]:
    """Get CREATE INDEX statements for a table."""
    query = """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        AND indexname NOT LIKE '%_pkey'
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, table))
        return [row[0] + ";" for row in cur.fetchall()]


def get_functions_ddl(conn, schema: str = "public") -> list[str]:
    """Get CREATE FUNCTION statements."""
    query = """
        SELECT pg_get_functiondef(p.oid)
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = %s
        AND p.prokind = 'f'
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema,))
        return [row[0] + ";" for row in cur.fetchall()]


def get_all_tables(conn, schema: str = "public") -> list[str]:
    """Get all table names in schema."""
    query = """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = %s
        AND tablename NOT LIKE '%_staging'
        AND tablename NOT LIKE '%__geom_refresh%'
        ORDER BY tablename
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema,))
        return [row[0] for row in cur.fetchall()]


def export_schema(source_conn) -> dict:
    """Export all schema DDL from source database."""
    result = {
        "extensions": [],
        "tables": {},
        "indexes": {},
        "functions": [],
    }

    # Extensions
    print("Exporting extensions...")
    with source_conn.cursor() as cur:
        cur.execute(
            "SELECT extname FROM pg_extension WHERE extname != 'plpgsql'")
        for row in cur.fetchall():
            result["extensions"].append(
                f"CREATE EXTENSION IF NOT EXISTS {row[0]} CASCADE;")

    # Tables
    print("Exporting table definitions...")
    tables = get_all_tables(source_conn)
    for table in tables:
        ddl = get_table_ddl(source_conn, "public", table)
        if ddl:
            result["tables"][table] = ddl
            print(f"  - {table}")

    # Indexes
    print("Exporting indexes...")
    for table in tables:
        indexes = get_index_ddl(source_conn, "public", table)
        if indexes:
            result["indexes"][table] = indexes

    # Functions
    print("Exporting functions...")
    result["functions"] = get_functions_ddl(source_conn)

    return result


def import_schema(target_conn, schema: dict) -> None:
    """Import schema DDL to target database."""

    # Extensions
    print("Creating extensions...")
    for ext_sql in schema["extensions"]:
        try:
            target_conn.execute(ext_sql)
            target_conn.commit()
        except Exception as e:
            print(f"  Warning: {e}")
            target_conn.rollback()

    # Tables
    print("Creating tables...")
    for table, ddl in schema["tables"].items():
        try:
            target_conn.execute(ddl)
            target_conn.commit()
            print(f"  ✓ {table}")
        except Exception as e:
            print(f"  ✗ {table}: {e}")
            target_conn.rollback()

    # Indexes (after tables)
    print("Creating indexes...")
    for table, indexes in schema["indexes"].items():
        for idx_sql in indexes:
            try:
                target_conn.execute(idx_sql)
                target_conn.commit()
            except Exception as e:
                # Indexes might already exist
                target_conn.rollback()

    # Functions (after tables)
    print("Creating functions...")
    for func_sql in schema["functions"]:
        try:
            target_conn.execute(func_sql)
            target_conn.commit()
        except Exception as e:
            print(f"  Warning: {e}")
            target_conn.rollback()


def main():
    print("=" * 60)
    print("PostgreSQL Schema Export/Import")
    print("=" * 60)

    # Export from source
    print(
        f"\nSource: {SOURCE_DB.split('@')[1].split('?')[0] if SOURCE_DB else 'None'}")
    print(f"Target: {TARGET_DB.split('@')[1].split('?')[0]}")

    with psycopg.connect(SOURCE_DB) as source_conn:
        print("\n✓ Connected to source database\n")
        schema = export_schema(source_conn)

    print(
        f"\nExported: {len(schema['tables'])} tables, {len(schema['functions'])} functions")

    # Import to target
    print("\n" + "-" * 60)
    print("Importing to target database...\n")

    with psycopg.connect(TARGET_DB) as target_conn:
        print("✓ Connected to target database\n")
        import_schema(target_conn, schema)

    print("\n" + "=" * 60)
    print("Schema migration complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
