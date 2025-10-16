"""
Database Schema Investigation: Red Light Cameras + ASE + Schools

Queries the PostGIS database to understand:
1. Red light camera table structure and data
2. ASE camera table structure and data
3. School table structure and data
4. What status/active columns exist for each
5. Proximity relationships
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, List, Any

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# ==============================================================================
# Configuration
# ==============================================================================

REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / "map-app" / ".env.local"

load_dotenv(ENV_PATH)
DATABASE_URL = os.getenv("TILES_DB_URL") or os.getenv("DATABASE_URL")

if not DATABASE_URL:
    print(f"❌ DATABASE_URL not found in {ENV_PATH}")
    sys.exit(1)

print(f"✓ Connected to: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'local'}")


# ==============================================================================
# Investigation Functions
# ==============================================================================


def get_all_tables() -> List[str]:
    """List all tables in public schema"""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]


def get_table_schema(table_name: str) -> Dict[str, Any]:
    """Get column names and types for a table"""
    query = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable
    FROM information_schema.columns
    WHERE table_name = %s
    ORDER BY ordinal_position
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (table_name,))
            return {
                row["column_name"]: {
                    "type": row["data_type"],
                    "nullable": row["is_nullable"] == "YES",
                }
                for row in cur.fetchall()
            }


def get_table_sample(table_name: str, limit: int = 3) -> List[Dict]:
    """Get sample rows from table"""
    query = f"SELECT * FROM {table_name} LIMIT %s"
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (limit,))
            rows = cur.fetchall()
            # Convert to serializable format
            return [
                {
                    k: (
                        str(v)
                        if hasattr(v, "__class__")
                        and v.__class__.__name__ == "Point"
                        else v
                    )
                    for k, v in row.items()
                }
                for row in rows
            ]


def get_table_stats(table_name: str) -> Dict[str, Any]:
    """Get row count and other stats"""
    query = f"""
    SELECT 
        COUNT(*) as row_count,
        pg_size_pretty(pg_total_relation_size('{table_name}'::regclass)) as size
    FROM {table_name}
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchone()


def check_red_light_status():
    """Analyze red light camera status options"""
    query = """
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN activation_date IS NOT NULL THEN 1 END) as with_activation_date,
        COUNT(CASE WHEN activation_date IS NULL THEN 1 END) as without_date
    FROM red_light_camera_locations
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchall()


def check_ase_status():
    """Analyze ASE camera status options"""
    query = """
    SELECT DISTINCT status, COUNT(*) as count
    FROM ase_camera_locations
    GROUP BY status
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchall()


def check_school_status():
    """Check if schools table exists and has status"""
    try:
        query = """
        SELECT DISTINCT status, COUNT(*) as count
        FROM schools
        GROUP BY status
        """
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        return {"error": str(e)}


def check_proximity_sample():
    """Check schools near ASE cameras"""
    query = """
    SELECT 
        ase.location_code as ase_code,
        ase.location as ase_location,
        ase.status as ase_status,
        s.name as school_name,
        ST_Distance(ase.geom, s.geom) as distance_meters,
        COUNT(*) OVER (PARTITION BY ase.location_code) as schools_nearby
    FROM ase_camera_locations ase
    JOIN schools s ON ST_DWithin(ase.geom, s.geom, 150)
    WHERE ase.geom IS NOT NULL AND s.geom IS NOT NULL
    LIMIT 10
    """
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        return {"error": str(e)}


def check_red_light_proximity():
    """Check schools near red light cameras"""
    query = """
    SELECT 
        rl.location_code as rl_code,
        rl.location_name as rl_location,
        rl.activation_date::text as rl_activation,
        s.name as school_name,
        ST_Distance(rl.geom, s.geom) as distance_meters
    FROM red_light_camera_locations rl
    JOIN schools s ON ST_DWithin(rl.geom, s.geom, 150)
    WHERE rl.geom IS NOT NULL AND s.geom IS NOT NULL
    LIMIT 10
    """
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        return {"error": str(e)}


def get_enforcement_summary():
    """Get summary statistics for enforcement cameras"""
    query = """
    SELECT 
        'ase' as type,
        COUNT(*) as total_cameras,
        COUNT(CASE WHEN status = 'Active' THEN 1 END) as active,
        COUNT(CASE WHEN status != 'Active' THEN 1 END) as inactive,
        SUM(CASE WHEN ticket_count IS NOT NULL THEN 1 ELSE 0 END) as with_tickets
    FROM ase_camera_locations
    UNION ALL
    SELECT 
        'red_light' as type,
        COUNT(*) as total_cameras,
        COUNT(CASE WHEN activation_date IS NOT NULL THEN 1 END) as active,
        COUNT(CASE WHEN activation_date IS NULL THEN 1 END) as inactive,
        COUNT(*) as with_tickets
    FROM red_light_camera_locations
    """
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            return cur.fetchall()


# ==============================================================================
# Main Investigation
# ==============================================================================


def main():
    print("\n" + "=" * 80)
    print("DATABASE SCHEMA INVESTIGATION")
    print("=" * 80)

    # 1. List all tables
    print("\n📋 AVAILABLE TABLES")
    print("-" * 80)
    tables = get_all_tables()
    for table in tables:
        stats = get_table_stats(table)
        print(f"  • {table:30} {stats['row_count']:>10} rows  {stats['size']}")

    # 2. ASE Schema
    print("\n🚗 ASE CAMERA SCHEMA")
    print("-" * 80)
    ase_schema = get_table_schema("ase_camera_locations")
    for col, info in ase_schema.items():
        print(f"  • {col:30} {info['type']:20} nullable={info['nullable']}")

    print("\n  ASE Status Distribution:")
    ase_status = check_ase_status()
    for row in ase_status:
        print(f"    - {row['status']:20} {row['count']:>5} cameras")

    # 3. Red Light Schema
    print("\n🚦 RED LIGHT CAMERA SCHEMA")
    print("-" * 80)
    rl_schema = get_table_schema("red_light_camera_locations")
    for col, info in rl_schema.items():
        print(f"  • {col:30} {info['type']:20} nullable={info['nullable']}")

    print("\n  Red Light Status Distribution:")
    rl_status = check_red_light_status()
    for row in rl_status:
        print(f"    - Total: {row['total']}, With activation_date: {row['with_activation_date']}, Without: {row['without_date']}")

    # 4. School Schema
    print("\n🏫 SCHOOL SCHEMA")
    print("-" * 80)
    school_schema = get_table_schema("schools")
    for col, info in school_schema.items():
        print(f"  • {col:30} {info['type']:20} nullable={info['nullable']}")

    school_status = check_school_status()
    if isinstance(school_status, list) and school_status:
        print("\n  School Status Distribution:")
        for row in school_status:
            print(f"    - {row['status']:20} {row['count']:>5} schools")

    # 5. Enforcement Summary
    print("\n📊 ENFORCEMENT CAMERA SUMMARY")
    print("-" * 80)
    summary = get_enforcement_summary()
    for row in summary:
        print(f"  {row['type'].upper():15}")
        print(f"    Total:    {row['total_cameras']:>5}")
        print(f"    Active:   {row['active']:>5}")
        print(f"    Inactive: {row['inactive']:>5}")

    # 6. Proximity Analysis
    print("\n🔍 PROXIMITY ANALYSIS: Schools near ASE Cameras (150m)")
    print("-" * 80)
    ase_proximity = check_proximity_sample()
    if isinstance(ase_proximity, list) and ase_proximity:
        for row in ase_proximity[:5]:
            print(
                f"  {row['ase_code']:8} → {row['school_name']:30} "
                f"Distance: {row['distance_meters']:.1f}m  Status: {row['ase_status']}"
            )
    else:
        print(f"  ⚠️  Error: {ase_proximity.get('error', 'Unknown')}")

    print("\n🔍 PROXIMITY ANALYSIS: Schools near Red Light Cameras (150m)")
    print("-" * 80)
    rl_proximity = check_red_light_proximity()
    if isinstance(rl_proximity, list) and rl_proximity:
        for row in rl_proximity[:5]:
            print(
                f"  {row['rl_code']:8} → {row['school_name']:30} "
                f"Distance: {row['distance_meters']:.1f}m  Activation: {row['rl_activation']}"
            )
    else:
        print(f"  ⚠️  Error: {rl_proximity.get('error', 'Unknown')}")

    # 7. Sample Data
    print("\n📦 SAMPLE DATA: ASE Camera")
    print("-" * 80)
    ase_sample = get_table_sample("ase_camera_locations", 1)
    for row in ase_sample:
        for k, v in row.items():
            print(f"  {k:30} {str(v)[:50]}")

    print("\n📦 SAMPLE DATA: Red Light Camera")
    print("-" * 80)
    rl_sample = get_table_sample("red_light_camera_locations", 1)
    for row in rl_sample:
        for k, v in row.items():
            print(f"  {k:30} {str(v)[:50]}")

    print("\n📦 SAMPLE DATA: School")
    print("-" * 80)
    school_sample = get_table_sample("schools", 1)
    for row in school_sample:
        for k, v in row.items():
            print(f"  {k:30} {str(v)[:50]}")

    # 8. Export findings
    findings = {
        "tables": tables,
        "ase_schema": ase_schema,
        "ase_status_dist": [dict(row) for row in ase_status],
        "red_light_schema": rl_schema,
        "red_light_status_dist": [dict(row) for row in rl_status],
        "school_schema": school_schema,
        "enforcement_summary": [dict(row) for row in summary],
    }

    output_path = Path(__file__).parent / "db_schema_findings.json"
    with open(output_path, "w") as f:
        json.dump(findings, f, indent=2, default=str)

    print(f"\n✅ Findings exported to: {output_path}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
