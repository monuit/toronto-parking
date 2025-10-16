import os
import json
from dotenv import load_dotenv
import psycopg
from psycopg import sql

# MARK: - Environment Setup
load_dotenv(r"c:\Users\boredbedouin\Desktop\toronto-parking\map-app\.env.local")
db_url = os.getenv("TILES_DB_URL")

if not db_url:
    raise ValueError("TILES_DB_URL not found in .env.local")

# MARK: - Database Connection
conn = psycopg.connect(db_url)
cursor = conn.cursor()

# MARK: - Query 1: Check for school-related tables
print("=" * 60)
print("QUERY 1: Available Tables with 'school' keyword")
print("=" * 60)

cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name ILIKE '%school%'
    ORDER BY table_name;
""")

school_tables = cursor.fetchall()
print(f"Found {len(school_tables)} tables:")
for row in school_tables:
    print(f"  • {row[0]}")

# MARK: - Query 2: Check for zone/boundary tables
print("\n" + "=" * 60)
print("QUERY 2: Available Tables with 'zone' keyword")
print("=" * 60)

cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name ILIKE '%zone%'
    ORDER BY table_name;
""")

zone_tables = cursor.fetchall()
print(f"Found {len(zone_tables)} tables:")
for row in zone_tables:
    print(f"  • {row[0]}")

# MARK: - Query 3: Check all public tables to understand structure
print("\n" + "=" * 60)
print("QUERY 3: All Public Tables (Overview)")
print("=" * 60)

cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")

all_tables = cursor.fetchall()
print(f"Total tables: {len(all_tables)}\n")
for row in all_tables:
    print(f"  • {row[0]}")

# MARK: - Query 4: Search camera locations for school keywords
print("\n" + "=" * 60)
print("QUERY 4: Camera Locations Containing School Keywords")
print("=" * 60)

cursor.execute("""
    SELECT location, location_code, ST_Y(geom::geometry) as lat, ST_X(geom::geometry) as lon, ticket_count
    FROM ase_camera_locations
    WHERE location ILIKE '%school%'
       OR location ILIKE '%college%'
       OR location ILIKE '%university%'
       OR location ILIKE '%academy%'
    ORDER BY location;
""")

school_cameras = cursor.fetchall()
print(f"Found {len(school_cameras)} cameras with school keywords:\n")
for row in school_cameras:
    print(f"  Location: {row[0]}")
    print(f"  Code: {row[1]}")
    print(f"  Lat/Lon: ({row[2]:.4f}, {row[3]:.4f})")
    print(f"  Tickets: {row[4]}")
    print()

# MARK: - Query 5: Get schema of camera table
print("=" * 60)
print("QUERY 5: ASE Camera Table Schema")
print("=" * 60)

cursor.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'ase_camera_locations'
    ORDER BY ordinal_position;
""")

columns = cursor.fetchall()
print(f"Columns in ase_camera_locations:\n")
for col in columns:
    print(f"  • {col[0]:<25} {col[1]}")

# MARK: - Query 6: Sample camera record
print("\n" + "=" * 60)
print("QUERY 6: Sample Camera Record (First Record)")
print("=" * 60)

cursor.execute("""
    SELECT * FROM ase_camera_locations LIMIT 1;
""")

col_names = [desc[0] for desc in cursor.description]
sample = cursor.fetchone()
if sample:
    sample_dict = dict(zip(col_names, sample))
    print(json.dumps(sample_dict, indent=2, default=str))
else:
    print("No records found")

# MARK: - Query 7: Check for PostGIS geometry tables
print("\n" + "=" * 60)
print("QUERY 7: Checking for PostGIS Geometry Tables")
print("=" * 60)

try:
    cursor.execute("""
        SELECT
            f_table_name,
            f_geometry_column,
            type,
            coord_dimension,
            srid
        FROM geometry_columns
        ORDER BY f_table_name;
    """)

    geom_tables = cursor.fetchall()
    print(f"Found {len(geom_tables)} geometry tables:\n")
    for row in geom_tables:
        print(f"  Table: {row[0]}")
        print(f"    Column: {row[1]}")
        print(f"    Type: {row[2]}")
        print(f"    SRID: {row[4]}")
        print()
except Exception as e:
    print(f"PostGIS geometry_columns query failed: {e}")
    # Rollback the failed transaction
    conn.rollback()
    cursor.close()
    cursor = conn.cursor()

# MARK: - Query 8: Location names containing specific wards/areas (your examples)
print("=" * 60)
print("QUERY 8: Cameras in Ward 5 Area (Humber, Rockcliffe, Keele)")
print("=" * 60)

cursor.execute("""
    SELECT location, location_code, ST_Y(geom::geometry) as lat, ST_X(geom::geometry) as lon, ticket_count
    FROM ase_camera_locations
    WHERE location ILIKE '%humber%'
       OR location ILIKE '%rockcliffe%'
       OR location ILIKE '%keele%'
       OR location ILIKE '%louvain%'
       OR location ILIKE '%nashville%'
    ORDER BY location;
""")

ward5_cameras = cursor.fetchall()
print(f"Found {len(ward5_cameras)} cameras in Ward 5 area:\n")
for row in ward5_cameras:
    print(f"  {row[0]:<45} | Code: {row[1]:<5} | ({row[2]:.4f}, {row[3]:.4f}) | Tickets: {row[4]}")

# MARK: - Query 9: Check for any boundary tables
print("\n" + "=" * 60)
print("QUERY 9: Looking for Boundary/Boundary-Related Tables")
print("=" * 60)

cursor.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND (table_name ILIKE '%boundary%'
       OR table_name ILIKE '%polygon%'
       OR table_name ILIKE '%ward%'
       OR table_name ILIKE '%neighbourhood%'
       OR table_name ILIKE '%neighborhood%')
    ORDER BY table_name;
""")

boundary_tables = cursor.fetchall()
print(f"Found {len(boundary_tables)} boundary-related tables:\n")
for row in boundary_tables:
    print(f"  • {row[0]}")

# MARK: - Query 10: Search for any table with 'school' in data (broader search)
print("\n" + "=" * 60)
print("QUERY 10: Tables with Spatial Columns (for distance queries)")
print("=" * 60)

cursor.execute("""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND (data_type LIKE '%geometry%' OR data_type LIKE '%geography%' OR column_name ILIKE '%geom%')
    ORDER BY table_name, ordinal_position;
""")

spatial_cols = cursor.fetchall()
print(f"Found {len(spatial_cols)} spatial columns:\n")
current_table = None
for row in spatial_cols:
    if row[0] != current_table:
        print(f"\n  {row[0]}:")
        current_table = row[0]
    print(f"    • {row[1]:<30} {row[2]}")

conn.close()
print("\n" + "=" * 60)
print("Query complete.")
print("=" * 60)
