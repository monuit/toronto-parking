"""
Import school location data from CSV with GeoJSON coordinates into PostGIS.
Cross-join with ASE cameras and find schools with cameras within 150m.
"""

import csv
import json
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg
from psycopg import AsyncConnection

# MARK: - Configuration

ENV_PATH = Path(__file__).parent.parent / "map-app" / ".env.local"
load_dotenv(ENV_PATH)

DATABASE_URL = os.getenv("TILES_DB_URL")
BUFFER_DISTANCE = 150  # meters
CSV_PATH = Path.home() / "Downloads" / "eb3e65f1-b80e-478a-a21d-fa7538a433f7.csv"

# MARK: - Database Operations

async def create_schools_table(conn: AsyncConnection) -> None:
    """Create schools table with geometry column."""
    await conn.execute("""
        DROP TABLE IF EXISTS schools CASCADE;
        CREATE TABLE schools (
            id SERIAL PRIMARY KEY,
            name TEXT,
            type TEXT,
            data JSONB,
            geom GEOMETRY(POINT, 4326)
        );
        CREATE INDEX idx_schools_geom ON schools USING GIST(geom);
    """)
    print("✓ Created schools table with spatial index")


async def load_csv_to_db(conn: AsyncConnection) -> int:
    """Load CSV data into schools table, parsing GeoJSON coordinates."""
    if not CSV_PATH.exists():
        print(f"✗ CSV file not found at {CSV_PATH}")
        return 0

    records_loaded = 0
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Get column names to find the geojson column (last column)
        if not reader.fieldnames:
            print("✗ CSV has no columns")
            return 0

        geojson_col = reader.fieldnames[-1]
        print(f"📍 Using '{geojson_col}' as geometry column")

        for row_num, row in enumerate(reader, start=2):
            try:
                # Extract geometry from last column
                geom_str = row.get(geojson_col, "").strip()
                if not geom_str:
                    continue

                # Parse GeoJSON
                geom_data = json.loads(geom_str)

                # Extract coordinates based on GeoJSON type
                if geom_data.get("type") == "Point":
                    lon, lat = geom_data["coordinates"]
                elif geom_data.get("type") == "Feature":
                    coords = geom_data["geometry"]["coordinates"]
                    lon, lat = coords
                else:
                    print(f"  ⚠ Row {row_num}: Unknown geometry type {geom_data.get('type')}")
                    continue

                # Prepare data (exclude geometry column)
                data = {k: v for k, v in row.items() if k != geojson_col}
                name = data.get("name") or data.get("school_name") or f"School {row_num}"
                school_type = data.get("type") or data.get("school_type") or "Unknown"

                # Insert into database
                await conn.execute(
                    """
                    INSERT INTO schools (name, type, data, geom)
                    VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
                    """,
                    (
                        name,
                        school_type,
                        json.dumps(data),
                        f"POINT({lon} {lat})"
                    )
                )
                records_loaded += 1

                if records_loaded % 50 == 0:
                    print(f"  ⏳ Loaded {records_loaded} records...")

            except (json.JSONDecodeError, KeyError, ValueError) as e:
                if records_loaded < 5:  # Only warn for early rows
                    print(f"  ⚠ Row {row_num}: {type(e).__name__}: {str(e)[:60]}")

    await conn.commit()
    print(f"✓ Loaded {records_loaded} school records into database")
    return records_loaded


async def find_cameras_near_schools(conn: AsyncConnection, distance: int = 150) -> list:
    """Find schools with ASE cameras within specified distance."""

    results = await conn.execute(f"""
        SELECT
            s.id,
            s.name as school_name,
            s.type as school_type,
            c.location_code,
            c.location,
            c.ward,
            c.ticket_count,
            ST_Distance(s.geom::geography, c.geom::geography) as distance_meters,
            ST_Y(s.geom) as school_lat,
            ST_X(s.geom) as school_lon,
            ST_Y(c.geom) as camera_lat,
            ST_X(c.geom) as camera_lon
        FROM schools s
        CROSS JOIN ase_camera_locations c
        WHERE ST_DWithin(s.geom::geography, c.geom::geography, {distance})
        ORDER BY s.name, distance_meters;
    """)

    return await results.fetchall()


async def create_school_camera_junction(conn: AsyncConnection) -> None:
    """Create a materialized view of schools with nearby cameras."""
    await conn.execute("""
        DROP TABLE IF EXISTS schools_with_nearby_cameras CASCADE;
        CREATE TABLE schools_with_nearby_cameras AS
        SELECT
            s.id as school_id,
            s.name as school_name,
            s.type as school_type,
            c.location_code,
            c.location as camera_location,
            c.ward,
            c.ticket_count,
            ROUND(ST_Distance(s.geom::geography, c.geom::geography)::numeric, 2) as distance_meters,
            s.geom as school_geom,
            c.geom as camera_geom
        FROM schools s
        CROSS JOIN ase_camera_locations c
        WHERE ST_DWithin(s.geom::geography, c.geom::geography, 150)
        ORDER BY s.name, distance_meters;

        CREATE INDEX idx_school_id ON schools_with_nearby_cameras(school_id);
        CREATE INDEX idx_location_code ON schools_with_nearby_cameras(location_code);
    """)
    print("✓ Created schools_with_nearby_cameras junction table")


async def generate_analysis_report(conn: AsyncConnection) -> None:
    """Generate summary statistics about schools with nearby cameras."""

    # Total schools loaded
    total_schools = await conn.execute(
        "SELECT COUNT(*) FROM schools"
    )
    total_schools_count = (await total_schools.fetchone())[0]

    # Schools with nearby cameras
    schools_with_cameras = await conn.execute(
        "SELECT COUNT(DISTINCT school_id) FROM schools_with_nearby_cameras"
    )
    schools_count = (await schools_with_cameras.fetchone())[0]

    # Total camera-school pairs
    camera_pairs = await conn.execute(
        "SELECT COUNT(*) FROM schools_with_nearby_cameras"
    )
    pairs_count = (await camera_pairs.fetchone())[0]

    # Camera counts by school
    cameras_per_school = await conn.execute("""
        SELECT
            school_name,
            COUNT(DISTINCT location_code) as camera_count,
            COUNT(DISTINCT location_code) as unique_cameras,
            STRING_AGG(DISTINCT location_code, ', ') as camera_codes,
            ROUND(AVG(distance_meters)::numeric, 2) as avg_distance,
            MIN(distance_meters) as closest_camera,
            ROUND(SUM(ticket_count)::numeric) as total_tickets
        FROM schools_with_nearby_cameras
        GROUP BY school_name
        ORDER BY camera_count DESC, school_name;
    """)

    school_camera_rows = await cameras_per_school.fetchall()

    # High-ticket schools
    high_ticket_schools = await conn.execute("""
        SELECT
            school_name,
            COUNT(DISTINCT location_code) as cameras_nearby,
            ROUND(SUM(ticket_count)::numeric) as total_tickets,
            ROUND(AVG(distance_meters)::numeric, 2) as avg_distance
        FROM schools_with_nearby_cameras
        GROUP BY school_name
        HAVING SUM(ticket_count) > 0
        ORDER BY SUM(ticket_count) DESC
        LIMIT 20;
    """)

    high_ticket_rows = await high_ticket_schools.fetchall()

    # Print report
    print("\n" + "=" * 80)
    print("🏫 SCHOOLS WITH NEARBY ASE CAMERAS (within 150m)")
    print("=" * 80)

    print(f"\n📊 SUMMARY STATISTICS")
    print(f"   Total schools in database: {total_schools_count}")
    print(f"   Schools with cameras nearby: {schools_count} ({100*schools_count/max(total_schools_count,1):.1f}%)")
    print(f"   Total school-camera pairs: {pairs_count}")

    print(f"\n🎯 TOP SCHOOLS BY CAMERA COUNT")
    print(f"   {'School Name':<50} | {'Cameras':<8} | {'Avg Dist':<10} | {'Total Tickets':<14}")
    print(f"   {'-'*50}-+-{'-'*8}-+-{'-'*10}-+-{'-'*14}")

    for row in school_camera_rows[:15]:
        school_name = row[0][:48]
        cameras = row[1]
        avg_dist = row[4]
        total_tix = row[6]
        print(f"   {school_name:<50} | {cameras:>8} | {avg_dist:>10} | {total_tix:>14,.0f}")

    print(f"\n💰 TOP SCHOOLS BY TICKET VOLUME FROM NEARBY CAMERAS")
    print(f"   {'School Name':<50} | {'Cameras':<8} | {'Tickets':<12}")
    print(f"   {'-'*50}-+-{'-'*8}-+-{'-'*12}")

    for row in high_ticket_rows:
        school_name = row[0][:48]
        cameras = row[1]
        tickets = row[2]
        print(f"   {school_name:<50} | {cameras:>8} | {tickets:>12,.0f}")

    print("\n" + "=" * 80)


async def main():
    """Main orchestration function."""
    print("🚀 Starting school location import and analysis...\n")

    try:
        # Connect to database
        async with await AsyncConnection.connect(DATABASE_URL) as conn:
            print(f"✓ Connected to PostGIS database")

            # Create table
            await create_schools_table(conn)

            # Load CSV data
            loaded = await load_csv_to_db(conn)

            if loaded == 0:
                print("✗ No records loaded from CSV")
                return

            # Create junction table with spatial analysis
            await create_school_camera_junction(conn)

            # Find nearby cameras
            nearby = await find_cameras_near_schools(conn, BUFFER_DISTANCE)
            print(f"✓ Found {len(nearby)} school-camera pairs within {BUFFER_DISTANCE}m")

            # Generate report
            await generate_analysis_report(conn)

    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    # Use SelectorEventLoop for Windows compatibility with psycopg3
    import selectors
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
