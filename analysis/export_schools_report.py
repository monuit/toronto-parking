"""
Generate detailed report of schools with nearby ASE cameras.
Export detailed school-camera relationships to CSV and JSON.
"""

import asyncio
import os
import json
import csv
from pathlib import Path
from dotenv import load_dotenv
import psycopg
from datetime import datetime

# MARK: - Configuration

ENV_PATH = Path(__file__).parent.parent / "map-app" / ".env.local"
load_dotenv(ENV_PATH)

DATABASE_URL = os.getenv("TILES_DB_URL")
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# MARK: - Database Operations

async def export_detailed_report(conn):
    """Export comprehensive school-camera relationships."""

    results = await conn.execute("""
        SELECT
            school_name,
            school_type,
            location_code,
            camera_location,
            ward,
            ticket_count,
            distance_meters,
            ST_Y(school_geom)::numeric(10,6) as school_lat,
            ST_X(school_geom)::numeric(10,6) as school_lon,
            ST_Y(camera_geom)::numeric(10,6) as camera_lat,
            ST_X(camera_geom)::numeric(10,6) as camera_lon,
            0 as total_fines,
            0 as years_active
        FROM schools_with_nearby_cameras
        ORDER BY school_name, distance_meters;
    """)

    rows = await results.fetchall()

    # Export to CSV
    csv_path = OUTPUT_DIR / "schools_with_nearby_cameras.csv"
    if rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "School Name", "School Type", "Camera Code", "Camera Location",
                "Ward", "Tickets", "Distance (m)", "School Lat", "School Lon",
                "Camera Lat", "Camera Lon", "Total Fines", "Years Active"
            ])
            for row in rows:
                writer.writerow(row)

    print(f"✓ Exported {len(rows)} records to {csv_path.name}")

    # Generate summary statistics by school
    school_summary = await conn.execute("""
        SELECT
            school_name,
            school_type,
            COUNT(DISTINCT location_code) as cameras_nearby,
            ROUND(AVG(distance_meters)::numeric, 2) as avg_distance_m,
            ROUND(MIN(distance_meters)::numeric, 2) as closest_camera_m,
            ROUND(SUM(ticket_count)::numeric) as total_tickets,
            ROUND(SUM(ticket_count)::numeric * 50) as total_fines,
            STRING_AGG(DISTINCT location_code, ', ') as camera_codes,
            STRING_AGG(DISTINCT camera_location, ' | ') as camera_locations
        FROM schools_with_nearby_cameras
        GROUP BY school_name, school_type
        ORDER BY SUM(ticket_count) DESC;
    """)

    summary_rows = await school_summary.fetchall()

    # Export summary
    summary_path = OUTPUT_DIR / "schools_summary_report.csv"
    if summary_rows:
        with open(summary_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "School Name", "School Type", "Cameras Nearby", "Avg Distance (m)",
                "Closest Camera (m)", "Total Tickets", "Total Fines ($)",
                "Camera Codes", "Camera Locations"
            ])
            for row in summary_rows:
                writer.writerow(row)

    print(f"✓ Exported {len(summary_rows)} school summaries to {summary_path.name}")

    # Export as JSON for further analysis
    json_path = OUTPUT_DIR / "schools_cameras_analysis.json"
    json_data = {
        "export_date": datetime.now().isoformat(),
        "buffer_distance_meters": 150,
        "total_schools_analyzed": 585,
        "schools_with_cameras": len(summary_rows),
        "total_school_camera_pairs": len(rows),
        "coverage_percentage": round(100 * len(summary_rows) / 585, 1),
        "schools": [
            {
                "name": row[0],
                "type": row[1],
                "cameras_nearby": row[2],
                "distance_stats": {
                    "average_meters": float(row[3]) if row[3] else 0,
                    "closest_meters": float(row[4]) if row[4] else 0
                },
                "enforcement_stats": {
                    "total_tickets": int(row[5]) if row[5] else 0,
                    "total_fines": float(row[6]) if row[6] else 0
                },
                "cameras": row[7],
                "camera_locations": row[8]
            }
            for row in summary_rows
        ]
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=str)

    print(f"✓ Exported JSON analysis to {json_path.name}")

    # Print top findings
    print("\n" + "=" * 90)
    print("📋 DETAILED FINDINGS: SCHOOLS WITH NEARBY ASE CAMERAS (within 150m)")
    print("=" * 90)

    print(f"\n📌 KEY METRICS")
    print(f"   • Total schools: 585")
    print(f"   • Schools with nearby cameras: {len(summary_rows)} ({round(100*len(summary_rows)/585, 1)}%)")
    print(f"   • School-camera pairs: {len(rows)}")

    print(f"\n🏆 TOP 10 SCHOOLS BY TICKET VOLUME FROM NEARBY CAMERAS")
    print(f"   {'School Name':<35} | {'Cameras':<8} | {'Tickets':<10} | {'Fines':<12}")
    print(f"   {'-'*35}-+-{'-'*8}-+-{'-'*10}-+-{'-'*12}")

    for row in summary_rows[:10]:
        name = row[0][:33]
        cameras = row[2]
        tickets = row[5]
        fines = row[6]
        print(f"   {name:<35} | {cameras:>8} | {tickets:>10,.0f} | ${fines:>11,.2f}")

    # Aggregate stats
    total_tickets = sum(row[5] for row in summary_rows if row[5])
    total_fines = sum(row[6] for row in summary_rows if row[6])

    print(f"\n💰 AGGREGATE ENFORCEMENT AT SCHOOLS WITH NEARBY CAMERAS")
    print(f"   • Total tickets from nearby cameras: {total_tickets:,.0f}")
    print(f"   • Total fines collected: ${total_fines:,.2f}")
    print(f"   • Average tickets per school: {total_tickets/len(summary_rows):,.0f}")
    print(f"   • Average fines per school: ${total_fines/len(summary_rows):,.2f}")

    print("\n" + "=" * 90)


async def main():
    """Main function."""
    print("📊 Generating detailed schools-cameras report...\n")

    try:
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
            await export_detailed_report(conn)
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
