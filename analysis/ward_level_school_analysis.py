"""
Analyze schools with nearby cameras by ward.
Show proportional enforcement activity and comparison to total ASE network.
"""

import asyncio
import os
import json
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

async def analyze_schools_by_ward(conn):
    """Analyze school enforcement activity by ward."""

    # Get total ASE network statistics
    total_stats = await conn.execute("""
        SELECT
            COUNT(*) as total_cameras,
            SUM(ticket_count) as total_tickets,
            ROUND(SUM(total_fine_amount)::numeric, 2) as total_fines
        FROM ase_camera_locations;
    """)

    total_row = await total_stats.fetchone()
    total_cameras = total_row[0]
    total_tickets = total_row[1]
    total_fines = total_row[2]

    # Normalize ward names (handle both numeric and named formats)
    # Get school enforcement by ward (normalized)
    school_ward_stats = await conn.execute("""
        WITH normalized_schools AS (
            SELECT
                CASE
                    WHEN ward LIKE '% - %' THEN TRIM(SPLIT_PART(ward, '-', 2))
                    ELSE TRIM(ward)
                END as ward_name,
                school_id, location_code, ticket_count, distance_meters
            FROM schools_with_nearby_cameras
        )
        SELECT
            COALESCE(ward_name, 'Unknown') as ward_name,
            COUNT(DISTINCT school_id) as schools_with_cameras,
            COUNT(DISTINCT location_code) as cameras_near_schools,
            ROUND(SUM(ticket_count)::numeric) as tickets_near_schools,
            ROUND(AVG(distance_meters)::numeric, 2) as avg_distance_to_camera,
            ROUND(MIN(distance_meters)::numeric, 2) as closest_camera,
            STRING_AGG(DISTINCT location_code, ', ' ORDER BY location_code) as camera_codes
        FROM normalized_schools
        GROUP BY COALESCE(ward_name, 'Unknown')
        ORDER BY SUM(ticket_count) DESC;
    """)

    ward_rows = await school_ward_stats.fetchall()

    # Calculate ward-level proportions
    ward_data = []
    for row in ward_rows:
        ward = row[0]
        schools = row[1]
        cameras = row[2]
        tickets = row[3]
        avg_dist = row[4]
        closest = row[5]
        camera_codes = row[6]

        pct_tickets = (tickets / total_tickets * 100) if total_tickets else 0

        ward_data.append({
            "ward": ward,
            "schools": schools,
            "cameras": cameras,
            "tickets": tickets,
            "percentage_of_total": round(pct_tickets, 2),
            "avg_distance_m": avg_dist,
            "closest_camera_m": closest,
            "camera_codes": camera_codes
        })

    # Get camera distribution by ward (all cameras, normalized)
    all_cameras_by_ward = await conn.execute("""
        WITH normalized_cameras AS (
            SELECT
                CASE
                    WHEN ward LIKE '% - %' THEN TRIM(SPLIT_PART(ward, '-', 2))
                    ELSE TRIM(ward)
                END as ward_name,
                ticket_count, total_fine_amount
            FROM ase_camera_locations
        )
        SELECT
            COALESCE(ward_name, 'Unknown') as ward_name,
            COUNT(*) as total_cameras,
            ROUND(SUM(ticket_count)::numeric) as total_tickets,
            ROUND(AVG(ticket_count)::numeric) as avg_tickets_per_camera,
            ROUND(SUM(total_fine_amount)::numeric, 2) as total_fines
        FROM normalized_cameras
        GROUP BY COALESCE(ward_name, 'Unknown')
        ORDER BY SUM(ticket_count) DESC;
    """)

    all_cam_rows = await all_cameras_by_ward.fetchall()

    # Create comparison table
    comparison = []
    for cam_row in all_cam_rows:
        ward_name = cam_row[0]
        all_cameras = cam_row[1]
        all_tickets = cam_row[2]
        all_fines = cam_row[3]

        # Find matching school data
        school_match = next((w for w in ward_data if w["ward"] == ward_name), None)

        school_cameras = school_match["cameras"] if school_match else 0
        school_tickets = school_match["tickets"] if school_match else 0

        pct_tickets_near_schools = (school_tickets / all_tickets * 100) if all_tickets else 0

        comparison.append({
            "ward": ward_name,
            "total_cameras": all_cameras,
            "cameras_near_schools": school_cameras,
            "pct_cameras_near_schools": round(100 * school_cameras / all_cameras, 1) if all_cameras else 0,
            "total_tickets": int(all_tickets),
            "tickets_near_schools": int(school_tickets),
            "pct_tickets_near_schools": round(pct_tickets_near_schools, 2),
            "total_fines": all_fines
        })

    return {
        "total_network": {
            "cameras": total_cameras,
            "tickets": total_tickets,
            "fines": total_fines
        },
        "school_focus_by_ward": ward_data,
        "network_comparison_by_ward": comparison
    }


async def export_ward_analysis(conn):
    """Export ward-level analysis to CSV and JSON."""

    analysis = await analyze_schools_by_ward(conn)

    # Export JSON
    json_path = OUTPUT_DIR / "ward_level_school_enforcement.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, default=str)

    print(f"✓ Exported JSON to {json_path.name}")

    # Print comprehensive report
    print("\n" + "=" * 120)
    print("🏙️  SCHOOL ZONE ENFORCEMENT ANALYSIS BY WARD/MUNICIPALITY")
    print("=" * 120)

    total = analysis["total_network"]
    print(f"\n📊 TOTAL ASE NETWORK BASELINE")
    print(f"   • Total cameras: {total['cameras']}")
    print(f"   • Total tickets: {total['tickets']:,.0f}")
    print(f"   • Total fines: ${total['fines']:,.2f}")

    print(f"\n" + "=" * 120)
    print(f"🎯 SCHOOL ENFORCEMENT BY WARD (Top 15)")
    print(f"=" * 120)

    print(f"\n{'Ward/Municipality':<25} | {'Schools':<8} | {'Cameras':<8} | {'Tickets':<12} | {'% of Total':<12} | {'Avg Distance':<14}")
    print(f"{'-'*25}-+-{'-'*8}-+-{'-'*8}-+-{'-'*12}-+-{'-'*12}-+-{'-'*14}")

    for item in analysis["school_focus_by_ward"][:15]:
        ward = item["ward"][:23]
        schools = item["schools"]
        cameras = item["cameras"]
        tickets = item["tickets"]
        pct = item["percentage_of_total"]
        dist = f"{item['avg_distance_m']}m"

        print(f"{ward:<25} | {schools:>8} | {cameras:>8} | {tickets:>12,.0f} | {pct:>11.2f}% | {dist:<14}")

    print(f"\n" + "=" * 120)
    print(f"📈 NETWORK COMPARISON: ALL CAMERAS vs. SCHOOL-ZONE CAMERAS BY WARD")
    print(f"=" * 120)

    print(f"\n{'Ward/Municipality':<25} | {'All Cameras':<13} | {'School Cameras':<16} | {'% Near Schools':<16} | {'Total Tickets':<15} | {'School Tickets':<15} | {'% Tickets@Schools':<18}")
    print(f"{'-'*25}-+-{'-'*13}-+-{'-'*16}-+-{'-'*16}-+-{'-'*15}-+-{'-'*15}-+-{'-'*18}")

    for item in analysis["network_comparison_by_ward"][:20]:
        ward = item["ward"][:23]
        all_cams = item["total_cameras"]
        school_cams = item["cameras_near_schools"]
        pct_cams = item["pct_cameras_near_schools"]
        total_tix = item["total_tickets"]
        school_tix = item["tickets_near_schools"]
        pct_tix = item["pct_tickets_near_schools"]

        print(f"{ward:<25} | {all_cams:>13} | {school_cams:>16} | {pct_cams:>15.1f}% | {total_tix:>15,.0f} | {school_tix:>15,.0f} | {pct_tix:>17.2f}%")

    # Aggregate statistics
    total_school_cameras = sum(w["cameras_near_schools"] for w in analysis["network_comparison_by_ward"])
    total_school_tickets = sum(w["tickets_near_schools"] for w in analysis["network_comparison_by_ward"])

    print(f"\n" + "=" * 120)
    print(f"📊 AGGREGATE SCHOOL ZONE STATISTICS")
    print(f"=" * 120)
    print(f"\n   Cameras near schools (citywide):     {total_school_cameras:>6} / {total['cameras']:>6}  ({100*total_school_cameras/total['cameras']:>5.1f}%)")
    print(f"   Tickets near schools (citywide):    {total_school_tickets:>12,.0f} / {total['tickets']:>12,.0f}  ({100*total_school_tickets/total['tickets']:>5.2f}%)")

    # Key insights
    print(f"\n" + "=" * 120)
    print(f"🔍 KEY INSIGHTS")
    print(f"=" * 120)

    # Find highest % school-zone concentration
    highest_pct_ward = max(analysis["network_comparison_by_ward"], key=lambda x: x["pct_tickets_near_schools"])
    print(f"\n   • Highest school-zone enforcement: {highest_pct_ward['ward']} ({highest_pct_ward['pct_tickets_near_schools']:.1f}% of ward tickets)")
    print(f"     → {highest_pct_ward['tickets_near_schools']:,.0f} of {highest_pct_ward['total_tickets']:,.0f} tickets in that ward")

    # Find lowest % school-zone concentration
    lowest_pct_ward = min(analysis["network_comparison_by_ward"], key=lambda x: x["pct_tickets_near_schools"] if x["pct_tickets_near_schools"] > 0 else float('inf'))
    print(f"\n   • Lowest school-zone enforcement: {lowest_pct_ward['ward']} ({lowest_pct_ward['pct_tickets_near_schools']:.1f}% of ward tickets)")
    print(f"     → {lowest_pct_ward['tickets_near_schools']:,.0f} of {lowest_pct_ward['total_tickets']:,.0f} tickets in that ward")

    # Find most school-zone focused ward (by camera concentration)
    highest_cam_pct_ward = max(analysis["network_comparison_by_ward"], key=lambda x: x["pct_cameras_near_schools"])
    print(f"\n   • Most school-focused deployment: {highest_cam_pct_ward['ward']} ({highest_cam_pct_ward['pct_cameras_near_schools']:.1f}% of cameras)")
    print(f"     → {highest_cam_pct_ward['cameras_near_schools']} of {highest_cam_pct_ward['total_cameras']} cameras near schools")

    print(f"\n   • Overall school-zone share: {100*total_school_tickets/total['tickets']:.2f}% of ALL Toronto ASE tickets")
    print(f"     → Implies {100-100*total_school_tickets/total['tickets']:.2f}% from non-school locations")

    print(f"\n" + "=" * 120)


async def main():
    """Main function."""
    print("📊 Generating ward-level school enforcement analysis...\n")

    try:
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
            await export_ward_analysis(conn)
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
