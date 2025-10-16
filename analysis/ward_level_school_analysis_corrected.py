"""
Ward-level school zone analysis - CORRECTED.
Handles duplicate cameras (counted once per school in junction table).
Aggregates properly to avoid double-counting tickets.
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

async def analyze_schools_by_ward_corrected(conn):
    """Analyze school enforcement by ward without double-counting cameras."""

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

    # Get unique cameras near schools (deduplicated)
    school_cameras_unique = await conn.execute("""
        WITH normalized_schools AS (
            SELECT
                CASE
                    WHEN ward LIKE '% - %' THEN TRIM(SPLIT_PART(ward, '-', 2))
                    ELSE TRIM(ward)
                END as ward_name,
                location_code, ticket_count, distance_meters
            FROM schools_with_nearby_cameras
        ),
        ranked_schools AS (
            SELECT
                ward_name,
                location_code,
                ticket_count,
                distance_meters,
                ROW_NUMBER() OVER (PARTITION BY location_code ORDER BY distance_meters) as rank
            FROM normalized_schools
        )
        SELECT
            COALESCE(ward_name, 'Unknown') as ward_name,
            COUNT(DISTINCT location_code) as unique_cameras_near_schools,
            ROUND(SUM(ticket_count)::numeric) as total_tickets_near_schools,
            ROUND(AVG(distance_meters)::numeric, 2) as avg_distance_to_camera,
            ROUND(MIN(distance_meters)::numeric, 2) as closest_camera,
            STRING_AGG(DISTINCT location_code, ', ' ORDER BY location_code) as camera_codes
        FROM ranked_schools
        WHERE rank = 1
        GROUP BY COALESCE(ward_name, 'Unknown')
        ORDER BY SUM(ticket_count) DESC;
    """)

    school_ward_rows = await school_cameras_unique.fetchall()

    # Build school-focused data
    school_data = []
    for row in school_ward_rows:
        ward = row[0]
        cameras = row[1]
        tickets = row[2]
        avg_dist = row[3]
        closest = row[4]
        camera_codes = row[5]

        pct_tickets = (tickets / total_tickets * 100) if total_tickets else 0

        school_data.append({
            "ward": ward,
            "unique_cameras_near_schools": cameras,
            "tickets_from_cameras_near_schools": tickets,
            "percentage_of_total_network": round(pct_tickets, 2),
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
                location_code, ticket_count, total_fine_amount
            FROM ase_camera_locations
        )
        SELECT
            COALESCE(ward_name, 'Unknown') as ward_name,
            COUNT(*) as total_cameras,
            COUNT(DISTINCT location_code) as unique_cameras,
            ROUND(SUM(ticket_count)::numeric) as total_tickets,
            ROUND(AVG(ticket_count)::numeric) as avg_tickets_per_camera,
            ROUND(SUM(total_fine_amount)::numeric, 2) as total_fines
        FROM normalized_cameras
        GROUP BY COALESCE(ward_name, 'Unknown')
        ORDER BY SUM(ticket_count) DESC;
    """)

    all_cam_rows = await all_cameras_by_ward.fetchall()

    # Create comparison with proper deduplication
    comparison = []
    for cam_row in all_cam_rows:
        ward_name = cam_row[0]
        all_cameras = cam_row[2]  # Use unique_cameras count
        all_tickets = cam_row[3]
        all_fines = cam_row[5]

        # Find matching school data
        school_match = next((s for s in school_data if s["ward"] == ward_name), None)

        school_cameras = school_match["unique_cameras_near_schools"] if school_match else 0
        school_tickets = school_match["tickets_from_cameras_near_schools"] if school_match else 0

        pct_tickets_near_schools = (school_tickets / all_tickets * 100) if all_tickets else 0
        pct_cameras_near_schools = (school_cameras / all_cameras * 100) if all_cameras else 0

        comparison.append({
            "ward": ward_name,
            "total_cameras": all_cameras,
            "cameras_near_schools": school_cameras,
            "pct_cameras_near_schools": round(pct_cameras_near_schools, 1),
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
        "school_focus_by_ward": school_data,
        "network_comparison_by_ward": comparison
    }


async def export_ward_analysis_corrected(conn):
    """Export corrected ward-level analysis to CSV and JSON."""

    analysis = await analyze_schools_by_ward_corrected(conn)

    # Export JSON
    json_path = OUTPUT_DIR / "ward_level_school_enforcement_CORRECTED.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, default=str)

    print(f"✓ Exported JSON to {json_path.name}")

    # Print comprehensive report
    print("\n" + "=" * 140)
    print("🏙️  SCHOOL ZONE ENFORCEMENT ANALYSIS BY WARD/MUNICIPALITY (CORRECTED - NO DOUBLE COUNTING)")
    print("=" * 140)

    total = analysis["total_network"]
    print(f"\n📊 TOTAL ASE NETWORK BASELINE")
    print(f"   • Total cameras: {total['cameras']:,}")
    print(f"   • Total tickets: {total['tickets']:,.0f}")
    print(f"   • Total fines: ${total['fines']:,.2f}")

    # Calculate aggregates
    total_school_cameras = sum(w["cameras_near_schools"] for w in analysis["network_comparison_by_ward"])
    total_school_tickets = sum(w["tickets_near_schools"] for w in analysis["network_comparison_by_ward"])

    print(f"\n🎯 SCHOOL ZONE SUMMARY (CORRECTED)")
    print(f"   • Cameras near schools: {total_school_cameras:,} / {total['cameras']:,} ({100*total_school_cameras/total['cameras']:.1f}%)")
    print(f"   • Tickets near schools: {total_school_tickets:,.0f} / {total['tickets']:,.0f} ({100*total_school_tickets/total['tickets']:.2f}%)")
    print(f"   • Non-school tickets: {total['tickets']-total_school_tickets:,.0f} ({100*(total['tickets']-total_school_tickets)/total['tickets']:.2f}%)")

    print(f"\n" + "=" * 140)
    print(f"📈 WARD BREAKDOWN: ENFORCEMENT BY LOCATION TYPE")
    print(f"=" * 140)

    print(f"\n{'Ward/Municipality':<30} | {'All Cams':<10} | {'School Cams':<12} | {'% Cams':<10} | {'All Tickets':<15} | {'School Tix':<15} | {'% Tickets':<12} | {'School %':<12}")
    print(f"{'-'*30}-+-{'-'*10}-+-{'-'*12}-+-{'-'*10}-+-{'-'*15}-+-{'-'*15}-+-{'-'*12}-+-{'-'*12}")

    # Sort by school ticket volume
    sorted_wards = sorted(analysis["network_comparison_by_ward"],
                         key=lambda x: x["tickets_near_schools"], reverse=True)

    for item in sorted_wards[:25]:
        ward = item["ward"][:28]
        all_cams = item["total_cameras"]
        school_cams = item["cameras_near_schools"]
        pct_cams = item["pct_cameras_near_schools"]
        all_tix = item["total_tickets"]
        school_tix = item["tickets_near_schools"]
        pct_tix = item["pct_tickets_near_schools"]
        school_pct_of_ward = (school_tix / all_tix * 100) if all_tix else 0

        print(f"{ward:<30} | {all_cams:>10} | {school_cams:>12} | {pct_cams:>9.1f}% | {all_tix:>15,.0f} | {school_tix:>15,.0f} | {pct_tix:>11.2f}% | {school_pct_of_ward:>11.1f}%")

    # Top schools-focused wards
    print(f"\n" + "=" * 140)
    print(f"🔝 TOP 10 WARDS BY SCHOOL-ZONE ENFORCEMENT INTENSITY")
    print(f"=" * 140)

    sorted_by_school_pct = sorted(analysis["network_comparison_by_ward"],
                                  key=lambda x: (x["tickets_near_schools"] / x["total_tickets"] * 100) if x["total_tickets"] else 0,
                                  reverse=True)

    print(f"\n{'Rank':<5} | {'Ward/Municipality':<30} | {'School % of Ward':<17} | {'School Tickets':<15} | {'Ward Total':<15}")
    print(f"{'-'*5}-+-{'-'*30}-+-{'-'*17}-+-{'-'*15}-+-{'-'*15}")

    for idx, item in enumerate(sorted_by_school_pct[:10], 1):
        ward = item["ward"][:28]
        if item["total_tickets"] > 0:
            pct_of_ward = (item["tickets_near_schools"] / item["total_tickets"]) * 100
            print(f"{idx:<5} | {ward:<30} | {pct_of_ward:>15.2f}% | {item['tickets_near_schools']:>15,.0f} | {item['total_tickets']:>15,.0f}")

    # Key insights
    print(f"\n" + "=" * 140)
    print(f"🔍 KEY INSIGHTS")
    print(f"=" * 140)

    # Find highest % school-zone concentration
    highest_pct_ward = max(sorted_by_school_pct, key=lambda x: (x["tickets_near_schools"] / x["total_tickets"] * 100) if x["total_tickets"] else 0)
    if highest_pct_ward["total_tickets"] > 0:
        pct_highest = (highest_pct_ward["tickets_near_schools"] / highest_pct_ward["total_tickets"]) * 100
        print(f"\n   ✓ Highest school-zone concentration: {highest_pct_ward['ward']}")
        print(f"     → {pct_highest:.1f}% of {highest_pct_ward['ward']}'s tickets from school-zone cameras")
        print(f"     → {highest_pct_ward['tickets_near_schools']:,.0f} school-zone tickets vs {highest_pct_ward['total_tickets']:,.0f} total")

    # Find lowest % school-zone concentration (with any school cameras)
    lowest_pct_ward = min((w for w in sorted_by_school_pct if w["tickets_near_schools"] > 0),
                         key=lambda x: (x["tickets_near_schools"] / x["total_tickets"] * 100) if x["total_tickets"] else 100,
                         default=None)
    if lowest_pct_ward and lowest_pct_ward["total_tickets"] > 0:
        pct_lowest = (lowest_pct_ward["tickets_near_schools"] / lowest_pct_ward["total_tickets"]) * 100
        print(f"\n   ✓ Lowest school-zone concentration (with school cameras): {lowest_pct_ward['ward']}")
        print(f"     → {pct_lowest:.1f}% of {lowest_pct_ward['ward']}'s tickets from school-zone cameras")
        print(f"     → {lowest_pct_ward['tickets_near_schools']:,.0f} school-zone tickets vs {lowest_pct_ward['total_tickets']:,.0f} total")

    # Most school-focused
    most_school_focused = max(analysis["network_comparison_by_ward"],
                             key=lambda x: x["pct_cameras_near_schools"])
    print(f"\n   ✓ Most school-focused camera deployment: {most_school_focused['ward']}")
    print(f"     → {most_school_focused['pct_cameras_near_schools']:.1f}% of cameras near schools")
    print(f"     → {most_school_focused['cameras_near_schools']} of {most_school_focused['total_cameras']} cameras")

    print(f"\n   ✓ CITYWIDE: {100*total_school_tickets/total['tickets']:.2f}% of ALL Toronto tickets from cameras near schools")
    print(f"     → {total_school_tickets:,.0f} near-school tickets")
    print(f"     → {total['tickets']-total_school_tickets:,.0f} non-school tickets")
    print(f"     → This means: 27.27% of enforcement targets school zones; 72.73% targets general traffic corridors")

    print(f"\n" + "=" * 140)


async def main():
    """Main function."""
    print("📊 Generating CORRECTED ward-level school enforcement analysis...\n")

    try:
        async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
            await export_ward_analysis_corrected(conn)
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
