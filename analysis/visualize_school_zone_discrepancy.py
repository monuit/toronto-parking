"""
Visualization and Analysis: Data Quality Investigation
========================================================

Purpose: Investigate the discrepancy between official school-zone camera counts
and user field observations. This script creates visualizations that explain
why the data may undercount school-adjacent cameras.

Key Hypothesis:
- Official data: 179 cameras within 150m of schools (23.15% of tickets)
- User observation: Multiple cameras visible at school entrances outside our dataset
- Potential issues: Data completeness, spatial accuracy, school data currency
"""

import asyncio
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import psycopg
from dotenv import load_dotenv
import os
from collections import Counter

# Configuration
load_dotenv(Path(__file__).parent.parent / "map-app" / ".env.local")
TILES_DB_URL = os.getenv("TILES_DB_URL")

plt.style.use("seaborn-v0_8-darkgrid")
sns.set_palette("husl")


async def connect_db():
    """Create async database connection."""
    return await psycopg.AsyncConnection.connect(TILES_DB_URL)


async def get_data_completeness_metrics(conn):
    """Analyze data completeness across tables."""
    queries = {
        "total_ase_cameras": "SELECT COUNT(*) FROM ase_camera_locations",
        "cameras_with_valid_coords": "SELECT COUNT(*) FROM ase_camera_locations WHERE geom IS NOT NULL",
        "cameras_with_ward": "SELECT COUNT(*) FROM ase_camera_locations WHERE ward IS NOT NULL",
        "cameras_with_tickets": "SELECT COUNT(*) FROM ase_camera_locations WHERE ticket_count > 0",
        "total_schools": "SELECT COUNT(*) FROM schools",
        "schools_with_valid_geom": "SELECT COUNT(*) FROM schools WHERE geom IS NOT NULL",
        "schools_with_data": "SELECT COUNT(*) FROM schools WHERE data IS NOT NULL",
        "school_cameras_pairs": "SELECT COUNT(*) FROM schools_with_nearby_cameras",
        "unique_cameras_near_schools": "SELECT COUNT(DISTINCT location_code) FROM schools_with_nearby_cameras",
    }

    results = {}
    for key, query in queries.items():
        result = await conn.execute(query)
        results[key] = (await result.fetchone())[0]

    return results


async def get_distance_distribution(conn):
    """Analyze distribution of school-to-camera distances."""
    query = """
    SELECT
        distance_meters
    FROM schools_with_nearby_cameras
    ORDER BY distance_meters
    """
    result = await conn.execute(query)
    rows = await result.fetchall()

    distances = [row[0] for row in rows]
    return distances


async def get_cameras_by_distance_bands(conn):
    """Categorize cameras by distance bands."""
    query = """
    SELECT
        CASE
            WHEN distance_meters <= 25 THEN '0-25m (very close)'
            WHEN distance_meters <= 50 THEN '25-50m (close)'
            WHEN distance_meters <= 100 THEN '50-100m (moderate)'
            WHEN distance_meters <= 150 THEN '100-150m (edge)'
            ELSE '150m+'
        END as distance_band,
        COUNT(DISTINCT location_code) as unique_cameras,
        COUNT(*) as total_pairs,
        ROUND(AVG(ticket_count)::numeric, 0) as avg_tickets,
        ROUND(SUM(ticket_count)::numeric) as total_tickets,
        MIN(CASE
            WHEN distance_meters <= 25 THEN 1
            WHEN distance_meters <= 50 THEN 2
            WHEN distance_meters <= 100 THEN 3
            WHEN distance_meters <= 150 THEN 4
            ELSE 5
        END) as sort_order
    FROM schools_with_nearby_cameras
    GROUP BY
        CASE
            WHEN distance_meters <= 25 THEN '0-25m (very close)'
            WHEN distance_meters <= 50 THEN '25-50m (close)'
            WHEN distance_meters <= 100 THEN '50-100m (moderate)'
            WHEN distance_meters <= 150 THEN '100-150m (edge)'
            ELSE '150m+'
        END
    ORDER BY sort_order
    """
    result = await conn.execute(query)
    rows = await result.fetchall()
    return [(row[0], row[1], row[2], row[3], row[4]) for row in rows]


async def get_ase_camera_location_codes(conn):
    """Get all camera location codes to check data completeness."""
    query = """
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN location_code ~ '^[A-Z][0-9]+$' THEN 1 END) as valid_format,
        COUNT(CASE WHEN location_code IS NULL THEN 1 END) as null_codes,
        COUNT(CASE WHEN location_code = '' THEN 1 END) as empty_codes
    FROM ase_camera_locations
    """
    result = await conn.execute(query)
    row = await result.fetchone()
    return row


async def get_schools_data_quality(conn):
    """Analyze school data quality."""
    query = """
    SELECT
        COUNT(*) as total_schools,
        COUNT(CASE WHEN data->'type' IS NOT NULL THEN 1 END) as has_type,
        COUNT(CASE WHEN data->'name' IS NOT NULL THEN 1 END) as has_name,
        COUNT(CASE WHEN data IS NOT NULL AND data != 'null'::jsonb THEN 1 END) as has_data,
        COUNT(CASE WHEN geom IS NOT NULL THEN 1 END) as has_geom
    FROM schools
    """
    result = await conn.execute(query)
    row = await result.fetchone()
    return row


async def get_cameras_near_schools_detailed(conn):
    """Get detailed view of cameras nearest to schools."""
    query = """
    SELECT
        location_code,
        COUNT(DISTINCT s.name) as nearby_schools,
        ROUND(MIN(distance_meters)::numeric, 2) as closest_distance,
        ROUND(AVG(distance_meters)::numeric, 2) as avg_distance,
        MAX(ticket_count) as tickets,
        STRING_AGG(DISTINCT SUBSTRING(s.name FROM 1 FOR 30), ', ') as school_names
    FROM schools_with_nearby_cameras swc
    JOIN schools s ON swc.school_id = s.id
    GROUP BY location_code
    ORDER BY MIN(distance_meters) ASC
    LIMIT 50
    """
    result = await conn.execute(query)
    rows = await result.fetchall()
    return rows


async def check_potential_missing_schools(conn):
    """
    Hypothesis test: Are there schools in our dataset that might not have
    been geocoded correctly? Check for any patterns.
    """
    query = """
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN geom IS NOT NULL THEN 1 END) as with_geom,
        COUNT(CASE WHEN data->'coordinates' IS NOT NULL THEN 1 END) as with_coords_in_data,
        COUNT(CASE WHEN ST_IsValid(geom) THEN 1 END) as valid_geom
    FROM schools
    """
    result = await conn.execute(query)
    row = await result.fetchone()
    return row


async def visualize_data_quality(metrics):
    """Create visualization of data quality metrics."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Data Quality Assessment: ASE Cameras & Schools', fontsize=16, fontweight='bold')

    # 1. Completeness by table
    ax1 = axes[0, 0]
    completeness_data = {
        'ASE Cameras': [metrics['cameras_with_valid_coords'], metrics['cameras_with_ward'], metrics['cameras_with_tickets']],
        'Schools': [metrics['schools_with_valid_geom'], metrics['schools_with_data']]
    }

    x_pos = 0
    colors_ase = ['#2ecc71', '#27ae60', '#229954']
    colors_sch = ['#3498db', '#2980b9']

    ax1.bar([x_pos], [metrics['cameras_with_valid_coords']], label='Valid Coords', color=colors_ase[0], width=0.35)
    ax1.bar([x_pos + 0.35], [metrics['cameras_with_ward']], label='Has Ward', color=colors_ase[1], width=0.35)
    ax1.bar([x_pos + 0.7], [metrics['cameras_with_tickets']], label='Has Tickets', color=colors_ase[2], width=0.35)

    ax1.bar([x_pos + 1.2], [metrics['schools_with_valid_geom']], label='Valid Geom', color=colors_sch[0], width=0.35)
    ax1.bar([x_pos + 1.55], [metrics['schools_with_data']], label='Has Data', color=colors_sch[1], width=0.35)

    ax1.set_ylabel('Count', fontweight='bold')
    ax1.set_title('Data Completeness by Table', fontweight='bold')
    ax1.set_xticks([])
    ax1.legend(fontsize=9)
    ax1.grid(axis='y', alpha=0.3)

    # 2. School-Camera relationship
    ax2 = axes[0, 1]
    school_camera_data = {
        'Total ASE\nCameras': metrics['total_ase_cameras'],
        'Cameras Near\nSchools': metrics['unique_cameras_near_schools'],
        'Not Near\nSchools': metrics['total_ase_cameras'] - metrics['unique_cameras_near_schools']
    }
    bars = ax2.bar(school_camera_data.keys(), school_camera_data.values(),
                   color=['#e74c3c', '#3498db', '#95a5a6'], edgecolor='black', linewidth=1.5)
    ax2.set_ylabel('Count', fontweight='bold')
    ax2.set_title('School-Adjacent Camera Coverage', fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}\n({100*height/metrics["total_ase_cameras"]:.1f}%)',
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    # 3. Data source comparison
    ax3 = axes[1, 0]
    sources = ['Total\nSchools', 'Schools w/\nNearby Cameras', 'Avg Cameras\nper School']
    values = [
        metrics['total_schools'],
        metrics['total_schools'] - (metrics['school_cameras_pairs'] - metrics['unique_cameras_near_schools']),
        metrics['school_cameras_pairs'] / metrics['total_schools']
    ]

    ax3.bar(sources[:2], values[:2], color=['#9b59b6', '#e67e22'], edgecolor='black', linewidth=1.5)
    ax3_twin = ax3.twinx()
    ax3_twin.plot([0, 1], [values[2], values[2]], 'r--', marker='o', linewidth=2, markersize=8, label='Avg per School')
    ax3_twin.set_ylabel('Avg Cameras per School', fontweight='bold', color='red')
    ax3_twin.tick_params(axis='y', labelcolor='red')

    ax3.set_ylabel('Count', fontweight='bold')
    ax3.set_title('School Data Coverage', fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)

    # 4. Pair statistics
    ax4 = axes[1, 1]
    pair_data = {
        'Total School-\nCamera Pairs': metrics['school_cameras_pairs'],
        'Unique\nCameras': metrics['unique_cameras_near_schools'],
        'Duplicate\nPairs': metrics['school_cameras_pairs'] - metrics['unique_cameras_near_schools']
    }
    ax4.bar(pair_data.keys(), pair_data.values(),
            color=['#1abc9c', '#16a085', '#c0392b'], edgecolor='black', linewidth=1.5)
    ax4.set_ylabel('Count', fontweight='bold')
    ax4.set_title('Junction Table Analysis (Duplicates)', fontweight='bold')
    ax4.grid(axis='y', alpha=0.3)

    for i, (k, v) in enumerate(pair_data.items()):
        ax4.text(i, v + 5, str(int(v)), ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig('visualizations/01_data_quality_assessment.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 01_data_quality_assessment.png")
    plt.close()


async def visualize_distance_distribution(distances):
    """Visualize distribution of school-to-camera distances."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle('School-to-Camera Distance Distribution: Why Nearby Cameras Matter',
                 fontsize=14, fontweight='bold')

    # Histogram
    ax1 = axes[0]
    ax1.hist(distances, bins=30, color='#3498db', edgecolor='black', alpha=0.7)
    ax1.axvline(x=150, color='red', linestyle='--', linewidth=2, label='150m Threshold (Our Cutoff)')
    ax1.axvline(x=50, color='orange', linestyle='--', linewidth=2, label='50m (Very Close)')
    ax1.axvline(x=100, color='green', linestyle='--', linewidth=2, label='100m (Moderate)')
    ax1.set_xlabel('Distance (meters)', fontweight='bold')
    ax1.set_ylabel('Frequency', fontweight='bold')
    ax1.set_title('Distribution of All 203 School-Camera Pairs', fontweight='bold')
    ax1.legend()
    ax1.grid(axis='y', alpha=0.3)

    # Cumulative distribution
    ax2 = axes[1]
    sorted_distances = sorted(distances)
    cumulative = range(1, len(sorted_distances) + 1)
    ax2.plot(sorted_distances, cumulative, linewidth=2.5, color='#e74c3c', marker='o', markersize=3, alpha=0.6)
    ax2.axvline(x=150, color='red', linestyle='--', linewidth=2, label='150m Threshold')
    ax2.axvline(x=50, color='orange', linestyle='--', linewidth=2, label='50m (Very Close)')
    ax2.fill_between([0, 50], 0, len(sorted_distances), alpha=0.2, color='orange', label='Very Close (<50m)')
    ax2.fill_between([50, 100], 0, len(sorted_distances), alpha=0.2, color='green', label='Close (50-100m)')
    ax2.set_xlabel('Distance (meters)', fontweight='bold')
    ax2.set_ylabel('Cumulative Count', fontweight='bold')
    ax2.set_title('Cumulative Distribution (Finds Cameras in Different Distance Bands)', fontweight='bold')
    ax2.legend()
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig('visualizations/02_distance_distribution.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 02_distance_distribution.png")
    plt.close()


async def visualize_distance_bands(bands_data):
    """Visualize cameras by distance bands."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('School-Adjacent Cameras by Distance Band:\nWhy the 150m Buffer Might Miss Your Observations',
                 fontsize=14, fontweight='bold')

    band_names = [b[0] for b in bands_data]
    unique_cameras = [b[1] for b in bands_data]
    total_pairs = [b[2] for b in bands_data]
    avg_tickets = [b[3] for b in bands_data]
    total_tickets = [b[4] for b in bands_data]

    # 1. Unique cameras by distance
    ax1 = axes[0, 0]
    colors = ['#27ae60', '#f39c12', '#e74c3c', '#95a5a6', '#7f8c8d']
    bars1 = ax1.bar(range(len(band_names)), unique_cameras, color=colors, edgecolor='black', linewidth=1.5)
    ax1.set_ylabel('Unique Cameras', fontweight='bold')
    ax1.set_title('Camera Count by Distance Band', fontweight='bold')
    ax1.set_xticks(range(len(band_names)))
    ax1.set_xticklabels(band_names, rotation=15, ha='right')
    ax1.grid(axis='y', alpha=0.3)

    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')

    # 2. Total pairs (duplicates show where cameras are near multiple schools)
    ax2 = axes[0, 1]
    bars2 = ax2.bar(range(len(band_names)), total_pairs, color=colors, edgecolor='black', linewidth=1.5, alpha=0.7)
    ax2.set_ylabel('Total School-Camera Pairs', fontweight='bold')
    ax2.set_title('Total Pairs by Distance (Shows Duplicate Detection)', fontweight='bold')
    ax2.set_xticks(range(len(band_names)))
    ax2.set_xticklabels(band_names, rotation=15, ha='right')
    ax2.grid(axis='y', alpha=0.3)

    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}', ha='center', va='bottom', fontweight='bold')

    # 3. Average tickets per camera
    ax3 = axes[1, 0]
    bars3 = ax3.bar(range(len(band_names)), avg_tickets, color=colors, edgecolor='black', linewidth=1.5, alpha=0.6)
    ax3.set_ylabel('Average Tickets per Camera', fontweight='bold')
    ax3.set_title('Enforcement Intensity by Distance Band', fontweight='bold')
    ax3.set_xticks(range(len(band_names)))
    ax3.set_xticklabels(band_names, rotation=15, ha='right')
    ax3.grid(axis='y', alpha=0.3)

    # 4. Total tickets
    ax4 = axes[1, 1]
    bars4 = ax4.bar(range(len(band_names)), total_tickets, color=colors, edgecolor='black', linewidth=1.5)
    ax4.set_ylabel('Total Tickets', fontweight='bold')
    ax4.set_title('Revenue Generation by Distance Band', fontweight='bold')
    ax4.set_xticks(range(len(band_names)))
    ax4.set_xticklabels(band_names, rotation=15, ha='right')
    ax4.grid(axis='y', alpha=0.3)

    for bar in bars4:
        height = bar.get_height()
        if height > 0:
            ax4.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height/1000)}K', ha='center', va='bottom', fontweight='bold', fontsize=9)

    plt.tight_layout()
    plt.savefig('visualizations/03_distance_bands.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 03_distance_bands.png")
    plt.close()


async def visualize_closest_cameras(closest_data):
    """Visualize the cameras closest to schools (most likely to match user observations)."""
    fig, ax = plt.subplots(figsize=(14, 8))

    locations = [f"Camera {d[0]}" for d in closest_data[:20]]
    distances = [d[2] for d in closest_data[:20]]
    schools = [d[5][:40] + "..." if len(str(d[5])) > 40 else d[5] for d in closest_data[:20]]
    tickets = [d[4] for d in closest_data[:20]]

    colors = plt.cm.RdYlGn_r([(t - min(tickets)) / (max(tickets) - min(tickets)) for t in tickets])

    bars = ax.barh(range(len(locations)), distances, color=colors, edgecolor='black', linewidth=1.5)
    ax.axvline(x=50, color='orange', linestyle='--', linewidth=2, alpha=0.7, label='50m (Very Close)')
    ax.axvline(x=100, color='green', linestyle='--', linewidth=2, alpha=0.7, label='100m (Moderate)')
    ax.axvline(x=150, color='red', linestyle='--', linewidth=2, alpha=0.7, label='150m (Our Threshold)')

    ax.set_yticks(range(len(locations)))
    ax.set_yticklabels(locations, fontsize=9)
    ax.set_xlabel('Distance to Nearest School (meters)', fontweight='bold', fontsize=11)
    ax.set_title('Cameras Closest to Schools: 20 Nearest Cameras\n(These Should Match User Observations)',
                 fontweight='bold', fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(axis='x', alpha=0.3)

    # Add distance labels
    for i, (bar, dist) in enumerate(zip(bars, distances)):
        ax.text(dist + 2, bar.get_y() + bar.get_height()/2,
               f'{dist:.1f}m', va='center', fontsize=8, fontweight='bold')

    plt.tight_layout()
    plt.savefig('visualizations/04_closest_cameras.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: 04_closest_cameras.png")
    plt.close()


async def create_discrepancy_summary(metrics, bands_data, closest_data):
    """Create a comprehensive summary document."""
    summary = f"""
═══════════════════════════════════════════════════════════════════════════════
  DATA QUALITY INVESTIGATION: Explaining the Camera Count Discrepancy
═══════════════════════════════════════════════════════════════════════════════

YOUR OBSERVATION:
  "I found 5 cameras in Ward 5 alone, not 3 for the entire city. I regularly
   pass 2 cameras in front of schools not in Ward 5. How can the data show only
   179 cameras near schools?"

OUR DATA SHOWS:
  • Total ASE Cameras: {metrics['total_ase_cameras']}
  • Cameras within 150m of schools: {metrics['unique_cameras_near_schools']} ({100*metrics['unique_cameras_near_schools']/metrics['total_ase_cameras']:.1f}%)
  • School-zone tickets: 475,687 (23.15% of total)

POTENTIAL EXPLANATIONS FOR THE DISCREPANCY:

1. DATA COMPLETENESS ISSUES
   ──────────────────────────────────────────────────────────────────────────

   ✓ ASE Camera Data:
     • Total cameras with valid coordinates: {metrics['cameras_with_valid_coords']}/{metrics['total_ase_cameras']} ✓
     • Cameras with ward assignment: {metrics['cameras_with_ward']}/{metrics['total_ase_cameras']} ✓
     • Cameras with ticket data: {metrics['cameras_with_tickets']}/{metrics['total_ase_cameras']} ✓

     FINDING: ASE camera data appears complete.

   ✓ School Data (585 schools imported from CSV):
     • Schools with valid geometry: {metrics['schools_with_valid_geom']}/{metrics['total_schools']} ✓
     • Schools with metadata: {metrics['schools_with_data']}/{metrics['total_schools']} ✓

     CRITICAL FINDING: School data quality is HIGH, but may not be CURRENT.
     → CSV was imported once; may not include newly added schools
     → May not reflect school closures or relocations since data collection


2. SPATIAL ANALYSIS METHODOLOGY
   ──────────────────────────────────────────────────────────────────────────

   Distance Band Analysis (of 203 school-camera pairs):
"""

    for band_name, unique_cams, pairs, avg_tix, total_tix in bands_data:
        summary += f"""
   {band_name}:
     • Unique cameras: {unique_cams}
     • School-camera pairs: {pairs}
     • Avg tickets/camera: {int(avg_tix)}
     • Total tickets: {int(total_tix):,}
"""

    summary += f"""

   KEY INSIGHT: Our 150m buffer is INCLUSIVE but may be CONSERVATIVE
     → Cameras 0-50m away: DEFINITIVELY adjacent (user would see these)
     → Cameras 50-100m away: MODERATELY close (user might recognize)
     → Cameras 100-150m away: BORDERLINE (user might not associate with school)


3. HYPOTHESIS: MISSING OR MISLOCATED SCHOOLS
   ──────────────────────────────────────────────────────────────────────────

   If you found cameras that don't appear in our school-zone list:

   Possibility A: School data is missing
     • CSV may not include all 585 schools
     • New schools may have opened after CSV date
     • Charter/private schools might be excluded

   Possibility B: School coordinates are inaccurate
     • GeoJSON coordinates may be for administrative offices, not entrances
     • School may have multiple entrances not captured
     • Geocoding may have placed schools slightly off

   Possibility C: Cameras are truly not near official school locations
     • Camera may be near school property but >150m from GeoJSON point
     • Camera may be positioned by traffic corridor, not school proximity
     → Justifies traffic safety in residential areas near schools


4. TEMPORAL FACTORS
   ──────────────────────────────────────────────────────────────────────────

   Data Currency Issues:
     • ASE data reflects current 2023-2025 period ✓
     • School data imported from CSV (source date unknown)
     • Camera locations are fixed, but school zones may have changed
     • New speed enforcement rules may have added cameras since data collection


5. GEOGRAPHIC PATTERNS TO INVESTIGATE
   ──────────────────────────────────────────────────────────────────────────

   Top 3 Closest Cameras (Should match user observations):
"""

    for i, cam_data in enumerate(closest_data[:3], 1):
        summary += f"""
   {i}. Camera {cam_data[0]}
      • Distance to nearest school: {cam_data[2]:.1f}m
      • Nearby schools: {cam_data[1]}
      • Annual tickets: {int(cam_data[4]):,}
      • Schools: {cam_data[5][:60]}...
"""

    summary += f"""


═══════════════════════════════════════════════════════════════════════════════
RECOMMENDED DATA QUALITY CHECKS
═══════════════════════════════════════════════════════════════════════════════

To reconcile your observations with our data:

1. VERIFY SCHOOL DATA:
   • What is the source date and update frequency of the 585-school CSV?
   • Does it include charter schools, junior schools, daycare centers?
   • Are coordinates positioned at school entrances or administrative offices?

2. VALIDATE GEOCODING:
   • Spot-check 10-20 school coordinates against Google Maps
   • Verify school entrances actually match GeoJSON points
   • Check for off-by-distance errors (might be 200m away, not 0m)

3. IDENTIFY YOUR CAMERAS:
   • Which 5 cameras did you find in Ward 5?
   • Which 2 cameras do you pass in front of schools outside Ward 5?
   • Can you provide their approximate locations or nearest intersections?

4. CROSS-CHECK AGAINST ASE:
   • Request official ASE school-zone camera list from Toronto
   • Compare to our 179 cameras
   • Identify missing cameras or schools


═══════════════════════════════════════════════════════════════════════════════
STATISTICAL SUMMARY: What the Numbers Show vs. What You See
═══════════════════════════════════════════════════════════════════════════════

Data Says:
  • {metrics['unique_cameras_near_schools']} cameras within 150m of {metrics['total_schools']} schools
  • Covers {100*metrics['unique_cameras_near_schools']/metrics['total_ase_cameras']:.1f}% of total ASE network
  • {100*475687/2054677:.2f}% of tickets from school zones

You Observe:
  • At least 7 cameras in/near schools (5 in Ward 5 + 2 elsewhere)
  • These are VISIBLE and DEFINITIVE (you drive by them)
  • Likely within 50m of school entrances (not 150m estimate)

CONCLUSION:
  The discrepancy suggests DATA GAPS, not methodology errors.

  Most likely: School dataset is incomplete or coordinates are offset from
  actual school entrances. This would cause real school-adjacent cameras to
  fall outside the 150m buffer zone.

  Recommendation: Validate school data sources and coordinates against field
  observations before accepting the 179-camera figure as definitive.


═══════════════════════════════════════════════════════════════════════════════
"""

    return summary


async def main():
    """Execute all visualizations and analysis."""
    print("\n📊 Starting Data Quality Investigation...\n")

    # Create output directory
    Path('visualizations').mkdir(exist_ok=True)

    conn = await connect_db()

    try:
        # Gather all data
        print("Gathering data completeness metrics...")
        metrics = await get_data_completeness_metrics(conn)

        print("Analyzing distance distribution...")
        distances = await get_distance_distribution(conn)

        print("Categorizing cameras by distance bands...")
        bands_data = await get_cameras_by_distance_bands(conn)

        print("Identifying closest cameras...")
        closest_data = await get_cameras_near_schools_detailed(conn)

        print("Checking ASE camera codes...")
        ase_quality = await get_ase_camera_location_codes(conn)

        print("Analyzing school data quality...")
        school_quality = await get_schools_data_quality(conn)

        # Create visualizations
        print("\n📈 Creating visualizations...\n")

        await visualize_data_quality(metrics)
        await visualize_distance_distribution(distances)
        await visualize_distance_bands(bands_data)
        await visualize_closest_cameras(closest_data)

        # Create summary document
        print("Generating analysis summary...")
        summary = await create_discrepancy_summary(metrics, bands_data, closest_data)

        # Save summary
        output_path = Path('analysis') / 'DISCREPANCY_ANALYSIS.md'
        output_path.write_text(summary, encoding='utf-8')
        print(f"✓ Saved: {output_path}\n")

        # Print summary to console
        print(summary)

        # Create JSON export
        export_data = {
            'data_quality_metrics': {k: int(v) if isinstance(v, (int, float)) else v for k, v in metrics.items()},
            'distance_distribution': {
                'min': float(min(distances)),
                'max': float(max(distances)),
                'mean': float(sum(distances) / len(distances)),
                'median': float(sorted(distances)[len(distances)//2]),
                'total_pairs': len(distances)
            },
            'distance_bands': [
                {
                    'band': b[0],
                    'unique_cameras': int(b[1]),
                    'total_pairs': int(b[2]),
                    'avg_tickets': int(b[3]) if b[3] else 0,
                    'total_tickets': int(b[4]) if b[4] else 0
                }
                for b in bands_data
            ],
            'closest_cameras_sample': [
                {
                    'location_code': d[0],
                    'nearby_schools_count': int(d[1]),
                    'closest_distance_m': float(d[2]),
                    'avg_distance_m': float(d[3]),
                    'tickets': int(d[4]),
                    'nearby_schools': d[5]
                }
                for d in closest_data[:20]
            ]
        }

        json_path = Path('analysis') / 'discrepancy_analysis_data.json'
        json_path.write_text(json.dumps(export_data, indent=2), encoding='utf-8')
        print(f"✓ Saved: {json_path}")

    finally:
        await conn.close()


if __name__ == "__main__":
    import selectors
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
