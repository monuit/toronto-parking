#!/usr/bin/env python3
"""
Verify Railway PostGIS data import status.
"""

import psycopg

POSTGIS_URL = "postgres://postgres:c31DB2b4eC5bD1fBfAfgfbbb6gFbae5d@centerbeam.proxy.rlwy.net:21753/railway?sslmode=require"

EXPECTED_TABLES = [
    ("public", "city_wards", 25),
    ("public", "centreline_segments", 65065),
    ("public", "schools", 585),
    ("public", "etl_state", 4),
    ("public", "parking_tickets", 37507602),
    ("public", "parking_ticket_yearly_locations", 3359703),
    ("public", "parking_ticket_yearly_streets", 234212),
    ("public", "parking_ticket_yearly_neighbourhoods", 208),
    ("public", "ase_camera_locations", 593),
    ("public", "red_light_camera_locations", 309),
    ("public", "ase_yearly_locations", 892),
    ("public", "red_light_yearly_locations", 2040),
    ("public", "schools_with_nearby_cameras", 203),
    ("public", "glow_lines", 20000),
    ("public", "parking_ticket_tiles", 1444290),
    ("public", "ase_camera_tiles", 2358),
    ("public", "red_light_camera_tiles", 1333),
]


def main():
    print("=" * 60)
    print("Railway PostGIS Data Verification")
    print("=" * 60)

    conn = psycopg.connect(POSTGIS_URL)
    cur = conn.cursor()

    print(f"\n{'Table':<45} {'Expected':>12} {'Actual':>12} {'Status'}")
    print("-" * 80)

    total_expected = 0
    total_actual = 0
    all_ok = True

    for schema, table, expected in EXPECTED_TABLES:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            actual = cur.fetchone()[0]
            status = "✓" if actual >= expected * 0.95 else "✗"  # Allow 5% variance
            if actual < expected * 0.95:
                all_ok = False
            print(f"{schema}.{table:<40} {expected:>12,} {actual:>12,} {status}")
            total_expected += expected
            total_actual += actual
        except Exception as e:
            print(f"{schema}.{table:<40} {expected:>12,} {'ERROR':>12} ✗ {e}")
            all_ok = False

    print("-" * 80)
    print(f"{'TOTAL':<45} {total_expected:>12,} {total_actual:>12,}")

    conn.close()

    print("\n" + "=" * 60)
    if all_ok:
        print("✓ All tables have expected data!")
    else:
        print("✗ Some tables missing data - check above")
    print("=" * 60)


if __name__ == "__main__":
    main()
