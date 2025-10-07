"""
Add Downtown Toronto Fallback Coordinates
For addresses that truly failed geocoding, assign approximate coordinates within downtown Toronto
"""

import json
from pathlib import Path
import random
import time

# Downtown Toronto approximate boundaries
# Using a rectangle that covers the main downtown area
DOWNTOWN_BOUNDS = {
    'min_lat': 43.63,   # South (waterfront)
    'max_lat': 43.70,   # North (Bloor area)
    'min_lon': -79.42,  # West
    'max_lon': -79.35   # East
}

def load_results():
    """Load geocoding results"""
    results_file = Path('output/geocoding_results.json')
    
    with open(results_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_results(results):
    """Save updated results"""
    results_file = Path('output/geocoding_results.json')
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def generate_downtown_coordinate():
    """Generate a random coordinate within downtown Toronto"""
    lat = random.uniform(DOWNTOWN_BOUNDS['min_lat'], DOWNTOWN_BOUNDS['max_lat'])
    lon = random.uniform(DOWNTOWN_BOUNDS['min_lon'], DOWNTOWN_BOUNDS['max_lon'])
    return lat, lon


def add_fallback_coordinates(results):
    """Add fallback coordinates for failed addresses"""
    
    failed_count = 0
    updated_count = 0
    
    for address, data in results.items():
        # Check if failed (no coordinates)
        if data.get('lat') is None or data.get('lon') is None:
            failed_count += 1
            
            # Generate fallback coordinate
            lat, lon = generate_downtown_coordinate()
            
            # Update with fallback
            data['lat'] = lat
            data['lon'] = lon
            data['status'] = 'fallback_downtown'
            data['display_name'] = f"{address} (approx. downtown Toronto)"
            data['timestamp'] = time.time()
            data['source'] = 'fallback'
            
            updated_count += 1
    
    return failed_count, updated_count


def main():
    """Main workflow"""
    print("="*80)
    print("ADD DOWNTOWN TORONTO FALLBACK COORDINATES")
    print("="*80)
    print("\nThis script will:")
    print("  1. Find all addresses that failed geocoding")
    print("  2. Assign random coordinates within downtown Toronto boundaries")
    print(f"  3. Bounds: Lat {DOWNTOWN_BOUNDS['min_lat']}-{DOWNTOWN_BOUNDS['max_lat']}, Lon {DOWNTOWN_BOUNDS['min_lon']}-{DOWNTOWN_BOUNDS['max_lon']}")
    print("="*80)
    
    # Load results
    results = load_results()
    
    # Count failed
    failed = sum(1 for r in results.values() if r.get('lat') is None or r.get('lon') is None)
    
    print(f"\n📍 Found {failed:,} addresses without coordinates")
    
    if failed == 0:
        print("✅ All addresses already have coordinates!")
        return
    
    print("\n" + "="*80)
    
    # Confirm
    response = input(f"\nAdd fallback coordinates for {failed:,} addresses? [y/N]: ")
    
    if response.lower() != 'y':
        print("❌ Cancelled")
        return
    
    print("="*80)
    
    # Add fallback coordinates
    print("\n🔄 Adding fallback coordinates...")
    failed_count, updated_count = add_fallback_coordinates(results)
    
    # Save
    save_results(results)
    
    # Stats
    total_success = sum(1 for r in results.values() if r.get('lat') is not None and r.get('lon') is not None)
    
    print("\n" + "="*80)
    print("FALLBACK COORDINATES COMPLETE")
    print("="*80)
    print(f"✅ Updated: {updated_count:,} addresses")
    print(f"📍 All addresses now have coordinates")
    
    print(f"\n📊 OVERALL STATISTICS")
    print("="*80)
    print(f"Total addresses: {len(results):,}")
    print(f"✅ With coordinates: {total_success:,} (100%)")
    print(f"\nBreakdown by source:")
    
    # Count by status
    status_counts = {}
    for data in results.values():
        status = data.get('status', 'unknown')
        status_counts[status] = status_counts.get(status, 0) + 1
    
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"   {status}: {count:,}")
    
    print(f"\nResults saved to: output/geocoding_results.json")
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
