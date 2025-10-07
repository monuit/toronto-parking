"""
Geocodio Batch Retry Script with Canada Support
Retries failed addresses using Geocodio's batch API with proper component-based geocoding
"""

import json
import os
from pathlib import Path
from geocodio import GeocodioClient
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Get Geocodio API key
GEOCODIO_API_KEY = os.getenv('GEOCODIO_API_KEY')

if not GEOCODIO_API_KEY:
    print("❌ Error: GEOCODIO_API_KEY not found in .env file")
    print("   Please add: GEOCODIO_API_KEY=your_key_here")
    exit(1)


def load_failed_addresses():
    """Load all failed addresses"""
    failed_file = Path('output/failed_addresses.json')
    
    if not failed_file.exists():
        print("❌ Error: output/failed_addresses.json not found!")
        return []
    
    with open(failed_file, 'r', encoding='utf-8') as f:
        failed_data = json.load(f)
    
    # Get all failed addresses (retryable + no_results)
    all_failed = []
    
    # Add retryable (rate limits, timeouts, errors)
    for item in failed_data.get('retryable', []):
        all_failed.append(item['query'])
    
    # Add no_results (maybe Geocodio can find them)
    for item in failed_data.get('no_results', []):
        all_failed.append(item['query'])
    
    return all_failed


def load_existing_results():
    """Load existing geocoding results"""
    results_file = Path('output/geocoding_results.json')
    
    if results_file.exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    return {}


def save_results(results):
    """Save updated results"""
    results_file = Path('output/geocoding_results.json')
    
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def parse_address_components(address):
    """Parse Toronto address into components for Geocodio"""
    # Toronto addresses are typically: "123 STREET NAME, TORONTO"
    # or just "STREET NAME" or "123 STREET NAME"
    
    # Clean the address
    address = address.strip()
    
    # Remove "TORONTO" suffix if present
    if ',' in address:
        parts = address.split(',')
        street = parts[0].strip()
    else:
        street = address
    
    # Return as component dictionary for Canada
    return {
        'street': street,
        'city': 'Toronto',
        'state': 'ON',
        'country': 'Canada'
    }


def batch_geocode_with_geocodio(addresses, batch_size=2500):
    """Geocode addresses using Geocodio batch API with Canada support"""
    
    client = GeocodioClient(GEOCODIO_API_KEY)
    
    results = {}
    total = len(addresses)
    
    print(f"\n📊 Processing {total:,} addresses using Geocodio batch API")
    print(f"   Batch size: {batch_size}")
    print(f"   Estimated batches: {(total + batch_size - 1) // batch_size}")
    print(f"   🍁 Using component-based geocoding for Canada\n")
    
    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch_addresses = addresses[batch_start:batch_end]
        batch_num = (batch_start // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size
        
        print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch_addresses):,} addresses)")
        
        try:
            # Convert addresses to component format for Canada
            batch_components = []
            for addr in batch_addresses:
                components = parse_address_components(addr)
                batch_components.append(components)
            
            print(f"   Sample component: {batch_components[0]}")
            
            # Geocode batch with components
            batch_start_time = time.time()
            response = client.geocode(batch_components)
            batch_duration = time.time() - batch_start_time
            
            # Process results
            successful = 0
            failed = 0
            
            # Response is a LocationCollection - iterate directly
            for idx, location in enumerate(response):
                query = batch_addresses[idx]
                
                if location and hasattr(location, 'coords'):
                    # Successfully geocoded
                    results[query] = {
                        'lat': location.coords[0],
                        'lon': location.coords[1],
                        'display_name': location.formatted_address if hasattr(location, 'formatted_address') else str(location),
                        'status': 'success_geocodio',
                        'timestamp': time.time(),
                        'accuracy': location.accuracy if hasattr(location, 'accuracy') else None,
                        'accuracy_type': location.accuracy_type if hasattr(location, 'accuracy_type') else None,
                        'source': 'geocodio'
                    }
                    successful += 1
                else:
                    # No results
                    results[query] = {
                        'lat': None,
                        'lon': None,
                        'status': 'no_results_geocodio',
                        'timestamp': time.time()
                    }
                    failed += 1
            
            print(f"  ✅ Batch {batch_num} complete in {batch_duration:.1f}s")
            print(f"     Success: {successful:,} | Failed: {failed:,}")
            print(f"     Rate: {len(batch_addresses)/batch_duration:.1f} addresses/second\n")
            
            # Small delay between batches
            if batch_num < total_batches:
                time.sleep(2)
        
        except Exception as e:
            print(f"  ❌ Error in batch {batch_num}: {e}")
            import traceback
            traceback.print_exc()
            print(f"     Skipping this batch...\n")
            
            # Mark as failed
            for addr in batch_addresses:
                results[addr] = {
                    'lat': None,
                    'lon': None,
                    'status': 'error_geocodio',
                    'error': str(e),
                    'timestamp': time.time()
                }
    
    return results


def main():
    """Main workflow"""
    print("="*80)
    print("GEOCODIO BATCH RETRY WITH CANADA SUPPORT")
    print("="*80)
    print("\nThis script will:")
    print("  1. Load all failed addresses from output/failed_addresses.json")
    print("  2. Parse addresses into components (street, city, state, country)")
    print("  3. Batch geocode them using Geocodio API with Canada support")
    print("  4. Update output/geocoding_results.json with new results")
    print("="*80)
    
    # Load failed addresses
    failed_addresses = load_failed_addresses()
    print(f"\n📍 Found {len(failed_addresses):,} failed addresses to retry\n")
    
    if not failed_addresses:
        print("✅ No failed addresses to retry!")
        return
    
    print("="*80)
    
    # Confirm
    response = input(f"\nRetry {len(failed_addresses):,} addresses with Geocodio (Canada mode)? [y/N]: ")
    
    if response.lower() != 'y':
        print("❌ Cancelled")
        return
    
    print("="*80)
    
    # Load existing results
    existing_results = load_existing_results()
    print(f"\n📦 Loaded {len(existing_results):,} existing results\n")
    
    # Start timing
    start_time = time.time()
    
    # Geocode
    new_results = batch_geocode_with_geocodio(failed_addresses)
    
    # Merge results
    existing_results.update(new_results)
    
    # Save
    save_results(existing_results)
    
    # Calculate stats
    duration = time.time() - start_time
    recovered = sum(1 for r in new_results.values() if r.get('status') == 'success_geocodio')
    still_failed = len(new_results) - recovered
    
    print("\n" + "="*80)
    print("GEOCODIO BATCH RETRY COMPLETE")
    print("="*80)
    print(f"Total processed: {len(new_results):,}")
    print(f"✅ Recovered: {recovered:,} ({100*recovered/len(new_results):.1f}%)")
    print(f"❌ Still failed: {still_failed:,}")
    print(f"Duration: {duration/60:.1f} minutes")
    print(f"Average rate: {len(new_results)/duration:.1f} addresses/second")
    
    print(f"\nResults saved to: output/geocoding_results.json")
    
    # Overall statistics
    total_success = sum(1 for r in existing_results.values() if r.get('status') in ['success', 'success_geocodio'])
    total_failed = len(existing_results) - total_success
    
    print(f"\n📊 OVERALL STATISTICS")
    print("="*80)
    print(f"Total addresses: {len(existing_results):,}")
    print(f"✅ Successfully geocoded: {total_success:,} ({100*total_success/len(existing_results):.1f}%)")
    print(f"❌ Failed: {total_failed:,}")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Check status: python geocoding/geocoding_status.py")
    print(f"   2. Generate map: python preprocessing/prepare_map_data.py")
    print(f"   3. View map: cd map-app && npm run dev")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
