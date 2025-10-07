"""
Geocodio Direct API Retry Script
Uses direct HTTP API calls instead of the library for Canada support
"""

import json
import os
from pathlib import Path
from dotenv import load_dotenv
import time
import requests

# Load environment variables
load_dotenv()

# Get Geocodio API key
GEOCODIO_API_KEY = os.getenv('GEOCODIO_API_KEY')

if not GEOCODIO_API_KEY:
    print("❌ Error: GEOCODIO_API_KEY not found in .env file")
    exit(1)


def load_failed_addresses():
    """Load all failed addresses"""
    failed_file = Path('output/failed_addresses.json')
    
    if not failed_file.exists():
        print("❌ Error: output/failed_addresses.json not found!")
        return []
    
    with open(failed_file, 'r', encoding='utf-8') as f:
        failed_data = json.load(f)
    
    all_failed = []
    
    for item in failed_data.get('retryable', []):
        all_failed.append(item['query'])
    
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
    """Parse Toronto address into components"""
    address = address.strip()
    
    if ',' in address:
        parts = address.split(',')
        street = parts[0].strip()
    else:
        street = address
    
    return {
        'street': street,
        'city': 'Toronto',
        'state': 'ON',
        'country': 'Canada'
    }


def geocode_single_address(address):
    """Geocode a single address using Geocodio direct API"""
    components = parse_address_components(address)
    
    url = 'https://api.geocod.io/v1.7/geocode'
    params = {
        'street': components['street'],
        'city': components['city'],
        'state': components['state'],
        'country': components['country'],
        'api_key': GEOCODIO_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('results') and len(data['results']) > 0:
            result = data['results'][0]
            location = result['location']
            
            return {
                'lat': location['lat'],
                'lon': location['lng'],
                'display_name': result['formatted_address'],
                'status': 'success_geocodio',
                'timestamp': time.time(),
                'accuracy': result.get('accuracy'),
                'accuracy_type': result.get('accuracy_type'),
                'source': 'geocodio'
            }
        else:
            return {
                'lat': None,
                'lon': None,
                'status': 'no_results_geocodio',
                'timestamp': time.time()
            }
            
    except Exception as e:
        return {
            'lat': None,
            'lon': None,
            'status': 'error_geocodio',
            'error': str(e),
            'timestamp': time.time()
        }


def geocode_addresses(addresses, delay=0.1):
    """Geocode addresses one by one"""
    results = {}
    total = len(addresses)
    
    print(f"\n📊 Processing {total:,} addresses using Geocodio Direct API")
    print(f"   🍁 Geocoding Toronto, ON, Canada addresses")
    print(f"   Rate limit: ~10 requests/second\n")
    
    successful = 0
    failed = 0
    
    for idx, address in enumerate(addresses, 1):
        result = geocode_single_address(address)
        results[address] = result
        
        if result.get('status') == 'success_geocodio':
            successful += 1
        else:
            failed += 1
        
        if idx % 10 == 0:
            print(f"   Progress: {idx:,}/{total:,} ({100*idx/total:.1f}%) | ✅ {successful} | ❌ {failed}")
        
        # Rate limiting
        time.sleep(delay)
    
    print(f"\n✅ Complete: {successful:,} successful | ❌ {failed:,} failed")
    
    return results


def main():
    """Main workflow"""
    print("="*80)
    print("GEOCODIO DIRECT API RETRY (CANADA SUPPORT)")
    print("="*80)
    
    # Load failed addresses
    failed_addresses = load_failed_addresses()
    print(f"\n📍 Found {len(failed_addresses):,} failed addresses to retry\n")
    
    if not failed_addresses:
        print("✅ No failed addresses to retry!")
        return
    
    print("="*80)
    
    # Confirm
    response = input(f"\nRetry {len(failed_addresses):,} addresses with Geocodio Direct API? [y/N]: ")
    
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
    new_results = geocode_addresses(failed_addresses)
    
    # Merge results
    existing_results.update(new_results)
    
    # Save
    save_results(existing_results)
    
    # Calculate stats
    duration = time.time() - start_time
    recovered = sum(1 for r in new_results.values() if r.get('status') == 'success_geocodio')
    still_failed = len(new_results) - recovered
    
    print("\n" + "="*80)
    print("GEOCODIO DIRECT API RETRY COMPLETE")
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
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
