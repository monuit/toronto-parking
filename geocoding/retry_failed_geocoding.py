"""
Retry Failed Geocoding
Attempts to geocode addresses that failed in previous runs
"""

import json
from pathlib import Path
from run_geocoding import RobustGeocoder
import os
from dotenv import load_dotenv

load_dotenv()


def main():
    print("="*60)
    print("Retry Failed Geocoding")
    print("="*60)
    
    # Check for failed addresses file
    failed_file = Path('failed_addresses.json')
    if not failed_file.exists():
        print(f"\n❌ No failed addresses file found!")
        print(f"   Expected: {failed_file}")
        print(f"   Run 'python run_geocoding.py' first to generate this file.")
        return
    
    # Load failed addresses
    with open(failed_file, 'r', encoding='utf-8') as f:
        failed_data = json.load(f)
    
    # Get retryable addresses
    retryable = failed_data.get('retryable', [])
    
    if not retryable:
        print(f"\n✅ No retryable addresses found!")
        
        # Show summary of non-retryable failures
        if 'no_results' in failed_data:
            print(f"   No results: {len(failed_data['no_results'])} addresses")
            print(f"   (These addresses don't exist in OpenStreetMap)")
        
        if 'forbidden' in failed_data:
            print(f"   Forbidden: {len(failed_data['forbidden'])} addresses")
            print(f"   (API access issues)")
        
        return
    
    print(f"\n📍 Found {len(retryable):,} addresses to retry")
    
    # Show failure breakdown
    status_counts = {}
    for item in retryable:
        status = item['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\nFailure types:")
    for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  - {status}: {count:,}")
    
    # Extract just the queries
    retry_queries = [item['query'] for item in retryable]
    
    # Show first few
    print(f"\nFirst 5 addresses to retry:")
    for i, query in enumerate(retry_queries[:5], 1):
        print(f"  {i}. {query}")
    
    if len(retry_queries) > 5:
        print(f"  ... and {len(retry_queries) - 5:,} more")
    
    # Confirm with user
    print(f"\n{'='*60}")
    response = input(f"Retry {len(retry_queries):,} failed addresses? [y/N]: ").strip().lower()
    
    if response != 'y':
        print("Cancelled.")
        return
    
    # Initialize geocoder
    api_key = os.getenv('GEOCODE_MAPS_CO_API_KEY', '68e35b92c4aa0836068625vlcd9bb74')
    
    print(f"\n🚀 Starting retry geocoding...")
    print(f"{'='*60}\n")
    
    geocoder = RobustGeocoder(
        api_key=api_key,
        cache_file='geocoding_results.json',  # Same file as before
        checkpoint_interval=100,
        rate_limit=1.5
    )
    
    # Run geocoding
    results = geocoder.run(retry_queries)
    
    # Analyze retry results
    print(f"\n{'='*60}")
    print(f"Retry Results Analysis")
    print(f"{'='*60}")
    
    retry_successful = sum(1 for query in retry_queries if results.get(query, {}).get('lat') is not None)
    retry_failed = len(retry_queries) - retry_successful
    
    print(f"Addresses retried: {len(retry_queries):,}")
    print(f"Now successful: {retry_successful:,} ({retry_successful/len(retry_queries)*100:.1f}%)")
    print(f"Still failed: {retry_failed:,} ({retry_failed/len(retry_queries)*100:.1f}%)")
    
    if retry_successful > 0:
        print(f"\n✅ Successfully geocoded {retry_successful:,} previously failed addresses!")
    
    if retry_failed > 0:
        print(f"\n⚠️  {retry_failed:,} addresses still failed")
        print(f"   Check 'failed_addresses.json' for updated failure list")
    
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
