"""
Unified Geocoding Retry Script
Retries failed addresses using multiple strategies: Geocodio, Geocodio Canada, or direct retry
"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from geocodio import GeocodioClient

# Load environment variables
load_dotenv()

# Get API key
GEOCODIO_API_KEY = os.getenv('GEOCODIO_API_KEY')


class RetryConfig:
    """Configuration for retry strategy"""
    def __init__(self, strategy: str, batch_size: int = 2500, use_components: bool = False):
        self.strategy = strategy
        self.batch_size = batch_size
        self.use_components = use_components


def load_failed_addresses(input_file: Optional[str] = None) -> List[str]:
    """Load all failed addresses from file"""
    failed_file = Path(input_file) if input_file else Path('output/failed_addresses.json')

    if not failed_file.exists():
        print(f"❌ Error: {failed_file} not found!")
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


def load_existing_results(results_file: str = 'output/geocoding_results.json') -> Dict:
    """Load existing geocoding results"""
    results_path = Path(results_file)

    if results_path.exists():
        with open(results_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    return {}


def save_results(results: Dict, results_file: str = 'output/geocoding_results.json'):
    """Save updated results"""
    results_path = Path(results_file)

    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def parse_address_components(address: str) -> Dict[str, str]:
    """Parse Toronto address into components for Geocodio Canada"""
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


def batch_geocode_with_geocodio(
    addresses: List[str],
    config: RetryConfig
) -> Dict[str, Dict]:
    """Geocode addresses using Geocodio batch API"""

    if not GEOCODIO_API_KEY:
        print("❌ Error: GEOCODIO_API_KEY not found in .env file")
        print("   Please add: GEOCODIO_API_KEY=your_key_here")
        return {}

    client = GeocodioClient(GEOCODIO_API_KEY)

    results = {}
    total = len(addresses)

    print(f"\n📊 Processing {total:,} addresses using Geocodio batch API")
    print(f"   Strategy: {config.strategy}")
    print(f"   Batch size: {config.batch_size}")
    print(f"   Use components: {config.use_components}")
    print(f"   Estimated batches: {(total + config.batch_size - 1) // config.batch_size}\n")

    for batch_start in range(0, total, config.batch_size):
        batch_end = min(batch_start + config.batch_size, total)
        batch_addresses = addresses[batch_start:batch_end]
        batch_num = (batch_start // config.batch_size) + 1
        total_batches = (total + config.batch_size - 1) // config.batch_size

        print(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch_addresses):,} addresses)")

        try:
            # Prepare batch input
            if config.use_components:
                # Use component-based geocoding for Canada
                batch_input = [parse_address_components(addr) for addr in batch_addresses]
            else:
                # Use simple string addresses
                batch_input = batch_addresses

            # Geocode batch
            batch_start_time = time.time()
            response = client.geocode(batch_input)
            batch_duration = time.time() - batch_start_time

            # Process results
            successful = 0
            failed = 0

            # Response is a LocationCollection - iterate directly
            for idx, location in enumerate(response):
                addr = batch_addresses[idx]

                # Check if we got valid coordinates
                if hasattr(location, 'coords') and location.coords:
                    lat, lon = location.coords

                    results[addr] = {
                        'lat': lat,
                        'lon': lon,
                        'status': f'success_{config.strategy}',
                        'timestamp': time.time()
                    }
                    successful += 1
                else:
                    # No results
                    results[addr] = {
                        'lat': None,
                        'lon': None,
                        'status': f'no_results_{config.strategy}',
                        'timestamp': time.time()
                    }
                    failed += 1

            print(f"   ✅ {successful:,} successful, ❌ {failed:,} failed")
            print(f"   ⏱️  Batch time: {batch_duration:.1f}s")
            print()

        except Exception as e:
            print(f"   ❌ Batch error: {e}")
            print(f"     Skipping this batch...\n")

            # Mark as error
            for addr in batch_addresses:
                results[addr] = {
                    'lat': None,
                    'lon': None,
                    'status': f'error_{config.strategy}_{type(e).__name__}',
                    'timestamp': time.time()
                }

    return results


def main():
    """Main workflow"""
    parser = argparse.ArgumentParser(
        description='Unified geocoding retry script with multiple strategies'
    )
    parser.add_argument(
        '--strategy',
        choices=['geocodio', 'geocodio-canada', 'geocodio-direct'],
        default='geocodio',
        help='Geocoding strategy to use'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=2500,
        help='Batch size for Geocodio API (max 2500)'
    )
    parser.add_argument(
        '--input',
        type=str,
        default='output/failed_addresses.json',
        help='Input file with failed addresses'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='output/geocoding_results.json',
        help='Output file for geocoding results'
    )

    args = parser.parse_args()

    # Configure retry strategy
    use_components = (args.strategy == 'geocodio-canada')
    config = RetryConfig(
        strategy=args.strategy,
        batch_size=args.batch_size,
        use_components=use_components
    )

    print("="*80)
    print("UNIFIED GEOCODING RETRY")
    print("="*80)
    print(f"Strategy: {args.strategy}")
    print(f"Input: {args.input}")
    print(f"Output: {args.output}")
    print("="*80)

    # Load failed addresses
    failed_addresses = load_failed_addresses(args.input)

    if not failed_addresses:
        print("✅ No failed addresses to retry!")
        return

    print(f"\n📍 Found {len(failed_addresses):,} failed addresses to retry")

    # Confirm
    print("\n⚠️  This will update your geocoding results.")
    response = input("Continue? (y/N): ")

    if response.lower() != 'y':
        print("❌ Cancelled")
        return

    print("="*80)

    # Load existing results
    existing_results = load_existing_results(args.output)
    print(f"\n📦 Loaded {len(existing_results):,} existing results\n")

    # Start timing
    start_time = time.time()

    # Geocode
    new_results = batch_geocode_with_geocodio(failed_addresses, config)

    # Merge results
    existing_results.update(new_results)

    # Save
    save_results(existing_results, args.output)

    # Calculate stats
    duration = time.time() - start_time
    recovered = sum(1 for r in new_results.values() if r.get('status', '').startswith('success'))
    still_failed = len(new_results) - recovered
    total_addresses = len(failed_addresses)

    # Calculate overall success rate
    all_successful = sum(1 for r in existing_results.values() if r.get('lat') is not None)
    overall_success_rate = all_successful / len(existing_results) * 100 if existing_results else 0

    print("\n" + "="*80)
    print(f"RETRY COMPLETE - {args.strategy.upper()}")
    print("="*80)
    print(f"⏱️  Duration: {duration/60:.1f} minutes")
    print(f"✅ Recovered: {recovered:,} / {total_addresses:,} ({recovered/total_addresses*100:.1f}%)")
    print(f"❌ Still failed: {still_failed:,}")
    print(f"\n📊 Overall geocoding status:")
    print(f"   Total: {len(existing_results):,}")
    print(f"   ✅ Successfully geocoded: {all_successful:,} ({overall_success_rate:.1f}%)")
    print(f"   ❌ Failed: {len(existing_results) - all_successful:,}")

    print(f"\n💡 Next steps:")
    print(f"   1. Check status: python geocoding/geocoding_status.py")
    print(f"   2. Try different strategy if needed:")
    print(f"      python geocoding/retry_geocoding.py --strategy geocodio-canada")
    print(f"   3. Generate map: python preprocessing/prepare_map_data.py")
    print(f"   4. View map: cd map-app && npm run dev")
    print(f"\n{'='*80}\n")


if __name__ == '__main__':
    main()
