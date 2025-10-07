"""
Geocode all addresses using geocode.maps.co API
Handles batching, rate limiting, retries, and saves progress incrementally
"""

import requests
import json
import time
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

# Load API key
load_dotenv()
API_KEY = os.getenv('GEOCODE_MAPS_CO_API_KEY')

if not API_KEY:
    print("❌ GEOCODE_MAPS_CO_API_KEY not found in .env file!")
    exit(1)

# Configuration
REQUESTS_PER_SECOND = 2  # API limit: 2 requests per second
REQUEST_DELAY = 0.5  # Wait 0.5s between requests (2 req/s)
BATCH_SIZE = 10  # Process 10 addresses per batch (5 seconds per batch)
SAVE_INTERVAL = 250  # Save progress every 250 addresses
RETRY_WAIT_429 = 30  # Wait 30s for rate limit (429)
RETRY_WAIT_503 = 30  # Wait 30s for service unavailable (503)
MAX_RETRIES = 3  # Max retries per address

# File paths
INPUT_FILE = 'output/unique_queries.json'
SUCCESS_FILE = 'output/geocoding_results.json'
FAILED_FILE = 'output/geocoding_failed.json'
PROGRESS_FILE = 'output/geocoding_progress.json'

def load_progress():
    """Load existing progress"""
    progress = {
        'processed_count': 0,
        'success_count': 0,
        'failed_count': 0,
        'last_index': 0,
        'start_time': None,
        'last_save_time': None
    }
    
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            progress = json.load(f)
    
    return progress

def save_progress(progress):
    """Save current progress"""
    progress['last_save_time'] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2)

def load_json_file(filepath, default=None):
    """Load JSON file or return default"""
    if Path(filepath).exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return default if default is not None else {}

def save_json_file(filepath, data):
    """Save data to JSON file"""
    Path(filepath).parent.mkdir(exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def geocode_address(address, retry_count=0):
    """
    Geocode a single address using geocode.maps.co
    Returns: (success, result_dict)
    """
    url = 'https://geocode.maps.co/search'
    params = {
        'q': address,
        'api_key': API_KEY,
        'format': 'json'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        # Handle rate limiting (429)
        if response.status_code == 429:
            if retry_count < MAX_RETRIES:
                print(f"      ⏳ Rate limit (429), waiting {RETRY_WAIT_429}s...")
                time.sleep(RETRY_WAIT_429)
                return geocode_address(address, retry_count + 1)
            else:
                return False, {'error': 'rate_limit', 'status': 429}
        
        # Handle service unavailable (503)
        if response.status_code == 503:
            if retry_count < MAX_RETRIES:
                print(f"      ⏳ Service unavailable (503), waiting {RETRY_WAIT_503}s...")
                time.sleep(RETRY_WAIT_503)
                return geocode_address(address, retry_count + 1)
            else:
                return False, {'error': 'service_unavailable', 'status': 503}
        
        # Handle other HTTP errors
        if response.status_code != 200:
            return False, {'error': 'http_error', 'status': response.status_code}
        
        # Parse response
        results = response.json()
        
        # Check if we got results
        if not results or len(results) == 0:
            return False, {'error': 'no_results', 'status': 200}
        
        # Get first result
        result = results[0]
        
        return True, {
            'lat': float(result.get('lat')),
            'lon': float(result.get('lon')),
            'display_name': result.get('display_name'),
            'type': result.get('type'),
            'importance': result.get('importance')
        }
    
    except requests.exceptions.Timeout:
        if retry_count < MAX_RETRIES:
            print(f"      ⏳ Timeout, retrying...")
            time.sleep(5)
            return geocode_address(address, retry_count + 1)
        return False, {'error': 'timeout'}
    
    except Exception as e:
        return False, {'error': 'exception', 'message': str(e)[:100]}

def geocode_batch(addresses, start_idx):
    """Geocode a batch of addresses with rate limiting"""
    results_success = {}
    results_failed = {}
    
    print(f"\n  📍 Processing batch: addresses {start_idx+1} to {start_idx+len(addresses)}")
    
    for i, address in enumerate(addresses):
        idx = start_idx + i
        print(f"    [{idx+1}] {address[:60]}...", end=' ', flush=True)
        
        success, result = geocode_address(address)
        
        if success:
            results_success[address] = result
            print(f"✅ ({result['lat']:.6f}, {result['lon']:.6f})")
        else:
            results_failed[address] = result
            error_type = result.get('error', 'unknown')
            print(f"❌ {error_type}")
        
        # Wait between requests to maintain rate limit (2 req/s)
        if i < len(addresses) - 1:  # Don't wait after last address in batch
            time.sleep(REQUEST_DELAY)
    
    return results_success, results_failed

def format_time(seconds):
    """Format seconds into readable time"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"

def main():
    print("=" * 70)
    print("Geocoding with geocode.maps.co")
    print("=" * 70)
    
    # Check if input file exists
    if not Path(INPUT_FILE).exists():
        print(f"❌ {INPUT_FILE} not found!")
        print(f"   Run 'python preprocessing/extract_all_addresses_resumable.py' first")
        return
    
    # Load addresses to geocode
    print(f"\n📂 Loading addresses from {INPUT_FILE}...")
    all_addresses = load_json_file(INPUT_FILE, [])
    print(f"   Total addresses: {len(all_addresses):,}")
    
    # Load existing results
    print(f"\n📂 Loading existing results...")
    success_results = load_json_file(SUCCESS_FILE, {})
    failed_results = load_json_file(FAILED_FILE, {})
    print(f"   Already geocoded: {len(success_results):,}")
    print(f"   Previously failed: {len(failed_results):,}")
    
    # Load progress
    progress = load_progress()
    if progress['start_time'] is None:
        progress['start_time'] = datetime.now().isoformat()
    
    start_idx = progress['last_index']
    
    # Filter out already processed
    remaining = [addr for addr in all_addresses[start_idx:] 
                 if addr not in success_results and addr not in failed_results]
    
    if not remaining:
        print(f"\n✅ All addresses already processed!")
        print(f"   Success: {len(success_results):,}")
        print(f"   Failed: {len(failed_results):,}")
        return
    
    print(f"\n🔄 Remaining to process: {len(remaining):,}")
    print(f"   Starting from index: {start_idx}")
    print(f"\n{'=' * 70}")
    print(f"⚙️  Configuration:")
    print(f"   Rate limit: {REQUESTS_PER_SECOND} requests/second")
    print(f"   Request delay: {REQUEST_DELAY}s between requests")
    print(f"   Batch size: {BATCH_SIZE} addresses (~{BATCH_SIZE * REQUEST_DELAY:.1f}s per batch)")
    print(f"   Save interval: every {SAVE_INTERVAL} addresses")
    print(f"   Retry wait (429/503): {RETRY_WAIT_429}s")
    print(f"{'=' * 70}")
    
    # Process in batches
    total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
    batch_num = 0
    addresses_since_save = 0
    
    try:
        for i in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[i:i+BATCH_SIZE]
            batch_num += 1
            batch_start_idx = start_idx + i
            
            print(f"\n{'=' * 70}")
            print(f"📦 BATCH {batch_num}/{total_batches}")
            print(f"{'=' * 70}")
            
            # Geocode batch
            batch_success, batch_failed = geocode_batch(batch, batch_start_idx)
            
            # Update results
            success_results.update(batch_success)
            failed_results.update(batch_failed)
            
            # Update progress
            progress['processed_count'] += len(batch)
            progress['success_count'] = len(success_results)
            progress['failed_count'] = len(failed_results)
            progress['last_index'] = batch_start_idx + len(batch)
            
            addresses_since_save += len(batch)
            
            # Save every SAVE_INTERVAL addresses
            if addresses_since_save >= SAVE_INTERVAL:
                print(f"\n  💾 Saving progress (processed {addresses_since_save} addresses)...")
                save_json_file(SUCCESS_FILE, success_results)
                save_json_file(FAILED_FILE, failed_results)
                save_progress(progress)
                addresses_since_save = 0
                print(f"  ✅ Saved!")
            
            # Calculate stats
            total_processed = len(success_results) + len(failed_results)
            success_rate = (len(success_results) / total_processed * 100) if total_processed > 0 else 0
            
            # Time estimates
            elapsed = (datetime.now() - datetime.fromisoformat(progress['start_time'])).total_seconds()
            rate = total_processed / elapsed if elapsed > 0 else 0
            remaining_count = len(all_addresses) - total_processed
            eta = remaining_count / rate if rate > 0 else 0
            
            print(f"\n  📊 Progress:")
            print(f"     Processed: {total_processed:,}/{len(all_addresses):,} ({total_processed/len(all_addresses)*100:.1f}%)")
            print(f"     Success: {len(success_results):,} ({success_rate:.1f}%)")
            print(f"     Failed: {len(failed_results):,}")
            print(f"     Rate: {rate:.2f} addr/s")
            print(f"     Elapsed: {format_time(elapsed)}")
            print(f"     ETA: {format_time(eta)}")
    
    except KeyboardInterrupt:
        print(f"\n\n⚠️  INTERRUPTED - Saving progress...")
        save_json_file(SUCCESS_FILE, success_results)
        save_json_file(FAILED_FILE, failed_results)
        save_progress(progress)
        print(f"✅ Progress saved! Run again to resume.")
        return
    
    # Final save
    print(f"\n\n{'=' * 70}")
    print(f"💾 Saving final results...")
    save_json_file(SUCCESS_FILE, success_results)
    save_json_file(FAILED_FILE, failed_results)
    save_progress(progress)
    
    print(f"\n{'=' * 70}")
    print(f"✅ GEOCODING COMPLETE!")
    print(f"{'=' * 70}")
    print(f"  Total addresses: {len(all_addresses):,}")
    print(f"  Successfully geocoded: {len(success_results):,} ({len(success_results)/len(all_addresses)*100:.1f}%)")
    print(f"  Failed: {len(failed_results):,} ({len(failed_results)/len(all_addresses)*100:.1f}%)")
    print(f"  Time taken: {format_time((datetime.now() - datetime.fromisoformat(progress['start_time'])).total_seconds())}")
    print(f"{'=' * 70}")
    
    # Show failure breakdown
    if failed_results:
        print(f"\n📊 Failure Breakdown:")
        error_types = {}
        for addr, result in failed_results.items():
            error_type = result.get('error', 'unknown')
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
            print(f"   {error_type}: {count:,}")
    
    print(f"\n📁 Output files:")
    print(f"   ✅ Success: {SUCCESS_FILE}")
    print(f"   ❌ Failed: {FAILED_FILE}")
    print(f"   📊 Progress: {PROGRESS_FILE}")
    
    if len(failed_results) > 0:
        print(f"\n💡 Next step: Retry failed addresses")
        print(f"   python geocoding/retry_failed_geocoding.py")

if __name__ == '__main__':
    main()
