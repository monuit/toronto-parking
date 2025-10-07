"""
Auto-Retry Geocoding Script
Continuously retries failed addresses until no more progress can be made
Perfect for running overnight!
"""

import json
import time
from pathlib import Path
import sys
import os

# Add parent directory to path so we can import from geocoding folder
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from run_geocoding_fast import FastBatchGeocoder


def load_failed_addresses():
    """Load failed addresses that can be retried"""
    failed_file = Path('output/failed_addresses.json')
    
    if not failed_file.exists():
        return []
    
    with open(failed_file, 'r', encoding='utf-8') as f:
        failed_data = json.load(f)
    
    # Get retryable addresses
    retryable = failed_data.get('retryable', [])
    return [item['query'] for item in retryable]


def get_current_stats():
    """Get current geocoding statistics"""
    results_file = Path('output/geocoding_results.json')
    queries_file = Path('output/unique_queries.json')
    
    if not results_file.exists() or not queries_file.exists():
        return None
    
    with open(results_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        all_queries = json.load(f)
    
    total = len(all_queries)
    successful = sum(1 for r in results.values() if r.get('lat') is not None)
    failed = len(results) - successful
    pending = total - len(results)
    
    return {
        'total': total,
        'successful': successful,
        'failed': failed,
        'pending': pending,
        'success_rate': successful / total * 100 if total > 0 else 0
    }


def extract_addresses_if_needed():
    """Extract unique addresses if not already done"""
    queries_file = Path('output/unique_queries.json')
    
    if queries_file.exists():
        print("✅ Addresses already extracted")
        return True
    
    print("\n📦 Extracting unique addresses from CSV files...")
    print("   This will take ~5 minutes...")
    
    try:
        import subprocess
        result = subprocess.run(
            ['python', 'preprocessing/prepare_map_data.py'],
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        
        if result.returncode == 0:
            print("✅ Address extraction complete!")
            return True
        else:
            print(f"\n❌ Error extracting addresses:")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"\n❌ Error running prepare_map_data.py: {e}")
        print("   Please run manually: python preprocessing/prepare_map_data.py")
        return False


def main():
    print("="*80)
    print("AUTO-RETRY GEOCODING - OVERNIGHT MODE")
    print("="*80)
    print("\nThis script will:")
    print("  1. Extract unique addresses (if not already done)")
    print("  2. Run initial geocoding for all pending addresses")
    print("  3. Automatically retry failed addresses")
    print("  4. Keep retrying until no more progress can be made")
    print("  5. Save progress after each round")
    print("\nYou can safely Ctrl+C anytime - progress is saved automatically!")
    print("="*80)
    
    # Extract addresses if needed
    if not extract_addresses_if_needed():
        return
    
    # Check if unique_queries.json exists
    queries_file = Path('output/unique_queries.json')
    if not queries_file.exists():
        print("\n❌ Error: output/unique_queries.json not found!")
        print("   Address extraction failed. Please check the error above.")
        return
    
    # Load all queries
    with open(queries_file, 'r', encoding='utf-8') as f:
        all_queries = json.load(f)
    
    print(f"\n📊 Total addresses to geocode: {len(all_queries):,}")
    
    # Get initial stats
    initial_stats = get_current_stats()
    if initial_stats:
        print(f"   Already successful: {initial_stats['successful']:,} ({initial_stats['success_rate']:.1f}%)")
        print(f"   Failed/Pending: {initial_stats['failed'] + initial_stats['pending']:,}")
    
    print("\n" + "="*80)
    input("\nPress ENTER to start auto-retry overnight run (or Ctrl+C to cancel)...")
    print("="*80 + "\n")
    
    # Create geocoder
    geocoder = FastBatchGeocoder(
        results_file='output/geocoding_results.json',
        batch_size=50,
        max_concurrent=10,
        checkpoint_interval=250
    )
    
    round_num = 1
    max_rounds = 10  # Safety limit
    last_successful_count = 0
    
    try:
        while round_num <= max_rounds:
            print(f"\n{'='*80}")
            print(f"ROUND {round_num} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*80}\n")
            
            # Get pending queries
            pending = geocoder.get_pending_queries(all_queries)
            
            if not pending:
                print("✅ All addresses geocoded! No pending queries.")
                break
            
            print(f"📍 Attempting {len(pending):,} addresses in this round\n")
            
            # Run geocoding
            round_start = time.time()
            geocoder.geocode_all(all_queries)
            round_duration = time.time() - round_start
            
            # Get stats after round
            stats = get_current_stats()
            
            print(f"\n{'='*80}")
            print(f"ROUND {round_num} COMPLETE")
            print(f"{'='*80}")
            print(f"Duration: {round_duration/60:.1f} minutes")
            print(f"Successful so far: {stats['successful']:,} / {stats['total']:,} ({stats['success_rate']:.1f}%)")
            print(f"Still need to geocode: {stats['failed'] + stats['pending']:,}")
            
            # Check if we made progress
            if stats['successful'] == last_successful_count:
                print("\n⚠️  No new successful geocoding in this round.")
                print("   Most likely all remaining addresses are:")
                print("   - Not found in OpenStreetMap")
                print("   - Permanent API errors")
                print("   - Invalid addresses")
                
                # Check if we have retryable failures
                retryable = load_failed_addresses()
                if not retryable:
                    print("\n✅ No more retryable failures. Stopping.")
                    break
                else:
                    print(f"\n   Still have {len(retryable):,} retryable failures...")
                    print("   Waiting 10 seconds before next attempt...")
                    time.sleep(10)
            else:
                new_successful = stats['successful'] - last_successful_count
                print(f"\n✅ Recovered {new_successful:,} addresses in this round!")
                last_successful_count = stats['successful']
                
                # Check if we're above target success rate
                if stats['success_rate'] >= 85:
                    print(f"\n🎉 SUCCESS! Achieved {stats['success_rate']:.1f}% success rate (target: 85%)")
                    print("   Stopping - excellent result!")
                    break
                
                # Wait a bit before next round
                if round_num < max_rounds:
                    print("\n   Waiting 10 seconds before next round...")
                    time.sleep(10)
            
            round_num += 1
            
            if round_num > max_rounds:
                print(f"\n⚠️  Reached maximum rounds ({max_rounds}). Stopping.")
                break
        
        # Final summary
        final_stats = get_current_stats()
        print(f"\n{'='*80}")
        print("FINAL RESULTS")
        print(f"{'='*80}")
        print(f"Total addresses: {final_stats['total']:,}")
        print(f"✅ Successful: {final_stats['successful']:,} ({final_stats['success_rate']:.1f}%)")
        print(f"❌ Failed: {final_stats['failed']:,}")
        print(f"⏳ Pending: {final_stats['pending']:,}")
        print(f"\nTotal rounds: {round_num - 1}")
        print(f"Results saved to: output/geocoding_results.json")
        print(f"Failed addresses: output/failed_addresses.json")
        
        if final_stats['success_rate'] >= 85:
            print(f"\n🎉 EXCELLENT! Achieved {final_stats['success_rate']:.1f}% success rate!")
        elif final_stats['success_rate'] >= 75:
            print(f"\n✅ GOOD! Achieved {final_stats['success_rate']:.1f}% success rate")
        else:
            print(f"\n⚠️  Success rate: {final_stats['success_rate']:.1f}%")
            print("   Some addresses may not be in OpenStreetMap database")
        
        print("\n💡 Next steps:")
        print("   1. Check results: python geocoding/geocoding_status.py")
        print("   2. Generate map: python preprocessing/prepare_map_data.py")
        print("   3. View map: cd map-app && npm run dev")
        print(f"\n{'='*80}\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        print("✅ Progress saved automatically")
        stats = get_current_stats()
        if stats:
            print(f"   Current success rate: {stats['success_rate']:.1f}%")
            print(f"   Successful: {stats['successful']:,} / {stats['total']:,}")
        print("\n💡 Run this script again to resume from where you left off")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
