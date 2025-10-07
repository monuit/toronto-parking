"""
Geocoding Status Report
Shows detailed status of geocoding progress
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict


def format_timestamp(ts):
    """Format unix timestamp to readable date"""
    if ts:
        return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return 'N/A'


def main():
    print("="*80)
    print("GEOCODING STATUS REPORT")
    print("="*80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check for required files
    queries_file = Path('unique_queries.json')
    results_file = Path('geocoding_results.json')
    failed_file = Path('failed_addresses.json')
    
    if not queries_file.exists():
        print(f"❌ unique_queries.json not found!")
        print(f"   Run 'python prepare_map_data.py' or 'python create_test_queries.py'")
        return
    
    # Load unique queries
    with open(queries_file, 'r', encoding='utf-8') as f:
        all_queries = json.load(f)
    
    total_queries = len(all_queries)
    
    print(f"📊 OVERALL STATISTICS")
    print(f"{'-'*80}")
    print(f"Total unique addresses to geocode: {total_queries:,}")
    
    # Load results if available
    if results_file.exists():
        with open(results_file, 'r', encoding='utf-8') as f:
            results = json.load(f)
        
        geocoded_count = len(results)
        successful = sum(1 for r in results.values() if r.get('lat') is not None)
        failed = geocoded_count - successful
        pending = total_queries - geocoded_count
        
        print(f"\nProgress: {geocoded_count:,} / {total_queries:,} ({geocoded_count/total_queries*100:.1f}%)")
        print(f"  ✅ Successful: {successful:,} ({successful/total_queries*100:.1f}%)")
        print(f"  ❌ Failed: {failed:,} ({failed/total_queries*100:.1f}%)")
        print(f"  ⏳ Pending: {pending:,} ({pending/total_queries*100:.1f}%)")
        
        # Analyze by status
        print(f"\n📋 RESULTS BREAKDOWN")
        print(f"{'-'*80}")
        
        status_counts = defaultdict(int)
        for result in results.values():
            if result.get('lat') is not None:
                status_counts['success'] += 1
            else:
                status = result.get('status', 'unknown')
                status_counts[status] += 1
        
        # Sort by count
        for status, count in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
            pct = count / geocoded_count * 100 if geocoded_count > 0 else 0
            
            if status == 'success':
                icon = '✅'
            elif status == 'no_results':
                icon = '🔍'
            elif 'rate_limit' in status:
                icon = '⚠️ '
            elif 'timeout' in status:
                icon = '⏱️ '
            else:
                icon = '❓'
            
            print(f"  {icon} {status:30s}: {count:6,} ({pct:5.1f}%)")
        
        # Recent activity
        if results:
            timestamps = [r.get('timestamp', 0) for r in results.values() if r.get('timestamp')]
            if timestamps:
                latest = max(timestamps)
                earliest = min(timestamps)
                
                print(f"\n⏰ TIMING")
                print(f"{'-'*80}")
                print(f"First geocoded: {format_timestamp(earliest)}")
                print(f"Last geocoded:  {format_timestamp(latest)}")
                
                duration = latest - earliest
                if duration > 0:
                    rate = geocoded_count / duration
                    print(f"Duration: {duration/60:.1f} minutes")
                    print(f"Average rate: {rate:.2f} addresses/second")
                    
                    if pending > 0:
                        eta_seconds = pending / rate
                        print(f"ETA for completion: {eta_seconds/3600:.1f} hours")
        
        # Top successful addresses
        print(f"\n🎯 SAMPLE SUCCESSFUL GEOCODING")
        print(f"{'-'*80}")
        successful_results = [(q, r) for q, r in results.items() if r.get('lat') is not None]
        for i, (query, result) in enumerate(successful_results[:5], 1):
            lat = result['lat']
            lon = result['lon']
            display = result.get('display_name', 'N/A')
            print(f"{i}. {query}")
            print(f"   → {lat:.6f}, {lon:.6f}")
            if len(display) > 70:
                display = display[:67] + "..."
            print(f"   → {display}\n")
    
    else:
        print(f"\n⚠️  No results file found yet")
        print(f"   Progress: 0 / {total_queries:,} (0.0%)")
        print(f"   Status: Not started")
        print(f"\n💡 To start geocoding, run:")
        print(f"   python run_geocoding.py")
    
    # Failed addresses report
    if failed_file.exists():
        print(f"\n🚫 FAILED ADDRESSES REPORT")
        print(f"{'-'*80}")
        
        with open(failed_file, 'r', encoding='utf-8') as f:
            failed_data = json.load(f)
        
        total_failed = sum(len(v) for v in failed_data.values())
        print(f"Total failed: {total_failed:,}")
        
        for category, items in failed_data.items():
            if category == 'retryable':
                icon = '🔄'
                note = '(Can be retried)'
            elif category == 'no_results':
                icon = '🔍'
                note = '(Not in OpenStreetMap)'
            elif category == 'forbidden':
                icon = '🚫'
                note = '(API access denied)'
            else:
                icon = '❓'
                note = '(Other errors)'
            
            print(f"\n  {icon} {category.upper()}: {len(items):,} addresses {note}")
            
            # Show first few examples
            if items:
                print(f"     Examples:")
                for item in items[:3]:
                    query = item['query']
                    if len(query) > 60:
                        query = query[:57] + "..."
                    print(f"       - {query}")
                
                if len(items) > 3:
                    print(f"       ... and {len(items) - 3:,} more")
        
        # Retry recommendation
        if failed_data.get('retryable'):
            print(f"\n💡 To retry failed addresses, run:")
            print(f"   python retry_failed_geocoding.py")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS")
    print(f"{'-'*80}")
    
    if not results_file.exists():
        print("  1. Start geocoding: python run_geocoding.py")
    elif pending > 0:
        print(f"  1. Continue geocoding: python run_geocoding.py (will resume)")
        if pending > 1000:
            print(f"     ({pending:,} addresses remaining, ~{pending/1.5/60:.0f} minutes)")
    elif failed_file.exists() and failed_data.get('retryable'):
        print(f"  1. Retry failed addresses: python retry_failed_geocoding.py")
    else:
        print("  ✅ Geocoding complete!")
    
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    main()
