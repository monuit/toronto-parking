"""
Robust Parallel Geocoding with Checkpointing and Resume Support
Handles large-scale geocoding with automatic progress saving
"""

import json
import asyncio
import aiohttp
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
import os
from dotenv import load_dotenv

load_dotenv()


class RobustGeocoder:
    """Geocoder with checkpointing, resume support, and parallel processing"""
    
    def __init__(self, 
                 api_key: str,
                 cache_file: str = 'geocoding_results.json',
                 checkpoint_interval: int = 100,
                 rate_limit: float = 1.5):
        """
        Initialize robust geocoder
        
        Args:
            api_key: geocode.maps.co API key
            cache_file: File to store geocoding results
            checkpoint_interval: Save progress every N addresses
            rate_limit: Requests per second
        """
        self.api_key = api_key
        self.cache_file = Path(cache_file)
        self.checkpoint_interval = checkpoint_interval
        self.rate_limit = rate_limit
        self.request_interval = 1.0 / rate_limit
        
        # Load existing results
        self.results = self._load_results()
        self.base_url = "https://geocode.maps.co/search"
        
        # Statistics
        self.requests_made = 0
        self.success_count = 0
        self.fail_count = 0
        self.cache_hits = 0
        self.start_time = None
    
    def _load_results(self) -> Dict:
        """Load existing geocoding results from disk"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    results = json.load(f)
                    print(f"📦 Loaded existing results: {len(results)} addresses")
                    return results
            except Exception as e:
                print(f"⚠️  Could not load results: {e}")
        return {}
    
    def save_results(self, force: bool = False):
        """Save results to disk"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2)
            if force:
                print(f"💾 Checkpoint saved: {len(self.results)} addresses")
        except Exception as e:
            print(f"❌ Error saving results: {e}")
    
    def get_pending_queries(self, all_queries: List[str]) -> List[str]:
        """Get list of queries that haven't been geocoded yet"""
        pending = []
        for query in all_queries:
            if query not in self.results:
                pending.append(query)
            else:
                # Check if previous result was successful
                result = self.results[query]
                if result.get('status') == 'pending' or result.get('lat') is None:
                    pending.append(query)
                else:
                    self.cache_hits += 1
        
        return pending
    
    async def geocode_single(self, session: aiohttp.ClientSession, query: str, semaphore: asyncio.Semaphore, max_retries: int = 3) -> Dict:
        """
        Geocode a single address with retry logic for 429/503 errors
        
        Returns:
            Dict with geocoding result
        """
        retry_count = 0
        
        while retry_count <= max_retries:
            async with semaphore:
                try:
                    # Build URL
                    encoded_query = quote(query)
                    url = f"{self.base_url}?q={encoded_query}&api_key={self.api_key}"
                    
                    # Make request
                    async with session.get(url, timeout=30) as response:
                        self.requests_made += 1
                        
                        if response.status == 200:
                            results = await response.json()
                            
                            if results and len(results) > 0:
                                # Success - get first result
                                best_result = results[0]
                                lat = float(best_result['lat'])
                                lon = float(best_result['lon'])
                                
                                self.success_count += 1
                                
                                return {
                                    'query': query,
                                    'lat': lat,
                                    'lon': lon,
                                    'display_name': best_result.get('display_name', ''),
                                    'type': best_result.get('type', ''),
                                    'importance': best_result.get('importance', 0),
                                    'status': 'success',
                                    'timestamp': time.time()
                                }
                            else:
                                # No results found
                                self.fail_count += 1
                                return {
                                    'query': query,
                                    'lat': None,
                                    'lon': None,
                                    'status': 'no_results',
                                    'timestamp': time.time()
                                }
                        
                        elif response.status == 429:
                            # Rate limit - retry after delay
                            retry_count += 1
                            if retry_count <= max_retries:
                                wait_time = 2 ** retry_count  # Exponential backoff: 2, 4, 8 seconds
                                print(f"⚠️  Rate limit (429) for query, retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue  # Retry
                            else:
                                # Max retries exceeded
                                self.fail_count += 1
                                return {
                                    'query': query,
                                    'lat': None,
                                    'lon': None,
                                    'status': 'rate_limit_max_retries',
                                    'timestamp': time.time()
                                }
                        
                        elif response.status == 503:
                            # Service unavailable - retry after delay
                            retry_count += 1
                            if retry_count <= max_retries:
                                wait_time = 3 ** retry_count  # Exponential backoff: 3, 9, 27 seconds
                                print(f"⚠️  Service unavailable (503), retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                                await asyncio.sleep(wait_time)
                                continue  # Retry
                            else:
                                # Max retries exceeded
                                self.fail_count += 1
                                return {
                                    'query': query,
                                    'lat': None,
                                    'lon': None,
                                    'status': 'service_unavailable',
                                    'timestamp': time.time()
                                }
                        
                        elif response.status == 403:
                            # Forbidden - don't retry
                            self.fail_count += 1
                            return {
                                'query': query,
                                'lat': None,
                                'lon': None,
                                'status': 'forbidden',
                                'timestamp': time.time()
                            }
                        
                        else:
                            # Other error - don't retry
                            self.fail_count += 1
                            return {
                                'query': query,
                                'lat': None,
                                'lon': None,
                                'status': f'http_{response.status}',
                                'timestamp': time.time()
                            }
                    
                except asyncio.TimeoutError:
                    # Timeout - retry
                    retry_count += 1
                    if retry_count <= max_retries:
                        print(f"⏱️  Timeout for query, retrying... (attempt {retry_count}/{max_retries})")
                        await asyncio.sleep(1)
                        continue
                    else:
                        self.fail_count += 1
                        return {
                            'query': query,
                            'lat': None,
                            'lon': None,
                            'status': 'timeout',
                            'timestamp': time.time()
                        }
                except Exception as e:
                    self.fail_count += 1
                    return {
                        'query': query,
                        'lat': None,
                        'lon': None,
                        'status': 'error',
                        'error_message': str(e),
                        'timestamp': time.time()
                    }
                finally:
                    # Rate limiting delay (only if not retrying)
                    if retry_count == 0 or retry_count > max_retries:
                        await asyncio.sleep(self.request_interval)
    
    async def geocode_batch(self, queries: List[str]) -> Dict[str, Dict]:
        """
        Geocode multiple addresses in parallel with checkpointing
        
        Returns:
            Dict mapping query -> result dict
        """
        # Get queries that need geocoding
        pending = self.get_pending_queries(queries)
        
        print(f"\n{'='*60}")
        print(f"Geocoding Status")
        print(f"{'='*60}")
        print(f"Total queries: {len(queries):,}")
        print(f"Already geocoded: {len(queries) - len(pending):,}")
        print(f"Pending: {len(pending):,}")
        print(f"{'='*60}\n")
        
        if not pending:
            print("🎉 All addresses already geocoded!")
            return self.results
        
        # Create semaphore for rate limiting
        semaphore = asyncio.Semaphore(int(self.rate_limit))
        
        self.start_time = time.time()
        completed = 0
        
        async with aiohttp.ClientSession() as session:
            # Process in batches for checkpointing
            batch_size = self.checkpoint_interval
            
            for batch_start in range(0, len(pending), batch_size):
                batch_end = min(batch_start + batch_size, len(pending))
                batch = pending[batch_start:batch_end]
                
                print(f"🔄 Processing batch {batch_start//batch_size + 1} "
                      f"(queries {batch_start+1:,} to {batch_end:,})")
                
                # Create tasks for this batch
                tasks = [self.geocode_single(session, query, semaphore) for query in batch]
                
                # Process batch
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    query = result['query']
                    
                    # Store result
                    self.results[query] = result
                    completed += 1
                    
                    # Progress update every 50 addresses
                    if completed % 50 == 0:
                        self._print_progress(completed, len(pending))
                
                # Checkpoint save after each batch
                self.save_results(force=True)
                
                # Show batch summary
                print(f"  ✅ Batch complete: {len(batch)} addresses processed\n")
        
        # Final progress and save
        self._print_progress(len(pending), len(pending))
        self.save_results(force=True)
        
        # Final statistics
        elapsed = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"Geocoding Complete!")
        print(f"{'='*60}")
        print(f"Total queries processed: {len(pending):,}")
        print(f"Successful: {self.success_count:,}")
        print(f"Failed: {self.fail_count:,}")
        print(f"Cache hits: {self.cache_hits:,}")
        print(f"API requests made: {self.requests_made:,}")
        print(f"Success rate: {self.success_count / len(pending) * 100:.1f}%")
        print(f"Time elapsed: {elapsed/60:.1f} minutes")
        print(f"Average rate: {self.requests_made / elapsed:.2f} req/s")
        print(f"Results saved to: {self.cache_file}")
        print(f"{'='*60}\n")
        
        # Save failed addresses for retry
        self._save_failed_addresses()
        
        return self.results
    
    def _save_failed_addresses(self):
        """Save failed addresses to a separate file for retry"""
        failed = {}
        retryable_statuses = ['rate_limit_max_retries', 'timeout', 'service_unavailable', 'error', 'http_503', 'http_500']
        
        for query, result in self.results.items():
            if result.get('lat') is None:
                status = result.get('status', 'unknown')
                
                # Categorize failures
                if status in retryable_statuses:
                    if 'retryable' not in failed:
                        failed['retryable'] = []
                    failed['retryable'].append({
                        'query': query,
                        'status': status,
                        'timestamp': result.get('timestamp')
                    })
                elif status == 'no_results':
                    if 'no_results' not in failed:
                        failed['no_results'] = []
                    failed['no_results'].append({
                        'query': query,
                        'status': status,
                        'timestamp': result.get('timestamp')
                    })
                elif status == 'forbidden':
                    if 'forbidden' not in failed:
                        failed['forbidden'] = []
                    failed['forbidden'].append({
                        'query': query,
                        'status': status,
                        'timestamp': result.get('timestamp')
                    })
                else:
                    if 'other' not in failed:
                        failed['other'] = []
                    failed['other'].append({
                        'query': query,
                        'status': status,
                        'timestamp': result.get('timestamp')
                    })
        
        if failed:
            failed_file = Path('failed_addresses.json')
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed, f, indent=2)
            
            # Print summary
            print(f"📋 Failed Addresses Summary:")
            print(f"{'='*60}")
            
            if 'retryable' in failed:
                print(f"⚠️  Retryable errors: {len(failed['retryable']):,} addresses")
                print(f"   (rate limits, timeouts, service errors)")
            
            if 'no_results' in failed:
                print(f"❌ No results found: {len(failed['no_results']):,} addresses")
                print(f"   (addresses not in OpenStreetMap)")
            
            if 'forbidden' in failed:
                print(f"🚫 Forbidden: {len(failed['forbidden']):,} addresses")
                print(f"   (API access denied)")
            
            if 'other' in failed:
                print(f"❓ Other errors: {len(failed['other']):,} addresses")
            
            print(f"\n💾 Failed addresses saved to: {failed_file}")
            
            if 'retryable' in failed:
                print(f"\n💡 To retry failed addresses, run:")
                print(f"   python retry_failed_geocoding.py")
            
            print(f"{'='*60}\n")
        else:
            print(f"✅ No failed addresses to save!\n")
    
    def _print_progress(self, completed: int, total: int):
        """Print progress statistics"""
        if self.start_time is None:
            return
        
        elapsed = time.time() - self.start_time
        rate = self.requests_made / elapsed if elapsed > 0 else 0
        eta = (total - completed) / rate if rate > 0 else 0
        pct = completed / total * 100 if total > 0 else 0
        
        print(f"  📊 Progress: {completed:,}/{total:,} ({pct:.1f}%) | "
              f"Success: {self.success_count:,} | "
              f"Failed: {self.fail_count:,} | "
              f"Rate: {rate:.2f} req/s | "
              f"ETA: {eta/60:.1f} min")
    
    def run(self, queries: List[str]) -> Dict[str, Dict]:
        """Synchronous wrapper"""
        return asyncio.run(self.geocode_batch(queries))


def main():
    """Main geocoding pipeline"""
    print("="*60)
    print("Robust Parallel Geocoding Pipeline")
    print("="*60)
    
    # Load API key
    api_key = os.getenv('GEOCODE_MAPS_CO_API_KEY', '68e35b92c4aa0836068625vlcd9bb74')
    
    # Check if unique_queries.json exists
    queries_file = Path('unique_queries.json')
    if not queries_file.exists():
        print(f"❌ Error: {queries_file} not found!")
        print("   Run prepare_map_data.py first to extract unique addresses")
        return
    
    # Load unique queries
    with open(queries_file, 'r', encoding='utf-8') as f:
        all_queries = json.load(f)
    
    print(f"\n📍 Loaded {len(all_queries):,} unique addresses to geocode")
    
    # Initialize geocoder
    geocoder = RobustGeocoder(
        api_key=api_key,
        cache_file='geocoding_results.json',
        checkpoint_interval=100,  # Save every 100 addresses
        rate_limit=1.5  # 1.5 requests per second to be safe
    )
    
    # Run geocoding
    results = geocoder.run(all_queries)
    
    # Generate summary report
    successful = sum(1 for r in results.values() if r.get('lat') is not None)
    failed = len(results) - successful
    
    print(f"\n📊 Final Summary:")
    print(f"   Total addresses: {len(results):,}")
    print(f"   Successfully geocoded: {successful:,} ({successful/len(results)*100:.1f}%)")
    print(f"   Failed: {failed:,} ({failed/len(results)*100:.1f}%)")
    
    # Analyze failures
    if failed > 0:
        failure_types = {}
        for result in results.values():
            if result.get('lat') is None:
                status = result.get('status', 'unknown')
                failure_types[status] = failure_types.get(status, 0) + 1
        
        print(f"\n   Failure breakdown:")
        for status, count in sorted(failure_types.items(), key=lambda x: x[1], reverse=True):
            print(f"     - {status}: {count:,}")


if __name__ == "__main__":
    main()
