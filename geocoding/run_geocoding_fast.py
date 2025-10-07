"""
Fast Batch Geocoding with geocode.maps.co API
Processes addresses in bulk batches for maximum speed
"""

import json
import asyncio
import aiohttp
import time
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os

# Load API key
load_dotenv()
API_KEY = os.getenv('GEOCODE_MAPS_CO_API_KEY')

if not API_KEY:
    raise ValueError("GEOCODE_MAPS_CO_API_KEY not found in .env file")


class FastBatchGeocoder:
    """Fast batch geocoder with aggressive parallel processing"""
    
    def __init__(self, 
                 results_file='geocoding_results.json',
                 batch_size=50,  # Process 50 at a time
                 max_concurrent=10,  # 10 concurrent requests
                 checkpoint_interval=500):  # Save every 500
        
        self.results_file = Path(results_file)
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self.checkpoint_interval = checkpoint_interval
        
        # Load existing results
        self.results = self._load_results()
        
        # Stats
        self.start_time = None
        self.processed_count = 0
        self.success_count = 0
        self.failed_count = 0
    
    def _load_results(self):
        """Load existing geocoding results"""
        if self.results_file.exists():
            with open(self.results_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            print(f"📂 Loaded {len(results):,} existing results")
            return results
        return {}
    
    def _save_results(self):
        """Save results to file"""
        with open(self.results_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
    
    def get_pending_queries(self, all_queries):
        """Get queries that haven't been geocoded yet"""
        pending = []
        
        for query in all_queries:
            if query not in self.results:
                pending.append(query)
            elif self.results[query].get('lat') is None:
                # Re-queue failed queries
                pending.append(query)
        
        return pending
    
    async def geocode_single(self, session, query, semaphore):
        """Geocode a single address with retry logic"""
        async with semaphore:
            url = f"https://geocode.maps.co/search"
            params = {
                'q': query,
                'api_key': API_KEY
            }
            
            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            if data and len(data) > 0:
                                result = data[0]
                                return {
                                    'query': query,
                                    'lat': float(result['lat']),
                                    'lon': float(result['lon']),
                                    'display_name': result.get('display_name'),
                                    'status': 'success',
                                    'timestamp': time.time()
                                }
                            else:
                                return {
                                    'query': query,
                                    'lat': None,
                                    'lon': None,
                                    'status': 'no_results',
                                    'timestamp': time.time()
                                }
                        
                        elif response.status == 429:
                            if attempt < max_retries:
                                wait = 1 * (attempt + 1)
                                await asyncio.sleep(wait)
                                continue
                            return {
                                'query': query,
                                'lat': None,
                                'lon': None,
                                'status': 'rate_limit',
                                'timestamp': time.time()
                            }
                        
                        else:
                            return {
                                'query': query,
                                'lat': None,
                                'lon': None,
                                'status': f'http_{response.status}',
                                'timestamp': time.time()
                            }
                
                except asyncio.TimeoutError:
                    if attempt < max_retries:
                        await asyncio.sleep(0.5)
                        continue
                    return {
                        'query': query,
                        'lat': None,
                        'lon': None,
                        'status': 'timeout',
                        'timestamp': time.time()
                    }
                
                except Exception as e:
                    return {
                        'query': query,
                        'lat': None,
                        'lon': None,
                        'status': f'error_{type(e).__name__}',
                        'timestamp': time.time()
                    }
    
    async def geocode_batch_parallel(self, queries):
        """Geocode a batch of queries in parallel"""
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent * 2)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [self.geocode_single(session, query, semaphore) for query in queries]
            results = await asyncio.gather(*tasks)
            return results
    
    def geocode_all(self, all_queries):
        """Geocode all queries with fast batch processing"""
        pending = self.get_pending_queries(all_queries)
        
        if not pending:
            print("✅ All queries already geocoded!")
            return self.results
        
        print(f"\n🚀 FAST BATCH GEOCODING")
        print(f"{'='*80}")
        print(f"Total queries: {len(all_queries):,}")
        print(f"Already done: {len(all_queries) - len(pending):,}")
        print(f"To process: {len(pending):,}")
        print(f"Batch size: {self.batch_size}")
        print(f"Max concurrent: {self.max_concurrent}")
        print(f"Expected time: ~{len(pending) / (self.max_concurrent * 0.5) / 60:.1f} minutes")
        print(f"{'='*80}\n")
        
        self.start_time = time.time()
        
        # Process in batches
        for batch_start in range(0, len(pending), self.batch_size):
            batch_end = min(batch_start + self.batch_size, len(pending))
            batch_queries = pending[batch_start:batch_end]
            batch_num = (batch_start // self.batch_size) + 1
            total_batches = (len(pending) + self.batch_size - 1) // self.batch_size
            
            print(f"🔄 Processing batch {batch_num}/{total_batches} (queries {batch_start + 1} to {batch_end})")
            
            # Process batch
            batch_start_time = time.time()
            results = asyncio.run(self.geocode_batch_parallel(batch_queries))
            batch_duration = time.time() - batch_start_time
            
            # Update results
            batch_success = 0
            batch_failed = 0
            
            for result in results:
                query = result['query']
                self.results[query] = result
                
                if result.get('lat') is not None:
                    batch_success += 1
                    self.success_count += 1
                else:
                    batch_failed += 1
                    self.failed_count += 1
                
                self.processed_count += 1
            
            # Calculate stats
            total_duration = time.time() - self.start_time
            rate = self.processed_count / total_duration if total_duration > 0 else 0
            remaining = len(pending) - self.processed_count
            eta = remaining / rate / 60 if rate > 0 else 0
            
            print(f"  ✅ Batch {batch_num}: {batch_success} success, {batch_failed} failed in {batch_duration:.1f}s")
            print(f"  📊 Overall: {self.processed_count:,}/{len(pending):,} ({self.processed_count/len(pending)*100:.1f}%) | "
                  f"Success: {self.success_count:,} | Failed: {self.failed_count:,} | "
                  f"Rate: {rate:.1f} req/s | ETA: {eta:.1f} min\n")
            
            # Checkpoint
            if self.processed_count % self.checkpoint_interval == 0:
                self._save_results()
                print(f"  💾 Checkpoint saved at {self.processed_count:,} queries\n")
            
            # Small delay between batches to avoid rate limiting
            time.sleep(0.5)
        
        # Final save
        self._save_results()
        
        # Final stats
        total_time = time.time() - self.start_time
        print(f"\n{'='*80}")
        print(f"✅ GEOCODING COMPLETE!")
        print(f"{'='*80}")
        print(f"Total processed: {self.processed_count:,}")
        print(f"  ✅ Successful: {self.success_count:,} ({self.success_count/self.processed_count*100:.1f}%)")
        print(f"  ❌ Failed: {self.failed_count:,} ({self.failed_count/self.processed_count*100:.1f}%)")
        print(f"Total time: {total_time/60:.1f} minutes")
        print(f"Average rate: {self.processed_count/total_time:.1f} queries/second")
        print(f"Results saved to: {self.results_file}")
        print(f"{'='*80}\n")
        
        # Save failed addresses
        self._save_failed_addresses()
        
        return self.results
    
    def _save_failed_addresses(self):
        """Save failed addresses for retry"""
        failed = {
            'retryable': [],
            'no_results': [],
            'forbidden': [],
            'other': []
        }
        
        retryable_statuses = ['rate_limit', 'timeout', 'error', 'http_503', 'http_500']
        
        for query, result in self.results.items():
            if result.get('lat') is None:
                status = result.get('status', 'unknown')
                
                item = {'query': query, 'status': status}
                
                if any(s in status for s in retryable_statuses):
                    failed['retryable'].append(item)
                elif status == 'no_results':
                    failed['no_results'].append(item)
                elif status == 'forbidden' or 'http_403' in status:
                    failed['forbidden'].append(item)
                else:
                    failed['other'].append(item)
        
        if sum(len(v) for v in failed.values()) > 0:
            failed_file = Path('failed_addresses.json')
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed, f, indent=2, ensure_ascii=False)
            
            print(f"🚫 Failed addresses saved to: {failed_file}")
            print(f"  ⚠️  Retryable: {len(failed['retryable'])}")
            print(f"  🔍 No results: {len(failed['no_results'])}")
            print(f"  🚫 Forbidden: {len(failed['forbidden'])}")
            print(f"  ❓ Other: {len(failed['other'])}")
            
            if failed['retryable']:
                print(f"\n💡 To retry failed addresses: python retry_failed_geocoding.py\n")


def main():
    """Main entry point"""
    # Load queries
    queries_file = Path('unique_queries.json')
    
    if not queries_file.exists():
        print(f"❌ {queries_file} not found!")
        print(f"   Run 'python prepare_map_data.py' first to extract unique queries")
        return
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        all_queries = json.load(f)
    
    print(f"📂 Loaded {len(all_queries):,} unique queries from {queries_file}")
    
    # Create geocoder
    geocoder = FastBatchGeocoder(
        batch_size=50,  # Process 50 at a time
        max_concurrent=10,  # 10 concurrent requests
        checkpoint_interval=500  # Save every 500
    )
    
    # Run geocoding
    try:
        geocoder.geocode_all(all_queries)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        geocoder._save_results()
        print(f"✅ Progress saved to {geocoder.results_file}")
        print(f"   Processed: {geocoder.processed_count:,}")
        print(f"   You can resume by running this script again")


if __name__ == "__main__":
    main()
