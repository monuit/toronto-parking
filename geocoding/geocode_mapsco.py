"""
Geocode.maps.co API Integration for Toronto Parking Tickets
Parallel geocoding with rate limiting (2 requests/second)
"""

import json
import os
import time
import asyncio
import aiohttp
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class GeocodeMapsCoCache:
    """Cache for geocoding results"""
    
    def __init__(self, cache_file='geocode_mapsco_cache.json'):
        self.cache_file = Path(cache_file)
        self.cache = self._load_cache()
        self.hits = 0
        self.misses = 0
        self.modified = False
    
    def _load_cache(self) -> Dict:
        """Load existing cache from disk"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    print(f"📦 Loaded cache: {len(cache_data)} entries")
                    return cache_data
            except Exception as e:
                print(f"⚠️  Warning: Could not load cache: {e}")
        return {}
    
    def save(self):
        """Save cache to disk"""
        if self.modified:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2)
            print(f"💾 Cache saved: {len(self.cache)} entries")
            self.modified = False
    
    def get(self, query: str) -> Optional[Dict]:
        """Get cached result by query string"""
        if query in self.cache:
            self.hits += 1
            return self.cache[query]
        self.misses += 1
        return None
    
    def set(self, query: str, result: Dict):
        """Store result in cache"""
        self.cache[query] = result
        self.modified = True
    
    def get_stats(self) -> str:
        """Get cache statistics"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return f"Cache: {len(self.cache)} entries | Hits: {self.hits} | Misses: {self.misses} | Hit rate: {hit_rate:.1f}%"


class GeocodeMapsCoGeocoder:
    """Geocoder using geocode.maps.co API with parallel requests"""
    
    def __init__(self, cache: GeocodeMapsCoCache, api_key: Optional[str] = None, rate_limit: float = 2.0):
        """
        Initialize geocoder
        
        Args:
            cache: GeocodeMapsCoCache instance
            api_key: API key (reads from env if not provided)
            rate_limit: Requests per second (default 2.0)
        """
        self.cache = cache
        self.api_key = api_key or os.getenv('GEOCODE_MAPS_CO_API_KEY', '68e35b92c4aa0836068625vlcd9bb74')
        self.base_url = "https://geocode.maps.co/search"
        self.rate_limit = rate_limit
        self.request_interval = 1.0 / rate_limit  # Seconds between requests
        self.requests_made = 0
        self.addresses_geocoded = 0
        self.semaphore = None
    
    async def geocode_single(self, session: aiohttp.ClientSession, query: str) -> Optional[Tuple[float, float]]:
        """
        Geocode a single address
        
        Returns:
            (lat, lon) tuple or None if not found
        """
        # Check cache first
        cached = self.cache.get(query)
        if cached and cached.get('lat') and cached.get('lon'):
            return (cached['lat'], cached['lon'])
        
        # Rate limiting via semaphore
        async with self.semaphore:
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
                            # Get first (best) result
                            best_result = results[0]
                            lat = float(best_result['lat'])
                            lon = float(best_result['lon'])
                            
                            # Cache the result
                            self.cache.set(query, {
                                'lat': lat,
                                'lon': lon,
                                'query': query,
                                'display_name': best_result.get('display_name', ''),
                                'type': best_result.get('type', ''),
                                'importance': best_result.get('importance', 0)
                            })
                            
                            self.addresses_geocoded += 1
                            return (lat, lon)
                        else:
                            # No results - cache as null
                            self.cache.set(query, {'lat': None, 'lon': None, 'query': query})
                            return None
                    
                    elif response.status == 429:
                        # Rate limit exceeded - log but don't retry recursively
                        print(f"⚠️  Rate limit hit for: {query[:50]}...")
                        # Cache as null to avoid retrying
                        self.cache.set(query, {'lat': None, 'lon': None, 'query': query, 'error': 'rate_limit'})
                        return None
                    
                    elif response.status == 403:
                        print(f"❌ API access forbidden (403) - check API key")
                        return None
                    
                    else:
                        print(f"⚠️  HTTP {response.status} for: {query}")
                        return None
                
            except asyncio.TimeoutError:
                print(f"⏱️  Timeout for: {query}")
                return None
            except Exception as e:
                print(f"❌ Error geocoding '{query}': {e}")
                return None
            
            finally:
                # Delay between requests to respect rate limit
                await asyncio.sleep(self.request_interval)
    
    async def geocode_batch_async(self, queries: List[str]) -> Dict[str, Tuple[float, float]]:
        """
        Geocode multiple addresses in parallel (respecting rate limit)
        
        Args:
            queries: List of address queries
        
        Returns:
            Dict mapping query -> (lat, lon)
        """
        # Filter out cached queries
        uncached_queries = []
        results = {}
        
        print(f"\n🔍 Checking cache for {len(queries):,} queries...")
        for query in queries:
            cached = self.cache.get(query)
            if cached and cached.get('lat') and cached.get('lon'):
                results[query] = (cached['lat'], cached['lon'])
            else:
                uncached_queries.append(query)
        
        print(f"✅ Cache hits: {len(results):,}")
        print(f"📍 Need to geocode: {len(uncached_queries):,}")
        
        if not uncached_queries:
            print("🎉 All addresses found in cache!")
            return results
        
        # Create semaphore to limit concurrent requests (rate limiting)
        self.semaphore = asyncio.Semaphore(int(self.rate_limit))
        
        # Process in parallel with rate limiting
        print(f"\n🚀 Starting parallel geocoding ({self.rate_limit} req/sec)...")
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            tasks = [self.geocode_single(session, query) for query in uncached_queries]
            
            # Process with progress reporting
            completed = 0
            total = len(tasks)
            
            for coro in asyncio.as_completed(tasks):
                coords = await coro
                completed += 1
                
                # Get the query from uncached_queries by index
                query = uncached_queries[completed - 1]
                if coords:
                    results[query] = coords
                
                # Progress reporting every 100 addresses
                if completed % 100 == 0 or completed == total:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed if elapsed > 0 else 0
                    eta = (total - completed) / rate if rate > 0 else 0
                    
                    print(f"  Progress: {completed:,}/{total:,} "
                          f"({completed/total*100:.1f}%) | "
                          f"Rate: {rate:.1f} req/s | "
                          f"ETA: {eta/60:.1f} min | "
                          f"Geocoded: {self.addresses_geocoded:,}")
                
                # Save cache periodically
                if completed % 500 == 0:
                    self.cache.save()
        
        # Final save
        self.cache.save()
        
        elapsed = time.time() - start_time
        
        print(f"\n✅ Geocoding complete!")
        print(f"   Total requests: {self.requests_made:,}")
        print(f"   Addresses geocoded: {self.addresses_geocoded:,}")
        print(f"   Success rate: {len(results) / len(queries) * 100:.1f}%")
        print(f"   Time elapsed: {elapsed/60:.1f} minutes")
        print(f"   {self.cache.get_stats()}")
        
        return results
    
    def geocode_batch(self, queries: List[str]) -> Dict[str, Tuple[float, float]]:
        """Synchronous wrapper for async batch geocoding"""
        # Check if we're already in an event loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop running, create one
            return asyncio.run(self.geocode_batch_async(queries))
        else:
            # Already in an event loop, use new_event_loop
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self.geocode_batch_async(queries))
            finally:
                loop.close()


def test_geocoding_sync():
    """Test geocoding with 4 sample addresses"""
    print("="*60)
    print("Testing geocode.maps.co API")
    print("="*60)
    
    # Test addresses
    test_queries = [
        "4700 KEELE ST, Toronto, ON, Canada",
        "20 EDWARD ST, Toronto, ON, Canada",
        "LOWTHER AVE and HURON ST, Toronto, ON, Canada",
        "2075 BAYVIEW AVE, Toronto, ON, Canada"
    ]
    
    print(f"\n📍 Testing with {len(test_queries)} addresses:")
    for i, query in enumerate(test_queries, 1):
        print(f"  {i}. {query}")
    
    # Initialize geocoder (use 1.5 req/sec to be safe)
    cache = GeocodeMapsCoCache('test_geocode_mapsco_cache.json')
    geocoder = GeocodeMapsCoGeocoder(cache, rate_limit=1.5)
    
    # Geocode
    results = geocoder.geocode_batch(test_queries)
    
    # Display results
    print("\n📊 Results:")
    for query, coords in results.items():
        if coords:
            print(f"✅ {query}")
            print(f"   → Lat: {coords[0]:.6f}, Lon: {coords[1]:.6f}")
        else:
            print(f"❌ {query} - No results found")
    
    cache.save()


if __name__ == "__main__":
    test_geocoding_sync()
