"""
Geocodio-based Geocoding Module for Toronto Parking Tickets
Fast batch geocoding with caching
"""

import pandas as pd
import json
import os
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import hashlib
from collections import defaultdict
from dotenv import load_dotenv
import requests

# Load environment variables
load_dotenv()

class AddressParser:
    """Parse location1-4 fields into geocodable addresses"""
    
    @staticmethod
    def parse_location(row) -> Tuple[Optional[str], Optional[str]]:
        """
        Parse location fields into (main_street, cross_street) tuple
        
        Examples:
        - location1='N/S', location2='LOWTHER AVE', location3='E/O', location4='HURON ST'
          -> ('LOWTHER AVE', 'HURON ST')
        - location1='AT', location2='4700 KEELE ST', location3='', location4=''
          -> ('4700 KEELE ST', None)
        """
        main_street = None
        cross_street = None
        
        # location2 is always the main street/address
        if pd.notna(row.get('location2', '')) and str(row['location2']).strip():
            main_street = str(row['location2']).strip()
        
        # location4 is cross street (if location3 is a proximity indicator)
        if pd.notna(row.get('location4', '')) and str(row['location4']).strip():
            cross_street = str(row['location4']).strip()
        
        return main_street, cross_street
    
    @staticmethod
    def construct_geocoding_query(main_street: str, cross_street: Optional[str] = None) -> str:
        """
        Construct geocoding query optimized for Geocodio
        
        Format: "MAIN_STREET, Toronto, ON, Canada" or 
                "MAIN_STREET and CROSS_STREET, Toronto, ON, Canada"
        """
        if cross_street:
            query = f"{main_street} and {cross_street}, Toronto, ON, Canada"
        else:
            query = f"{main_street}, Toronto, ON, Canada"
        
        return query
    
    @staticmethod
    def get_query_hash(query: str) -> str:
        """Generate hash for caching"""
        return hashlib.md5(query.encode()).hexdigest()


class GeocodingCache:
    """Persistent cache for geocoding results"""
    
    def __init__(self, cache_file='geocoding_cache.json'):
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
                    print(f"Loaded cache: {len(cache_data)} entries")
                    return cache_data
            except Exception as e:
                print(f"Warning: Could not load cache: {e}")
        return {}
    
    def save(self):
        """Save cache to disk"""
        if self.modified:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2)
            print(f"Cache saved: {len(self.cache)} entries")
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


class GeocodioGeocoder:
    """Batch geocoder using Geocodio API"""
    
    def __init__(self, cache: GeocodingCache, api_key: Optional[str] = None):
        """
        Initialize geocoder
        
        Args:
            cache: GeocodingCache instance
            api_key: Geocodio API key (reads from env if not provided)
        """
        self.cache = cache
        self.api_key = api_key or os.getenv('GEOCODIO_API_KEY')
        
        if not self.api_key:
            raise ValueError("Geocodio API key not found. Set GEOCODIO_API_KEY in .env file")
        
        self.base_url = "https://api.geocod.io/v1.9/geocode"
        self.session = requests.Session()
        self.requests_made = 0
        self.addresses_geocoded = 0
    
    def batch_geocode(self, queries: List[str], batch_size: int = 10000) -> Dict[str, Tuple[float, float]]:
        """
        Batch geocode addresses using Geocodio
        
        Args:
            queries: List of address queries
            batch_size: Max addresses per batch (Geocodio limit is 10,000)
        
        Returns:
            Dict mapping query -> (lat, lon)
        """
        # Filter out cached queries
        uncached_queries = []
        results = {}
        
        print(f"\nChecking cache for {len(queries)} queries...")
        for query in queries:
            cached = self.cache.get(query)
            if cached and cached.get('lat') and cached.get('lon'):
                results[query] = (cached['lat'], cached['lon'])
            else:
                uncached_queries.append(query)
        
        print(f"Cache hits: {len(results)} | Need to geocode: {len(uncached_queries)}")
        
        if not uncached_queries:
            print("All addresses found in cache!")
            return results
        
        # Process in batches
        total_batches = (len(uncached_queries) + batch_size - 1) // batch_size
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(uncached_queries))
            batch = uncached_queries[start_idx:end_idx]
            
            print(f"\n📍 Processing batch {batch_num + 1}/{total_batches} ({len(batch)} addresses)...")
            
            try:
                # Geocodio batch request via REST API
                response = self.session.post(
                    self.base_url,
                    params={'api_key': self.api_key},
                    json=batch,
                    timeout=600  # 10 minute timeout for large batches
                )
                response.raise_for_status()
                self.requests_made += 1
                
                # Process results
                data = response.json()
                
                for result in data['results']:
                    query = result['query']
                    
                    if result['response']['results']:
                        # Get first (best) result
                        best_result = result['response']['results'][0]
                        lat = best_result['location']['lat']
                        lon = best_result['location']['lng']
                        
                        # Store in results and cache
                        results[query] = (lat, lon)
                        self.cache.set(query, {
                            'lat': lat,
                            'lon': lon,
                            'query': query,
                            'formatted_address': best_result['formatted_address'],
                            'accuracy': best_result.get('accuracy', None),
                            'accuracy_type': best_result.get('accuracy_type', None)
                        })
                        self.addresses_geocoded += 1
                    else:
                        # No results found - cache as null
                        self.cache.set(query, {'lat': None, 'lon': None, 'query': query})
                
                print(f"  ✅ Geocoded {len(batch)} addresses | Total: {self.addresses_geocoded}")
                
                # Save cache after each batch
                self.cache.save()
                
            except Exception as e:
                print(f"  ❌ Error in batch {batch_num + 1}: {e}")
                import traceback
                traceback.print_exc()
                # Continue with next batch
        
        print(f"\n✅ Geocoding complete!")
        print(f"   Total API requests: {self.requests_made}")
        print(f"   Addresses geocoded: {self.addresses_geocoded}")
        print(f"   Success rate: {len(results) / len(queries) * 100:.1f}%")
        print(f"   {self.cache.get_stats()}")
        
        return results


def create_address_lookup_table(tickets_df: pd.DataFrame) -> Tuple[Dict, Dict]:
    """
    Create lookup table of unique location combinations
    
    Returns:
        Tuple of (location_details, location_frequencies)
        - location_details: Dict mapping location_key -> (main_street, cross_street)
        - location_frequencies: Dict mapping location_key -> count
    """
    print("\n" + "="*60)
    print("Analyzing unique locations in dataset...")
    print("="*60)
    
    location_counts = defaultdict(int)
    location_details = {}
    
    for _, row in tickets_df.iterrows():
        main_street, cross_street = AddressParser.parse_location(row)
        
        if main_street:
            # Create key from location fields
            key = f"{row.get('location1', '')}|{row.get('location2', '')}|{row.get('location3', '')}|{row.get('location4', '')}"
            location_counts[key] += 1
            
            if key not in location_details:
                location_details[key] = (main_street, cross_street)
    
    print(f"\n✅ Found {len(location_counts):,} unique location combinations")
    
    # Sort by frequency for better caching
    sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Show top locations
    print("\n📊 Top 10 most common locations:")
    for i, (key, count) in enumerate(sorted_locations[:10], 1):
        main, cross = location_details[key]
        cross_str = f" and {cross}" if cross else ""
        pct = count / len(tickets_df) * 100
        print(f"  {i:2d}. {main}{cross_str}")
        print(f"      → {count:,} tickets ({pct:.2f}%)")
    
    return location_details, dict(sorted_locations)


if __name__ == "__main__":
    # Test with sample data
    print("="*60)
    print("Testing Geocodio Geocoding Module")
    print("="*60)
    
    # Create sample DataFrame
    sample_data = {
        'location1': ['N/S', 'AT', 'E/S', 'N/S', 'AT'],
        'location2': ['LOWTHER AVE', '4700 KEELE ST', 'HURON ST', 'LOWTHER AVE', '20 EDWARD ST'],
        'location3': ['E/O', '', 'N/O', 'E/O', ''],
        'location4': ['HURON ST', '', 'BLOOR ST W', 'HURON ST', ''],
    }
    df = pd.DataFrame(sample_data)
    
    # Test address parsing
    print("\n--- Address Parsing Test ---")
    queries = []
    for i, row in df.iterrows():
        main, cross = AddressParser.parse_location(row)
        query = AddressParser.construct_geocoding_query(main, cross)
        queries.append(query)
        print(f"{i+1}. {query}")
    
    # Test geocoding
    print("\n--- Geocoding Test ---")
    cache = GeocodingCache('test_geocoding_cache.json')
    geocoder = GeocodioGeocoder(cache)
    
    # Get unique queries
    unique_queries = list(set(queries))
    results = geocoder.batch_geocode(unique_queries)
    
    print("\n--- Results ---")
    for query, coords in results.items():
        print(f"✅ {query}")
        print(f"   → Lat: {coords[0]:.6f}, Lon: {coords[1]:.6f}")
    
    cache.save()
