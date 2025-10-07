"""
Address Geocoding Module for Toronto Parking Tickets
Handles parsing location fields and batch geocoding with caching
"""

import pandas as pd
import json
import time
import requests
from pathlib import Path
from typing import Dict, Tuple, Optional
import hashlib
from collections import defaultdict

class AddressParser:
    """Parse location1-4 fields into geocodable addresses"""
    
    # Common proximity indicators
    PROXIMITY_MAP = {
        'NR': 'near',
        'AT': 'at',
        'OPP': 'opposite',
        'S/S': 'south side of',
        'N/S': 'north side of',
        'E/S': 'east side of',
        'W/S': 'west side of',
        'E/O': 'east of',
        'W/O': 'west of',
        'N/O': 'north of',
        'S/O': 'south of',
    }
    
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
        Construct geocoding query optimized for Nominatim
        
        Format: "MAIN_STREET, Toronto, Ontario, Canada" or 
                "MAIN_STREET and CROSS_STREET, Toronto, Ontario, Canada"
        """
        if cross_street:
            query = f"{main_street} and {cross_street}, Toronto, Ontario, Canada"
        else:
            query = f"{main_street}, Toronto, Ontario, Canada"
        
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
    
    def _load_cache(self) -> Dict:
        """Load existing cache from disk"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load cache: {e}")
        return {}
    
    def save(self):
        """Save cache to disk"""
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.cache, f, indent=2)
        print(f"\nCache saved: {len(self.cache)} entries ({self.hits} hits, {self.misses} misses)")
    
    def get(self, query_hash: str) -> Optional[Dict]:
        """Get cached result"""
        if query_hash in self.cache:
            self.hits += 1
            return self.cache[query_hash]
        self.misses += 1
        return None
    
    def set(self, query_hash: str, result: Dict):
        """Store result in cache"""
        self.cache[query_hash] = result


class NominatimGeocoder:
    """Batch geocoder using Nominatim (OpenStreetMap)"""
    
    def __init__(self, cache: GeocodingCache, rate_limit=1.0):
        """
        Initialize geocoder
        
        Args:
            cache: GeocodingCache instance
            rate_limit: Seconds between requests (Nominatim requires 1 req/sec)
        """
        self.cache = cache
        self.rate_limit = rate_limit
        self.base_url = "https://nominatim.openstreetmap.org/search"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TorontoParkingAnalysis/1.0 (Educational Project)'
        })
        self.last_request_time = 0
        self.requests_made = 0
    
    def geocode(self, query: str) -> Optional[Tuple[float, float]]:
        """
        Geocode a single address
        
        Returns:
            (lat, lon) tuple or None if not found
        """
        query_hash = AddressParser.get_query_hash(query)
        
        # Check cache first
        cached = self.cache.get(query_hash)
        if cached is not None:
            if cached.get('lat') and cached.get('lon'):
                return (cached['lat'], cached['lon'])
            return None
        
        # Rate limiting
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        
        # Make request
        try:
            params = {
                'q': query,
                'format': 'json',
                'limit': 1,
                'countrycodes': 'ca',
                'bounded': 1,
                'viewbox': '-79.639,43.581,-79.127,43.855',  # Toronto bounding box
            }
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            self.last_request_time = time.time()
            self.requests_made += 1
            
            if response.status_code == 200:
                results = response.json()
                if results:
                    lat = float(results[0]['lat'])
                    lon = float(results[0]['lon'])
                    
                    # Cache the result
                    self.cache.set(query_hash, {'lat': lat, 'lon': lon, 'query': query})
                    
                    return (lat, lon)
            
            # Cache negative result to avoid re-querying
            self.cache.set(query_hash, {'lat': None, 'lon': None, 'query': query})
            return None
            
        except Exception as e:
            print(f"Geocoding error for '{query}': {e}")
            return None
    
    def batch_geocode_unique(self, queries: list, max_requests: Optional[int] = None) -> Dict[str, Tuple[float, float]]:
        """
        Batch geocode unique queries with progress reporting
        
        Args:
            queries: List of address queries
            max_requests: Maximum number of API requests to make (for testing)
        
        Returns:
            Dict mapping query -> (lat, lon)
        """
        unique_queries = list(set(queries))
        results = {}
        
        print(f"\nBatch geocoding {len(unique_queries)} unique addresses...")
        print(f"Cache: {len(self.cache.cache)} entries")
        
        requests_remaining = max_requests if max_requests else len(unique_queries)
        
        for i, query in enumerate(unique_queries):
            # Check cache first
            query_hash = AddressParser.get_query_hash(query)
            cached = self.cache.get(query_hash)
            
            if cached is not None:
                if cached.get('lat') and cached.get('lon'):
                    results[query] = (cached['lat'], cached['lon'])
                continue
            
            # Stop if we've hit max requests
            if max_requests and self.requests_made >= max_requests:
                print(f"\nReached maximum request limit ({max_requests})")
                break
            
            # Geocode
            coords = self.geocode(query)
            if coords:
                results[query] = coords
            
            # Progress reporting
            if (i + 1) % 10 == 0:
                cache_rate = (self.cache.hits / (self.cache.hits + self.cache.misses) * 100) if self.cache.misses > 0 else 100
                print(f"  Progress: {i+1}/{len(unique_queries)} | "
                      f"API requests: {self.requests_made} | "
                      f"Cache hit rate: {cache_rate:.1f}% | "
                      f"Found: {len(results)}")
            
            # Save cache periodically
            if self.requests_made % 50 == 0:
                self.cache.save()
        
        # Final save
        self.cache.save()
        
        print(f"\nGeocoding complete:")
        print(f"  Total queries: {len(unique_queries)}")
        print(f"  Successfully geocoded: {len(results)}")
        print(f"  API requests made: {self.requests_made}")
        print(f"  Cache hit rate: {self.cache.hits / (self.cache.hits + self.cache.misses) * 100:.1f}%")
        
        return results


def create_address_lookup_table(tickets_df: pd.DataFrame) -> Dict[str, Tuple[str, Optional[str]]]:
    """
    Create lookup table of unique location combinations
    
    Returns:
        Dict mapping location_key -> (main_street, cross_street)
    """
    print("\nAnalyzing unique locations...")
    
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
    
    print(f"Found {len(location_counts)} unique location combinations")
    
    # Sort by frequency for better caching
    sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Show top locations
    print("\nTop 10 most common locations:")
    for i, (key, count) in enumerate(sorted_locations[:10], 1):
        main, cross = location_details[key]
        cross_str = f" and {cross}" if cross else ""
        print(f"  {i}. {main}{cross_str}: {count:,} tickets")
    
    return location_details, dict(sorted_locations)


if __name__ == "__main__":
    # Test with sample data
    print("Testing geocoding module...")
    
    # Create sample DataFrame
    sample_data = {
        'location1': ['N/S', 'AT', 'E/S', 'N/S'],
        'location2': ['LOWTHER AVE', '4700 KEELE ST', 'HURON ST', 'LOWTHER AVE'],
        'location3': ['E/O', '', 'N/O', 'E/O'],
        'location4': ['HURON ST', '', 'BLOOR ST W', 'HURON ST'],
    }
    df = pd.DataFrame(sample_data)
    
    # Test address parsing
    print("\n--- Address Parsing Test ---")
    for i, row in df.iterrows():
        main, cross = AddressParser.parse_location(row)
        query = AddressParser.construct_geocoding_query(main, cross)
        print(f"{i+1}. {query}")
    
    # Test geocoding (with limit)
    print("\n--- Geocoding Test ---")
    cache = GeocodingCache('test_geocoding_cache.json')
    geocoder = NominatimGeocoder(cache, rate_limit=1.0)
    
    queries = []
    for _, row in df.iterrows():
        main, cross = AddressParser.parse_location(row)
        query = AddressParser.construct_geocoding_query(main, cross)
        queries.append(query)
    
    results = geocoder.batch_geocode_unique(queries, max_requests=3)
    
    print("\n--- Results ---")
    for query, coords in results.items():
        print(f"{query}")
        print(f"  -> {coords[0]:.6f}, {coords[1]:.6f}")
