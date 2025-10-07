"""
Test the robust geocoder with a small batch
"""
import json
from run_geocoding import RobustGeocoder
import os
from dotenv import load_dotenv

load_dotenv()

# Load just first 10 queries
with open('unique_queries.json', 'r') as f:
    all_queries = json.load(f)

test_queries = all_queries[:10]

print(f"Testing with {len(test_queries)} addresses:")
for i, q in enumerate(test_queries, 1):
    print(f"  {i}. {q}")

# Run geocoder
api_key = os.getenv('GEOCODE_MAPS_CO_API_KEY', '68e35b92c4aa0836068625vlcd9bb74')

geocoder = RobustGeocoder(
    api_key=api_key,
    cache_file='test_robust_geocoding.json',
    checkpoint_interval=5,  # Save every 5 addresses for testing
    rate_limit=1.5
)

results = geocoder.run(test_queries)

print("\n" + "="*60)
print("Results:")
print("="*60)
for query, result in results.items():
    if result.get('lat'):
        print(f"✅ {query[:50]}...")
        print(f"   → {result['lat']:.6f}, {result['lon']:.6f}")
    else:
        print(f"❌ {query[:50]}... - {result.get('status')}")
