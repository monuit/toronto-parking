"""
Quick script to extract unique addresses from already-run prepare_map_data.py output
"""
import json
from pathlib import Path

# Check if we already have the 1000-address test run results
test_cache = Path('geocoding_cache.json')

if test_cache.exists():
    print("Found geocoding_cache.json from previous test run")
    with open(test_cache, 'r') as f:
        cache_data = json.load(f)
    
    # Extract just the queries
    queries = list(cache_data.keys())
    
    print(f"Extracted {len(queries)} unique queries from cache")
    
    # Save as unique_queries.json
    with open('unique_queries.json', 'w', encoding='utf-8') as f:
        json.dump(queries, f, indent=2)
    
    print(f"✅ Saved to unique_queries.json")
    
    # Show first 10
    print("\nFirst 10 queries:")
    for i, q in enumerate(queries[:10], 1):
        print(f"  {i}. {q}")
else:
    print("No cache found. Need to run prepare_map_data.py first")
    print("\nCreating test queries manually...")
    
    test_queries = [
        "4700 KEELE ST, Toronto, ON, Canada",
        "2075 BAYVIEW AVE, Toronto, ON, Canada",
        "20 EDWARD ST, Toronto, ON, Canada",
        "1265 MILITARY TRL, Toronto, ON, Canada",
        "15 MARINE PARADE DR, Toronto, ON, Canada",
        "103 THE QUEENSWAY, Toronto, ON, Canada",
        "40 ORCHARD VIEW BLVD, Toronto, ON, Canada",
        "1750 FINCH AVE E, Toronto, ON, Canada",
        "1 BRIMLEY RD S, Toronto, ON, Canada",
        "LOWTHER AVE and HURON ST, Toronto, ON, Canada"
    ]
    
    with open('unique_queries.json', 'w', encoding='utf-8') as f:
        json.dump(test_queries, f, indent=2)
    
    print(f"✅ Created test file with {len(test_queries)} queries")
    for i, q in enumerate(test_queries, 1):
        print(f"  {i}. {q}")
