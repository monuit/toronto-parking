"""
Extract and save all unique addresses from parking ticket data
This allows us to move the data between systems without reprocessing
"""

import pandas as pd
import json
from pathlib import Path
import glob
from collections import defaultdict

def parse_location(row):
    """Parse location fields into (main_street, cross_street) tuple"""
    main_street = None
    cross_street = None
    
    if pd.notna(row.get('location2', '')) and str(row['location2']).strip():
        main_street = str(row['location2']).strip()
    
    if pd.notna(row.get('location4', '')) and str(row['location4']).strip():
        cross_street = str(row['location4']).strip()
    
    return main_street, cross_street

def construct_geocoding_query(main_street, cross_street=None):
    """Construct geocoding query"""
    if cross_street:
        query = f"{main_street} and {cross_street}, Toronto, ON, Canada"
    else:
        query = f"{main_street}, Toronto, ON, Canada"
    return query

def extract_unique_addresses():
    """Extract all unique addresses from parking data"""
    
    print("="*60)
    print("Extracting Unique Addresses from Parking Data")
    print("="*60)
    
    data_dir = Path("parking_data/extracted")
    year_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    
    # Track unique location combinations and their frequencies
    location_counts = defaultdict(int)
    location_details = {}
    
    total_records = 0
    
    for year_dir in year_dirs:
        csv_files = sorted(glob.glob(str(year_dir / "*.csv")))
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file, low_memory=False)
                
                for _, row in df.iterrows():
                    main_street, cross_street = parse_location(row)
                    
                    if main_street:
                        # Create location key
                        loc1 = str(row.get('location1', ''))
                        loc2 = str(row.get('location2', ''))
                        loc3 = str(row.get('location3', ''))
                        loc4 = str(row.get('location4', ''))
                        
                        key = f"{loc1}|{loc2}|{loc3}|{loc4}"
                        location_counts[key] += 1
                        
                        if key not in location_details:
                            query = construct_geocoding_query(main_street, cross_street)
                            location_details[key] = {
                                'location1': loc1,
                                'location2': loc2,
                                'location3': loc3,
                                'location4': loc4,
                                'main_street': main_street,
                                'cross_street': cross_street,
                                'query': query
                            }
                
                total_records += len(df)
                print(f"  Processed: {csv_file} ({len(df):,} records)")
                
            except Exception as e:
                print(f"  Skipped: {csv_file} - {e}")
    
    print(f"\nTotal records processed: {total_records:,}")
    print(f"Unique location combinations: {len(location_counts):,}")
    
    # Get unique queries
    unique_queries = {}
    for key, details in location_details.items():
        query = details['query']
        if query not in unique_queries:
            unique_queries[query] = {
                'query': query,
                'location_keys': [],
                'total_tickets': 0
            }
        unique_queries[query]['location_keys'].append(key)
        unique_queries[query]['total_tickets'] += location_counts[key]
    
    print(f"Unique addresses to geocode: {len(unique_queries):,}")
    
    # Sort by frequency (most common first)
    sorted_queries = sorted(
        unique_queries.items(),
        key=lambda x: x[1]['total_tickets'],
        reverse=True
    )
    
    # Show top 10
    print("\n📊 Top 10 most common addresses:")
    for i, (query, data) in enumerate(sorted_queries[:10], 1):
        print(f"  {i:2d}. {query}")
        print(f"      → {data['total_tickets']:,} tickets")
    
    # Save all unique addresses
    output_data = {
        'total_records': total_records,
        'unique_locations': len(location_counts),
        'unique_addresses': len(unique_queries),
        'location_details': location_details,
        'location_frequencies': dict(location_counts),
        'unique_queries': dict(sorted_queries)
    }
    
    output_file = Path("unique_addresses.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n✅ Saved unique addresses to: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
    
    # Also save just the queries list for easy geocoding
    queries_only = [query for query, _ in sorted_queries]
    queries_file = Path("unique_queries.json")
    with open(queries_file, 'w', encoding='utf-8') as f:
        json.dump(queries_only, f, indent=2)
    
    print(f"   Queries only: {queries_file} ({queries_file.stat().st_size / 1024:.2f} KB)")
    
    return location_details, dict(location_counts), unique_queries

if __name__ == "__main__":
    extract_unique_addresses()
