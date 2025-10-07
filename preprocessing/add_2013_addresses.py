"""
Add 2013 addresses to the final unique_queries.json
"""

import json
from pathlib import Path
import csv
import sys

# Increase CSV field size limit
csv.field_size_limit(sys.maxsize)

def extract_2013_addresses():
    """Extract addresses from 2013 file"""
    csv_file = 'parking_data/extracted/2013/Parking_Tags_Data_2013.csv'
    addresses = set()
    
    print(f"🔄 Extracting from 2013...")
    
    with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = next(reader)
        location2_idx = header.index('location2')
        
        for row in reader:
            try:
                if len(row) > location2_idx:
                    loc = row[location2_idx].strip()
                    if loc and loc.lower() != 'nan' and loc != '':
                        addresses.add(loc)
            except:
                continue
    
    print(f"✅ Found {len(addresses):,} unique addresses from 2013")
    return addresses

# Load existing addresses
print("📂 Loading existing unique_queries.json...")
with open('output/unique_queries.json', 'r', encoding='utf-8') as f:
    existing = json.load(f)

print(f"   Current count: {len(existing):,}")

# Extract 2013
addresses_2013 = extract_2013_addresses()

# Convert existing to set (remove ", Toronto, ON, Canada" suffix)
existing_raw = set()
for addr in existing:
    raw = addr.replace(', Toronto, ON, Canada', '')
    existing_raw.add(raw)

print(f"\n🔄 Merging addresses...")
print(f"   Before: {len(existing_raw):,}")

# Add 2013 addresses
existing_raw.update(addresses_2013)

print(f"   After: {len(existing_raw):,}")
print(f"   Added from 2013: {len(existing_raw) - len(existing):,}")

# Format for geocoding
formatted = sorted([f"{addr}, Toronto, ON, Canada" for addr in existing_raw])

# Save
print(f"\n💾 Saving updated unique_queries.json...")
with open('output/unique_queries.json', 'w', encoding='utf-8') as f:
    json.dump(formatted, f, indent=2, ensure_ascii=False)

print(f"✅ Saved {len(formatted):,} total unique addresses")

# Update summary
if Path('output/extraction_summary.json').exists():
    with open('output/extraction_summary.json', 'r', encoding='utf-8') as f:
        summary = json.load(f)
    
    summary['unique_addresses_count'] = len(formatted)
    summary['includes_2013_fix'] = True
    
    with open('output/extraction_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Updated extraction_summary.json")

print(f"\n🎉 Ready for geocoding with {len(formatted):,} addresses!")
