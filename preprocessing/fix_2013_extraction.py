"""
Extract addresses from the corrupted 2013 file
Handles the EOF inside string error by processing line by line
"""

import csv
import json
from pathlib import Path
import sys

# Increase CSV field size limit
csv.field_size_limit(sys.maxsize)

def extract_from_2013():
    """Extract addresses from 2013 CSV using line-by-line reading"""
    
    csv_file = 'parking_data/extracted/2013/Parking_Tags_Data_2013.csv'
    
    if not Path(csv_file).exists():
        print(f"❌ File not found: {csv_file}")
        return set()
    
    print(f"🔄 Processing 2013 file with special handling...")
    print(f"   File: {csv_file}")
    
    addresses = set()
    total_rows = 0
    skipped_rows = 0
    location2_col_idx = None
    
    # Try different encodings
    for encoding in ['utf-8', 'utf-16', 'latin-1']:
        try:
            print(f"\n   Trying encoding: {encoding}")
            
            with open(csv_file, 'r', encoding=encoding, errors='replace') as f:
                # Read header
                reader = csv.reader(f)
                header = next(reader)
                
                # Find location2 column
                try:
                    location2_col_idx = header.index('location2')
                    print(f"   ✅ Found location2 at column {location2_col_idx}")
                except ValueError:
                    print(f"   ❌ No location2 column found in header: {header[:10]}")
                    continue
                
                # Process rows
                print(f"   📖 Reading rows...")
                for row_num, row in enumerate(reader, start=2):
                    try:
                        if len(row) > location2_col_idx:
                            location = row[location2_col_idx].strip()
                            if location and location.lower() != 'nan' and location != '':
                                addresses.add(location)
                        total_rows += 1
                        
                        # Progress indicator
                        if total_rows % 100000 == 0:
                            print(f"      Processed {total_rows:,} rows, {len(addresses):,} unique addresses", end='\r')
                    
                    except Exception as e:
                        skipped_rows += 1
                        if skipped_rows <= 5:  # Show first 5 errors
                            print(f"\n   ⚠️  Row {row_num}: {str(e)[:50]}")
                        continue
                
                print(f"\n   ✅ Completed with {encoding} encoding")
                break
                
        except Exception as e:
            print(f"   ❌ Failed with {encoding}: {str(e)[:80]}")
            continue
    
    print(f"\n" + "=" * 60)
    print(f"✅ 2013 Extraction Complete")
    print(f"   Total rows processed: {total_rows:,}")
    print(f"   Rows skipped (errors): {skipped_rows:,}")
    print(f"   Unique addresses found: {len(addresses):,}")
    print("=" * 60)
    
    return addresses

def update_progress_with_2013():
    """Update the extraction progress with 2013 data"""
    
    # Extract 2013 addresses
    addresses_2013 = extract_from_2013()
    
    if not addresses_2013:
        print("\n❌ No addresses extracted from 2013")
        return
    
    # Load existing progress
    progress_file = 'output/extraction_progress.json'
    if not Path(progress_file).exists():
        print(f"\n❌ Progress file not found: {progress_file}")
        print("   Run extract_all_addresses_resumable.py first")
        return
    
    with open(progress_file, 'r', encoding='utf-8') as f:
        progress = json.load(f)
    
    print(f"\n🔄 Updating progress file...")
    print(f"   Before: {len(progress['all_addresses']):,} addresses")
    
    # Add 2013 addresses
    all_addresses = set(progress['all_addresses'])
    all_addresses.update(addresses_2013)
    
    # Update progress
    progress['all_addresses'] = sorted(list(all_addresses))
    if '2013' not in progress['processed_years']:
        progress['processed_years'].append('2013')
        progress['processed_years'].sort()
    
    print(f"   After: {len(progress['all_addresses']):,} addresses")
    print(f"   Added from 2013: {len(addresses_2013):,} new addresses")
    
    # Save updated progress
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Progress file updated!")
    print(f"\n📊 Current Status:")
    print(f"   Years processed: {len(progress['processed_years'])}/17")
    print(f"   Years: {progress['processed_years']}")
    print(f"   Total unique addresses: {len(progress['all_addresses']):,}")
    
    # Show sample 2013 addresses
    sample_2013 = sorted(list(addresses_2013))[:10]
    print(f"\n📋 Sample 2013 addresses:")
    for addr in sample_2013:
        print(f"   • {addr}")

if __name__ == '__main__':
    update_progress_with_2013()
