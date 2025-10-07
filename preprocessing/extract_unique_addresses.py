"""
Extract unique addresses from all parking ticket CSVs efficiently
Processes files one at a time, extracting only unique addresses
"""

import pandas as pd
import json
from pathlib import Path
import glob

def extract_unique_addresses():
    """Extract unique addresses from all CSV files without loading everything into memory"""
    
    print("=" * 60)
    print("Extracting Unique Addresses from Parking Tickets")
    print("=" * 60)
    
    data_dir = Path('parking_data/extracted')
    unique_addresses = set()
    
    year_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    
    total_files = 0
    total_records = 0
    
    print("\nProcessing files...")
    
    for year_dir in year_dirs:
        csv_files = sorted(glob.glob(str(year_dir / "*.csv")))
        
        for csv_file in csv_files:
            total_files += 1
            try:
                # Determine encoding
                encoding = 'utf-8'
                try:
                    # Test read first line to determine encoding
                    pd.read_csv(csv_file, nrows=1, encoding='utf-8')
                except UnicodeDecodeError:
                    encoding = 'utf-16'
                except Exception as e:
                    print(f"  ⚠️  Failed: {Path(csv_file).name} - {str(e)[:80]}")
                    continue
                
                # Read in chunks to save memory
                chunk_size = 100000  # Process 100k rows at a time
                file_records = 0
                
                try:
                    # Only read the location2 column to save memory
                    for chunk in pd.read_csv(csv_file, encoding=encoding, on_bad_lines='skip', 
                                            chunksize=chunk_size, usecols=['location2']):
                        # Get non-null location2 values
                        locations = chunk['location2'].dropna()
                        locations = locations[locations != '']
                        
                        # Add to set (automatically handles duplicates)
                        for loc in locations.unique():
                            loc_str = str(loc).strip()
                            if loc_str and loc_str.lower() != 'nan':
                                # Add with Toronto, ON, Canada suffix for geocoding
                                unique_addresses.add(f"{loc_str}, Toronto, ON, Canada")
                        
                        file_records += len(chunk)
                    
                    total_records += file_records
                    print(f"  ✓ {Path(csv_file).name}: {file_records:,} records, {len(unique_addresses):,} unique addresses so far")
                    
                except Exception as e:
                    print(f"  ⚠️  Error reading chunks from {Path(csv_file).name}: {str(e)[:80]}")
                
            except Exception as e:
                print(f"  ⚠️  Error processing {Path(csv_file).name}: {str(e)[:80]}")
                continue
    
    print(f"\n" + "=" * 60)
    print(f"Processing complete!")
    print(f"  Files processed: {total_files}")
    print(f"  Total records: {total_records:,}")
    print(f"  Unique addresses: {len(unique_addresses):,}")
    print("=" * 60)
    
    # Convert to sorted list
    unique_addresses_list = sorted(list(unique_addresses))
    
    # Save to file
    output_file = 'output/unique_queries.json'
    Path('output').mkdir(exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(unique_addresses_list, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Saved {len(unique_addresses_list):,} unique addresses to {output_file}")
    
    # Also save a summary
    summary = {
        'total_files_processed': total_files,
        'total_records_processed': total_records,
        'unique_addresses_count': len(unique_addresses_list),
        'sample_addresses': unique_addresses_list[:10]
    }
    
    summary_file = 'output/extraction_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved summary to {summary_file}")
    
    return unique_addresses_list

if __name__ == '__main__':
    extract_unique_addresses()
