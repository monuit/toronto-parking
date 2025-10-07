"""
Extract ALL unique addresses with RESUME capability
Saves progress after each year so you can stop/start anytime
"""

import pandas as pd
import json
from pathlib import Path
import glob
from datetime import datetime

def extract_addresses_from_file(csv_file):
    """Extract unique addresses from a single CSV file"""
    addresses = set()
    
    try:
        # Determine encoding by testing first
        encoding = 'utf-8'
        try:
            pd.read_csv(csv_file, nrows=1, encoding='utf-8')
        except UnicodeDecodeError:
            encoding = 'utf-16'
        
        # Read only location2 column in chunks
        chunk_size = 100000
        file_records = 0
        
        for chunk in pd.read_csv(csv_file, encoding=encoding, on_bad_lines='skip', 
                                chunksize=chunk_size, usecols=['location2'], dtype={'location2': str}):
            # Extract non-null addresses
            locations = chunk['location2'].dropna()
            
            # Add unique addresses from this chunk
            for loc in locations.unique():
                loc_str = str(loc).strip()
                if loc_str and loc_str.lower() != 'nan' and loc_str != '':
                    addresses.add(loc_str)
            
            file_records += len(chunk)
        
        return addresses, file_records, None
        
    except Exception as e:
        return set(), 0, str(e)[:100]

def load_progress():
    """Load existing progress if any"""
    progress_file = 'output/extraction_progress.json'
    if Path(progress_file).exists():
        with open(progress_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'processed_years': [],
        'all_addresses': [],
        'total_records': 0,
        'total_files': 0,
        'failed_files': 0
    }

def save_progress(progress):
    """Save current progress"""
    progress_file = 'output/extraction_progress.json'
    Path('output').mkdir(exist_ok=True)
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)

def main():
    print("=" * 70)
    print("Extracting ALL Unique Addresses from Toronto Parking Tickets")
    print("WITH RESUME CAPABILITY - Safe to stop anytime (Ctrl+C)")
    print("=" * 70)
    
    # Load existing progress
    progress = load_progress()
    all_unique_addresses = set(progress['all_addresses'])
    processed_years = set(progress['processed_years'])
    
    if processed_years:
        print(f"\n📂 RESUMING: Already processed {len(processed_years)} years")
        print(f"   Current unique addresses: {len(all_unique_addresses):,}")
        print(f"   Years done: {sorted(processed_years)}\n")
    
    data_dir = Path('parking_data/extracted')
    year_dirs = sorted([d for d in data_dir.iterdir() if d.is_dir()])
    
    total_files_processed = progress['total_files']
    total_files_failed = progress['failed_files']
    total_records = progress['total_records']
    
    print("\n📂 Processing files by year...\n")
    
    try:
        for year_dir in year_dirs:
            year = year_dir.name
            
            # Skip if already processed
            if year in processed_years:
                print(f"  ⏭️  {year}: Already processed (skipping)")
                continue
            
            csv_files = sorted(glob.glob(str(year_dir / "*.csv")))
            
            if not csv_files:
                print(f"  ⚠️  {year}: No CSV files found")
                processed_years.add(year)
                continue
            
            year_addresses = set()
            year_records = 0
            year_failed = 0
            
            print(f"  🔄 Processing {year}...")
            
            for csv_file in csv_files:
                file_name = Path(csv_file).name
                print(f"    ⏳ {file_name}...", end=' ', flush=True)
                
                addresses, records, error = extract_addresses_from_file(csv_file)
                
                if error:
                    print(f"❌ FAILED - {error}")
                    year_failed += 1
                    total_files_failed += 1
                else:
                    year_addresses.update(addresses)
                    year_records += records
                    total_files_processed += 1
                    print(f"✅ {records:,} records, {len(addresses):,} unique addresses")
            
            # Add year's addresses to global set
            all_unique_addresses.update(year_addresses)
            total_records += year_records
            processed_years.add(year)
            
            print(f"  ✅ {year} COMPLETE: {len(csv_files)} files, {year_records:,} records, {len(year_addresses):,} unique addresses")
            print(f"     📊 TOTAL SO FAR: {len(all_unique_addresses):,} unique addresses across {len(processed_years)} years\n")
            
            # SAVE PROGRESS AFTER EACH YEAR
            progress['processed_years'] = sorted(list(processed_years))
            progress['all_addresses'] = sorted(list(all_unique_addresses))
            progress['total_records'] = total_records
            progress['total_files'] = total_files_processed
            progress['failed_files'] = total_files_failed
            progress['last_updated'] = datetime.now().isoformat()
            save_progress(progress)
            print(f"  💾 Progress saved (safe to stop anytime)\n")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  INTERRUPTED - Saving progress...")
        progress['processed_years'] = sorted(list(processed_years))
        progress['all_addresses'] = sorted(list(all_unique_addresses))
        progress['total_records'] = total_records
        progress['total_files'] = total_files_processed
        progress['failed_files'] = total_files_failed
        progress['last_updated'] = datetime.now().isoformat()
        save_progress(progress)
        print(f"✅ Progress saved! Run again to resume from {len(processed_years)} years processed")
        return
    
    print("=" * 70)
    print("✅ Extraction Complete!")
    print("=" * 70)
    print(f"  Files processed: {total_files_processed}")
    print(f"  Files failed: {total_files_failed}")
    print(f"  Total records: {total_records:,}")
    print(f"  Unique addresses (raw): {len(all_unique_addresses):,}")
    print("=" * 70)
    
    # Format addresses for geocoding (add Toronto, ON, Canada)
    print("\n🔄 Formatting addresses for geocoding...")
    formatted_addresses = []
    for addr in sorted(all_unique_addresses):
        formatted_addresses.append(f"{addr}, Toronto, ON, Canada")
    
    print(f"✅ Formatted {len(formatted_addresses):,} addresses")
    
    # Save to file
    output_file = 'output/unique_queries.json'
    print(f"\n💾 Saving to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(formatted_addresses, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved {len(formatted_addresses):,} unique addresses")
    
    # Save summary
    summary = {
        'total_files_processed': total_files_processed,
        'total_files_failed': total_files_failed,
        'total_records_processed': total_records,
        'unique_addresses_count': len(formatted_addresses),
        'years_processed': sorted(list(processed_years)),
        'completed_date': datetime.now().isoformat(),
        'sample_addresses': formatted_addresses[:20]
    }
    
    summary_file = 'output/extraction_summary.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved summary to {summary_file}")
    
    # Clean up progress file
    progress_file = 'output/extraction_progress.json'
    if Path(progress_file).exists():
        Path(progress_file).unlink()
        print(f"✅ Cleaned up progress file")
    
    print("\n" + "=" * 70)
    print("🎉 READY FOR GEOCODING!")
    print("=" * 70)
    print(f"Total unique addresses to geocode: {len(formatted_addresses):,}")
    print("\nNext step:")
    print("  python geocoding/run_geocoding_fast.py")
    print("=" * 70)

if __name__ == '__main__':
    main()
