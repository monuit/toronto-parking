"""
Fix and Standardize Historical Data (2008-2011)
Converts malformed 2008/2010 data to proper CSV format
"""

import pandas as pd
from pathlib import Path
import sys

def fix_historical_data():
    """Fix encoding and format issues in historical data"""
    
    print("="*80)
    print("FIXING HISTORICAL DATA (2008-2011)")
    print("="*80)
    
    # Years with encoding issues
    problematic_years = [2008, 2010]
    
    for year in problematic_years:
        csv_path = Path(f'parking_data/extracted/{year}/Parking_Tags_data_{year}.csv')
        fixed_path = Path(f'parking_data/extracted/{year}/Parking_Tags_Data_{year}_fixed.csv')
        
        print(f"\n📝 Processing {year}...")
        
        try:
            # Read UTF-16 encoded file
            with open(csv_path, 'r', encoding='utf-16') as f:
                content = f.read()
            
            # Split into lines
            lines = content.strip().split('\n')
            
            print(f"   Total lines: {len(lines):,}")
            
            # Parse the data - it's actually comma-separated, not tab-separated
            # First line is header
            header = lines[0].strip()
            
            # Write fixed CSV
            with open(fixed_path, 'w', encoding='utf-8') as f:
                f.write(header + '\n')
                for line in lines[1:]:
                    if line.strip():
                        f.write(line.strip() + '\n')
            
            # Verify
            df_test = pd.read_csv(fixed_path, nrows=5)
            print(f"   ✅ Fixed! Columns: {len(df_test.columns)}")
            print(f"   Saved to: {fixed_path}")
            
            # Show sample
            print(f"\n   Sample data:")
            print(f"   {df_test.columns.tolist()}")
            print(f"   First row: {df_test.iloc[0].to_dict()}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
    
    # Verify all years now work
    print(f"\n{'='*80}")
    print("VERIFICATION - ALL YEARS")
    print(f"{'='*80}\n")
    
    all_years = [2008, 2009, 2010, 2011]
    summary = []
    
    for year in all_years:
        # Try fixed file first, then original
        paths_to_try = [
            Path(f'parking_data/extracted/{year}/Parking_Tags_Data_{year}_fixed.csv'),
            Path(f'parking_data/extracted/{year}/Parking_Tags_data_{year}.csv')
        ]
        
        for path in paths_to_try:
            if path.exists():
                try:
                    df = pd.read_csv(path, nrows=1000)
                    
                    # Count total rows
                    row_count = sum(1 for _ in open(path, encoding='utf-8')) - 1
                    
                    summary.append({
                        'year': year,
                        'file': path.name,
                        'total_rows': row_count,
                        'columns': len(df.columns),
                        'status': '✅'
                    })
                    
                    print(f"{year}: ✅ {row_count:,} rows, {len(df.columns)} columns")
                    break
                except Exception as e:
                    print(f"{year}: ❌ Error: {e}")
                    summary.append({
                        'year': year,
                        'file': path.name if path.exists() else 'N/A',
                        'status': f'❌ {e}'
                    })
                    break
    
    # Summary
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    
    total_tickets = sum(item['total_rows'] for item in summary if 'total_rows' in item)
    print(f"\n📊 Total historical tickets (2008-2011): {total_tickets:,}")
    
    for item in summary:
        if 'total_rows' in item:
            print(f"   {item['year']}: {item['total_rows']:,} tickets")
    
    print(f"\n✅ Historical data is now ready for analysis!")
    print(f"\n💡 Next steps:")
    print(f"   1. Include 2008-2011 data in extract_addresses.py")
    print(f"   2. Re-run geocoding with full historical data")
    print(f"   3. Update map visualization with all years")

if __name__ == "__main__":
    fix_historical_data()
