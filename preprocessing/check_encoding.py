"""
Fix encoding issues for 2008 and 2010 data
"""
import pandas as pd
from pathlib import Path

for year in [2008, 2010]:
    path = Path(f'parking_data/extracted/{year}/Parking_Tags_data_{year}.csv')
    print(f"\n{'='*60}")
    print(f"Analyzing {year} data")
    print(f"{'='*60}")
    
    try:
        # Try different encodings
        encodings = ['latin1', 'cp1252', 'iso-8859-1']
        
        for enc in encodings:
            try:
                df = pd.read_csv(path, encoding=enc, nrows=10)
                print(f"✓ Successfully read with encoding: {enc}")
                print(f"\nColumns: {df.columns.tolist()}")
                print(f"\nSample data:")
                print(df.head(3))
                
                # Check if columns match expected format
                expected_cols = ['tag_number_masked', 'date_of_infraction', 'infraction_code', 
                               'infraction_description', 'set_fine_amount', 'time_of_infraction',
                               'location1', 'location2', 'location3', 'location4', 'province']
                
                if df.columns.tolist() == expected_cols:
                    print(f"\n✅ {year} format matches 2024 format!")
                else:
                    print(f"\n⚠️ {year} columns differ from expected format")
                    print("Missing:", set(expected_cols) - set(df.columns.tolist()))
                    print("Extra:", set(df.columns.tolist()) - set(expected_cols))
                
                break
            except Exception as e:
                print(f"❌ {enc} failed: {e}")
                continue
    except Exception as e:
        print(f"❌ Error: {e}")
