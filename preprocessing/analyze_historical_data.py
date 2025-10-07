"""
Analyze and Fix Historical Parking Data (2008-2011)
Downloads, extracts, and preprocesses historical data to match current format
"""

import pandas as pd
import zipfile
import os
from pathlib import Path
import requests
from io import BytesIO
import json

# Toronto Open Data API endpoints for historical data
HISTORICAL_DATA_URLS = {
    2008: "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/parking-tickets/resource/c3a8ec13-a5f2-4e10-b8f5-b867f53b6a1e/download/parking_tickets_2008.zip",
    2009: "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/parking-tickets/resource/3e0c72e1-c6d7-442d-93b7-18bd7f3f53e7/download/parking_tickets_2009.zip",
    2010: "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/parking-tickets/resource/ca2bb73d-3e37-4a02-bf39-c0fd75dc616d/download/parking_tickets_2010.zip",
    2011: "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/parking-tickets/resource/3dd97449-95ee-42b4-a7f4-01a991f5f7c8/download/parking_tickets_2011.zip"
}

def download_and_extract(year, url):
    """Download and extract historical data"""
    year_dir = Path(f'parking_data/extracted/{year}')
    year_dir.mkdir(parents=True, exist_ok=True)
    
    zip_path = year_dir / f'parking_tickets_{year}.zip'
    
    # Check if already downloaded
    if zip_path.exists():
        print(f"✓ {year} data already downloaded")
    else:
        print(f"📥 Downloading {year} data...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✓ Downloaded {year} data")
    
    # Extract if not already extracted
    csv_files = list(year_dir.glob('*.csv'))
    if csv_files:
        print(f"✓ {year} data already extracted: {csv_files[0].name}")
        return csv_files[0]
    else:
        print(f"📦 Extracting {year} data...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(year_dir)
        
        csv_files = list(year_dir.glob('*.csv'))
        if csv_files:
            print(f"✓ Extracted: {csv_files[0].name}")
            return csv_files[0]
        else:
            print(f"❌ No CSV found in {year} zip")
            return None


def analyze_data_structure(year, csv_path):
    """Analyze the structure of historical data"""
    print(f"\n{'='*80}")
    print(f"ANALYZING {year} DATA STRUCTURE")
    print(f"{'='*80}")
    
    # Read sample
    df = pd.read_csv(csv_path, nrows=1000)
    
    print(f"\n📊 Basic Info:")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Sample rows: {len(df)}")
    
    print(f"\n📋 Column Names:")
    for i, col in enumerate(df.columns, 1):
        print(f"   {i:2d}. {col}")
    
    print(f"\n🔍 Sample Data (first 3 rows):")
    print(df.head(3).to_string())
    
    print(f"\n📍 Location Fields:")
    location_cols = [col for col in df.columns if 'location' in col.lower() or 'address' in col.lower() or 'street' in col.lower()]
    for col in location_cols:
        print(f"   {col}: {df[col].head(3).tolist()}")
    
    print(f"\n📅 Date Fields:")
    date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
    for col in date_cols:
        print(f"   {col}: {df[col].head(3).tolist()}")
    
    return {
        'year': year,
        'columns': df.columns.tolist(),
        'row_count_sample': len(df),
        'location_columns': location_cols,
        'date_columns': date_cols,
        'sample_data': df.head(5).to_dict()
    }


def compare_with_current_format():
    """Compare historical format with current 2024 format"""
    print(f"\n{'='*80}")
    print(f"CURRENT FORMAT (2024) REFERENCE")
    print(f"{'='*80}")
    
    # Check current format
    current_files = list(Path('parking_data/extracted/2024').glob('*.csv'))
    if current_files:
        df_2024 = pd.read_csv(current_files[0], nrows=5)
        print(f"\n📋 2024 Columns:")
        for i, col in enumerate(df_2024.columns, 1):
            print(f"   {i:2d}. {col}")
        
        print(f"\n🔍 Sample 2024 Data:")
        print(df_2024.head(3).to_string())
        
        return df_2024.columns.tolist()
    else:
        print("❌ No 2024 data found for reference")
        return []


def create_standardization_mapping(analysis_results, current_columns):
    """Create mapping to standardize historical data"""
    print(f"\n{'='*80}")
    print(f"COLUMN MAPPING ANALYSIS")
    print(f"{'='*80}")
    
    mappings = {}
    
    for result in analysis_results:
        year = result['year']
        hist_cols = result['columns']
        
        print(f"\n{year} Mapping:")
        
        # Common patterns
        mapping = {}
        for hist_col in hist_cols:
            # Try to match with current columns
            hist_lower = hist_col.lower()
            for curr_col in current_columns:
                curr_lower = curr_col.lower()
                if hist_lower == curr_lower:
                    mapping[hist_col] = curr_col
                    break
        
        mappings[year] = mapping
        print(f"   Matched {len(mapping)} columns")
        for old, new in mapping.items():
            if old != new:
                print(f"   {old} → {new}")
    
    return mappings


def main():
    """Main analysis workflow"""
    print("="*80)
    print("HISTORICAL DATA ANALYSIS AND PREPROCESSING")
    print("="*80)
    
    # Step 1: Compare with current format
    current_columns = compare_with_current_format()
    
    # Step 2: Download and analyze historical data
    analysis_results = []
    
    for year, url in HISTORICAL_DATA_URLS.items():
        try:
            csv_path = download_and_extract(year, url)
            if csv_path:
                result = analyze_data_structure(year, csv_path)
                analysis_results.append(result)
        except Exception as e:
            print(f"\n❌ Error processing {year}: {e}")
            continue
    
    # Step 3: Create standardization mapping
    if current_columns and analysis_results:
        mappings = create_standardization_mapping(analysis_results, current_columns)
        
        # Save analysis results
        output_dir = Path('analysis_output')
        output_dir.mkdir(exist_ok=True)
        
        with open(output_dir / 'historical_data_analysis.json', 'w') as f:
            json.dump({
                'current_format': current_columns,
                'historical_analysis': analysis_results,
                'column_mappings': mappings
            }, f, indent=2)
        
        print(f"\n{'='*80}")
        print(f"✅ ANALYSIS COMPLETE")
        print(f"{'='*80}")
        print(f"Results saved to: analysis_output/historical_data_analysis.json")
    
    # Step 4: Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    print(f"   1. Review analysis_output/historical_data_analysis.json")
    print(f"   2. Create standardization script based on mappings")
    print(f"   3. Process and merge historical data with current data")


if __name__ == "__main__":
    main()
