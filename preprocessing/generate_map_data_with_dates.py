"""
Generate proper map data with year/month aggregation and counts for heatmap
"""

import pandas as pd
import json
from pathlib import Path
from collections import defaultdict

def load_geocoding_results():
    """Load geocoded addresses"""
    with open('output/geocoding_results.json', 'r', encoding='utf-8') as f:
        return json.load(f)

def aggregate_tickets_with_dates():
    """Aggregate tickets by location with date information"""
    
    print("Loading geocoding results...")
    geocoded = load_geocoding_results()
    
    # Create reverse lookup: coordinates -> address
    coord_to_address = {}
    for address, data in geocoded.items():
        if data.get('lat') and data.get('lon'):
            coord_key = f"{data['lat']:.6f},{data['lon']:.6f}"
            coord_to_address[coord_key] = address
    
    print(f"Loaded {len(coord_to_address)} geocoded locations")
    
    # Load all parking data from 2008-2024
    print("\nLoading parking ticket data...")
    data_files = []
    for year in range(2008, 2025):
        year_path = Path(f'parking_data/extracted/{year}')
        if year_path.exists():
            year_files = list(year_path.glob('*.csv'))
            data_files.extend(year_files)
            print(f"Found {len(year_files)} files for {year}")
    
    if not data_files:
        print("No data files found!")
        return
    
    print(f"\nTotal files to process: {len(data_files)}")
    
    # Aggregate by location with date information
    location_data = defaultdict(lambda: {
        'tickets': [],
        'total_count': 0,
        'total_revenue': 0,
        'years': set(),
        'months': set(),
        'infractions': defaultdict(int)
    })
    
    processed_count = 0
    skipped_count = 0
    
    for file_path in data_files:
        print(f"Processing {file_path.name}...")
        
        # Try different encodings for older files
        try:
            df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', low_memory=False)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding='utf-16', on_bad_lines='skip', low_memory=False)
            except Exception as e:
                print(f"  ⚠️  Failed to read {file_path.name}: {e}, skipping...")
                continue
        except Exception as e:
            print(f"  ⚠️  Failed to read {file_path.name}: {e}, skipping...")
            continue
        
        for _, row in df.iterrows():
            # Parse address - try location2 first (street address)
            try:
                location2 = str(row['location2'] if 'location2' in df.columns else '').strip()
            except:
                location2 = ''
            
            if not location2 or location2 == 'nan':
                skipped_count += 1
                continue
            
            # Get geocoded coordinates - try different formats
            geocode_data = None
            
            # Try formats in order of likelihood
            for format_template in [
                "{}, Toronto, ON, Canada",  # Full format used in geocoding
                "{}",  # Exact match
                "{}, TORONTO",  # With city
            ]:
                lookup_key = format_template.format(location2)
                if lookup_key in geocoded:
                    geocode_data = geocoded[lookup_key]
                    break
            
            if not geocode_data:
                skipped_count += 1
                continue
            
            lat = geocode_data.get('lat')
            lon = geocode_data.get('lon')
            
            if not lat or not lon:
                skipped_count += 1
                continue
            
            processed_count += 1
            
            # Extract date components
            try:
                date_str = str(row['date_of_infraction'] if 'date_of_infraction' in df.columns else '')
                if len(date_str) == 8:
                    year = int(date_str[:4])
                    month = int(date_str[4:6])
                else:
                    continue
            except:
                continue
            
            coord_key = f"{lat:.6f},{lon:.6f}"
            
            # Aggregate
            location_data[coord_key]['total_count'] += 1
            try:
                fine_amount = float(row['set_fine_amount'] if 'set_fine_amount' in df.columns else 0)
            except:
                fine_amount = 0
            location_data[coord_key]['total_revenue'] += fine_amount
            location_data[coord_key]['years'].add(year)
            location_data[coord_key]['months'].add(month)
            
            try:
                infraction_code = str(row['infraction_code'] if 'infraction_code' in df.columns else '')
            except:
                infraction_code = ''
            location_data[coord_key]['infractions'][infraction_code] += 1
            
            # Store individual ticket for point layer
            location_data[coord_key]['tickets'].append({
                'year': year,
                'month': month,
                'infraction_code': infraction_code,
                'fine': float(row.get('set_fine_amount', 0))
            })
    
    print(f"\nProcessed: {processed_count:,} tickets")
    print(f"Skipped (no geocode): {skipped_count:,} tickets")
    print(f"Aggregated {len(location_data)} unique locations")
    
    # Convert to GeoJSON
    features = []
    
    for coord_key, data in location_data.items():
        lat, lon = map(float, coord_key.split(','))
        
        # Get most common infraction
        top_infraction = max(data['infractions'].items(), key=lambda x: x[1])[0] if data['infractions'] else None
        
        feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [lon, lat]
            },
            'properties': {
                'location': coord_to_address.get(coord_key, 'Unknown'),
                'count': data['total_count'],
                'total_revenue': round(data['total_revenue'], 2),
                'years': sorted(list(data['years'])),
                'months': sorted(list(data['months'])),
                'top_infraction': top_infraction,
                'infraction_count': len(data['infractions'])
            }
        }
        
        features.append(feature)
    
    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }
    
    # Save
    output_path = Path('map-app/public/data/tickets_aggregated.geojson')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    
    print(f"\n✅ Saved to {output_path}")
    print(f"   Total locations: {len(features)}")
    print(f"   Total tickets: {sum(d['total_count'] for d in location_data.values())}")
    print(f"   Total revenue: ${sum(d['total_revenue'] for d in location_data.values()):,.2f}")

if __name__ == "__main__":
    aggregate_tickets_with_dates()
