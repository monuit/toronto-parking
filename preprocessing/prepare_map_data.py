"""
Data Aggregation Pipeline for Map Visualization
Single responsibility: process parking ticket CSVs and create geospatial outputs
"""

import pandas as pd
import json
from pathlib import Path
from collections import defaultdict

class TicketAggregator:
    """Aggregate parking ticket data by various dimensions"""
    
    def __init__(
        self,
        data_dir="parking_data/extracted"
    ):
        self.data_dir = Path(data_dir)
        self.tickets = []

    def _get_csv_files_for_year(self, year_dir):
        """Return list of CSV files for a given year, preferring cleaned files."""
        fixed_files = sorted(year_dir.glob("*_fixed.csv"))
        if fixed_files:
            return [str(path) for path in fixed_files]

        raw_files = sorted(
            path for path in year_dir.glob("*.csv")
            if not path.name.endswith("_fixed.csv")
        )
        return [str(path) for path in raw_files]
        
    def load_all_tickets(self):
        """Load all CSV files from extracted directories"""
        print("Loading parking ticket data...")
        
        year_dirs = sorted([d for d in self.data_dir.iterdir() if d.is_dir()])
        
        for year_dir in year_dirs:
            csv_files = self._get_csv_files_for_year(year_dir)
            
            for csv_file in csv_files:
                try:
                    # Try UTF-8 first, then UTF-16 for older files
                    df = None
                    try:
                        df = pd.read_csv(csv_file, low_memory=False, encoding='utf-8', on_bad_lines='skip')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(csv_file, low_memory=False, encoding='utf-16', on_bad_lines='skip')
                        except Exception as e:
                            print(f"  ⚠️  Failed (UTF-16): {csv_file} - {str(e)[:100]}")
                            continue
                    except Exception as e:
                        print(f"  ⚠️  Failed (UTF-8): {csv_file} - {str(e)[:100]}")
                        continue
                    
                    if df is not None and len(df) > 0:
                        self.tickets.append(df)
                        print(f"  Loaded: {csv_file} ({len(df):,} records)")
                except Exception as e:
                    print(f"  ⚠️  Failed: {csv_file} - {str(e)[:100]}")
        
        if self.tickets:
            self.tickets = pd.concat(self.tickets, ignore_index=True)
            print(f"\nTotal records loaded: {len(self.tickets):,}")
        else:
            print("No data loaded!")
            
    def construct_full_address(self, row):
        """Construct full address from location fields"""
        parts = []
        
        # location2 often has the main address (street number + name)
        if pd.notna(row.get('location2', '')) and row['location2'] != '':
            parts.append(str(row['location2']))
        
        # location4 sometimes has additional street info
        if pd.notna(row.get('location4', '')) and row['location4'] != '':
            parts.append(str(row['location4']))
        
        # Add Toronto, ON for geocoding
        if parts:
            address = ', '.join(parts) + ', Toronto, ON, Canada'
            return address
        return None
    
    def aggregate_by_street(self):
        """Aggregate statistics by street address for top locations"""
        print("\nAggregating by street address...")
        
        street_stats = {}
        
        # Use location2 for street grouping
        if 'location2' in self.tickets.columns:
            grouped = self.tickets.groupby('location2')
            
            for street, group in grouped:
                if pd.notna(street) and street != '' and len(group) > 10:  # Only streets with 10+ tickets
                    infractions = group['infraction_code'].value_counts()
                    
                    street_stats[str(street)] = {
                        'ticketCount': int(len(group)),
                        'totalRevenue': float(group['set_fine_amount'].sum()) 
                            if 'set_fine_amount' in group.columns else 0,
                        'topInfraction': str(infractions.index[0]) if len(infractions) > 0 else None,
                        'address': str(street) + ', Toronto, ON'
                    }
        
        # Sort and keep top 100 streets
        sorted_streets = dict(sorted(
            street_stats.items(), 
            key=lambda x: x[1]['ticketCount'], 
            reverse=True
        )[:100])
        
        print(f"  Found {len(sorted_streets)} top streets")
        return sorted_streets
    
    def aggregate_by_neighbourhood(self):
        """
        Aggregate statistics by neighbourhood
        Note: CSV doesn't contain neighbourhood info, so we'll create a 
        placeholder that will be filled after geocoding and spatial join
        """
        print("\nAggregating by neighbourhood...")
        print("  Note: Neighbourhood data will be computed after geocoding")
        
        # Return empty dict - will be populated after geocoding
        return {}


class GeoJSONGenerator:
    """Generate GeoJSON files for map visualization"""
    
    def __init__(self, output_dir="map-app/public/data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def download_toronto_neighbourhoods(self):
        """Download Toronto neighbourhoods from Open Data Portal"""
        print("\nDownloading Toronto neighbourhood boundaries...")
        
        import requests
        
        # Try direct GeoJSON URL from Toronto Open Data
        geojson_urls = [
            "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/4def3f65-2a65-4a4f-83c4-b2a4aed72d46/resource/a45bd45a-ede8-4a31-b09c-bf3405e99ac2/download/Neighbourhoods.geojson",
            "https://open.toronto.ca/dataset/neighbourhoods/",
        ]
        
        for geojson_url in geojson_urls:
            try:
                print(f"  Trying: {geojson_url}")
                response = requests.get(geojson_url, timeout=30)
                
                if response.status_code == 200:
                    # Try to parse as JSON
                    try:
                        geojson_data = response.json()
                        
                        output_path = self.output_dir / "neighbourhoods.geojson"
                        with open(output_path, 'w') as f:
                            json.dump(geojson_data, f, indent=2)
                        
                        print(f"  ✓ Saved: {output_path}")
                        return True
                    except:
                        print(f"  Not valid JSON, trying next URL...")
                        continue
            except Exception as e:
                print(f"  Error: {e}")
                continue
        
        print("  ⚠ Could not download, using placeholder")
        return False
    
    def create_sample_ticket_points(self, tickets_df, sample_size=10000, geocoded_coords=None):
        """Create sample GeoJSON of ticket points for visualization
        
        Args:
            tickets_df: DataFrame of parking tickets
            sample_size: Number of sample points to create
            geocoded_coords: Dict mapping location_key -> (lat, lon)
        """
        print(f"\nCreating sample ticket points ({sample_size} points)...")
        
        # Sample tickets that have location data
        sample = tickets_df.sample(min(sample_size, len(tickets_df)))
        
        features = []
        coords_found = 0
        coords_missing = 0
        
        for _, row in sample.iterrows():
            # Try to get geocoded coordinates
            lat, lon = None, None
            
            if geocoded_coords:
                # Create location key matching the geocoding lookup
                location_key = f"{row.get('location1', '')}|{row.get('location2', '')}|{row.get('location3', '')}|{row.get('location4', '')}"
                
                if location_key in geocoded_coords:
                    coords = geocoded_coords[location_key]
                    lat, lon = coords[0], coords[1]
                    coords_found += 1
                else:
                    coords_missing += 1
                    continue  # Skip tickets without coordinates
            else:
                # Fallback: skip if no geocoding data provided
                coords_missing += 1
                continue
            
            if lat and lon:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]  # GeoJSON is [lon, lat]
                    },
                    "properties": {
                        "infraction_code": str(row.get('infraction_code', '')),
                        "set_fine_amount": float(row.get('set_fine_amount', 0)),
                        "date_of_infraction": str(row.get('date_of_infraction', '')),
                        "time_of_infraction": str(row.get('time_of_infraction', '')),
                        "location": str(row.get('location2', ''))
                    }
                }
                features.append(feature)
        
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        
        output_path = self.output_dir / "tickets_aggregated.geojson"
        with open(output_path, 'w') as f:
            json.dump(geojson, f)
        
        print(f"  Saved: {output_path}")
        print(f"  Geocoded: {coords_found}, Missing: {coords_missing}, Total features: {len(features)}")


def main(max_geocoding_requests=None):
    """Main pipeline execution
    
    Args:
        max_geocoding_requests: Limit number of geocoding API calls (for testing)
    """
    print("=" * 60)
    print("Toronto Parking Tickets - Data Aggregation Pipeline")
    print("=" * 60)
    
    # Initialize aggregator
    aggregator = TicketAggregator()
    aggregator.load_all_tickets()
    
    if aggregator.tickets is None or len(aggregator.tickets) == 0:
        print("\nNo data to process. Exiting.")
        return
    
    # Import geocoding module (using geocode.maps.co)
    from geocode_mapsco import (
        GeocodeMapsCoCache, GeocodeMapsCoGeocoder
    )
    
    # Helper function for address parsing
    def parse_location_local(row):
        main_street = None
        cross_street = None
        if pd.notna(row.get('location2', '')) and str(row['location2']).strip():
            main_street = str(row['location2']).strip()
        if pd.notna(row.get('location4', '')) and str(row['location4']).strip():
            cross_street = str(row['location4']).strip()
        return main_street, cross_street
    
    def construct_query_local(main_street, cross_street=None):
        if cross_street:
            return f"{main_street} and {cross_street}, Toronto, ON, Canada"
        else:
            return f"{main_street}, Toronto, ON, Canada"
    
    # Step 1: Analyze unique locations
    print("\n" + "="*60)
    print("Analyzing unique locations in dataset...")
    print("="*60)
    
    location_counts = defaultdict(int)
    location_details = {}
    
    for _, row in aggregator.tickets.iterrows():
        main_street, cross_street = parse_location_local(row)
        
        if main_street:
            key = f"{row.get('location1', '')}|{row.get('location2', '')}|{row.get('location3', '')}|{row.get('location4', '')}"
            location_counts[key] += 1
            
            if key not in location_details:
                location_details[key] = (main_street, cross_street)
    
    print(f"\n✅ Found {len(location_counts):,} unique location combinations")
    
    # Step 2: Prepare geocoding queries
    print("\nPreparing geocoding queries...")
    query_map = {}  # Maps location_key -> query string
    
    for location_key, (main_street, cross_street) in location_details.items():
        query = construct_query_local(main_street, cross_street)
        query_map[location_key] = query
    
    # Save unique queries for portability
    unique_queries_list = list(set(query_map.values()))
    with open('unique_queries.json', 'w', encoding='utf-8') as f:
        json.dump(unique_queries_list, f, indent=2)
    print(f"💾 Saved {len(unique_queries_list):,} unique queries to: unique_queries.json")
    
    # Also save the location mapping for later use
    location_mapping = {
        'location_details': location_details,
        'location_counts': dict(location_counts),
        'query_map': query_map
    }
    with open('location_mapping.json', 'w', encoding='utf-8') as f:
        json.dump(location_mapping, f, indent=2)
    print(f"💾 Saved location mapping to: location_mapping.json")
    
    # Step 3: Load geocoding results if available
    geocoded_coords = {}
    results_file = Path('geocoding_results.json')
    
    if results_file.exists():
        print(f"\n📦 Loading geocoding results from: {results_file}")
        with open(results_file, 'r', encoding='utf-8') as f:
            geocoding_results = json.load(f)
        
        # Map results back to location keys
        for location_key, query in query_map.items():
            if query in geocoding_results:
                result = geocoding_results[query]
                if result.get('lat') and result.get('lon'):
                    geocoded_coords[location_key] = (result['lat'], result['lon'])
        
        print(f"✅ Loaded {len(geocoded_coords):,} geocoded locations")
    else:
        print(f"\n⚠️  No geocoding results found. Run 'python run_geocoding.py' to geocode addresses.")
        print(f"   For now, will skip geocoded point generation.")
    
    # Step 5: Aggregate data
    street_stats = aggregator.aggregate_by_street()
    hood_stats = aggregator.aggregate_by_neighbourhood()
    
    # Step 6: Initialize GeoJSON generator
    geo_generator = GeoJSONGenerator()
    
    # Download neighbourhood boundaries
    geo_generator.download_toronto_neighbourhoods()
    
    # Create sample ticket points WITH geocoded coordinates
    geo_generator.create_sample_ticket_points(
        aggregator.tickets, 
        sample_size=10000,
        geocoded_coords=geocoded_coords
    )
    
    # Step 7: Save aggregated stats as JSON
    output_dir = Path("map-app/public/data")
    
    with open(output_dir / "street_stats.json", 'w') as f:
        json.dump(street_stats, f, indent=2)
    print(f"\nSaved: {output_dir / 'street_stats.json'}")
    
    with open(output_dir / "neighbourhood_stats.json", 'w') as f:
        json.dump(hood_stats, f, indent=2)
    print(f"Saved: {output_dir / 'neighbourhood_stats.json'}")
    
    print("\n" + "=" * 60)
    print("Pipeline complete!")
    print(f"Geocoding success rate: {len(geocoded_coords) / len(location_details) * 100:.1f}%")
    print("=" * 60)


if __name__ == "__main__":
    import sys
    
    # Check for command line arguments
    max_requests = None
    if len(sys.argv) > 1:
        if sys.argv[1] == '--test':
            max_requests = 100  # Test mode: geocode only 100 addresses
            print("🧪 Running in TEST mode (100 geocoding requests max)\n")
        elif sys.argv[1] == '--limit':
            max_requests = int(sys.argv[2]) if len(sys.argv) > 2 else 500
            print(f"🔢 Running with geocoding limit: {max_requests} requests\n")
    
    main(max_geocoding_requests=max_requests)
