"""
Toronto Parking Data Downloader

This script downloads all parking ticket data from Toronto's Open Data Portal
and organizes it by year.

Toronto Open Data is stored in a CKAN instance. APIs are documented here:
https://docs.ckan.org/en/latest/api/
"""

import requests
import os
import json
from pathlib import Path
from urllib.parse import urlparse
import time

# Configuration
BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
PACKAGE_NAME = "parking-tickets"
DOWNLOAD_DIR = "parking_data"

def get_package_metadata():
    """
    Fetch package metadata from Toronto Open Data API
    Returns the package information including all resources
    """
    print("Fetching package metadata...")
    try:
        url = BASE_URL + "/api/3/action/package_show"
        params = {"id": PACKAGE_NAME}
        response = requests.get(url, params=params)
        response.raise_for_status()
        package = response.json()
        return package['result']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching package metadata: {e}")
        return None

def categorize_resource(resource):
    """
    Categorize a resource based on its name and format
    Returns a dictionary with category information
    """
    name = resource.get('name', '').lower()
    format_type = resource.get('format', '').lower()
    
    category = {
        'name': resource.get('name'),
        'format': format_type,
        'year': None,
        'type': 'unknown',
        'url': resource.get('url')
    }
    
    # Identify if it's a README
    if 'readme' in name or 'data dictionary' in name:
        category['type'] = 'readme'
        return category
    
    # Try to extract year from the name
    import re
    year_match = re.search(r'20\d{2}', name)
    if year_match:
        category['year'] = year_match.group()
        category['type'] = 'data'
    
    return category

def download_file(url, destination_path):
    """
    Download a file from URL to destination path with progress indication
    """
    try:
        print(f"Downloading: {os.path.basename(destination_path)}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(destination_path, 'wb') as f:
            if total_size == 0:
                f.write(response.content)
            else:
                downloaded = 0
                chunk_size = 8192
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        percent = (downloaded / total_size) * 100
                        print(f"  Progress: {percent:.1f}%", end='\r')
                print()  # New line after progress
        
        print(f"✓ Successfully downloaded: {os.path.basename(destination_path)}")
        return True
    except Exception as e:
        print(f"✗ Error downloading {url}: {e}")
        return False

def organize_downloads(package):
    """
    Organize and download all resources from the package
    """
    resources = package.get('resources', [])
    
    if not resources:
        print("No resources found in package")
        return
    
    print(f"\nFound {len(resources)} resources")
    
    # Create base download directory
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
    
    # Create subdirectories
    data_dir = Path(DOWNLOAD_DIR) / "extracted"
    readme_dir = Path(DOWNLOAD_DIR) / "documentation"
    other_dir = Path(DOWNLOAD_DIR) / "other"
    
    data_dir.mkdir(exist_ok=True)
    readme_dir.mkdir(exist_ok=True)
    other_dir.mkdir(exist_ok=True)
    
    # Download and organize files
    download_summary = {
        'data_files': [],
        'readme_files': [],
        'other_files': [],
        'failed': []
    }
    
    for idx, resource in enumerate(resources):
        print(f"\n[{idx+1}/{len(resources)}] Processing resource: {resource.get('name', 'Unknown')}")
        
        # Get resource metadata for non-datastore resources
        if not resource.get("datastore_active", False):
            try:
                metadata_url = BASE_URL + "/api/3/action/resource_show?id=" + resource["id"]
                resource_metadata = requests.get(metadata_url).json()
                resource = resource_metadata.get('result', resource)
            except Exception as e:
                print(f"  Warning: Could not fetch detailed metadata: {e}")
        
        # Use the URL attribute to download the file
        download_url = resource.get('url')
        if not download_url:
            print(f"  Skipping: No download URL available")
            continue
        
        # Categorize the resource
        category_info = categorize_resource(resource)
        item = category_info
        
        # Determine destination directory and filename
        filename = os.path.basename(urlparse(item['url']).path)
        if not filename or filename == '':
            # Generate filename from resource name
            ext = f".{item['format']}" if item['format'] else ''
            filename = f"{item['name'].replace(' ', '_')}{ext}"
        
        if item['type'] == 'readme':
            dest_path = readme_dir / filename
            download_summary['readme_files'].append(filename)
        elif item['type'] == 'data' and item['year']:
            # Create year subdirectory
            year_dir = data_dir / item['year']
            year_dir.mkdir(exist_ok=True)
            dest_path = year_dir / filename
            download_summary['data_files'].append(f"{item['year']}/{filename}")
        else:
            dest_path = other_dir / filename
            download_summary['other_files'].append(filename)
        
        # Download the file
        success = download_file(item['url'], dest_path)
        if not success:
            download_summary['failed'].append(filename)
        
        # Be respectful to the server
        time.sleep(0.5)
    
    return download_summary

def print_summary(summary, package):
    """
    Print download summary
    """
    print("\n" + "="*60)
    print("DOWNLOAD SUMMARY")
    print("="*60)
    
    print(f"\nPackage: {package.get('title', 'N/A')}")
    print(f"Description: {package.get('notes', 'N/A')[:100]}...")
    
    print(f"\n📊 Data Files ({len(summary['data_files'])}):")
    for f in sorted(summary['data_files']):
        print(f"  - {f}")
    
    print(f"\n📖 Documentation Files ({len(summary['readme_files'])}):")
    for f in summary['readme_files']:
        print(f"  - {f}")
    
    if summary['other_files']:
        print(f"\n📁 Other Files ({len(summary['other_files'])}):")
        for f in summary['other_files']:
            print(f"  - {f}")
    
    if summary['failed']:
        print(f"\n❌ Failed Downloads ({len(summary['failed'])}):")
        for f in summary['failed']:
            print(f"  - {f}")
    
    print(f"\n✓ All files saved to: {os.path.abspath(DOWNLOAD_DIR)}")
    print("="*60)

def save_metadata(package):
    """
    Save package metadata to JSON file for reference
    """
    Path(DOWNLOAD_DIR).mkdir(exist_ok=True)
    metadata_file = Path(DOWNLOAD_DIR) / "package_metadata.json"
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(package, f, indent=2, ensure_ascii=False)
    print(f"\n📄 Package metadata saved to: {metadata_file}")

def main():
    """
    Main execution function
    """
    print("="*60)
    print("Toronto Parking Data Downloader")
    print("="*60)
    
    # Get package metadata
    package = get_package_metadata()
    if not package:
        print("Failed to retrieve package metadata. Exiting.")
        return
    
    # Save metadata
    save_metadata(package)
    
    # Download and organize files
    summary = organize_downloads(package)
    
    # Print summary
    if summary:
        print_summary(summary, package)
    
    print("\n✓ Download complete!")

if __name__ == "__main__":
    main()
