"""
Unzip Toronto Parking Data

This script unzips all downloaded parking ticket data files
and organizes them by year.
"""

import zipfile
import os
from pathlib import Path

# Configuration
DOWNLOAD_DIR = "parking_data"
EXTRACTED_DIR = Path(DOWNLOAD_DIR) / "extracted"

def unzip_file(zip_path, extract_to):
    """
    Unzip a file to the specified directory
    """
    try:
        print(f"Unzipping: {zip_path.name}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of files in the zip
            file_list = zip_ref.namelist()
            print(f"  Contains {len(file_list)} file(s)")
            
            # Extract all files
            zip_ref.extractall(extract_to)
            
        print(f"✓ Successfully extracted to: {extract_to}")
        return True, file_list
    except zipfile.BadZipFile:
        print(f"✗ Error: {zip_path.name} is not a valid zip file")
        return False, []
    except Exception as e:
        print(f"✗ Error extracting {zip_path.name}: {e}")
        return False, []

def get_zip_files():
    """
    Get all zip files from the extracted directory
    """
    zip_files = []
    
    if not EXTRACTED_DIR.exists():
        print(f"Error: {EXTRACTED_DIR} does not exist")
        return zip_files
    
    # Walk through all year subdirectories
    for year_dir in sorted(EXTRACTED_DIR.iterdir()):
        if year_dir.is_dir():
            # Find all .zip files in this year directory
            for file_path in year_dir.glob("*.zip"):
                zip_files.append((year_dir.name, file_path))
    
    return zip_files

def unzip_all_files():
    """
    Unzip all parking data files
    """
    # Create extracted directory
    EXTRACTED_DIR.mkdir(exist_ok=True)
    
    # Get all zip files
    zip_files = get_zip_files()
    
    if not zip_files:
        print("No zip files found to extract")
        return
    
    print(f"Found {len(zip_files)} zip file(s) to extract\n")
    
    summary = {
        'success': [],
        'failed': [],
        'total_files': 0
    }
    
    # Process each zip file
    for year, zip_path in zip_files:
        print(f"\n{'='*60}")
        print(f"Processing Year: {year}")
        print('='*60)
        
        # Create year directory in extracted folder
        year_extract_dir = EXTRACTED_DIR / year
        year_extract_dir.mkdir(exist_ok=True)
        
        # Unzip the file
        success, file_list = unzip_file(zip_path, year_extract_dir)
        
        if success:
            summary['success'].append(year)
            summary['total_files'] += len(file_list)
            
            # List extracted files
            print(f"  Extracted files:")
            for filename in file_list[:5]:  # Show first 5 files
                print(f"    - {filename}")
            if len(file_list) > 5:
                print(f"    ... and {len(file_list) - 5} more file(s)")
        else:
            summary['failed'].append(year)
    
    return summary

def get_file_sizes(directory):
    """
    Calculate total size of files in directory
    """
    total_size = 0
    file_count = 0
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = Path(root) / file
            try:
                total_size += file_path.stat().st_size
                file_count += 1
            except:
                pass
    
    return total_size, file_count

def format_bytes(bytes_size):
    """
    Convert bytes to human readable format
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"

def print_summary(summary):
    """
    Print extraction summary
    """
    print("\n" + "="*60)
    print("EXTRACTION SUMMARY")
    print("="*60)
    
    print(f"\n✓ Successfully extracted: {len(summary['success'])} year(s)")
    for year in summary['success']:
        year_dir = EXTRACTED_DIR / year
        size, count = get_file_sizes(year_dir)
        print(f"  - {year}: {count} file(s), {format_bytes(size)}")
    
    if summary['failed']:
        print(f"\n✗ Failed to extract: {len(summary['failed'])} year(s)")
        for year in summary['failed']:
            print(f"  - {year}")
    
    print(f"\n📊 Total files extracted: {summary['total_files']}")
    
    # Calculate total extracted size
    if EXTRACTED_DIR.exists():
        total_size, total_count = get_file_sizes(EXTRACTED_DIR)
        print(f"📦 Total extracted size: {format_bytes(total_size)}")
    
    print(f"\n📁 All extracted files saved to: {os.path.abspath(EXTRACTED_DIR)}")
    print("="*60)

def main():
    """
    Main execution function
    """
    print("="*60)
    print("Toronto Parking Data Unzipper")
    print("="*60)
    
    # Check if data directory exists
    if not EXTRACTED_DIR.exists():
        print(f"\nError: {EXTRACTED_DIR} does not exist")
        print("Please run download_parking_data.py first to download the data")
        return
    
    # Unzip all files
    summary = unzip_all_files()
    
    # Print summary
    if summary:
        print_summary(summary)
    
    print("\n✓ Extraction complete!")
    print("\nYou can now analyze the CSV files in the 'extracted' directory")

if __name__ == "__main__":
    main()
