"""
DataLoader Module

Handles loading and preprocessing of parking ticket CSV files.
Single Responsibility: Data loading and initial transformation.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional


class DataLoader:
    """Loads and preprocesses parking ticket data with proper data types."""
    
    # Define proper data types based on Toronto's field descriptions
    DTYPES = {
        'tag_number_masked': str,
        'date_of_infraction': 'Int32',
        'infraction_code': 'Int16',
        'infraction_description': str,
        'set_fine_amount': 'float32',
        'time_of_infraction': 'Int16',
        'location1': str,
        'location2': str,
        'location3': str,
        'location4': str,
        'province': str
    }
    
    def __init__(self, verbose: bool = True):
        """
        Initialize DataLoader.
        
        Args:
            verbose: Whether to print loading progress
        """
        self.verbose = verbose
        self._loaded_files = []
    
    def load_single_file(self, file_path: str) -> pd.DataFrame:
        """
        Load a single CSV file with proper data types.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with loaded and preprocessed data
        """
        try:
            df = pd.read_csv(file_path, dtype=self.DTYPES, low_memory=False)
            
            if self.verbose:
                print(f"✓ Loaded {Path(file_path).name}: {len(df):,} records")
            
            self._loaded_files.append(file_path)
            return df
            
        except Exception as e:
            print(f"✗ Error loading {file_path}: {e}")
            return pd.DataFrame()
    
    def load_multiple_files(self, file_paths: List[str]) -> pd.DataFrame:
        """
        Load and concatenate multiple CSV files.
        
        Args:
            file_paths: List of paths to CSV files
            
        Returns:
            Combined DataFrame
        """
        dfs = []
        
        for file_path in file_paths:
            df = self.load_single_file(file_path)
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            print("No data loaded")
            return pd.DataFrame()
        
        combined_df = pd.concat(dfs, ignore_index=True)
        
        if self.verbose:
            print(f"\n📊 Total records loaded: {len(combined_df):,}")
        
        return combined_df
    
    def get_loaded_files(self) -> List[str]:
        """Return list of successfully loaded files."""
        return self._loaded_files.copy()


class DataPreprocessor:
    """Preprocesses parking ticket data for analysis."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize DataPreprocessor.
        
        Args:
            verbose: Whether to print preprocessing progress
        """
        self.verbose = verbose
    
    def add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract temporal features from date and time columns.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with added temporal features
        """
        # Convert date to datetime
        df['date'] = pd.to_datetime(
            df['date_of_infraction'].astype(str),
            format='%Y%m%d',
            errors='coerce'
        )
        
        # Extract date components
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['day_of_week'] = df['date'].dt.dayofweek
        df['week_of_year'] = df['date'].dt.isocalendar().week
        
        # Extract time components
        if 'time_of_infraction' in df.columns:
            df['hour'] = (df['time_of_infraction'] // 100).clip(0, 23)
            df['minute'] = (df['time_of_infraction'] % 100).clip(0, 59)
        
        if self.verbose:
            print("✓ Added temporal features")
        
        return df
    
    def extract_tag_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract numeric portion from masked tag numbers.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with tag_numeric column
        """
        # Remove asterisks and convert to integers
        # Use pd.to_numeric with errors='coerce' to handle malformed data
        df['tag_numeric'] = pd.to_numeric(
            df['tag_number_masked'].str.replace('*', '', regex=False),
            errors='coerce'
        ).astype('Int64')
        
        if self.verbose:
            print("✓ Extracted numeric tag values")
        
        return df
    
    def add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add additional derived features for analysis.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with derived features
        """
        # Last digit of tag number (important for pattern analysis)
        df['tag_last_digit'] = df['tag_numeric'] % 10
        
        # Is weekend
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        
        # Time period (morning, afternoon, evening, night)
        if 'hour' in df.columns:
            df['time_period'] = pd.cut(
                df['hour'],
                bins=[0, 6, 12, 18, 24],
                labels=['night', 'morning', 'afternoon', 'evening'],
                include_lowest=True
            )
        
        if self.verbose:
            print("✓ Added derived features")
        
        return df
    
    def preprocess_full_pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run full preprocessing pipeline.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Fully preprocessed DataFrame
        """
        if self.verbose:
            print("\n=== PREPROCESSING DATA ===")
        
        df = self.add_temporal_features(df)
        df = self.extract_tag_numeric(df)
        df = self.add_derived_features(df)
        
        # Remove rows with invalid tag numbers
        initial_count = len(df)
        df = df.dropna(subset=['tag_numeric'])
        removed = initial_count - len(df)
        
        if removed > 0 and self.verbose:
            print(f"⚠ Removed {removed:,} rows with invalid tag numbers")
        
        return df
