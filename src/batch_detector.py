"""
Batch Detector Module

Detects batch boundaries and officer patterns in ticket numbering.
Single Responsibility: Identify batches and estimate officer behavior.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional


class BatchBoundaryDetector:
    """Detects batch boundaries in sequential ticket numbers."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize BatchBoundaryDetector.
        
        Args:
            verbose: Whether to print analysis results
        """
        self.verbose = verbose
        self.batch_size = None
    
    def find_large_gaps(
        self,
        df: pd.DataFrame,
        gap_threshold: int = 100
    ) -> pd.DataFrame:
        """
        Identify large gaps that might indicate batch boundaries.
        
        Args:
            df: Sorted DataFrame with diff column
            gap_threshold: Minimum gap size to be considered a boundary
            
        Returns:
            DataFrame of large gaps
        """
        large_gaps = df[df['diff'] > gap_threshold].copy()
        large_gaps = large_gaps[['tag_numeric', 'diff', 'date', 'time_of_infraction']]
        
        if self.verbose:
            print(f"\n=== LARGE GAPS (>{gap_threshold}) ===")
            print(f"Found {len(large_gaps):,} large gaps")
            if len(large_gaps) > 0:
                print(f"Median gap size: {large_gaps['diff'].median():,.0f}")
                print(f"Average gap size: {large_gaps['diff'].mean():,.0f}")
                print(f"\nTop 10 largest gaps:")
                top_gaps = large_gaps.nlargest(10, 'diff')
                for _, row in top_gaps.iterrows():
                    print(f"  Tag {int(row['tag_numeric']):,} → "
                          f"+{int(row['diff']):,} (Date: {row['date']})")
        
        return large_gaps
    
    def detect_batch_size(self, df: pd.DataFrame) -> Optional[int]:
        """
        Detect if there's a consistent batch size (like SF's 100).
        
        Args:
            df: DataFrame with diff column
            
        Returns:
            Detected batch size or None
        """
        # Look at large gaps to find patterns
        large_gaps = df[df['diff'] > 50]['diff']
        
        if len(large_gaps) == 0:
            return None
        
        # Check if gaps are multiples of common batch sizes
        common_sizes = [50, 100, 200, 500, 1000]
        best_match = None
        best_score = 0
        
        for size in common_sizes:
            # Calculate how many gaps are close to multiples of this size
            remainders = large_gaps % size
            close_to_multiple = (remainders < 10) | (remainders > size - 10)
            score = close_to_multiple.sum() / len(large_gaps)
            
            if score > best_score:
                best_score = score
                best_match = size
        
        if best_score > 0.3:  # At least 30% match
            self.batch_size = best_match
            
            if self.verbose:
                print(f"\n=== BATCH SIZE DETECTION ===")
                print(f"Detected batch size: {best_match}")
                print(f"Confidence: {best_score*100:.1f}%")
            
            return best_match
        
        return None
    
    def segment_into_batches(
        self,
        df: pd.DataFrame,
        batch_size: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Segment ticket numbers into batches.
        
        Args:
            df: Sorted DataFrame
            batch_size: Size of batches (auto-detected if None)
            
        Returns:
            DataFrame with batch_id column
        """
        if batch_size is None:
            batch_size = self.batch_size or 100
        
        # Assign batch IDs based on tag numbers
        df['batch_id'] = df['tag_numeric'] // batch_size
        
        # Count tickets per batch
        batch_counts = df.groupby('batch_id').size()
        
        if self.verbose:
            print(f"\n=== BATCH SEGMENTATION (size={batch_size}) ===")
            print(f"Total batches: {len(batch_counts):,}")
            print(f"Avg tickets per batch: {batch_counts.mean():.1f}")
            print(f"Full batches (={batch_size}): {(batch_counts == batch_size).sum():,}")
            print(f"Partial batches (<{batch_size}): {(batch_counts < batch_size).sum():,}")
        
        return df


class OfficerPatternAnalyzer:
    """Analyzes patterns that may indicate individual officer behavior."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize OfficerPatternAnalyzer.
        
        Args:
            verbose: Whether to print analysis results
        """
        self.verbose = verbose
    
    def identify_potential_officer_batches(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Identify batches that likely belong to same officer.
        
        Args:
            df: DataFrame with batch_id column
            
        Returns:
            DataFrame with officer batch statistics
        """
        if 'batch_id' not in df.columns:
            if self.verbose:
                print("⚠ No batch_id column. Run batch segmentation first.")
            return pd.DataFrame()
        
        # Analyze each batch
        batch_stats = df.groupby('batch_id').agg({
            'tag_numeric': ['min', 'max', 'count'],
            'date': ['min', 'max', 'nunique'],
            'infraction_code': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None,
            'location2': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None
        }).reset_index()
        
        batch_stats.columns = [
            'batch_id', 'first_tag', 'last_tag', 'ticket_count',
            'first_date', 'last_date', 'days_span',
            'common_infraction', 'common_location'
        ]
        
        # Calculate batch completion time (if same day)
        batch_stats['same_day'] = batch_stats['first_date'] == batch_stats['last_date']
        
        if self.verbose:
            print("\n=== OFFICER BATCH ANALYSIS ===")
            same_day_batches = batch_stats[batch_stats['same_day']]
            print(f"Single-day batches: {len(same_day_batches):,} "
                  f"({len(same_day_batches)/len(batch_stats)*100:.1f}%)")
            
            if len(same_day_batches) > 0:
                print(f"These likely represent individual officer assignments")
        
        return batch_stats
    
    def analyze_location_clustering(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze if consecutive tickets are in similar locations.
        
        Args:
            df: DataFrame with location columns
            
        Returns:
            DataFrame with location clustering metrics
        """
        # Check if consecutive tickets share locations
        df['same_location_as_prev'] = (
            df['location2'] == df['location2'].shift(1)
        )
        
        # Calculate clustering score (moving window)
        window_size = 10
        df['location_cluster_score'] = (
            df['same_location_as_prev']
            .rolling(window=window_size, min_periods=1)
            .mean()
        )
        
        if self.verbose:
            clustering_pct = df['same_location_as_prev'].sum() / len(df) * 100
            print("\n=== LOCATION CLUSTERING ===")
            print(f"Consecutive tickets at same location: {clustering_pct:.1f}%")
            print("(Higher % suggests officers work in focused areas)")
        
        return df
    
    def estimate_active_officers_by_period(
        self,
        df: pd.DataFrame,
        period: str = 'D'
    ) -> pd.DataFrame:
        """
        Estimate number of active officers per time period.
        
        Args:
            df: DataFrame with batch_id and date
            period: Pandas time period ('D'=day, 'H'=hour, 'W'=week)
            
        Returns:
            DataFrame with officer estimates
        """
        if 'batch_id' not in df.columns:
            return pd.DataFrame()
        
        # Group by time period
        df['period'] = df['date'].dt.to_period(period)
        
        officer_estimates = df.groupby('period').agg({
            'batch_id': 'nunique',  # Unique batches = approximate officers
            'tag_numeric': 'count'
        }).reset_index()
        
        officer_estimates.columns = ['period', 'estimated_officers', 'total_tickets']
        officer_estimates['tickets_per_officer'] = (
            officer_estimates['total_tickets'] / officer_estimates['estimated_officers']
        )
        
        if self.verbose:
            print(f"\n=== OFFICER ESTIMATES (per {period}) ===")
            print(f"Average officers active: {officer_estimates['estimated_officers'].mean():.0f}")
            print(f"Peak officers: {officer_estimates['estimated_officers'].max()}")
            print(f"Avg tickets per officer: {officer_estimates['tickets_per_officer'].mean():.1f}")
        
        return officer_estimates


class SequencePredictor:
    """Predicts incomplete batch ranges for monitoring."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize SequencePredictor.
        
        Args:
            verbose: Whether to print predictions
        """
        self.verbose = verbose
    
    def find_incomplete_batches(
        self,
        df: pd.DataFrame,
        batch_size: int = 100
    ) -> List[Dict]:
        """
        Find batches that are not yet complete.
        
        Args:
            df: DataFrame with batch_id
            batch_size: Expected batch size
            
        Returns:
            List of incomplete batch information
        """
        if 'batch_id' not in df.columns:
            return []
        
        # Find batches with fewer tickets than expected
        batch_counts = df.groupby('batch_id').agg({
            'tag_numeric': ['min', 'max', 'count']
        }).reset_index()
        
        batch_counts.columns = ['batch_id', 'first_tag', 'last_tag', 'count']
        
        incomplete = batch_counts[batch_counts['count'] < batch_size].copy()
        
        # Calculate expected next tag for each incomplete batch
        incomplete['expected_range_start'] = incomplete['batch_id'] * batch_size
        incomplete['expected_range_end'] = (incomplete['batch_id'] + 1) * batch_size - 1
        incomplete['tickets_remaining'] = batch_size - incomplete['count']
        
        if self.verbose:
            print(f"\n=== INCOMPLETE BATCHES ===")
            print(f"Found {len(incomplete)} incomplete batches")
            print(f"Total expected tickets: {incomplete['tickets_remaining'].sum():,}")
            
            if len(incomplete) > 0:
                print("\nTop 5 most active incomplete batches:")
                top_incomplete = incomplete.nlargest(5, 'count')
                for _, batch in top_incomplete.iterrows():
                    print(f"  Batch {int(batch['batch_id'])}: "
                          f"{int(batch['count'])}/{batch_size} tickets "
                          f"(range {int(batch['expected_range_start']):,} - "
                          f"{int(batch['expected_range_end']):,})")
        
        return incomplete.to_dict('records')
