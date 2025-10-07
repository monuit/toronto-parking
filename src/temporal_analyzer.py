"""
Temporal Analyzer Module

Analyzes time-based patterns in parking ticket issuance.
Single Responsibility: Time-series and temporal pattern analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
from datetime import datetime


class TemporalPatternAnalyzer:
    """Analyzes temporal patterns in ticket issuance."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize TemporalPatternAnalyzer.
        
        Args:
            verbose: Whether to print analysis results
        """
        self.verbose = verbose
    
    def analyze_daily_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze ticket patterns by day.
        
        Args:
            df: Input DataFrame with date column
            
        Returns:
            DataFrame with daily statistics
        """
        daily_stats = df.groupby('date').agg({
            'tag_numeric': ['min', 'max', 'count', 'nunique'],
            'infraction_code': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None,
            'set_fine_amount': ['sum', 'mean']
        }).reset_index()
        
        daily_stats.columns = [
            'date', 'min_tag', 'max_tag', 'ticket_count', 'unique_tags',
            'most_common_infraction', 'total_fines', 'avg_fine'
        ]
        
        daily_stats['tag_range'] = daily_stats['max_tag'] - daily_stats['min_tag']
        
        if self.verbose:
            print("\n=== DAILY PATTERN SUMMARY ===")
            print(f"Average tickets per day: {daily_stats['ticket_count'].mean():,.0f}")
            print(f"Peak day: {daily_stats.loc[daily_stats['ticket_count'].idxmax(), 'date']} "
                  f"({daily_stats['ticket_count'].max():,} tickets)")
            print(f"Lowest day: {daily_stats.loc[daily_stats['ticket_count'].idxmin(), 'date']} "
                  f"({daily_stats['ticket_count'].min():,} tickets)")
        
        return daily_stats
    
    def analyze_hourly_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze ticket patterns by hour of day.
        
        Args:
            df: Input DataFrame with hour column
            
        Returns:
            DataFrame with hourly statistics
        """
        if 'hour' not in df.columns:
            if self.verbose:
                print("⚠ No hour data available for hourly analysis")
            return pd.DataFrame()
        
        hourly_stats = df.groupby('hour').agg({
            'tag_numeric': 'count',
            'infraction_code': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None,
            'set_fine_amount': 'mean'
        }).reset_index()
        
        hourly_stats.columns = ['hour', 'ticket_count', 'common_infraction', 'avg_fine']
        
        if self.verbose:
            print("\n=== HOURLY PATTERN SUMMARY ===")
            peak_hour = hourly_stats.loc[hourly_stats['ticket_count'].idxmax()]
            print(f"Peak hour: {int(peak_hour['hour']):02d}:00 "
                  f"({peak_hour['ticket_count']:,} tickets)")
            
            print("\nTickets by hour:")
            for _, row in hourly_stats.iterrows():
                bar_length = int(row['ticket_count'] / hourly_stats['ticket_count'].max() * 30)
                bar = '█' * bar_length
                print(f"  {int(row['hour']):02d}:00 | {bar} {row['ticket_count']:,}")
        
        return hourly_stats
    
    def analyze_day_of_week_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze ticket patterns by day of week.
        
        Args:
            df: Input DataFrame with day_of_week column
            
        Returns:
            DataFrame with day-of-week statistics
        """
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                     'Friday', 'Saturday', 'Sunday']
        
        dow_stats = df.groupby('day_of_week').agg({
            'tag_numeric': 'count',
            'set_fine_amount': 'sum'
        }).reset_index()
        
        dow_stats.columns = ['day_of_week', 'ticket_count', 'total_fines']
        dow_stats['day_name'] = dow_stats['day_of_week'].apply(lambda x: day_names[x])
        
        if self.verbose:
            print("\n=== DAY OF WEEK PATTERNS ===")
            for _, row in dow_stats.iterrows():
                print(f"  {row['day_name']:<10} : {row['ticket_count']:,} tickets, "
                      f"${row['total_fines']:,.0f} in fines")
        
        return dow_stats
    
    def analyze_monthly_trends(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze ticket patterns by month.
        
        Args:
            df: Input DataFrame with year and month columns
            
        Returns:
            DataFrame with monthly statistics
        """
        monthly_stats = df.groupby(['year', 'month']).agg({
            'tag_numeric': 'count',
            'set_fine_amount': 'sum'
        }).reset_index()
        
        monthly_stats.columns = ['year', 'month', 'ticket_count', 'total_fines']
        
        if self.verbose:
            print("\n=== MONTHLY TRENDS ===")
            for year in sorted(monthly_stats['year'].unique()):
                year_data = monthly_stats[monthly_stats['year'] == year]
                total = year_data['ticket_count'].sum()
                print(f"\n{int(year)}: {total:,} tickets total")
                
                for _, row in year_data.head(3).iterrows():
                    month_name = pd.Timestamp(year=int(row['year']), 
                                             month=int(row['month']), day=1).strftime('%B')
                    print(f"  {month_name}: {row['ticket_count']:,} tickets")
        
        return monthly_stats


class IssuanceRateAnalyzer:
    """Analyzes ticket issuance rates over time."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize IssuanceRateAnalyzer.
        
        Args:
            verbose: Whether to print analysis results
        """
        self.verbose = verbose
    
    def calculate_tickets_per_minute(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate issuance rate (tickets per minute).
        
        Args:
            df: Sorted DataFrame with date and time
            
        Returns:
            DataFrame with issuance rates
        """
        # Create datetime column
        df['datetime'] = pd.to_datetime(
            df['date_of_infraction'].astype(str) + 
            df['time_of_infraction'].astype(str).str.zfill(4),
            format='%Y%m%d%H%M',
            errors='coerce'
        )
        
        # Calculate time differences
        df['time_diff_minutes'] = df['datetime'].diff().dt.total_seconds() / 60
        
        if self.verbose:
            valid_diffs = df['time_diff_minutes'].dropna()
            if len(valid_diffs) > 0:
                print("\n=== ISSUANCE RATE ANALYSIS ===")
                print(f"Median time between tickets: {valid_diffs.median():.2f} minutes")
                print(f"Average time between tickets: {valid_diffs.mean():.2f} minutes")
                print(f"Fastest issuance: {valid_diffs.min():.2f} minutes")
        
        return df
    
    def identify_burst_periods(
        self,
        df: pd.DataFrame,
        threshold_minutes: float = 5.0
    ) -> pd.DataFrame:
        """
        Identify periods of rapid ticket issuance (bursts).
        
        Args:
            df: DataFrame with time_diff_minutes column
            threshold_minutes: Max minutes between tickets to be a burst
            
        Returns:
            DataFrame of burst periods
        """
        burst_mask = df['time_diff_minutes'] <= threshold_minutes
        bursts = df[burst_mask].copy()
        
        if self.verbose and len(bursts) > 0:
            print(f"\n=== BURST PERIODS (≤{threshold_minutes} min apart) ===")
            print(f"Found {len(bursts):,} tickets in burst periods")
            print(f"That's {len(bursts)/len(df)*100:.1f}% of all tickets")
        
        return bursts
    
    def analyze_officer_productivity(self, df: pd.DataFrame) -> Dict:
        """
        Estimate officer count based on simultaneous ticket issuance.
        
        Args:
            df: DataFrame with datetime column
            
        Returns:
            Dictionary with productivity metrics
        """
        # Group by 10-minute windows
        df['time_window'] = df['datetime'].dt.floor('10min')
        
        window_counts = df.groupby('time_window').size()
        
        productivity = {
            'max_simultaneous': int(window_counts.max()),
            'avg_per_window': float(window_counts.mean()),
            'estimated_officers': int(window_counts.quantile(0.95))
        }
        
        if self.verbose:
            print("\n=== OFFICER PRODUCTIVITY ESTIMATE ===")
            print(f"Max tickets in 10-min window: {productivity['max_simultaneous']}")
            print(f"Average per 10-min window: {productivity['avg_per_window']:.1f}")
            print(f"Estimated active officers (95th percentile): ~{productivity['estimated_officers']}")
        
        return productivity
