"""
Pattern Analyzer Module

Analyzes sequential numbering patterns in parking ticket IDs.
Single Responsibility: Detect and characterize numbering patterns.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import Counter


class SequentialPatternAnalyzer:
    """Analyzes sequential patterns in ticket numbering."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize SequentialPatternAnalyzer.
        
        Args:
            verbose: Whether to print analysis results
        """
        self.verbose = verbose
        self.pattern_rules = {}
    
    def sort_by_tag_number(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sort DataFrame by tag number for sequential analysis.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Sorted DataFrame
        """
        return df.sort_values('tag_numeric').reset_index(drop=True)
    
    def calculate_differences(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate differences between consecutive tag numbers.
        
        Args:
            df: Sorted DataFrame
            
        Returns:
            DataFrame with diff column added
        """
        df['diff'] = df['tag_numeric'].diff()
        return df
    
    def analyze_common_differences(
        self, 
        df: pd.DataFrame, 
        top_n: int = 20
    ) -> pd.Series:
        """
        Find most common differences between consecutive tags.
        
        Args:
            df: DataFrame with diff column
            top_n: Number of top differences to return
            
        Returns:
            Series of most common differences
        """
        diff_counts = df['diff'].value_counts().head(top_n)
        
        if self.verbose:
            print("\n=== MOST COMMON DIFFERENCES ===")
            print(f"Top {top_n} differences between consecutive tags:")
            for diff_val, count in diff_counts.items():
                if pd.notna(diff_val):
                    print(f"  +{int(diff_val):,}: {count:,} times")
        
        return diff_counts
    
    def detect_sf_style_pattern(self, df: pd.DataFrame) -> Dict:
        """
        Detect if there's an SF-style conditional pattern (like +11/+4).
        
        Args:
            df: DataFrame with tag_numeric and diff columns
            
        Returns:
            Dictionary with pattern detection results
        """
        # Calculate previous tag's last digit
        df['prev_last_digit'] = (df['tag_numeric'].shift(1) % 10).fillna(-1)
        
        # Group differences by previous last digit
        pattern_by_digit = {}
        
        for digit in range(10):
            mask = df['prev_last_digit'] == digit
            if mask.sum() > 0:
                diffs = df.loc[mask, 'diff'].dropna()
                if len(diffs) > 0:
                    most_common = diffs.mode()
                    if len(most_common) > 0:
                        pattern_by_digit[digit] = {
                            'most_common_diff': int(most_common.iloc[0]),
                            'count': len(diffs),
                            'percentage': (diffs == most_common.iloc[0]).sum() / len(diffs) * 100
                        }
        
        if self.verbose:
            print("\n=== SF-STYLE PATTERN DETECTION ===")
            print("Increment patterns by last digit of previous tag:")
            for digit, info in sorted(pattern_by_digit.items()):
                print(f"  Last digit {digit} → +{info['most_common_diff']} "
                      f"({info['percentage']:.1f}% of {info['count']:,} cases)")
        
        self.pattern_rules = pattern_by_digit
        return pattern_by_digit
    
    def analyze_forbidden_digits(self, df: pd.DataFrame) -> Dict:
        """
        Identify which digits never appear as last digits (like 7,8,9 in SF).
        
        Args:
            df: DataFrame with tag_last_digit column
            
        Returns:
            Dictionary with digit frequency analysis
        """
        digit_counts = df['tag_last_digit'].value_counts().sort_index()
        total = len(df)
        
        digit_analysis = {}
        for digit in range(10):
            count = digit_counts.get(digit, 0)
            digit_analysis[digit] = {
                'count': int(count),
                'percentage': (count / total * 100) if total > 0 else 0,
                'appears': count > 0
            }
        
        forbidden = [d for d in range(10) if not digit_analysis[d]['appears']]
        
        if self.verbose:
            print("\n=== DIGIT FREQUENCY ANALYSIS ===")
            print("Last digit distribution:")
            for digit, info in digit_analysis.items():
                status = "✓" if info['appears'] else "✗ NEVER"
                print(f"  {digit}: {info['count']:,} times ({info['percentage']:.2f}%) {status}")
            
            if forbidden:
                print(f"\n⚠ Forbidden digits (never appear): {forbidden}")
        
        return digit_analysis
    
    def detect_mathematical_sequences(self, df: pd.DataFrame) -> Dict:
        """
        Check for multi-step mathematical sequences.
        
        Args:
            df: Sorted DataFrame
            
        Returns:
            Dictionary with sequence analysis
        """
        # Two-step differences
        df['two_step_diff'] = df['tag_numeric'].diff(periods=2)
        two_step_common = df['two_step_diff'].value_counts().head(10)
        
        # Three-step differences
        df['three_step_diff'] = df['tag_numeric'].diff(periods=3)
        three_step_common = df['three_step_diff'].value_counts().head(10)
        
        if self.verbose:
            print("\n=== MULTI-STEP SEQUENCES ===")
            print("Two-step differences (current - 2 positions back):")
            for diff, count in two_step_common.items():
                if pd.notna(diff):
                    print(f"  +{int(diff):,}: {count:,} times")
            
            print("\nThree-step differences (current - 3 positions back):")
            for diff, count in three_step_common.items():
                if pd.notna(diff):
                    print(f"  +{int(diff):,}: {count:,} times")
        
        return {
            'two_step': two_step_common.to_dict(),
            'three_step': three_step_common.to_dict()
        }


class TransitionMatrixAnalyzer:
    """Analyzes state transitions in last digits."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize TransitionMatrixAnalyzer.
        
        Args:
            verbose: Whether to print analysis results
        """
        self.verbose = verbose
        self.transition_matrix = None
    
    def build_transition_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build transition matrix of last digit changes.
        
        Args:
            df: DataFrame with tag_last_digit column
            
        Returns:
            Transition matrix as DataFrame
        """
        # Get current and next last digits
        current_digits = df['tag_last_digit'].iloc[:-1]
        next_digits = df['tag_last_digit'].iloc[1:].reset_index(drop=True)
        
        # Build transition counts
        transitions = pd.crosstab(
            current_digits,
            next_digits,
            rownames=['From'],
            colnames=['To']
        )
        
        # Convert to percentages
        self.transition_matrix = transitions.div(
            transitions.sum(axis=1),
            axis=0
        ) * 100
        
        if self.verbose:
            print("\n=== TRANSITION MATRIX ===")
            print("Probability (%) of transitioning from one last digit to another:")
            print(self.transition_matrix.round(1))
        
        return self.transition_matrix
    
    def find_dominant_transitions(
        self,
        threshold: float = 50.0
    ) -> List[Tuple[int, int, float]]:
        """
        Find dominant transitions (>threshold%).
        
        Args:
            threshold: Minimum percentage to be considered dominant
            
        Returns:
            List of (from_digit, to_digit, percentage) tuples
        """
        if self.transition_matrix is None:
            return []
        
        dominant = []
        for from_digit in self.transition_matrix.index:
            for to_digit in self.transition_matrix.columns:
                prob = self.transition_matrix.loc[from_digit, to_digit]
                if prob >= threshold:
                    dominant.append((int(from_digit), int(to_digit), prob))
        
        if self.verbose and dominant:
            print(f"\n=== DOMINANT TRANSITIONS (≥{threshold}%) ===")
            for from_d, to_d, prob in sorted(dominant, key=lambda x: -x[2]):
                print(f"  {from_d} → {to_d}: {prob:.1f}%")
        
        return dominant
