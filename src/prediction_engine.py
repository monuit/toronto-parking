"""
Prediction Engine Module

Predicts next ticket numbers based on discovered patterns.
Single Responsibility: Generate predictions for ticket sequences.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple


class TicketNumberPredictor:
    """Predicts next ticket numbers using discovered patterns."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize TicketNumberPredictor.
        
        Args:
            verbose: Whether to print predictions
        """
        self.verbose = verbose
        self.pattern_rules = {}
        self.batch_size = None
    
    def set_pattern_rules(self, pattern_rules: Dict) -> None:
        """
        Set the discovered pattern rules.
        
        Args:
            pattern_rules: Dictionary mapping last digits to increment rules
        """
        self.pattern_rules = pattern_rules
    
    def set_batch_size(self, batch_size: int) -> None:
        """
        Set the detected batch size.
        
        Args:
            batch_size: Size of ticket batches
        """
        self.batch_size = batch_size
    
    def predict_next_ticket(
        self,
        current_ticket: int,
        use_pattern: bool = True
    ) -> int:
        """
        Predict the next ticket number after the given ticket.
        
        Args:
            current_ticket: Current ticket number
            use_pattern: Whether to use discovered pattern rules
            
        Returns:
            Predicted next ticket number
        """
        if not use_pattern or not self.pattern_rules:
            # Simple increment
            return current_ticket + 1
        
        # Get last digit
        last_digit = current_ticket % 10
        
        # Apply pattern rule if exists
        if last_digit in self.pattern_rules:
            increment = self.pattern_rules[last_digit]['most_common_diff']
            return current_ticket + increment
        
        # Default to +1
        return current_ticket + 1
    
    def predict_sequence(
        self,
        start_ticket: int,
        count: int = 10
    ) -> List[int]:
        """
        Predict a sequence of ticket numbers.
        
        Args:
            start_ticket: Starting ticket number
            count: Number of tickets to predict
            
        Returns:
            List of predicted ticket numbers
        """
        sequence = [start_ticket]
        current = start_ticket
        
        for _ in range(count - 1):
            next_ticket = self.predict_next_ticket(current)
            sequence.append(next_ticket)
            current = next_ticket
        
        if self.verbose:
            print(f"\n=== PREDICTED SEQUENCE (from {start_ticket:,}) ===")
            for i, ticket in enumerate(sequence[:10], 1):
                print(f"  {i}. {ticket:,}")
            if len(sequence) > 10:
                print(f"  ... and {len(sequence) - 10} more")
        
        return sequence
    
    def validate_prediction_accuracy(
        self,
        df: pd.DataFrame,
        sample_size: int = 1000
    ) -> Dict:
        """
        Test prediction accuracy against actual data.
        
        Args:
            df: DataFrame with sorted tag_numeric
            sample_size: Number of samples to test
            
        Returns:
            Dictionary with accuracy metrics
        """
        if len(df) < 2:
            return {'accuracy': 0.0, 'samples': 0}
        
        # Take random sample
        sample_indices = np.random.choice(
            len(df) - 1,
            size=min(sample_size, len(df) - 1),
            replace=False
        )
        
        correct = 0
        total = 0
        errors = []
        
        for idx in sample_indices:
            current = int(df.iloc[idx]['tag_numeric'])
            actual_next = int(df.iloc[idx + 1]['tag_numeric'])
            predicted_next = self.predict_next_ticket(current)
            
            if predicted_next == actual_next:
                correct += 1
            else:
                errors.append(abs(predicted_next - actual_next))
            
            total += 1
        
        accuracy = (correct / total * 100) if total > 0 else 0
        avg_error = np.mean(errors) if errors else 0
        
        if self.verbose:
            print(f"\n=== PREDICTION ACCURACY TEST ===")
            print(f"Samples tested: {total:,}")
            print(f"Correct predictions: {correct:,} ({accuracy:.1f}%)")
            if errors:
                print(f"Average error when wrong: {avg_error:.1f}")
        
        return {
            'accuracy': accuracy,
            'samples': total,
            'correct': correct,
            'avg_error': avg_error
        }


class BatchMonitoringPredictor:
    """Predicts which ticket numbers to monitor for new tickets."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize BatchMonitoringPredictor.
        
        Args:
            verbose: Whether to print predictions
        """
        self.verbose = verbose
    
    def generate_monitoring_list(
        self,
        incomplete_batches: List[Dict],
        batch_size: int = 100
    ) -> pd.DataFrame:
        """
        Generate a list of ticket numbers to monitor.
        
        Args:
            incomplete_batches: List of incomplete batch information
            batch_size: Size of batches
            
        Returns:
            DataFrame with monitoring priorities
        """
        monitoring_list = []
        
        for batch in incomplete_batches:
            # Start of batch range
            start = batch['expected_range_start']
            
            # We want to monitor the first ticket in this batch
            # that we haven't seen yet
            next_expected = batch['last_tag'] + 1 if 'last_tag' in batch else start
            
            monitoring_list.append({
                'batch_id': batch['batch_id'],
                'monitor_ticket': next_expected,
                'tickets_remaining': batch['tickets_remaining'],
                'priority': batch['count']  # More filled = higher priority
            })
        
        monitoring_df = pd.DataFrame(monitoring_list)
        
        if len(monitoring_df) == 0:
            return monitoring_df
        
        monitoring_df = monitoring_df.sort_values('priority', ascending=False)
        
        if self.verbose:
            print(f"\n=== MONITORING RECOMMENDATIONS ===")
            print(f"Total batches to monitor: {len(monitoring_df)}")
            print(f"Estimated missing tickets: {monitoring_df['tickets_remaining'].sum():,}")
            print("\nTop 10 batches to monitor (by activity):")
            for _, row in monitoring_df.head(10).iterrows():
                print(f"  Batch {int(row['batch_id'])}: "
                      f"Check ticket {int(row['monitor_ticket']):,} "
                      f"({int(row['tickets_remaining'])} remaining)")
        
        return monitoring_df
    
    def estimate_collection_efficiency(
        self,
        incomplete_batches: List[Dict],
        check_interval_seconds: int = 3,
        batch_size: int = 100
    ) -> Dict:
        """
        Estimate how efficiently tickets can be collected.
        
        Args:
            incomplete_batches: List of incomplete batches
            check_interval_seconds: Seconds between checks
            batch_size: Size of batches
            
        Returns:
            Dictionary with efficiency metrics
        """
        num_batches = len(incomplete_batches)
        
        # Time to check all batches once
        cycle_time = num_batches * check_interval_seconds
        
        # Estimated tickets per hour (assuming moderate issuance rate)
        # Toronto issues ~2.8M tickets/year = ~320/hour
        tickets_per_hour = 320
        
        # How often we'd check each batch per hour
        checks_per_batch_per_hour = 3600 / cycle_time if cycle_time > 0 else 0
        
        metrics = {
            'batches_to_monitor': num_batches,
            'cycle_time_seconds': cycle_time,
            'checks_per_hour': checks_per_batch_per_hour * num_batches,
            'expected_capture_rate': min(99.5, checks_per_batch_per_hour * 100),
            'api_calls_per_day': (3600 / check_interval_seconds) * 24 if check_interval_seconds > 0 else 0
        }
        
        if self.verbose:
            print(f"\n=== COLLECTION EFFICIENCY ===")
            print(f"Batches to monitor: {metrics['batches_to_monitor']}")
            print(f"Check cycle time: {metrics['cycle_time_seconds']:.1f} seconds")
            print(f"API calls per day: {metrics['api_calls_per_day']:,.0f}")
            print(f"Expected capture rate: {metrics['expected_capture_rate']:.1f}%")
            print("\nOptimal strategy: Check each incomplete batch start sequentially")
        
        return metrics


class PatternBasedScraper:
    """Generates efficient scraping strategy based on patterns."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize PatternBasedScraper.
        
        Args:
            verbose: Whether to print strategy
        """
        self.verbose = verbose
    
    def generate_scraping_strategy(
        self,
        current_max_ticket: int,
        pattern_rules: Dict,
        incomplete_batches: List[Dict],
        batch_size: int = 100
    ) -> Dict:
        """
        Generate an efficient scraping strategy.
        
        Args:
            current_max_ticket: Highest known ticket number
            pattern_rules: Dictionary of increment patterns
            incomplete_batches: List of incomplete batches
            batch_size: Size of batches
            
        Returns:
            Dictionary with scraping strategy
        """
        strategy = {
            'method': 'hybrid',
            'primary_targets': [],
            'secondary_targets': []
        }
        
        # Primary: Monitor incomplete batches
        for batch in incomplete_batches[:50]:  # Top 50 most active
            start = batch.get('expected_range_start', 0)
            strategy['primary_targets'].append({
                'type': 'incomplete_batch',
                'start_ticket': start,
                'priority': 'high'
            })
        
        # Secondary: Predict forward from max ticket
        next_ticket = current_max_ticket
        for _ in range(10):
            last_digit = next_ticket % 10
            if last_digit in pattern_rules:
                increment = pattern_rules[last_digit]['most_common_diff']
            else:
                increment = 1
            next_ticket += increment
            
            strategy['secondary_targets'].append({
                'type': 'forward_prediction',
                'ticket_number': next_ticket,
                'priority': 'medium'
            })
        
        if self.verbose:
            print(f"\n=== SCRAPING STRATEGY ===")
            print(f"Strategy: {strategy['method']}")
            print(f"Primary targets: {len(strategy['primary_targets'])} incomplete batches")
            print(f"Secondary targets: {len(strategy['secondary_targets'])} predicted tickets")
            print("\nRecommended approach:")
            print("1. Check all incomplete batch start points (every few seconds)")
            print("2. Predict forward from highest known ticket")
            print("3. Validate new tickets immediately when found")
        
        return strategy
