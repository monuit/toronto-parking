"""
Visualizer Module

Creates charts and graphs for pattern visualization.
Single Responsibility: Generate visual representations of analysis.
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, List


class PatternVisualizer:
    """Creates visualizations for sequential patterns."""
    
    def __init__(self, output_dir: str = "analysis_output"):
        """
        Initialize PatternVisualizer.
        
        Args:
            output_dir: Directory to save visualizations
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        plt.style.use('seaborn-v0_8-darkgrid')
    
    def plot_difference_distribution(
        self,
        df: pd.DataFrame,
        top_n: int = 20
    ) -> str:
        """
        Plot distribution of differences between consecutive tickets.
        
        Args:
            df: DataFrame with diff column
            top_n: Number of top differences to show
            
        Returns:
            Path to saved plot
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        diff_counts = df['diff'].value_counts().head(top_n)
        
        ax.bar(range(len(diff_counts)), diff_counts.values)
        ax.set_xlabel('Difference Value', fontsize=12)
        ax.set_ylabel('Frequency', fontsize=12)
        ax.set_title('Top Differences Between Consecutive Ticket Numbers', 
                     fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(diff_counts)))
        ax.set_xticklabels([f'+{int(d)}' for d in diff_counts.index], rotation=45)
        
        plt.tight_layout()
        output_path = self.output_dir / 'difference_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)
    
    def plot_transition_matrix(
        self,
        transition_matrix: pd.DataFrame
    ) -> str:
        """
        Plot last digit transition matrix as heatmap.
        
        Args:
            transition_matrix: Transition probability matrix
            
        Returns:
            Path to saved plot
        """
        fig, ax = plt.subplots(figsize=(10, 8))
        
        im = ax.imshow(transition_matrix, cmap='YlOrRd', aspect='auto')
        
        ax.set_xticks(range(len(transition_matrix.columns)))
        ax.set_yticks(range(len(transition_matrix.index)))
        ax.set_xticklabels(transition_matrix.columns)
        ax.set_yticklabels(transition_matrix.index)
        
        ax.set_xlabel('To Digit', fontsize=12)
        ax.set_ylabel('From Digit', fontsize=12)
        ax.set_title('Last Digit Transition Probabilities (%)', 
                     fontsize=14, fontweight='bold')
        
        # Add text annotations
        for i in range(len(transition_matrix.index)):
            for j in range(len(transition_matrix.columns)):
                text = ax.text(j, i, f'{transition_matrix.iloc[i, j]:.0f}',
                             ha="center", va="center", color="black", fontsize=8)
        
        plt.colorbar(im, ax=ax, label='Probability (%)')
        plt.tight_layout()
        
        output_path = self.output_dir / 'transition_matrix.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)


class TemporalVisualizer:
    """Creates visualizations for temporal patterns."""
    
    def __init__(self, output_dir: str = "analysis_output"):
        """
        Initialize TemporalVisualizer.
        
        Args:
            output_dir: Directory to save visualizations
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        plt.style.use('seaborn-v0_8-darkgrid')
    
    def plot_hourly_distribution(
        self,
        hourly_stats: pd.DataFrame
    ) -> str:
        """
        Plot ticket distribution by hour of day.
        
        Args:
            hourly_stats: DataFrame with hourly statistics
            
        Returns:
            Path to saved plot
        """
        fig, ax = plt.subplots(figsize=(14, 6))
        
        ax.bar(hourly_stats['hour'], hourly_stats['ticket_count'], 
               color='steelblue', alpha=0.8)
        ax.set_xlabel('Hour of Day', fontsize=12)
        ax.set_ylabel('Number of Tickets', fontsize=12)
        ax.set_title('Ticket Issuance by Hour of Day', 
                     fontsize=14, fontweight='bold')
        ax.set_xticks(range(24))
        ax.set_xticklabels([f'{h:02d}:00' for h in range(24)], rotation=45)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        output_path = self.output_dir / 'hourly_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)
    
    def plot_day_of_week_patterns(
        self,
        dow_stats: pd.DataFrame
    ) -> str:
        """
        Plot ticket patterns by day of week.
        
        Args:
            dow_stats: DataFrame with day-of-week statistics
            
        Returns:
            Path to saved plot
        """
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        # Ticket counts
        ax1.bar(dow_stats['day_name'], dow_stats['ticket_count'], 
                color='coral', alpha=0.8)
        ax1.set_xlabel('Day of Week', fontsize=12)
        ax1.set_ylabel('Number of Tickets', fontsize=12)
        ax1.set_title('Tickets by Day of Week', fontsize=13, fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)
        
        # Fine amounts
        ax2.bar(dow_stats['day_name'], dow_stats['total_fines'], 
                color='seagreen', alpha=0.8)
        ax2.set_xlabel('Day of Week', fontsize=12)
        ax2.set_ylabel('Total Fines ($)', fontsize=12)
        ax2.set_title('Total Fines by Day of Week', fontsize=13, fontweight='bold')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        output_path = self.output_dir / 'day_of_week_patterns.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)
    
    def plot_monthly_trends(
        self,
        monthly_stats: pd.DataFrame
    ) -> str:
        """
        Plot monthly ticket trends over time.
        
        Args:
            monthly_stats: DataFrame with monthly statistics
            
        Returns:
            Path to saved plot
        """
        fig, ax = plt.subplots(figsize=(14, 6))
        
        # Create period labels
        monthly_stats['period_label'] = (
            monthly_stats['year'].astype(str) + '-' + 
            monthly_stats['month'].astype(str).str.zfill(2)
        )
        
        ax.plot(range(len(monthly_stats)), monthly_stats['ticket_count'], 
                marker='o', linewidth=2, markersize=4, color='darkblue')
        ax.set_xlabel('Month', fontsize=12)
        ax.set_ylabel('Number of Tickets', fontsize=12)
        ax.set_title('Monthly Ticket Trends', fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3)
        
        # Show every nth label to avoid crowding
        step = max(1, len(monthly_stats) // 12)
        ax.set_xticks(range(0, len(monthly_stats), step))
        ax.set_xticklabels(monthly_stats['period_label'].iloc[::step], rotation=45)
        
        plt.tight_layout()
        output_path = self.output_dir / 'monthly_trends.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)


class BatchVisualizer:
    """Creates visualizations for batch patterns."""
    
    def __init__(self, output_dir: str = "analysis_output"):
        """
        Initialize BatchVisualizer.
        
        Args:
            output_dir: Directory to save visualizations
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        plt.style.use('seaborn-v0_8-darkgrid')
    
    def plot_batch_completion(
        self,
        batch_stats: pd.DataFrame,
        batch_size: int = 100
    ) -> str:
        """
        Plot batch completion distribution.
        
        Args:
            batch_stats: DataFrame with batch statistics
            batch_size: Expected batch size
            
        Returns:
            Path to saved plot
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        completion_pct = (batch_stats['ticket_count'] / batch_size * 100)
        
        bins = [0, 25, 50, 75, 95, 100]
        hist, _ = np.histogram(completion_pct, bins=bins)
        
        ax.bar(range(len(hist)), hist, color='mediumpurple', alpha=0.8)
        ax.set_xlabel('Batch Completion %', fontsize=12)
        ax.set_ylabel('Number of Batches', fontsize=12)
        ax.set_title('Distribution of Batch Completion', 
                     fontsize=14, fontweight='bold')
        ax.set_xticks(range(len(hist)))
        ax.set_xticklabels(['0-25%', '25-50%', '50-75%', '75-95%', '95-100%'])
        
        for i, v in enumerate(hist):
            ax.text(i, v, str(int(v)), ha='center', va='bottom')
        
        plt.tight_layout()
        output_path = self.output_dir / 'batch_completion.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)
    
    def plot_gap_distribution(
        self,
        large_gaps: pd.DataFrame
    ) -> str:
        """
        Plot distribution of large gaps in sequence.
        
        Args:
            large_gaps: DataFrame of large gaps
            
        Returns:
            Path to saved plot
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.hist(large_gaps['diff'], bins=30, color='crimson', alpha=0.7, edgecolor='black')
        ax.set_xlabel('Gap Size', fontsize=12)
        ax.set_ylabel('Frequency', fontsize=12)
        ax.set_title('Distribution of Large Gaps Between Ticket Numbers', 
                     fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        output_path = self.output_dir / 'gap_distribution.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)
