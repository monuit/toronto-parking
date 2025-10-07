"""
Main Analysis Pipeline

Orchestrates all analysis modules to discover parking ticket patterns.
Single Responsibility: Coordinate analysis workflow and generate reports.
"""

import sys
from pathlib import Path
import pandas as pd
from typing import List, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_loader import DataLoader, DataPreprocessor
from pattern_analyzer import SequentialPatternAnalyzer, TransitionMatrixAnalyzer
from temporal_analyzer import TemporalPatternAnalyzer, IssuanceRateAnalyzer
from batch_detector import BatchBoundaryDetector, OfficerPatternAnalyzer, SequencePredictor
from prediction_engine import TicketNumberPredictor, BatchMonitoringPredictor, PatternBasedScraper
from visualizer import PatternVisualizer, TemporalVisualizer, BatchVisualizer


class AnalysisPipeline:
    """Main pipeline that orchestrates all analysis modules."""
    
    def __init__(self, verbose: bool = True):
        """
        Initialize AnalysisPipeline.
        
        Args:
            verbose: Whether to print detailed progress
        """
        self.verbose = verbose
        self.data = None
        self.sorted_data = None
        self.results = {}
    
    def run_full_analysis(
        self,
        file_paths: List[str],
        output_dir: str = "analysis_output",
        create_visualizations: bool = True
    ) -> dict:
        """
        Run complete analysis pipeline.
        
        Args:
            file_paths: List of CSV file paths to analyze
            output_dir: Directory for output files
            create_visualizations: Whether to generate charts
            
        Returns:
            Dictionary containing all analysis results
        """
        print("="*70)
        print("TORONTO PARKING TICKET PATTERN ANALYSIS")
        print("="*70)
        
        # Stage 1: Load and preprocess data
        print("\n[1/6] LOADING AND PREPROCESSING DATA")
        print("-"*70)
        self.data = self._load_data(file_paths)
        
        if self.data is None or len(self.data) == 0:
            print("❌ No data loaded. Exiting.")
            return {}
        
        # Stage 2: Sequential pattern analysis
        print("\n[2/6] ANALYZING SEQUENTIAL PATTERNS")
        print("-"*70)
        self._analyze_sequential_patterns()
        
        # Stage 3: Temporal pattern analysis
        print("\n[3/6] ANALYZING TEMPORAL PATTERNS")
        print("-"*70)
        self._analyze_temporal_patterns()
        
        # Stage 4: Batch detection
        print("\n[4/6] DETECTING BATCH PATTERNS")
        print("-"*70)
        self._detect_batches()
        
        # Stage 5: Generate predictions
        print("\n[5/6] GENERATING PREDICTIONS")
        print("-"*70)
        self._generate_predictions()
        
        # Stage 6: Create visualizations
        if create_visualizations:
            print("\n[6/6] CREATING VISUALIZATIONS")
            print("-"*70)
            self._create_visualizations(output_dir)
        
        # Print summary
        self._print_summary()
        
        return self.results
    
    def _load_data(self, file_paths: List[str]) -> pd.DataFrame:
        """Load and preprocess data."""
        loader = DataLoader(verbose=self.verbose)
        df = loader.load_multiple_files(file_paths)
        
        if df.empty:
            return None
        
        preprocessor = DataPreprocessor(verbose=self.verbose)
        df = preprocessor.preprocess_full_pipeline(df)
        
        self.results['total_records'] = len(df)
        self.results['date_range'] = (df['date'].min(), df['date'].max())
        
        return df
    
    def _analyze_sequential_patterns(self):
        """Analyze sequential numbering patterns."""
        analyzer = SequentialPatternAnalyzer(verbose=self.verbose)
        
        # Sort by tag number
        self.sorted_data = analyzer.sort_by_tag_number(self.data)
        self.sorted_data = analyzer.calculate_differences(self.sorted_data)
        
        # Analyze patterns
        diff_counts = analyzer.analyze_common_differences(self.sorted_data)
        pattern_rules = analyzer.detect_sf_style_pattern(self.sorted_data)
        digit_analysis = analyzer.analyze_forbidden_digits(self.sorted_data)
        sequences = analyzer.detect_mathematical_sequences(self.sorted_data)
        
        # Transition matrix
        transition_analyzer = TransitionMatrixAnalyzer(verbose=self.verbose)
        transition_matrix = transition_analyzer.build_transition_matrix(self.sorted_data)
        dominant_transitions = transition_analyzer.find_dominant_transitions()
        
        self.results['pattern_rules'] = pattern_rules
        self.results['digit_analysis'] = digit_analysis
        self.results['transition_matrix'] = transition_matrix
        self.results['dominant_transitions'] = dominant_transitions
    
    def _analyze_temporal_patterns(self):
        """Analyze time-based patterns."""
        temporal_analyzer = TemporalPatternAnalyzer(verbose=self.verbose)
        
        daily_stats = temporal_analyzer.analyze_daily_patterns(self.data)
        hourly_stats = temporal_analyzer.analyze_hourly_patterns(self.data)
        dow_stats = temporal_analyzer.analyze_day_of_week_patterns(self.data)
        monthly_stats = temporal_analyzer.analyze_monthly_trends(self.data)
        
        # Issuance rate analysis
        rate_analyzer = IssuanceRateAnalyzer(verbose=self.verbose)
        self.sorted_data = rate_analyzer.calculate_tickets_per_minute(self.sorted_data)
        bursts = rate_analyzer.identify_burst_periods(self.sorted_data)
        productivity = rate_analyzer.analyze_officer_productivity(self.sorted_data)
        
        self.results['daily_stats'] = daily_stats
        self.results['hourly_stats'] = hourly_stats
        self.results['dow_stats'] = dow_stats
        self.results['monthly_stats'] = monthly_stats
        self.results['productivity'] = productivity
    
    def _detect_batches(self):
        """Detect batch patterns and officer behavior."""
        batch_detector = BatchBoundaryDetector(verbose=self.verbose)
        
        large_gaps = batch_detector.find_large_gaps(self.sorted_data)
        batch_size = batch_detector.detect_batch_size(self.sorted_data)
        
        if batch_size:
            self.sorted_data = batch_detector.segment_into_batches(
                self.sorted_data,
                batch_size
            )
        
        # Officer pattern analysis
        officer_analyzer = OfficerPatternAnalyzer(verbose=self.verbose)
        batch_stats = officer_analyzer.identify_potential_officer_batches(self.sorted_data)
        self.sorted_data = officer_analyzer.analyze_location_clustering(self.sorted_data)
        officer_estimates = officer_analyzer.estimate_active_officers_by_period(
            self.sorted_data
        )
        
        # Find incomplete batches
        predictor = SequencePredictor(verbose=self.verbose)
        incomplete_batches = predictor.find_incomplete_batches(
            self.sorted_data,
            batch_size or 100
        )
        
        self.results['batch_size'] = batch_size
        self.results['large_gaps'] = large_gaps
        self.results['batch_stats'] = batch_stats
        self.results['incomplete_batches'] = incomplete_batches
    
    def _generate_predictions(self):
        """Generate predictions for next tickets."""
        predictor = TicketNumberPredictor(verbose=self.verbose)
        
        # Set pattern rules
        if 'pattern_rules' in self.results:
            predictor.set_pattern_rules(self.results['pattern_rules'])
        
        if 'batch_size' in self.results and self.results['batch_size']:
            predictor.set_batch_size(self.results['batch_size'])
        
        # Get highest ticket
        max_ticket = int(self.sorted_data['tag_numeric'].max())
        
        # Predict next sequence
        predicted_sequence = predictor.predict_sequence(max_ticket, count=20)
        
        # Validate accuracy
        accuracy = predictor.validate_prediction_accuracy(self.sorted_data)
        
        # Monitoring recommendations
        monitor_predictor = BatchMonitoringPredictor(verbose=self.verbose)
        monitoring_list = monitor_predictor.generate_monitoring_list(
            self.results.get('incomplete_batches', []),
            self.results.get('batch_size', 100)
        )
        
        efficiency = monitor_predictor.estimate_collection_efficiency(
            self.results.get('incomplete_batches', []),
            batch_size=self.results.get('batch_size', 100)
        )
        
        # Scraping strategy
        scraper = PatternBasedScraper(verbose=self.verbose)
        strategy = scraper.generate_scraping_strategy(
            max_ticket,
            self.results.get('pattern_rules', {}),
            self.results.get('incomplete_batches', []),
            self.results.get('batch_size', 100)
        )
        
        self.results['predicted_sequence'] = predicted_sequence
        self.results['prediction_accuracy'] = accuracy
        self.results['monitoring_list'] = monitoring_list
        self.results['scraping_strategy'] = strategy
    
    def _create_visualizations(self, output_dir: str):
        """Create all visualizations."""
        try:
            # Pattern visualizations
            pattern_viz = PatternVisualizer(output_dir)
            pattern_viz.plot_difference_distribution(self.sorted_data)
            
            if 'transition_matrix' in self.results:
                pattern_viz.plot_transition_matrix(self.results['transition_matrix'])
            
            # Temporal visualizations
            temporal_viz = TemporalVisualizer(output_dir)
            
            if 'hourly_stats' in self.results and not self.results['hourly_stats'].empty:
                temporal_viz.plot_hourly_distribution(self.results['hourly_stats'])
            
            if 'dow_stats' in self.results:
                temporal_viz.plot_day_of_week_patterns(self.results['dow_stats'])
            
            if 'monthly_stats' in self.results:
                temporal_viz.plot_monthly_trends(self.results['monthly_stats'])
            
            # Batch visualizations
            batch_viz = BatchVisualizer(output_dir)
            
            if 'batch_stats' in self.results and not self.results['batch_stats'].empty:
                batch_viz.plot_batch_completion(
                    self.results['batch_stats'],
                    self.results.get('batch_size', 100)
                )
            
            if 'large_gaps' in self.results and not self.results['large_gaps'].empty:
                batch_viz.plot_gap_distribution(self.results['large_gaps'])
            
            print(f"✓ Visualizations saved to {output_dir}/")
            
        except Exception as e:
            print(f"⚠ Could not create some visualizations: {e}")
    
    def _print_summary(self):
        """Print analysis summary."""
        print("\n" + "="*70)
        print("ANALYSIS SUMMARY")
        print("="*70)
        
        print(f"\n📊 Dataset Overview:")
        print(f"  Total tickets: {self.results.get('total_records', 0):,}")
        
        if 'date_range' in self.results:
            start, end = self.results['date_range']
            print(f"  Date range: {start} to {end}")
        
        if 'pattern_rules' in self.results and self.results['pattern_rules']:
            print(f"\n🔢 Pattern Rules Discovered:")
            for digit, rule in sorted(self.results['pattern_rules'].items()):
                print(f"  Last digit {digit} → +{rule['most_common_diff']} "
                      f"({rule['percentage']:.1f}% confidence)")
        
        if 'batch_size' in self.results and self.results['batch_size']:
            print(f"\n📦 Batch Pattern:")
            print(f"  Detected batch size: {self.results['batch_size']}")
        
        if 'prediction_accuracy' in self.results:
            acc = self.results['prediction_accuracy']
            print(f"\n🎯 Prediction Accuracy:")
            print(f"  {acc['accuracy']:.1f}% correct ({acc['correct']:,}/{acc['samples']:,} samples)")
        
        print("\n" + "="*70)
        print("✅ ANALYSIS COMPLETE")
        print("="*70)


def main():
    """Main entry point."""
    import glob
    
    # Get ALL CSV files from all years
    base_dir = Path("parking_data/extracted")
    
    if not base_dir.exists():
        print(f"❌ Data directory not found: {base_dir}")
        print("Please run download_parking_data.py and unzip_parking_data.py first")
        return
    
    # Find all CSV files across all year directories
    file_paths = []
    year_dirs = sorted([d for d in base_dir.iterdir() if d.is_dir()])
    
    print(f"Scanning {len(year_dirs)} year directories...")
    for year_dir in year_dirs:
        year_files = sorted(glob.glob(str(year_dir / "*.csv")))
        if year_files:
            print(f"  {year_dir.name}: {len(year_files)} files")
            file_paths.extend(year_files)
    
    if not file_paths:
        print(f"❌ No CSV files found in {base_dir}")
        return
    
    print(f"\n📊 Total: {len(file_paths)} CSV files across all years")
    print(f"⚠️  This will take several minutes to process...\n")
    
    # Run analysis
    pipeline = AnalysisPipeline(verbose=True)
    results = pipeline.run_full_analysis(
        file_paths,
        output_dir="analysis_output_all_years",
        create_visualizations=True
    )
    
    print("\n📁 Results saved to analysis_output_all_years/")


if __name__ == "__main__":
    main()
