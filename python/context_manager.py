# python/context_manager_practice.py

import time
import pandas as pd
from contextlib import contextmanager

class DataPipelineManager:
    """Professional context manager for data pipelines"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.start_time = None
        self.metrics = {}
    
    def __enter__(self):
        self.start_time = time.time()
        print(f"🚀 STARTING PIPELINE: {self.pipeline_name}")
        print("=" * 50)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        
        print("\n" + "=" * 50)
        if exc_type:
            print(f" PIPELINE FAILED: {self.pipeline_name}")
            print(f"   Error: {exc_type.__name__}: {exc_val}")
        else:
            print(f" PIPELINE COMPLETED: {self.pipeline_name}")
            print(f"   Duration: {duration:.2f} seconds")
        
        # Report metrics
        if self.metrics:
            print("   Metrics:")
            for key, value in self.metrics.items():
                print(f"     {key}: {value}")
        
        return False  # Don't suppress exceptions

@contextmanager
def data_quality_checker(df, required_columns=None, min_rows=1):
    """Context manager for data quality validation"""
    print("🔍 RUNNING DATA QUALITY CHECKS...")
    
    checks_passed = True
    issues = []
    
    try:
        # Check 1: Minimum rows
        if len(df) < min_rows:
            issues.append(f"Insufficient data: {len(df)} rows (minimum: {min_rows})")
            checks_passed = False
        
        # Check 2: Required columns
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                issues.append(f"Missing columns: {missing_columns}")
                checks_passed = False
        
        # Check 3: Null values
        null_counts = df.isnull().sum()
        high_null_columns = null_counts[null_counts > len(df) * 0.5]  # >50% null
        if not high_null_columns.empty:
            issues.append(f"High null values in: {list(high_null_columns.index)}")
            checks_passed = False
        
        yield checks_passed, issues
        
    except Exception as e:
        issues.append(f"Quality check error: {e}")
        yield False, issues

# Practice using both context managers
def practice_context_managers():
    """Practice using professional context managers"""
    
    # Example 1: Complete pipeline with manager
    with DataPipelineManager("Sales Data ETL") as pipeline:
        # Simulate pipeline work
        time.sleep(1)
        
        # Create sample data
        data = {
            'sale_id': range(1000),
            'amount': [i * 10 for i in range(1000)],
            'product': [f'Product_{i % 100}' for i in range(1000)]
        }
        df = pd.DataFrame(data)
        
        # Run quality checks
        with data_quality_checker(df, required_columns=['sale_id', 'amount'], min_rows=10) as (passed, issues):
            if passed:
                print(" Data quality checks passed")
                pipeline.metrics['records_processed'] = len(df)
                pipeline.metrics['quality_score'] = 'A'
            else:
                print(" Data quality issues found:")
                for issue in issues:
                    print(f"     - {issue}")
                raise ValueError("Data quality check failed")
        
        # Simulate more work
        time.sleep(0.5)
        pipeline.metrics['final_status'] = 'success'

if __name__ == "__main__":
    practice_context_managers()