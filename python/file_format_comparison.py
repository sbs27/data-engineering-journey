import pandas as pd
import numpy as np
import time
import os

def create_realistic_dataset():
    """Create a realistic e-commerce dataset"""
    print("Creating realistic e-commerce dataset...")
    
    np.random.seed(42)
    num_records = 100000
    
    # Realistic data distributions
    products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor']
    categories = ['Electronics', 'Computers', 'Audio', 'Accessories']
    countries = ['US', 'UK', 'CA', 'AU', 'DE']
    
    data = {
        'order_id': range(1000, 1000 + num_records),
        'customer_id': np.random.randint(100, 5000, num_records),
        'product': np.random.choice(products, num_records),
        'category': np.random.choice(categories, num_records),
        'quantity': np.random.randint(1, 5, num_records),
        'price': np.random.normal(500, 200, num_records).round(2),
        'country': np.random.choice(countries, num_records),
        'order_date': pd.date_range('2024-01-01', periods=num_records, freq='H')
    }
    
    df = pd.DataFrame(data)
    # Remove negative prices
    df['price'] = df['price'].clip(lower=10)
    
    return df

def benchmark_real_scenarios(df):
    """Test real-world scenarios"""
    print("\n" + "="*60)
    print("REAL-WORLD FILE FORMAT COMPARISON")
    print("="*60)
    
    # Scenario 1: Full dataset storage
    print("\n SCENARIO 1: Full Dataset Storage")
    
    start = time.time()
    df.to_csv('data/full_dataset.csv', index=False)
    csv_time = time.time() - start
    csv_size = os.path.getsize('data/full_dataset.csv')
    
    start = time.time()
    df.to_parquet('data/full_dataset.parquet', index=False)
    parquet_time = time.time() - start
    parquet_size = os.path.getsize('data/full_dataset.parquet')
    
    print(f"CSV:      {csv_time:.2f}s, {csv_size:,} bytes")
    print(f"Parquet:  {parquet_time:.2f}s, {parquet_size:,} bytes")
    print(f"Savings:  {((csv_size - parquet_size) / csv_size * 100):.1f}% smaller")
    
    # Scenario 2: Reading specific columns (common in analytics)
    print("\n SCENARIO 2: Reading Specific Columns")
    
    start = time.time()
    csv_df = pd.read_csv('data/full_dataset.csv', usecols=['product', 'price'])
    csv_read_time = time.time() - start
    
    start = time.time()
    parquet_df = pd.read_parquet('data/full_dataset.parquet', columns=['product', 'price'])
    parquet_read_time = time.time() - start
    
    print(f"CSV read 2 columns:   {csv_read_time:.2f}s")
    print(f"Parquet read 2 columns: {parquet_read_time:.2f}s")
    print(f"Parquet is {((csv_read_time - parquet_read_time) / csv_read_time * 100):.1f}% faster!")
    
    # Scenario 3: Filtering during read
    print("\n SCENARIO 3: Filtering During Read")
    
    start = time.time()
    expensive_orders_csv = pd.read_csv('data/full_dataset.csv')
    expensive_orders_csv = expensive_orders_csv[expensive_orders_csv['price'] > 1000]
    csv_filter_time = time.time() - start
    
    start = time.time()
    expensive_orders_parquet = pd.read_parquet(
        'data/full_dataset.parquet', 
        filters=[('price', '>', 1000)]
    )
    parquet_filter_time = time.time() - start
    
    print(f"CSV filter:    {csv_filter_time:.2f}s, {len(expensive_orders_csv)} records")
    print(f"Parquet filter: {parquet_filter_time:.2f}s, {len(expensive_orders_parquet)} records")
    print(f"Parquet filtering is {((csv_filter_time - parquet_filter_time) / csv_filter_time * 100):.1f}% faster!")

if __name__ == "__main__":
    df = create_realistic_dataset()
    print(f"Dataset: {len(df):,} records, {len(df.columns)} columns")
    benchmark_real_scenarios(df)