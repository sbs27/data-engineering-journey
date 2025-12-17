"""
Cloud-optimized ETL pipeline for Google Cloud Run.
"""

import pandas as pd
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Cloud ETL Pipeline")
    
    # Create sample data
    data = {
        'date': ['2024-12-17', '2024-12-17', '2024-12-18'],
        'product': ['Cloud_Laptop', 'Cloud_Mouse', 'Cloud_Keyboard'],
        'amount': [1500.00, 30.00, 90.00],
        'quantity': [2, 15, 8]
    }
    
    df = pd.DataFrame(data)
    logger.info(f"Processing {len(df)} records")
    
    # Transform
    df['date'] = pd.to_datetime(df['date'])
    df['total_sales'] = df['amount'] * df['quantity']
    df['processed_at'] = datetime.now()
    df['category'] = 'Electronics'
    
    # Save output
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = f"{output_dir}/cloud_output_{timestamp}.csv"
    df.to_csv(csv_file, index=False)
    
    print(f" ETL Complete! Saved {len(df)} records to {csv_file}")
    print(f" Total Revenue: ${df['total_sales'].sum():,.2f}")
    print(f" Average Sale: ${df['amount'].mean():,.2f}")
    
    return 0

if __name__ == "__main__":
    exit(main())
