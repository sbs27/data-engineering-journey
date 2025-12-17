"""
Simple test script for cloud deployment.
This runs without PostgreSQL to verify cloud setup works.
"""

import pandas as pd
import os
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=== CLOUD DEPLOYMENT TEST ===")
    logger.info("Testing if environment works...")
    
    # Test 1: Check Python version
    import sys
    logger.info(f"Python version: {sys.version}")
    
    # Test 2: Check pandas
    logger.info(f"Pandas version: {pd.__version__}")
    
    # Test 3: Create sample data
    data = {
        'product': ['Cloud_Test_A', 'Cloud_Test_B'],
        'sales': [100, 200],
        'date': [datetime.now().date(), datetime.now().date()]
    }
    df = pd.DataFrame(data)
    
    # Test 4: Save to file
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"cloud_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(output_dir, filename)
    
    df.to_csv(filepath, index=False)
    logger.info(f"Test file created: {filepath}")
    
    # Test 5: Read it back
    df_read = pd.read_csv(filepath)
    logger.info(f"File read back successfully: {len(df_read)} records")
    
    logger.info("=== CLOUD TEST COMPLETE ===")
    logger.info("If you see this, your container works in cloud!")
    
    # Show file contents
    print("\n=== FILE CONTENTS ===")
    with open(filepath, 'r') as f:
        print(f.read())

if __name__ == "__main__":
    main()
