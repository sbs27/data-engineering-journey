"""
Data processing functions for the ETL pipeline.
This module handles data extraction, transformation, and file operations.
"""

import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any, Tuple
from config.settings import FILE_PATHS, ETL_SETTINGS
import logging

logger = logging.getLogger(__name__)

class DataProcessor:
    """
    Handles data extraction and transformation operations.
    This class reads CSV files, processes data, and manages file output.
    """
    
    def __init__(self):
        """Initialize with file paths from configuration."""
        self.input_file = FILE_PATHS['input_csv']
        self.output_dir = FILE_PATHS['output_dir']
        self.batch_size = ETL_SETTINGS['batch_size']
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
    
    def extract_data(self) -> Tuple[pd.DataFrame, bool]:
        """
        Extract data from CSV file.
        
        Returns:
            tuple: (dataframe, success_flag)
            The dataframe contains the extracted data
            The success_flag indicates whether extraction was successful
        """
        logger.info(f"Extracting data from {self.input_file}")
        
        try:
            # Read CSV file
            df = pd.read_csv(self.input_file)
            
            # Validate required columns
            required_columns = ['date', 'product', 'amount', 'quantity']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                error_msg = f"Missing required columns: {missing_columns}"
                logger.error(error_msg)
                return pd.DataFrame(), False
            
            logger.info(f"Successfully extracted {len(df)} records")
            return df, True
            
        except FileNotFoundError:
            logger.error(f"Input file not found: {self.input_file}")
            return pd.DataFrame(), False
            
        except pd.errors.EmptyDataError:
            logger.error("Input file is empty")
            return pd.DataFrame(), False
            
        except Exception as e:
            logger.error(f"Error extracting data: {e}")
            return pd.DataFrame(), False
    
    def transform_data(self, df: pd.DataFrame) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
        """
        Transform raw data into processed format.
        
        Args:
            df: Raw dataframe from CSV
            
        Returns:
            tuple: (list_of_dicts_for_db, processed_dataframe_for_file)
            The list_of_dicts is formatted for database insertion
            The processed_dataframe is enriched with additional columns
        """
        logger.info("Transforming data")
        
        try:
            # Make a copy to avoid modifying original
            processed_df = df.copy()
            
            # Convert date column to datetime
            processed_df['date'] = pd.to_datetime(processed_df['date'])
            
            # Calculate derived columns
            processed_df['total_sales'] = processed_df['amount'] * processed_df['quantity']
            processed_df['processed_at'] = datetime.now()
            
            # Add product category based on rules
            def categorize_product(product: str) -> str:
                """Categorize products for analysis."""
                product_lower = product.lower()
                
                if any(word in product_lower for word in ['laptop', 'monitor', 'tablet']):
                    return 'Computers'
                elif any(word in product_lower for word in ['keyboard', 'mouse', 'headphones']):
                    return 'Accessories'
                elif any(word in product_lower for word in ['printer', 'scanner']):
                    return 'Office Equipment'
                else:
                    return 'Other'
            
            processed_df['category'] = processed_df['product'].apply(categorize_product)
            
            # Add profit margin (example: 20% for electronics, 30% for accessories)
            def calculate_profit(row):
                if row['category'] == 'Computers':
                    return row['amount'] * 0.20
                elif row['category'] == 'Accessories':
                    return row['amount'] * 0.30
                else:
                    return row['amount'] * 0.15
            
            processed_df['estimated_profit'] = processed_df.apply(calculate_profit, axis=1)
            
            # Prepare data for database insertion
            db_records = []
            for _, row in processed_df.iterrows():
                record = {
                    'date': row['date'].date(),  # Convert to Python date object
                    'product': row['product'],
                    'amount': float(row['amount']),
                    'quantity': int(row['quantity']),
                    'total_sales': float(row['total_sales']),
                    'processed_at': row['processed_at']
                }
                db_records.append(record)
            
            logger.info(f"Transformed {len(db_records)} records")
            return db_records, processed_df
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            return [], pd.DataFrame()
    
    def save_to_file(self, df: pd.DataFrame, file_type: str = 'csv') -> str:
        """
        Save processed data to file.
        
        Args:
            df: Processed dataframe to save
            file_type: Type of file to save ('csv' or 'parquet')
            
        Returns:
            str: Path to the saved file, or empty string on error
        """
        if df.empty:
            logger.warning("No data to save")
            return ""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        try:
            if file_type.lower() == 'csv':
                filename = f"processed_sales_{timestamp}.csv"
                filepath = os.path.join(self.output_dir, filename)
                df.to_csv(filepath, index=False)
                logger.info(f"Data saved to CSV: {filepath}")
                
            elif file_type.lower() == 'parquet':
                filename = f"processed_sales_{timestamp}.parquet"
                filepath = os.path.join(self.output_dir, filename)
                df.to_parquet(filepath, index=False)
                logger.info(f"Data saved to Parquet: {filepath}")
                
            else:
                logger.error(f"Unsupported file type: {file_type}")
                return ""
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving file: {e}")
            return ""
    
    def generate_report(self, df: pd.DataFrame, inserted_count: int) -> Dict[str, Any]:
        """
        Generate a summary report of the ETL process.
        
        Args:
            df: Processed dataframe
            inserted_count: Number of records inserted into database
            
        Returns:
            dict: Summary statistics
        """
        logger.info("Generating ETL report")
        
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'records_processed': len(df),
                'records_inserted': inserted_count,
                'total_revenue': float(df['total_sales'].sum()),
                'average_sale': float(df['amount'].mean()),
                'unique_products': df['product'].nunique(),
                'categories': df['category'].unique().tolist(),
                'processing_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Add category breakdown
            category_breakdown = df.groupby('category').agg({
                'total_sales': 'sum',
                'quantity': 'sum'
            }).to_dict()
            
            report['category_breakdown'] = category_breakdown
            
            logger.info(f"Report generated: {report['records_processed']} records processed")
            return report
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return {}
