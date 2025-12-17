"""
Main ETL pipeline that orchestrates the complete data flow.
This is the entry point for the data pipeline.
"""

import sys
import os
import logging
from datetime import datetime

# Add src to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import FILE_PATHS, LOG_CONFIG
from src.utils.database import DatabaseConnection
from src.utils.data_processor import DataProcessor

def setup_logging():
    """
    Set up logging configuration.
    Creates both console and file logging.
    """
    # Ensure log directory exists
    log_dir = FILE_PATHS['log_dir']
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    log_file = os.path.join(log_dir, f'etl_pipeline_{timestamp}.log')
    
    # Configure logging
    logging.basicConfig(
        level=LOG_CONFIG['level'],
        format=LOG_CONFIG['format'],
        datefmt=LOG_CONFIG['date_format'],
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("ETL Pipeline Started")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 60)
    
    return logger

def run_etl_pipeline():
    """
    Main ETL pipeline execution.
    Coordinates data extraction, transformation, and loading.
    """
    logger = setup_logging()
    
    logger.info("Initializing ETL pipeline components")
    
    # Initialize components
    data_processor = DataProcessor()
    db_connection = DatabaseConnection()
    
    try:
        # Step 1: Extract data from CSV
        logger.info("Step 1: Extracting data")
        raw_data, extraction_success = data_processor.extract_data()
        
        if not extraction_success or raw_data.empty:
            logger.error("Data extraction failed. Exiting pipeline.")
            return False
        
        logger.info(f"Extracted {len(raw_data)} records successfully")
        
        # Step 2: Transform data
        logger.info("Step 2: Transforming data")
        db_records, processed_data = data_processor.transform_data(raw_data)
        
        if not db_records or processed_data.empty:
            logger.error("Data transformation failed. Exiting pipeline.")
            return False
        
        logger.info(f"Transformed {len(db_records)} records")
        
        # Step 3: Connect to database
        logger.info("Step 3: Connecting to database")
        connection_success = db_connection.connect()
        
        if not connection_success:
            logger.error("Database connection failed. Saving data to file only.")
            
            # Save transformed data to file as fallback
            file_path = data_processor.save_to_file(processed_data, 'csv')
            if file_path:
                logger.info(f"Data saved to file: {file_path}")
            
            return False
        
        # Step 4: Create/verify database table
        logger.info("Step 4: Setting up database table")
        table_success = db_connection.create_sales_table()
        
        if not table_success:
            logger.error("Failed to setup database table")
            return False
        
        # Step 5: Insert data into database
        logger.info("Step 5: Loading data into database")
        inserted_count = db_connection.insert_sales_data(db_records)
        
        if inserted_count == 0:
            logger.error("No data inserted into database")
            return False
        
        logger.info(f"Successfully inserted {inserted_count} records into database")
        
        # Step 6: Save processed data to file
        logger.info("Step 6: Saving processed data to file")
        csv_file = data_processor.save_to_file(processed_data, 'csv')
        parquet_file = data_processor.save_to_file(processed_data, 'parquet')
        
        if csv_file:
            logger.info(f"CSV file saved: {csv_file}")
        if parquet_file:
            logger.info(f"Parquet file saved: {parquet_file}")
        
        # Step 7: Generate and display report
        logger.info("Step 7: Generating final report")
        report = data_processor.generate_report(processed_data, inserted_count)
        
        # Display report summary
        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Total Records Processed: {report.get('records_processed', 0)}")
        logger.info(f"Records Inserted to DB: {report.get('records_inserted', 0)}")
        logger.info(f"Total Revenue: ${report.get('total_revenue', 0):,.2f}")
        logger.info(f"Average Sale: ${report.get('average_sale', 0):,.2f}")
        logger.info(f"Unique Products: {report.get('unique_products', 0)}")
        logger.info(f"Product Categories: {', '.join(report.get('categories', []))}")
        logger.info("=" * 60)
        
        # Log detailed category breakdown
        if 'category_breakdown' in report:
            logger.info("CATEGORY BREAKDOWN:")
            for category, data in report['category_breakdown'].items():
                revenue = data.get('total_sales', {}).get(category, 0)
                quantity = data.get('quantity', {}).get(category, 0)
                logger.info(f"  {category}: ${revenue:,.2f} revenue, {quantity} units")
        
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error in ETL pipeline: {e}")
        logger.error("Pipeline failed with unexpected error")
        return False
        
    finally:
        # Step 8: Cleanup - always close database connection
        logger.info("Step 8: Cleaning up resources")
        db_connection.close()
        logger.info("ETL pipeline execution completed")

if __name__ == "__main__":
    """
    Entry point when script is run directly.
    """
    success = run_etl_pipeline()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)
