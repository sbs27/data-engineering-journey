
import pandas as pd
import psycopg2
from datetime import datetime
import os
import time
import sys
import traceback

def setup_logging():
    """Create a log file to record what happens"""
    log_dir = '/app/logs'
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = f"{log_dir}/pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    # This function will write messages to both console and log file
    def log_message(message, level="INFO"):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] [{level}] {message}"
        
        print(log_entry)  # Show in console
        
        # Also write to log file
        with open(log_file, 'a') as f:
            f.write(log_entry + '\n')
    
    return log_message

# Initialize logging
log = setup_logging()

def extract_data(file_path):
    """Extract data from CSV file with error handling"""
    log("Starting data extraction...")
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            error_msg = f"File not found: {file_path}"
            log(error_msg, "ERROR")
            raise FileNotFoundError(error_msg)
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        log(f"Successfully read {len(df)} records from {file_path}")
        
        # Check if file has data
        if len(df) == 0:
            log("Warning: CSV file is empty", "WARNING")
        
        return df
        
    except pd.errors.EmptyDataError:
        error_msg = "CSV file is empty or has no columns"
        log(error_msg, "ERROR")
        raise
    except pd.errors.ParserError as e:
        error_msg = f"Error parsing CSV file: {str(e)}"
        log(error_msg, "ERROR")
        raise
    except Exception as e:
        error_msg = f"Unexpected error during extraction: {str(e)}"
        log(error_msg, "ERROR")
        raise

def validate_data(df):
    """Validate the data before processing"""
    log("Validating data...")
    
    # Required columns
    required_columns = ['date', 'product', 'amount', 'quantity']
    
    # Check for required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        log(error_msg, "ERROR")
        raise ValueError(error_msg)
    
    # Check for null values
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        log(f"Warning: Found null values:\n{null_counts}", "WARNING")
    
    # Validate data types
    try:
        df['amount'] = pd.to_numeric(df['amount'])
        df['quantity'] = pd.to_numeric(df['quantity'])
    except ValueError as e:
        error_msg = f"Invalid data types in amount/quantity columns: {str(e)}"
        log(error_msg, "ERROR")
        raise
    
    log("Data validation completed")
    return df

def transform_data(df):
    """Transform the data with error handling"""
    log("Starting data transformation...")
    
    try:
        # Convert date column
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = df['date'].isnull().sum()
        if invalid_dates > 0:
            log(f"Warning: {invalid_dates} records have invalid dates", "WARNING")
        
        # Calculate derived columns
        df['total_sales'] = df['amount'] * df['quantity']
        df['processed_at'] = datetime.now()
        
        # Add product category
        def categorize_product(product):
            electronics = ['Laptop', 'Monitor', 'Tablet', 'Printer']
            accessories = ['Mouse', 'Keyboard', 'Headphones']
            
            if product in electronics:
                return 'Electronics'
            elif product in accessories:
                return 'Accessories'
            else:
                return 'Other'
        
        df['category'] = df['product'].apply(categorize_product)
        
        log(f"Transformed {len(df)} records successfully")
        return df
        
    except Exception as e:
        error_msg = f"Error during transformation: {str(e)}"
        log(error_msg, "ERROR")
        raise

def connect_to_database(max_retries=5, retry_delay=3):
    """Connect to PostgreSQL with retry logic"""
    log("Connecting to PostgreSQL...")
    
    connection_params = {
        'host': 'postgres',
        'database': 'salesdb',
        'user': 'salesuser',
        'password': 'salespass',
        'port': '5432'
    }
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**connection_params)
            log(f"Successfully connected to PostgreSQL (attempt {attempt + 1}/{max_retries})")
            return conn
            
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                log(f"Database not ready, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})", "WARNING")
                time.sleep(retry_delay)
            else:
                error_msg = f"Failed to connect to database after {max_retries} attempts: {str(e)}"
                log(error_msg, "ERROR")
                raise
    
    # This should never be reached
    raise ConnectionError("Failed to establish database connection")

def load_data_to_db(df, conn):
    """Load data into PostgreSQL database"""
    log("Loading data to database...")
    
    try:
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            product VARCHAR(100) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            quantity INTEGER NOT NULL,
            total_sales DECIMAL(10,2) NOT NULL,
            category VARCHAR(50),
            processed_at TIMESTAMP NOT NULL
        )
        """)
        conn.commit()
        log("Sales table verified/created")
        
        # Insert data
        inserted_count = 0
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO sales (date, product, amount, quantity, total_sales, category, processed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['date'].date() if pd.notnull(row['date']) else None,
                str(row['product']),
                float(row['amount']),
                int(row['quantity']),
                float(row['total_sales']),
                str(row.get('category', 'Unknown')),
                row['processed_at']
            ))
            inserted_count += 1
        
        conn.commit()
        log(f"Successfully inserted {inserted_count} records")
        
        # Get summary statistics
        cursor.execute("SELECT COUNT(*) as total_records FROM sales")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(total_sales) as total_revenue FROM sales")
        total_revenue = cursor.fetchone()[0] or 0
        
        cursor.close()
        
        log(f"Database now has {total_records} total records")
        log(f"Total revenue in database: ${total_revenue:,.2f}")
        
        return inserted_count
        
    except Exception as e:
        conn.rollback()  # Undo any partial changes
        error_msg = f"Error loading data to database: {str(e)}"
        log(error_msg, "ERROR")
        raise

def save_backup(df, backup_dir='/app/output'):
    """Save processed data as backup"""
    log("Creating backup file...")
    
    try:
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f"{backup_dir}/sales_backup_{timestamp}.csv"
        
        df.to_csv(backup_file, index=False)
        log(f"Backup saved to: {backup_file}")
        
        return backup_file
        
    except Exception as e:
        error_msg = f"Error creating backup: {str(e)}"
        log(error_msg, "ERROR")
        # Don't raise here - backup failure shouldn't stop the whole pipeline
        return None

def main():
    """Main pipeline function"""
    log("=" * 50)
    log("STARTING ETL PIPELINE")
    log("=" * 50)
    
    start_time = datetime.now()
    success = False
    conn = None
    
    try:
        # Step 1: Extract
        df = extract_data('/app/data/sales.csv')
        
        # Step 2: Validate
        df = validate_data(df)
        
        # Step 3: Transform
        df = transform_data(df)
        
        # Step 4: Connect to database
        conn = connect_to_database()
        
        # Step 5: Load to database
        records_inserted = load_data_to_db(df, conn)
        
        # Step 6: Save backup
        backup_file = save_backup(df)
        
        # Calculate execution time
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Success!
        success = True
        log("=" * 50)
        log("ETL PIPELINE COMPLETED SUCCESSFULLY")
        log(f"Records processed: {len(df)}")
        log(f"Records inserted: {records_inserted}")
        log(f"Execution time: {execution_time:.2f} seconds")
        if backup_file:
            log(f"Backup saved: {backup_file}")
        log("=" * 50)
        
        return 0  # Success exit code
        
    except Exception as e:
        log("=" * 50)
        log("ETL PIPELINE FAILED")
        log(f"Error: {str(e)}")
        log("Error details:")
        log(traceback.format_exc())
        log("=" * 50)
        
        return 1  # Error exit code
        
    finally:
        # Always close database connection if it was opened
        if conn:
            try:
                conn.close()
                log("Database connection closed")
            except:
                pass  # Ignore errors during cleanup

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)