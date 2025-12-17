"""
Database utility functions for the ETL pipeline.
This module handles all database operations.
"""

import psycopg2
import psycopg2.extras
from psycopg2 import OperationalError
import time
from typing import List, Dict, Any, Optional
from config.settings import DB_CONFIG, ETL_SETTINGS
import logging

# Set up module-level logger
logger = logging.getLogger(__name__)

class DatabaseConnection:
    """
    A class to manage database connections with retry logic.
    This handles the connection to PostgreSQL with automatic retries.
    """
    
    def __init__(self):
        """Initialize with database configuration."""
        self.config = DB_CONFIG
        self.max_retries = ETL_SETTINGS['max_retries']
        self.retry_delay = ETL_SETTINGS['retry_delay']
        self.connection = None
        
    def connect(self) -> bool:
        """
        Establish connection to PostgreSQL with retry logic.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Attempting database connection (attempt {attempt + 1}/{self.max_retries})")
                
                # Try to establish connection
                self.connection = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password'],
                    connect_timeout=10
                )
                
                logger.info("Database connection established successfully")
                return True
                
            except OperationalError as e:
                logger.warning(f"Connection failed: {e}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to connect after {self.max_retries} attempts")
                    return False
        
        return False
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[tuple]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Parameters for the query (prevents SQL injection)
            
        Returns:
            List of tuples containing query results, or None if error
        """
        if not self.connection:
            logger.error("No database connection available")
            return None
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                
                # Check if this is a SELECT query
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    logger.debug(f"Query returned {len(results)} rows")
                    return results
                else:
                    # For INSERT/UPDATE/DELETE, commit the transaction
                    self.connection.commit()
                    rows_affected = cursor.rowcount
                    logger.info(f"Query affected {rows_affected} rows")
                    return None
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            self.connection.rollback()
            return None
    
    def create_sales_table(self) -> bool:
        """
        Create the sales table if it doesn't exist.
        
        Returns:
            bool: True if table created/exists, False on error
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            product VARCHAR(100) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            quantity INTEGER NOT NULL,
            total_sales DECIMAL(10,2) NOT NULL,
            processed_at TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        logger.info("Creating/verifying sales table")
        result = self.execute_query(create_table_query)
        
        if result is None:
            # execute_query returns None for non-SELECT queries
            logger.info("Sales table operation completed")
            return True
        else:
            logger.error("Failed to create sales table")
            return False
    
    def insert_sales_data(self, sales_data: List[Dict[str, Any]]) -> int:
        """
        Insert sales data into the database.
        
        Args:
            sales_data: List of dictionaries containing sales records
            
        Returns:
            int: Number of rows successfully inserted
        """
        if not sales_data:
            logger.warning("No data to insert")
            return 0
            
        insert_query = """
        INSERT INTO sales (date, product, amount, quantity, total_sales, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        logger.info(f"Preparing to insert {len(sales_data)} records")
        
        try:
            with self.connection.cursor() as cursor:
                # Prepare data for insertion
                records = []
                for record in sales_data:
                    records.append((
                        record['date'],
                        record['product'],
                        record['amount'],
                        record['quantity'],
                        record['total_sales'],
                        record['processed_at']
                    ))
                
                # Insert all records
                psycopg2.extras.execute_batch(cursor, insert_query, records)
                self.connection.commit()
                
                inserted_count = len(records)
                logger.info(f"Successfully inserted {inserted_count} records")
                return inserted_count
                
        except Exception as e:
            logger.error(f"Failed to insert data: {e}")
            self.connection.rollback()
            return 0
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
