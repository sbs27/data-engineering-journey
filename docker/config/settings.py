"""
Configuration settings for the ETL pipeline.
This separates configuration from code, making it easier to manage.
"""

import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'salesdb'),
    'user': os.getenv('DB_USER', 'salesuser'),
    'password': os.getenv('DB_PASSWORD', 'salespass')
}

# File paths
FILE_PATHS = {
    'input_csv': '/app/data/sales.csv',
    'output_dir': '/app/output',
    'log_dir': '/app/logs'
}

# ETL settings
ETL_SETTINGS = {
    'max_retries': 5,
    'retry_delay': 2,  # seconds
    'batch_size': 100  # rows per batch insert
}

# Logging configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}
