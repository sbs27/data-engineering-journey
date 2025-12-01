import pandas as pd
import psycopg2
from datetime import datetime
import os
import time
import sys

print("Starting Database ETL Pipeline")

# 1. Read CSV
print("Reading data from CSV...")
try:
    df = pd.read_csv('/app/data/sales.csv')
    print(f"Found {len(df)} records")
except FileNotFoundError:
    print("Error: sales.csv not found in /app/data/")
    sys.exit(1)

# 2. Process data
df['total_sales'] = df['amount'] * df['quantity']
df['processed_at'] = datetime.now()

print("Processed Data:")
print(df[['date', 'product', 'amount', 'quantity', 'total_sales']])
print()

# 3. Connect to PostgreSQL
print("Connecting to PostgreSQL...")

# Wait for PostgreSQL to be ready (max 30 seconds)
max_retries = 15
connected = False

for i in range(max_retries):
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="salesdb",
            user="salesuser",
            password="salespass",
            port="5432",
            connect_timeout=5
        )
        print("Connected to PostgreSQL successfully")
        connected = True
        break
    except Exception as e:
        if i < max_retries - 1:
            print(f"Attempt {i+1}/{max_retries}: PostgreSQL not ready, waiting 2 seconds...")
            time.sleep(2)
        else:
            print(f"Failed to connect to PostgreSQL after {max_retries} attempts")
            print(f"Error: {e}")
            print("Saving data to file as fallback...")
            
            # Fallback: save to file
            output_dir = '/app/output'
            os.makedirs(output_dir, exist_ok=True)
            fallback_file = f"{output_dir}/fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(fallback_file, index=False)
            print(f"Data saved to: {fallback_file}")
            sys.exit(0)

if connected:
    try:
        cursor = conn.cursor()
        
        # 4. Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            product VARCHAR(100) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            quantity INTEGER NOT NULL,
            total_sales DECIMAL(10,2) NOT NULL,
            processed_at TIMESTAMP NOT NULL
        )
        """)
        conn.commit()
        print("Sales table created/verified")
        
        # 5. Insert data
        print("Inserting data into PostgreSQL...")
        inserted_count = 0
        
        for _, row in df.iterrows():
            cursor.execute("""
            INSERT INTO sales (date, product, amount, quantity, total_sales, processed_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['date'],
                row['product'],
                float(row['amount']),
                int(row['quantity']),
                float(row['total_sales']),
                row['processed_at']
            ))
            inserted_count += 1
        
        conn.commit()
        print(f"Successfully inserted {inserted_count} rows")
        
        # 6. Query to verify
        cursor.execute("SELECT COUNT(*) as total_records FROM sales")
        total_records = cursor.fetchone()[0]
        print(f"Total records in sales table: {total_records}")
        
        cursor.execute("SELECT SUM(total_sales) as total_revenue FROM sales")
        total_revenue = cursor.fetchone()[0]
        if total_revenue:
            print(f"Total revenue in database: ${total_revenue:,.2f}")
        
        # 7. Also save to file for backup
        output_dir = '/app/output'
        os.makedirs(output_dir, exist_ok=True)
        backup_file = f"{output_dir}/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(backup_file, index=False)
        print(f"Backup saved to: {backup_file}")
        
        cursor.close()
        conn.close()
        print("Database connection closed")
        
    except Exception as e:
        print(f"Database error: {e}")
        conn.rollback()

print("Database ETL pipeline completed")
