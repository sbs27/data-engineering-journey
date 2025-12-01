import pandas as pd
import os
from datetime import datetime

print("🚀 Starting SIMPLE ETL Pipeline")

# 1. Read CSV
print("📥 Reading data...")
df = pd.read_csv('/app/data/sales.csv')
print(f"   Found {len(df)} records")

# 2. Simple transformations
print("🔄 Processing data...")
df['total_sales'] = df['amount'] * df['quantity']
df['date'] = pd.to_datetime(df['date'])

# Add a simple category
df['category'] = df['product'].apply(
    lambda x: 'Electronics' if x in ['Laptop', 'Monitor'] else 'Accessories'
)

# 3. Save results
print("💾 Saving results...")
output_dir = '/app/output'
os.makedirs(output_dir, exist_ok=True)

# Save to CSV
df.to_csv(f'{output_dir}/simple_results.csv', index=False)

# Create a summary
summary = f"""
📊 ETL SUMMARY
==============
Processed: {len(df)} records
Total Sales: ${df['total_sales'].sum():,.2f}
Average Sale: ${df['amount'].mean():,.2f}
Unique Products: {df['product'].nunique()}
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

print(summary)

# Save summary to file
with open(f'{output_dir}/summary.txt', 'w') as f:
    f.write(summary)

print("✅ Done! Check /app/output folder")
