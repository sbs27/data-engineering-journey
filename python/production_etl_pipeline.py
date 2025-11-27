# python/production_etl_pipeline.py
"""
PRODUCTION-READY ETL PIPELINE
Includes all the features needed for real-world data processing
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import sys
import traceback

class ProductionETLPipeline:
    """
    PRODUCTION-GRADE ETL PIPELINE
    With comprehensive error handling, logging, and monitoring
    """
    
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.setup_logging()
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'errors': [],
            'warnings': [],
            'data_quality_issues': []
        }
    
    def setup_logging(self):
        """Configure professional logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/{self.pipeline_name}_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(self.pipeline_name)
    
    def extract(self) -> pd.DataFrame:
        """
        EXTRACT: Get data from source with robust error handling
        """
        self.metrics['start_time'] = datetime.now()
        self.logger.info(f" Starting ETL pipeline: {self.pipeline_name}")
        
        try:
            self.logger.info(" Extracting data from source...")
            
            # Simulate data extraction from multiple sources
            customer_data = self._extract_customer_data()
            order_data = self._extract_order_data()
            product_data = self._extract_product_data()
            
            # Validate extraction
            if customer_data.empty:
                raise ValueError("No customer data extracted")
            if order_data.empty:
                raise ValueError("No order data extracted")
            
            self.logger.info(f" Extraction completed: {len(customer_data)} customers, {len(order_data)} orders")
            return customer_data, order_data, product_data
            
        except Exception as e:
            self.logger.error(f" Extraction failed: {e}")
            self.metrics['errors'].append(f"Extraction: {str(e)}")
            raise
    
    def _extract_customer_data(self) -> pd.DataFrame:
        """Extract customer data with simulated real-world scenarios"""
        try:
            # Simulate API/database extraction
            customers = [
                {'customer_id': 1, 'name': 'John Smith', 'email': 'john@email.com', 'status': 'active', 'signup_date': '2024-01-15'},
                {'customer_id': 2, 'name': 'Sarah Johnson', 'email': 'sarah@email.com', 'status': 'active', 'signup_date': '2024-02-20'},
                {'customer_id': 3, 'name': 'Mike Brown', 'email': 'mike@email.com', 'status': 'inactive', 'signup_date': '2024-01-10'},
                {'customer_id': 4, 'name': 'Lisa Davis', 'email': 'lisa@email.com', 'status': 'active', 'signup_date': '2024-03-05'},
                # Simulate some data issues
                {'customer_id': 5, 'name': None, 'email': 'invalid-email', 'status': 'active', 'signup_date': '2024-03-10'},
            ]
            
            df = pd.DataFrame(customers)
            self.logger.info(f" Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            self.logger.error(f"Customer data extraction failed: {e}")
            return pd.DataFrame()
    
    def _extract_order_data(self) -> pd.DataFrame:
        """Extract order data with realistic patterns"""
        try:
            orders = [
                {'order_id': 101, 'customer_id': 1, 'product_id': 1, 'quantity': 2, 'amount': 199.98, 'order_date': '2024-03-15', 'status': 'delivered'},
                {'order_id': 102, 'customer_id': 2, 'product_id': 2, 'quantity': 1, 'amount': 89.99, 'order_date': '2024-03-16', 'status': 'shipped'},
                {'order_id': 103, 'customer_id': 1, 'product_id': 3, 'quantity': 3, 'amount': 149.97, 'order_date': '2024-03-17', 'status': 'pending'},
                {'order_id': 104, 'customer_id': 4, 'product_id': 1, 'quantity': 1, 'amount': 99.99, 'order_date': '2024-03-18', 'status': 'delivered'},
                # Edge cases
                {'order_id': 105, 'customer_id': 999, 'product_id': 2, 'quantity': 0, 'amount': 0, 'order_date': '2024-03-19', 'status': 'cancelled'},
            ]
            
            df = pd.DataFrame(orders)
            self.logger.info(f" Extracted {len(df)} order records")
            return df
            
        except Exception as e:
            self.logger.error(f"Order data extraction failed: {e}")
            return pd.DataFrame()
    
    def _extract_product_data(self) -> pd.DataFrame:
        """Extract product reference data"""
        try:
            products = [
                {'product_id': 1, 'product_name': 'Laptop', 'category': 'Electronics', 'price': 99.99},
                {'product_id': 2, 'product_name': 'Book', 'category': 'Education', 'price': 89.99},
                {'product_id': 3, 'product_name': 'Headphones', 'category': 'Electronics', 'price': 49.99},
            ]
            
            return pd.DataFrame(products)
            
        except Exception as e:
            self.logger.error(f"Product data extraction failed: {e}")
            return pd.DataFrame()

    def transform(self, customer_data: pd.DataFrame, order_data: pd.DataFrame, product_data: pd.DataFrame) -> pd.DataFrame:
        """
        TRANSFORM: Clean, validate, and enrich data
        """
        self.logger.info(" Starting data transformation...")
        
        try:
            # STEP 1: Data Cleaning
            self.logger.info(" Cleaning data...")
            clean_customers = self._clean_customer_data(customer_data)
            clean_orders = self._clean_order_data(order_data)
            
            # STEP 2: Data Validation
            self.logger.info(" Validating data quality...")
            validation_results = self._validate_data(clean_customers, clean_orders)
            if not validation_results['is_valid']:
                self.metrics['data_quality_issues'].extend(validation_results['issues'])
                self.logger.warning(f"Data quality issues found: {validation_results['issues']}")
            
            # STEP 3: Data Enrichment and Joins
            self.logger.info(" Enriching data with business logic...")
            enriched_data = self._enrich_data(clean_customers, clean_orders, product_data)
            
            # STEP 4: Aggregation for analytics
            self.logger.info(" Creating analytical dataset...")
            analytical_data = self._create_analytical_dataset(enriched_data)
            
            self.metrics['records_processed'] = len(analytical_data)
            self.logger.info(f"Transformation completed: {len(analytical_data)} analytical records created")
            
            return analytical_data
            
        except Exception as e:
            self.logger.error(f" Transformation failed: {e}")
            self.metrics['errors'].append(f"Transformation: {str(e)}")
            raise
    
    def _clean_customer_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean customer data with professional data cleaning patterns"""
        df_clean = df.copy()
        
        # Handle missing values
        df_clean['name'] = df_clean['name'].fillna('Unknown Customer')
        
        # Validate emails
        email_mask = df_clean['email'].str.contains('@', na=False)
        df_clean.loc[~email_mask, 'email'] = 'invalid@email.com'
        self.logger.info(f"   Fixed {sum(~email_mask)} invalid emails")
        
        # Standardize status
        df_clean['status'] = df_clean['status'].str.lower().str.strip()
        
        return df_clean
    
    def _clean_order_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean order data with business rules"""
        df_clean = df.copy()
        
        # Remove invalid orders
        initial_count = len(df_clean)
        df_clean = df_clean[df_clean['quantity'] > 0]
        df_clean = df_clean[df_clean['amount'] >= 0]
        
        removed_count = initial_count - len(df_clean)
        if removed_count > 0:
            self.logger.warning(f"   Removed {removed_count} invalid orders")
        
        return df_clean
    
    def _validate_data(self, customers: pd.DataFrame, orders: pd.DataFrame) -> Dict:
        """Comprehensive data validation"""
        issues = []
        
        # Referential integrity check
        valid_customer_ids = set(customers['customer_id'])
        invalid_orders = orders[~orders['customer_id'].isin(valid_customer_ids)]
        
        if not invalid_orders.empty:
            issues.append(f"Found {len(invalid_orders)} orders with invalid customer IDs")
        
        # Business logic validation
        zero_amount_orders = orders[orders['amount'] == 0]
        if not zero_amount_orders.empty:
            issues.append(f"Found {len(zero_amount_orders)} orders with zero amount")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues
        }
    
    def _enrich_data(self, customers: pd.DataFrame, orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
        """Enrich data with additional business context"""
        # Join orders with customer and product data
        enriched = orders.merge(customers, on='customer_id', how='inner')
        enriched = enriched.merge(products, on='product_id', how='left')
        
        # Add derived fields
        enriched['order_date'] = pd.to_datetime(enriched['order_date'])
        enriched['order_month'] = enriched['order_date'].dt.to_period('M')
        enriched['customer_segment'] = enriched['amount'].apply(
            lambda x: 'VIP' if x > 150 else 'Standard' if x > 50 else 'Small'
        )
        
        return enriched
    
    def _create_analytical_dataset(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create dataset optimized for analytics"""
        analytical_data = data.groupby([
            'customer_id', 'name', 'customer_segment', 'order_month', 'category'
        ]).agg({
            'order_id': 'count',
            'quantity': 'sum',
            'amount': 'sum'
        }).reset_index()
        
        analytical_data.columns = [
            'customer_id', 'customer_name', 'customer_segment', 'order_month',
            'product_category', 'order_count', 'total_quantity', 'total_amount'
        ]
        
        # Add calculated metrics
        analytical_data['avg_order_value'] = analytical_data['total_amount'] / analytical_data['order_count']
        
        return analytical_data

    def load(self, data: pd.DataFrame):
        """
        LOAD: Save processed data to destination systems
        """
        self.logger.info(" Loading data to destination...")
        
        try:
            # Save to multiple formats (common production pattern)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 1. Primary storage (Parquet for analytics)
            data.to_parquet(f'data/output/analytical_data_{timestamp}.parquet', index=False)
            self.logger.info(" Saved analytical data to Parquet")
            
            # 2. Backup storage (CSV for accessibility)
            data.to_csv(f'data/output/analytical_data_{timestamp}.csv', index=False)
            self.logger.info("Saved backup to CSV")
            
            # 3. Save metrics and metadata
            self._save_pipeline_metrics()
            self.logger.info("Saved pipeline metrics")
            
            # 4. In production, you'd also load to:
            # - Data warehouse (Snowflake, BigQuery, Redshift)
            # - Database tables
            # - Cloud storage (S3, GCS)
            
        except Exception as e:
            self.logger.error(f"Load failed: {e}")
            self.metrics['errors'].append(f"Load: {str(e)}")
            raise
    
    def _save_pipeline_metrics(self):
        """Save pipeline execution metrics"""
        self.metrics['end_time'] = datetime.now()
        self.metrics['duration_seconds'] = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
        
        metrics_df = pd.DataFrame([self.metrics])
        metrics_df['pipeline_name'] = self.pipeline_name
        metrics_df['execution_date'] = self.metrics['start_time'].date()
        
        metrics_df.to_parquet(f'data/metrics/pipeline_metrics_{datetime.now().strftime("%Y%m%d")}.parquet', 
                            index=False, mode='a')  # Append mode

    def execute(self) -> bool:
        """
        EXECUTE: Run the complete ETL pipeline with comprehensive error handling
        """
        self.logger.info("=" * 60)
        self.logger.info(f" EXECUTING PRODUCTION ETL PIPELINE: {self.pipeline_name}")
        self.logger.info("=" * 60)
        
        try:
            # EXTRACT
            customer_data, order_data, product_data = self.extract()
            
            # TRANSFORM
            transformed_data = self.transform(customer_data, order_data, product_data)
            
            # LOAD
            self.load(transformed_data)
            
            # SUCCESS
            self.logger.info("🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
            self._report_success()
            return True
            
        except Exception as e:
            self.logger.error(f"ETL PIPELINE FAILED: {e}")
            self.logger.error(traceback.format_exc())
            self._report_failure()
            return False
    
    def _report_success(self):
        """Report successful pipeline execution"""
        duration = self.metrics['duration_seconds']
        records = self.metrics['records_processed']
        
        self.logger.info(f"PIPELINE METRICS:")
        self.logger.info(f"Duration: {duration:.2f} seconds")
        self.logger.info(f"Records Processed: {records:,}")
        self.logger.info(f"Errors: {len(self.metrics['errors'])}")
        self.logger.info(f"Data Quality Issues: {len(self.metrics['data_quality_issues'])}")
        
        if self.metrics['warnings']:
            self.logger.warning("   Warnings:")
            for warning in self.metrics['warnings']:
                self.logger.warning(f"     - {warning}")
    
    def _report_failure(self):
        """Report pipeline failure details"""
        self.logger.error(" FAILURE ANALYSIS:")
        for error in self.metrics['errors']:
            self.logger.error(f"   - {error}")

def demonstrate_production_etl():
    """Demonstrate the production ETL pipeline"""
    print(" DEMONSTRATING PRODUCTION ETL PIPELINE")
    print("=" * 60)
    
    # Create and run pipeline
    pipeline = ProductionETLPipeline("CustomerAnalyticsETL")
    success = pipeline.execute()
    
    if success:
        print("\n PIPELINE EXECUTION COMPLETED!")
        print("Check the 'logs/' directory for detailed execution logs")
        print("Check the 'data/output/' directory for processed data")
    else:
        print("\n PIPELINE EXECUTION FAILED!")
        print("Check the logs for error details")

if __name__ == "__main__":
    # Create necessary directories
    import os
    os.makedirs('logs', exist_ok=True)
    os.makedirs('data/output', exist_ok=True)
    os.makedirs('data/metrics', exist_ok=True)
    
    demonstrate_production_etl()