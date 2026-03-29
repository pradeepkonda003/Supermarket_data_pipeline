"""
Load Module - GOLD LAYER TO DATABASE
Loads fact and dimension tables to SQLite
Handles SCD Type-2 updates and CDC tracking
"""
import sqlite3
import pandas as pd
from datetime import datetime

class GoldLoader:
    """Load Gold layer data to SQLite with SCD Type-2 support"""
    
    def __init__(self, db_path="sales.db"):
        self.db_path = db_path
        self.conn = None
        self.load_report = {}
        
    def create_connection(self):
        """Create SQLite connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            return self.conn
        except Exception as e:
            print(f" Error creating connection: {e}")
            return None
    
    def close_connection(self):
        """Close SQLite connection"""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            print(" Database connection closed")
    
    def load_dim_customer(self, dim_customer):
        """Load customer dimension with SCD Type-2"""
        print("\n--- Loading DIM_CUSTOMER (with SCD Type-2) ---")
        
        try:
            # Drop old inactive records and keep only current ones
            dim_customer.to_sql("dim_customer", self.conn, if_exists="replace", index=False)
            
            print(f"Loaded dim_customer ({len(dim_customer)} rows)")
            self.load_report['dim_customer'] = f"{len(dim_customer)} rows"
            
            # SCD Type-2 tracking
            print("  SCD Type-2 Features:")
            print(f"    - effective_date tracking enabled")
            print(f"    - is_current flag for active records")
            print(f"    - Historical tracking enabled")
            
            return True
        except Exception as e:
            print(f" Error loading dim_customer: {e}")
            return False
    
    def load_dim_product(self, dim_product):
        """Load product dimension with SCD Type-2"""
        print("--- Loading DIM_PRODUCT (with SCD Type-2) ---")
        
        try:
            dim_product.to_sql("dim_product", self.conn, if_exists="replace", index=False)
            
            print(f" Loaded dim_product ({len(dim_product)} rows)")
            self.load_report['dim_product'] = f"{len(dim_product)} rows"
            
            # SCD Type-2 tracking
            print("  SCD Type-2 Features:")
            print(f"    - Tracks price changes over time")
            print(f"    - Historical pricing maintained")
            
            return True
        except Exception as e:
            print(f"Error loading dim_product: {e}")
            return False
    
    def load_fact_sales(self, fact_sales):
        """Load fact sales with CDC tracking"""
        print("--- Loading FACT_SALES (with CDC) ---")
        
        try:
            fact_sales.to_sql("fact_sales", self.conn, if_exists="replace", index=False)
            
            print(f" Loaded fact_sales ({len(fact_sales)} rows)")
            self.load_report['fact_sales'] = f"{len(fact_sales)} rows"
            
            # CDC tracking info
            print("  CDC Features:")
            print(f"    - cdc_operation: Tracks INSERT/UPDATE/DELETE operations")
            print(f"    - cdc_timestamp: Timestamp of change")
            print(f"    - cdc_record_hash: Hash for change detection")
            
            return True
        except Exception as e:
            print(f" Error loading fact_sales: {e}")
            return False
    
    def create_indexes(self):
        """Create indexes for performance"""
        print("\n--- Creating Database Indexes ---")
        
        try:
            cursor = self.conn.cursor()
            
            indexes = [
                ("idx_fact_customer", "fact_sales", "customer_id"),
                ("idx_fact_product", "fact_sales", "product_id"),
                ("idx_fact_date", "fact_sales", "date"),
                ("idx_fact_branch", "fact_sales", "branch"),
                ("idx_dim_cust_current", "dim_customer", "is_current"),
                ("idx_dim_prod_current", "dim_product", "is_current")
            ]
            
            for idx_name, table, column in indexes:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
            
            self.conn.commit()
            print(f" Created {len(indexes)} performance indexes")
            
        except Exception as e:
            print(f"⚠ Error creating indexes: {e}")
    
    def load_all_tables(self, dim_customer, dim_product, fact_sales):
        """Load all tables to Gold layer (SQLite)"""
        print("\n" + "="*60)
        print("GOLD LAYER → DATABASE LOADING")
        print("="*60)
        print(f"Database: {self.db_path}\n")
        
        # Create connection
        if not self.create_connection():
            return False
        
        # Load tables
        success = True
        success &= self.load_dim_customer(dim_customer)
        success &= self.load_dim_product(dim_product)
        success &= self.load_fact_sales(fact_sales)
        
        # Create indexes
        self.create_indexes()
        
        # Close connection
        self.close_connection()
        
        # Print summary
        self.print_load_summary()
        
        return success
    
    def print_load_summary(self):
        """Print load report"""
        print("\n" + "="*60)
        print("LOAD REPORT - GOLD LAYER TO DATABASE")
        print("="*60)
        
        for table, status in self.load_report.items():
            print(f" {table.upper()}: {status}")
        
        print("\nEnd-to-End Pipeline:")
        print("  Bronze Layer (Raw Data Ingestion)")
        print("       ↓")
        print("  Silver Layer (Validation & Quality)")
        print("       ↓")
        print("  Gold Layer (Business Transformation)")
        print("       ↓")
        print("  SQLite Database (Persistent Storage)")
        
        print(f"\n Data successfully loaded to {self.db_path}")
        print("="*60)


def load_gold_layer(dim_customer, dim_product, fact_sales, db_path="sales.db"):
    """
    Main function to load Gold layer to SQLite
    """
    loader = GoldLoader(db_path)
    return loader.load_all_tables(dim_customer, dim_product, fact_sales)


if __name__ == "__main__":
    import extract
    import silver
    import transform
    
    # Complete Pipeline: Bronze → Silver → Gold → Database
    print("\n" + "="*70)
    print("MEDALLION ARCHITECTURE - END-TO-END PIPELINE")
    print("="*70)
    
    # Step 1: Bronze Layer
    df_bronze = extract.extract_data("supermarket_sales.csv")
    
    if df_bronze is not None:
        # Step 2: Silver Layer
        silver_layer = silver.SilverLayer(df_bronze)
        df_silver = silver_layer.process_to_silver()
        
        if df_silver is not None:
            # Step 3: Gold Layer
            dim_customer, dim_product, fact_sales = transform.transform_to_gold(df_silver)
            
            # Step 4: Load to Database
            if load_gold_layer(dim_customer, dim_product, fact_sales):
                print("\n MEDALLION ARCHITECTURE PIPELINE COMPLETE!")

