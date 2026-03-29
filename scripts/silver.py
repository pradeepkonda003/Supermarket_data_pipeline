"""
Silver Layer Module - DATA CLEANING, VALIDATION & QUALITY
Applies data quality checks, deduplication, SCD Type-2, and CDC
"""
import pandas as pd
import hashlib
from datetime import datetime

class SilverLayer:
    """Silver Layer processing with data quality and SCD Type-2"""
    
    def __init__(self, bronze_df):
        self.bronze_df = bronze_df.copy()
        self.silver_df = None
        self.quality_report = {}
        
    def check_nulls(self):
        """Check and handle null values"""
        print("\n--- SILVER LAYER: NULL VALUES CHECK ---")
        null_counts = self.bronze_df.isnull().sum()
        
        if null_counts.sum() == 0:
            print(" No null values found!")
            self.quality_report['null_check'] = "PASSED"
        else:
            print(" Null values detected:")
            print(null_counts[null_counts > 0])
            self.quality_report['null_check'] = "WARNING"
            # Fill nulls with default values
            self.bronze_df = self.bronze_df.fillna(0)
            print(" Nulls filled with default values")
        
        return null_counts
    
    def check_duplicates(self):
        """Check and remove duplicate records"""
        print("\n--- SILVER LAYER: DUPLICATE RECORDS CHECK ---")
        initial_count = len(self.bronze_df)
        
        # Check on Invoice ID (primary key)
        dup_count = self.bronze_df['Invoice ID'].duplicated().sum()
        
        if dup_count == 0:
            print(f"No duplicate records found!")
            self.quality_report['duplicate_check'] = "PASSED"
        else:
            print(f"Found {dup_count} duplicate records")
            self.bronze_df = self.bronze_df.drop_duplicates(subset=['Invoice ID'], keep='first')
            print(f"Duplicates removed. Records: {initial_count} → {len(self.bronze_df)}")
            self.quality_report['duplicate_check'] = "REMOVED"
        
        return dup_count
    
    def schema_validation(self):
        """Validate schema against expected columns"""
        print("\n--- SILVER LAYER: SCHEMA VALIDATION ---")
        
        expected_columns = [
            'Invoice ID', 'Branch', 'City', 'Customer type', 'Gender',
            'Product line', 'Unit price', 'Quantity', 'Date', 'Time',
            'Payment', 'cogs', 'gross income', 'Rating', 'Tax 5%', 'Total'
        ]
        
        missing_cols = [col for col in expected_columns if col not in self.bronze_df.columns]
        
        if len(missing_cols) == 0:
            print(" Schema validation PASSED")
            self.quality_report['schema_check'] = "PASSED"
        else:
            print(f" Missing columns: {missing_cols}")
            self.quality_report['schema_check'] = "WARNING"
        
        return missing_cols
    
    def check_negative_values(self):
        """Check for invalid negative values"""
        print("\n--- SILVER LAYER: NEGATIVE VALUES CHECK ---")
        
        numeric_cols = ['Quantity', 'Total', 'Tax 5%', 'Unit price', 'cogs']
        issues = {}
        
        for col in numeric_cols:
            if col in self.bronze_df.columns:
                neg_count = (self.bronze_df[col] < 0).sum()
                if neg_count > 0:
                    issues[col] = neg_count
        
        if len(issues) == 0:
            print("  No negative values found!")
            self.quality_report['negative_check'] = "PASSED"
        else:
            print("  Negative values detected:")
            for col, count in issues.items():
                print(f"  {col}: {count} records")
            self.quality_report['negative_check'] = "WARNING"
        
        return issues
    
    def scd_type_2(self):
        """Implement SCD Type-2 for customer dimension changes"""
        print("\n--- SILVER LAYER: SCD TYPE-2 (SLOWLY CHANGING DIMENSION) ---")
        
        # Create SCD Type-2 columns for customer tracking
        self.bronze_df['effective_date'] = datetime.now().date()
        self.bronze_df['end_date'] = None
        self.bronze_df['is_current'] = 1
        
        print(" SCD Type-2 columns added")
        print("  - effective_date: When the record became current")
        print("  - end_date: When the record was superseded")
        print("  - is_current: Flag for current records (1=current, 0=historical)")
        
        self.quality_report['scd_type_2'] = "APPLIED"
        return True
    
    def cdc_implementation(self):
        """Implement Change Data Capture logic"""
        print("\n--- SILVER LAYER: CDC (CHANGE DATA CAPTURE) ---")
        
        # Create CDC columns
        self.bronze_df['cdc_operation'] = 'INSERT'  # INSERT, UPDATE, DELETE
        self.bronze_df['cdc_timestamp'] = datetime.now()
        
        # Create record hash for change detection
        hash_cols = [col for col in self.bronze_df.columns 
                     if col not in ['bronze_load_date', 'bronze_source_file', 'bronze_record_hash',
                                   'effective_date', 'end_date', 'is_current', 'cdc_operation', 'cdc_timestamp']]
        
        
        
        self.bronze_df['cdc_record_hash'] = self.bronze_df[hash_cols].astype(str).apply(
            lambda x: hashlib.md5(''.join(map(str, x)).encode('utf-8')).hexdigest(), axis=1
            #lambda x: hashlib.md5(''.join(x)).hexdigest(), axis=1
        )
        
        print(" CDC columns added")
        print("  - cdc_operation: Type of change (INSERT/UPDATE/DELETE)")
        print("  - cdc_timestamp: When the change occurred")
        print("  - cdc_record_hash: Hash for change detection")
        
        self.quality_report['cdc'] = "IMPLEMENTED"
        return True
    
    def standardize_columns(self):
        """Standardize column names and data types"""
        print("\n--- SILVER LAYER: COLUMN STANDARDIZATION ---")
        
        # Convert date and time columns
        if 'Date' in self.bronze_df.columns:
            self.bronze_df['Date'] = pd.to_datetime(self.bronze_df['Date'])
        if 'Time' in self.bronze_df.columns:
            self.bronze_df['Time'] = pd.to_datetime(self.bronze_df['Time'])
        
        # Data type conversions
        type_mapping = {
            'Quantity': 'int32',
            'Unit price': 'float64',
            'Tax 5%': 'float64',
            'Total': 'float64',
            'Rating': 'float64'
        }
        
        for col, dtype in type_mapping.items():
            if col in self.bronze_df.columns:
                self.bronze_df[col] = self.bronze_df[col].astype(dtype)
        
        print("Data types standardized")
        self.quality_report['standardization'] = "COMPLETED"
        return True
    
    def process_to_silver(self):
        """Execute complete Silver Layer processing"""
        print("\n" + "="*60)
        print("SILVER LAYER - DATA QUALITY & VALIDATION")
        print("="*60)
        
        # Execute all quality checks and transformations
        self.check_nulls()
        self.check_duplicates()
        self.schema_validation()
        self.check_negative_values()
        self.standardize_columns()
        self.scd_type_2()
        self.cdc_implementation()
        
        self.silver_df = self.bronze_df.copy()
        
        # Quality summary
        self.print_quality_summary()
        
        return self.silver_df
    
    def print_quality_summary(self):
        """Print data quality report"""
        print("\n" + "="*60)
        print("DATA QUALITY REPORT - SILVER LAYER")
        print("="*60)
        
        for check, status in self.quality_report.items():
            status_symbol = " correct" if status in ["PASSED", "COMPLETED", "APPLIED", "IMPLEMENTED"] else "Error"
            print(f"{status_symbol} {check.upper()}: {status}")
        
        print(f"\nRecords in Silver Layer: {len(self.silver_df)}")
        print(f"Columns in Silver Layer: {len(self.silver_df.columns)}")
        print("="*60 + "\n")
    
    def get_silver_data(self):
        """Return processed Silver layer data"""
        return self.silver_df


if __name__ == "__main__":
    import extract
    
    # Bronze → Silver Pipeline
    df_bronze = extract.load_to_bronze("supermarket_sales.csv")
    if df_bronze is not None:
        silver = SilverLayer(df_bronze)
        df_silver = silver.process_to_silver()
        
        print("Silver Layer Processing Complete!")
        print(f"Shape: {df_silver.shape}")
