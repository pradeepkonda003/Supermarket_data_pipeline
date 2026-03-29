"""
Data Quality Module - SILVER LAYER VALIDATION
Comprehensive data quality checks integrated with Medallion Architecture
"""
import pandas as pd

class DataQualityValidator:
    """Integrated data quality validation for Silver Layer"""
    
    def __init__(self, df, layer_name="SILVER LAYER"):
        self.df = df
        self.layer_name = layer_name
        self.quality_metrics = {}
    
    def check_null_values(self):
        """Check for null values in dataset"""
        print("\n[QUALITY CHECK] Null Values Detection")
        null_counts = self.df.isnull().sum()
        total_nulls = null_counts.sum()
        
        if total_nulls == 0:
            print("    Status: PASSED - No null values found")
            self.quality_metrics['null_check'] = "PASSED"
        else:
            print(f"    Status: WARNING - Found {total_nulls} null values")
            print("  Null distribution:")
            for col, count in null_counts[null_counts > 0].items():
                print(f"    {col}: {count} nulls ({count/len(self.df)*100:.1f}%)")
            self.quality_metrics['null_check'] = "WARNING"
        
        return null_counts
    
    def check_duplicates(self, key_column='Invoice ID'):
        """Check for duplicate records"""
        print(f"\n[QUALITY CHECK] Duplicate Records Detection ({key_column})")
        
        if key_column not in self.df.columns:
            print(f"    Column '{key_column}' not found")
            return 0
        
        dup_count = self.df[key_column].duplicated().sum()
        dup_percentage = (dup_count / len(self.df) * 100) if len(self.df) > 0 else 0
        
        if dup_count == 0:
            print(f"    Status: PASSED - No duplicates found")
            self.quality_metrics['duplicate_check'] = "PASSED"
        else:
            print(f"    Status: WARNING - Found {dup_count} duplicates ({dup_percentage:.2f}%)")
            self.quality_metrics['duplicate_check'] = "WARNING"
        
        return dup_count
    
    def check_negative_values(self, numeric_columns=None):
        """Check for negative values in numeric columns"""
        print("\n[QUALITY CHECK] Negative Values Detection")
        
        if numeric_columns is None:
            numeric_columns = ['Quantity', 'Total', 'Tax 5%', 'Unit price', 'cogs']
        
        issues = {}
        total_negatives = 0
        
        for col in numeric_columns:
            if col in self.df.columns:
                neg_count = (self.df[col] < 0).sum()
                if neg_count > 0:
                    issues[col] = neg_count
                    total_negatives += neg_count
        
        if len(issues) == 0:
            print("    Status: PASSED - No negative values found")
            self.quality_metrics['negative_check'] = "PASSED"
        else:
            print(f"    Status: WARNING - Found {total_negatives} negative values")
            for col, count in issues.items():
                print(f"    {col}: {count} negative values")
            self.quality_metrics['negative_check'] = "WARNING"
        
        return issues
    
    def check_schema_validation(self, expected_columns=None):
        """Validate schema against expected columns"""
        print("\n[QUALITY CHECK] Schema Validation")
        
        if expected_columns is None:
            expected_columns = [
                'Invoice ID', 'Branch', 'City', 'Customer type', 'Gender',
                'Product line', 'Unit price', 'Quantity', 'Date', 'Time',
                'Payment', 'cogs', 'gross income', 'Rating', 'Tax 5%', 'Total'
            ]
        
        missing_cols = [col for col in expected_columns if col not in self.df.columns]
        extra_cols = [col for col in self.df.columns if col not in expected_columns]
        
        if len(missing_cols) == 0:
            print(f"    Status: PASSED - All expected columns present")
            self.quality_metrics['schema_check'] = "PASSED"
        else:
            print(f"    Status: WARNING - Missing columns: {missing_cols}")
            self.quality_metrics['schema_check'] = "WARNING"
        
        if len(extra_cols) > 0:
            print(f"  ℹ Additional metadata columns: {extra_cols}")
        
        return {'missing': missing_cols, 'extra': extra_cols}
    
    def check_data_types(self):
        """Check and display current data types"""
        print("\n[QUALITY CHECK] Data Types Verification")
        
        type_check = True
        expected_types = {
            'Quantity': ['int', 'float'],
            'Unit price': ['float'],
            'Tax 5%': ['float'],
            'Total': ['float'],
            'Rating': ['float'],
            'Date': ['datetime'],
            'Time': ['datetime']
        }
        
        for col, expected in expected_types.items():
            if col in self.df.columns:
                actual_type = str(self.df[col].dtype)
                type_match = any(exp in actual_type for exp in expected)
                print(f"  {col}: {actual_type}")
        
        print(f"    Status: Data types logged")
        self.quality_metrics['data_types'] = "CHECKED"
        
        return self.df.dtypes
    
    def scd_type_2_validation(self):
        """Validate SCD Type-2 columns"""
        print("\n[QUALITY CHECK] SCD Type-2 Columns Validation")
        
        scd_cols = ['effective_date', 'end_date', 'is_current']
        missing_scd = [col for col in scd_cols if col not in self.df.columns]
        
        if len(missing_scd) == 0:
            print("    Status: PASSED - All SCD Type-2 columns present")
            self.quality_metrics['scd_type_2'] = "PASSED"
        else:
            print(f"    Status: WARNING - Missing SCD columns: {missing_scd}")
            self.quality_metrics['scd_type_2'] = "WARNING"
        
        return missing_scd
    
    def cdc_validation(self):
        """Validate CDC columns"""
        print("\n[QUALITY CHECK] CDC Columns Validation")
        
        cdc_cols = ['cdc_operation', 'cdc_timestamp', 'cdc_record_hash']
        missing_cdc = [col for col in cdc_cols if col not in self.df.columns]
        
        if len(missing_cdc) == 0:
            print("    Status: PASSED - All CDC columns present")
            
            # Check CDC operation values
            if 'cdc_operation' in self.df.columns:
                operations = self.df['cdc_operation'].unique()
                print(f"  CDC Operations found: {operations}")
            
            self.quality_metrics['cdc'] = "PASSED"
        else:
            print(f"    Status: WARNING - Missing CDC columns: {missing_cdc}")
            self.quality_metrics['cdc'] = "WARNING"
        
        return missing_cdc
    
    def run_all_checks(self):
        """Execute all quality checks"""
        print("\n" + "="*70)
        print(f"DATA QUALITY VALIDATION - {self.layer_name}")
        print("="*70)
        
        self.check_schema_validation()
        self.check_null_values()
        self.check_duplicates()
        self.check_negative_values()
        self.check_data_types()
        self.scd_type_2_validation()
        self.cdc_validation()
        
        self.print_quality_summary()
    
    def print_quality_summary(self):
        """Print comprehensive quality summary"""
        print("\n" + "="*70)
        print("QUALITY CHECK SUMMARY")
        print("="*70)
        
        print(f"\nDataset Statistics:")
        print(f"  Total Records: {len(self.df):,}")
        print(f"  Total Columns: {len(self.df.columns)}")
        print(f"  Memory Usage: {self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        print(f"\nQuality Checks:")
        for check, status in sorted(self.quality_metrics.items()):
            status_symbol = " " if status == "PASSED" else " " if status == "WARNING" else "ℹ"
            print(f"  {status_symbol} {check.upper()}: {status}")
        
        # Overall status
        passed = sum(1 for v in self.quality_metrics.values() if v == "PASSED")
        total = len(self.quality_metrics)
        print(f"\nOverall Quality: {passed}/{total} checks passed")
        
        if passed == total:
            print("  Data quality APPROVED for downstream processing")
        else:
            print("  Data quality concerns detected - review recommended")
        
        print("="*70 + "\n")
    
    def get_quality_report(self):
        """Return quality metrics dictionary"""
        return self.quality_metrics


def validate_silver_layer(df, layer_name="SILVER LAYER"):
    """Main function for Silver Layer validation"""
    validator = DataQualityValidator(df, layer_name)
    validator.run_all_checks()
    return validator


if __name__ == "__main__":
    import extract
    import silver
    
    # Example: Silver Layer Quality Validation
    df_bronze = extract.extract_data("supermarket_sales.csv")
    if df_bronze is not None:
        silver_layer = silver.SilverLayer(df_bronze)
        df_silver = silver_layer.process_to_silver()
        
        if df_silver is not None:
            validator = validate_silver_layer(df_silver)


def check_data_types(df):
    """
    Check and display data types
    """
    print("\n--- Data Types Check ---")
    print(df.dtypes)
    return df.dtypes

def run_quality_checks(df):
    """
    Run all data quality checks
    """
    print("=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)
    validator = DataQualityValidator(df)
    null_results = validator.check_null_values()
    dup_results  = validator.check_duplicates()
    neg_results  = validator.check_negative_values(numeric_columns=['Quantity','Total','Tax 5%','Unit price'])
    dtype_results = validator.check_data_types()
    print("\n" + "=" * 60)
    print("QUALITY CHECK SUMMARY")
    print("=" * 60)
    print(f"Total Records: {len(df)}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"Null Values: {null_results.sum()}")
    print(f"Duplicate Records: {dup_results}")
    print(f"Issues Found: {'No' if null_results.sum() == 0 and dup_results == 0 and not neg_results else 'Yes'}")
    print("=" * 60)

if __name__ == "__main__":
    import extract
    
    # Example usage
    df = extract.extract_data("supermarket_sales.csv")
    if df is not None:
        run_quality_checks(df)
