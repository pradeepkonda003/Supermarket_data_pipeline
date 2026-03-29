"""
Extract Module - BRONZE LAYER
Loads raw data as-is from source without transformations
Adds metadata columns for traceability
"""
import pandas as pd
from pathlib import Path
from datetime import datetime

def extract_data(source_path):
    """
    Load raw data into BRONZE layer
    Minimal processing - just load as-is and add metadata
    
    Args:
        source_path (str): Path to source CSV file
        
    Returns:
        pd.DataFrame: Raw data with metadata columns
    """
    try:
        # Load raw dataset
        df = pd.read_csv(source_path)
        
        # Add metadata columns for data lineage and CDC
        df['bronze_load_date'] = datetime.now()
        df['bronze_source_file'] = source_path
        df['bronze_record_hash'] = df.astype(str).apply(lambda x: hash(tuple(x)), axis=1)
        
        print(f"\n{'='*60}")
        print(f"BRONZE LAYER - RAW DATA INGESTION")
        print(f"{'='*60}")
        print(f" Successfully loaded {len(df)} raw records from {source_path}")
        print(f" Source: {source_path}")
        print(f" Load Timestamp: {datetime.now()}")
        print(f" Columns: {len(df.columns)}")
        print(f"{'='*60}\n")
        
        return df
        
    except Exception as e:
        print(f" Error loading data to Bronze: {e}")
        return None

if __name__ == "__main__":
    # Example usage
    df_bronze = extract_data("supermarket_sales.csv")
    if df_bronze is not None:
        print(f"Bronze Layer Data Shape: {df_bronze.shape}")
        print(f"\nFirst few rows:")
        print(df_bronze.head())
