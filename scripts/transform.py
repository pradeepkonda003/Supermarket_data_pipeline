"""
Transform Module - GOLD LAYER
Business transformations: create fact and dimension tables
Star schema design for analytics
"""
import pandas as pd
from datetime import datetime

def create_dim_customer(df):
    """
    Create Customer Dimension Table (Gold Layer)
    SCD Type-2: Tracks historical changes
    """
    print("\n--- GOLD LAYER: Creating DIM_CUSTOMER ---")
    
    dim_customer = df[['Customer type', 'Gender']].drop_duplicates().reset_index(drop=True)
    dim_customer['customer_id'] = dim_customer.index + 1

    if 'effective_date' in df.columns:
        try:
            effective_date_val = pd.to_datetime(df['effective_date'].iloc[0])
        except Exception:
            effective_date_val = pd.to_datetime(df['Date'].iloc[0]) if 'Date' in df.columns else datetime.now()
    else:
        effective_date_val = pd.to_datetime(df['Date'].iloc[0]) if 'Date' in df.columns else datetime.now()

    
    # Add SCD Type-2 columns
    #dim_customer['effective_date'] = df['effective_date'].iloc[0]
    dim_customer['effective_date'] = effective_date_val
    dim_customer['end_date'] = None
    dim_customer['is_current'] = 1
    dim_customer['created_date'] = datetime.now()
    
    # Rename columns
    dim_customer.columns = ['customer_type', 'gender', 'customer_id', 
                           'effective_date', 'end_date', 'is_current', 'created_date']
    
    print(f"  Created dim_customer with {len(dim_customer)} records")
    return dim_customer

def create_dim_product(df):
    """
    Create Product Dimension Table (Gold Layer)
    SCD Type-2: Tracks historical price changes
    """
    print("--- GOLD LAYER: Creating DIM_PRODUCT ---")
    
    dim_product = df[['Product line', 'Unit price']].drop_duplicates().reset_index(drop=True)
    dim_product['product_id'] = dim_product.index + 1
    
    if 'effective_date' in df.columns:
        try:
            effective_date_val = pd.to_datetime(df['effective_date'].iloc[0])
        except Exception:
            effective_date_val = pd.to_datetime(df['Date'].iloc[0]) if 'Date' in df.columns else datetime.now()
    else:
        effective_date_val = pd.to_datetime(df['Date'].iloc[0]) if 'Date' in df.columns else datetime.now()

    dim_product['effective_date'] = effective_date_val
    dim_product['end_date'] = None
    dim_product['is_current'] = 1
    dim_product['created_date'] = datetime.now()
    
    dim_product.columns = ['product_line', 'unit_price', 'product_id',
                          'effective_date', 'end_date', 'is_current', 'created_date']
    
    print(f"  Created dim_product with {len(dim_product)} records")
    return dim_product

def create_fact_sales(df, dim_customer, dim_product):
    """
    Create Fact Sales Table (Gold Layer)
    Aggregated business facts with CDC tracking
    """
    print("--- GOLD LAYER: Creating FACT_SALES ---")
    
    # Merge with dimension tables to get IDs
    fact = df.merge(dim_customer[['customer_type', 'gender', 'customer_id']], 
                    left_on=['Customer type', 'Gender'],
                    right_on=['customer_type', 'gender'],
                    how='left') \
           .merge(dim_product[['product_line', 'unit_price', 'product_id']], 
                  left_on=['Product line', 'Unit price'],
                  right_on=['product_line', 'unit_price'],
                  how='left')

    # Ensure CDC tracking columns exist (provide safe defaults if missing)
    required_cdc = ['cdc_operation', 'cdc_timestamp', 'cdc_record_hash']
    for col in required_cdc:
        if col not in fact.columns:
            if col == 'cdc_timestamp':
                fact[col] = pd.to_datetime(datetime.now())
            elif col == 'cdc_operation':
                fact[col] = 'I'
            else:
                fact[col] = None
    
    # Select fact table columns
    fact_sales = fact[[
        'Invoice ID',
        'customer_id',
        'product_id',
        'Branch',
        'City',
        'Payment',
        'Date',
        'Time',
        'Quantity',
        'Tax 5%',
        'Total',
        'cogs',
        'gross income',
        'Rating',
        'cdc_operation',
        'cdc_timestamp',
        'cdc_record_hash'
    ]].copy()
    
    # Rename columns for consistency
    fact_sales.columns = [
        'invoice_id',
        'customer_id',
        'product_id',
        'branch',
        'city',
        'payment',
        'date',
        'time',
        'quantity',
        'tax',
        'total',
        'cogs',
        'gross_income',
        'rating',
        'cdc_operation',
        'cdc_timestamp',
        'cdc_record_hash'
    ]
    
    # Add business metrics
    fact_sales['profit'] = fact_sales['total'] - fact_sales['cogs']
    fact_sales['profit_margin'] = (fact_sales['profit'] / fact_sales['total'] * 100).round(2)
    
    # Add load timestamp
    fact_sales['loaded_date'] = datetime.now()
    
    print(f"  Created fact_sales with {len(fact_sales)} records")
    return fact_sales

def transform_to_gold(silver_df):
    """
    Transform Silver layer data to Gold layer
    Creates star schema dimensions and facts
    """
    print("\n" + "="*60)
    print("GOLD LAYER - BUSINESS TRANSFORMATION")
    print("="*60)
    
    print(f"Input Records: {len(silver_df)}")
    print(f"Input Columns: {len(silver_df.columns)}\n")
    
    # Create dimension tables
    dim_customer = create_dim_customer(silver_df)
    dim_product = create_dim_product(silver_df)
    
    # Create fact table
    fact_sales = create_fact_sales(silver_df, dim_customer, dim_product)
    
    print(f"\n{'='*60}")
    print("GOLD LAYER - SUMMARY")
    print(f"{'='*60}")
    print(f"  dim_customer: {dim_customer.shape}")
    print(f"  dim_product: {dim_product.shape}")
    print(f"  fact_sales: {fact_sales.shape}")
    print(f"{'='*60}\n")
    
    return dim_customer, dim_product, fact_sales

if __name__ == "__main__":
    import extract
    import silver
    
    # Bronze → Silver → Gold Pipeline
    df_bronze = extract.extract_data("supermarket_sales.csv")
    if df_bronze is not None:
        silver_layer = silver.SilverLayer(df_bronze)
        df_silver = silver_layer.process_to_silver()
        
        if df_silver is not None:
            dim_customer, dim_product, fact_sales = transform_to_gold(df_silver)
            print("\nGold Layer Transformation Complete!")


def transform_data(df):
    """
    Transform raw data into star schema format
    """
    print("\n--- Starting Data Transformation ---")
    print(f"Raw data shape: {df.shape}")
    
    # Create dimension tables
    dim_customer = create_dim_customer(df)
    dim_product = create_dim_product(df)
    
    # Create fact table
    fact_sales = create_fact_sales(df, dim_customer, dim_product)
    
    return dim_customer, dim_product, fact_sales

if __name__ == "__main__":
    import extract
    
    # Example usage
    df = extract.extract_data("supermarket_sales.csv")
    if df is not None:
        dim_customer, dim_product, fact_sales = transform_data(df)
        print(f"\nTransformation complete!")
        print(f"Dim Customer shape: {dim_customer.shape}")
        print(f"Dim Product shape: {dim_product.shape}")
        print(f"Fact Sales shape: {fact_sales.shape}")
