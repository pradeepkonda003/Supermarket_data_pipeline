-- Star Schema Table Creation
-- Database schema for supermarket sales pipeline

-- DIMENSION TABLE 1: Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_type TEXT NOT NULL,
    gender TEXT NOT NULL
);

-- DIMENSION TABLE 2: Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_id INTEGER PRIMARY KEY,
    product_line TEXT NOT NULL,
    unit_price REAL NOT NULL
);

-- FACT TABLE: Sales
CREATE TABLE IF NOT EXISTS fact_sales (
    invoice_id TEXT PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    branch TEXT NOT NULL,
    city TEXT NOT NULL,
    payment TEXT NOT NULL,
    date DATE NOT NULL,
    time TIME,
    quantity INTEGER NOT NULL,
    tax REAL NOT NULL,
    total REAL NOT NULL,
    cogs REAL NOT NULL,
    gross_income REAL NOT NULL,
    rating REAL,
    FOREIGN KEY(customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY(product_id) REFERENCES dim_product(product_id)
);

-- CREATE INDEXES FOR PERFORMANCE
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_sales(date);
CREATE INDEX IF NOT EXISTS idx_fact_branch ON fact_sales(branch);