-- Reporting Queries with Window Functions
-- Analytics and reporting queries for supermarket sales data

-- QUERY 1: Top Product per Customer Type by Revenue (Window Function Required)
SELECT 
    customer_type,
    product_line,
    revenue,
    rank
FROM (
    SELECT 
        dc.customer_type,
        dp.product_line,
        SUM(fs.total) AS revenue,
        RANK() OVER (
            PARTITION BY dc.customer_type 
            ORDER BY SUM(fs.total) DESC
        ) AS rank
    FROM fact_sales fs
    JOIN dim_customer dc ON fs.customer_id = dc.customer_id
    JOIN dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dc.customer_type, dp.product_line
) t
WHERE rank = 1;

-- QUERY 2: Daily Sales Summary
SELECT 
    fs.date,
    COUNT(*) as num_transactions,
    SUM(fs.total) as daily_revenue,
    AVG(fs.total) as avg_transaction_value,
    SUM(fs.quantity) as total_quantity_sold
FROM fact_sales fs
GROUP BY fs.date
ORDER BY fs.date DESC;

-- QUERY 3: Top 10 Products by Revenue
SELECT 
    dp.product_line,
    SUM(fs.total) as total_revenue,
    SUM(fs.quantity) as total_quantity,
    AVG(fs.rating) as avg_rating,
    COUNT(*) as transaction_count
FROM fact_sales fs
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY dp.product_line
ORDER BY total_revenue DESC
LIMIT 10;

-- QUERY 4: Branch Performance Analysis
SELECT 
    fs.branch,
    fs.city,
    COUNT(DISTINCT fs.invoice_id) as total_transactions,
    SUM(fs.total) as total_revenue,
    AVG(fs.total) as avg_transaction,
    AVG(fs.rating) as avg_customer_rating
FROM fact_sales fs
GROUP BY fs.branch, fs.city
ORDER BY total_revenue DESC;

-- QUERY 5: Customer Segment Analysis
SELECT 
    dc.customer_type,
    dc.gender,
    COUNT(DISTINCT fs.customer_id) as unique_customers,
    SUM(fs.total) as total_revenue,
    AVG(fs.total) as avg_purchase_value,
    AVG(fs.rating) as avg_rating,
    SUM(fs.quantity) as total_items_purchased
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_type, dc.gender
ORDER BY total_revenue DESC;

-- QUERY 6: Payment Method Analysis
SELECT 
    fs.payment,
    COUNT(*) as transaction_count,
    SUM(fs.total) as total_revenue,
    AVG(fs.total) as avg_transaction,
    AVG(fs.rating) as avg_rating
FROM fact_sales fs
GROUP BY fs.payment
ORDER BY total_revenue DESC;

-- QUERY 7: Monthly Revenue Trend
SELECT 
    strftime('%Y-%m', fs.date) as month,
    SUM(fs.total) as monthly_revenue,
    COUNT(*) as transaction_count,
    AVG(fs.total) as avg_transaction_value
FROM fact_sales fs
GROUP BY strftime('%Y-%m', fs.date)
ORDER BY month DESC;