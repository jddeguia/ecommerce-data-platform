{{ 
  config(
    materialized='ephemeral'
  )
}}


WITH remove_missing_customer_id AS (
    SELECT * EXCLUDE (InvoiceDate),
        CAST(InvoiceDate AS DATE) AS InvoiceDate,
    FROM ecommerce_transactions
    WHERE CustomerID IS NOT NULL
),

remove_null_description AS (
    SELECT *
    FROM remove_missing_customer_id
    WHERE Description IS NOT NULL
),

remove_weird_stockcodes AS (
    SELECT *
    FROM remove_null_description
    WHERE StockCode NOT IN (
        'POST', 
        'D', 
        'C2', 
        'M', 
        'BANK CHARGES', 
        'PADS', 
        'DOT', 
        'CRUK',
        '23444',
        '23702')
),

add_product_status AS (
    SELECT *,
    CASE 
        WHEN Quantity > 1 THEN 'Completed'
        ELSE 'Cancelled'
    END AS TransactionStatus
    FROM remove_weird_stockcodes
),

remove_free_products AS (
    SELECT * 
    FROM add_product_status
    WHERE UnitPrice > 0
),

latest_transaction AS (
    SELECT *,
    row_number() OVER (PARTITION BY CustomerID, InvoiceNo, StockCode ORDER BY InvoiceDate DESC) AS row_num
    FROM remove_free_products   
)

SELECT * FROM latest_transaction WHERE row_num = 1