/*
    Table:
        mart_dim_customers_product_purchase
    
    Authors:
        Justin de Guia (JG)
    
    Description & Comments:
        Dimension table for customer's product purchase
        
*/

{% set partitions_to_replace = dates_in_range(
    start_date_str=var("from_date"),
    end_date_str=var("to_date"),
    in_fmt="%Y-%m-%d",
    out_fmt="'%Y-%m-%d'"
)%}

{{ 
  config(
    materialized='incremental',
     partition_by={
      "field": "InvoiceDate",
      "data_type": "date",
      "granularity": "day"
    },
    unique_key = ['CustomerID'],
    incremental_strategy = 'delete+insert',
    on_schema_change="append_new_columns"
  )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('stg_fct_ecommerce_transactions_v1') }}
    {%- if is_incremental() %}
    WHERE InvoiceDate BETWEEN '{{ var("from_date") }}'AND '{{ var("to_date") }}'
    {%- endif %}
),

total_transactions AS (
    SELECT 
        CustomerID, 
        COUNT(DISTINCT InvoiceNo) AS total_transactions,
        SUM(Quantity) AS total_products_purchased,
        COUNT(DISTINCT StockCode) AS unique_products_purchased,
        SUM(Quantity * UnitPrice) AS total_spending
    FROM base    
    GROUP BY CustomerID
),

cancellation_frequency AS (
    SELECT 
        CustomerID, 
        COUNT(DISTINCT InvoiceNo) AS cancellation_frequency
    FROM base
    WHERE TransactionStatus = 'Cancelled'
    GROUP BY CustomerID
)

SELECT 
    t.CustomerID,
    total_transactions,
    total_products_purchased,
    unique_products_purchased,
    total_spending,
    CASE WHEN total_transactions = 0 THEN 0
        ELSE total_spending / total_transactions 
    END AS average_transaction_value,
    CAST(cancellation_frequency/total_transactions AS DOUBLE) AS cancellation_rate
FROM total_transactions t
LEFT JOIN cancellation_frequency c USING (CustomerID)