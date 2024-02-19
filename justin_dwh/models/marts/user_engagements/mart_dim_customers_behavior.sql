/*
    Table:
        mart_dim_customers_behavior
    
    Authors:
        Justin de Guia (JG)
    
    Description & Comments:
        Dimension table for customer's purchase behavior
        
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
    SELECT * ,
    dayofweek(InvoiceDatetime) AS DayofWeek,
    hour(InvoiceDatetime) AS TransactionHour,
    LAG(InvoiceDate) OVER (PARTITION BY InvoiceNo,CustomerID ORDER BY InvoiceDate) AS PreviousTransactionDate
    FROM {{ ref('stg_fct_ecommerce_transactions_v1') }}
    {%- if is_incremental() %}
    WHERE InvoiceDate BETWEEN '{{ var("from_date") }}'AND '{{ var("to_date") }}'
    {%- endif %}
),

days_between_purchases AS (
    SELECT
        CustomerID,
        AVG(DATE_DIFF('day',InvoiceDate,PreviousTransactionDate)) AS AverageDaysBetweenPurchases
    FROM base
    GROUP BY CustomerID
),

favorite_shopping_day AS (
    SELECT
        CustomerID,
        DayOfWeek
    FROM (
        SELECT
            CustomerID,
            DayOfWeek,
            ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY COUNT(*) DESC) AS rn
        FROM base
        GROUP BY CustomerID, DayOfWeek
    ) t
    WHERE rn = 1
),

favorite_shopping_hour AS (
    SELECT
        CustomerID,
        TransactionHour
    FROM (
        SELECT
            CustomerID,
            TransactionHour,
            ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY COUNT(*) DESC) AS rn
        FROM base
        GROUP BY CustomerID, TransactionHour
    ) t
    WHERE rn = 1
)

SELECT
    b.CustomerId,
    AverageDaysBetweenPurchases AS ADBP,
    s.DayOfWeek AS FavoriteShoppingDay,
    h.TransactionHour AS FavoriteShoppingHour
FROM base b
INNER JOIN days_between_purchases d USING (CustomerID)
INNER JOIN favorite_shopping_day s USING (CustomerID)
INNER JOIN favorite_shopping_hour h USING (CustomerID)