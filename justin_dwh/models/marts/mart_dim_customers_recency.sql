{% set partitions_to_replace = dates_in_range(
    start_date_str=var("from_date"),
    end_date_str=var("to_date"),
    in_fmt="%Y-%m-%d",
    out_fmt="'%Y-%m-%d'"
)%}

{% set status_ranges = [{'min': 1, 'max': 10, 'status': 'Current Users'},
                        {'min': 11, 'max': 30, 'status': 'At Risk'},
                        {'min': 31, 'max': None, 'status': 'Dormant'}]
%}

{{ 
  config(
    materialized='incremental',
     partition_by={
      "field": "InvoiceDate",
      "data_type": "date",
      "granularity": "day"
    },
    unique_key = ['InvoiceNo', 'StockCode'],
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

recent_purchase_per_customer AS (
    SELECT
        CustomerID,
        MAX(InvoiceDate) AS latest_transaction
    FROM base
    GROUP BY CustomerID
),

get_most_recent_date AS (
    SELECT 
        MAX(latest_transaction) AS most_recent_date_all
    FROM recent_purchase_per_customer
),

days_since_last_seen AS (
    SELECT
        CustomerID,
        DATE_DIFF('day', latest_transaction,(SELECT most_recent_date_all FROM get_most_recent_date)) AS days_since_last_seen
    FROM recent_purchase_per_customer
),

categorize_customer_status AS (
    SELECT *,
        CASE
        {% for range in status_ranges %}
        WHEN days_since_last_seen BETWEEN {{ range['min'] }} AND {% if range['max'] %}{{ range['max'] }}{% else %}999999{% endif %} THEN '{{ range['status'] }}'
        {% endfor %}
        ELSE 'Unknown'
        END AS recency_category
    FROM days_since_last_seen
)

SELECT * FROM categorize_customer_status

