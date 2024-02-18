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
    unique_key = ['InvoiceNo', 'StockCode'],
    incremental_strategy = 'delete+insert',
    on_schema_change="append_new_columns"
  )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('int_fct_ecommerce_transactions_cleaned_v1') }}
    
    {%- if is_incremental() %}
    WHERE InvoiceDate BETWEEN '{{ var("from_date") }}'AND '{{ var("to_date") }}'
    {%- endif %}
)

SELECT * FROM base