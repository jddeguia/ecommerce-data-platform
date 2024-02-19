COPY ecommerce_transactions FROM 'C:/Users/GL65/Desktop/Worker/ECommercePlatform/ecommerce-data-platform/parquet-files/mart_models_1/ecommerce_transactions.parquet' (FORMAT 'parquet');
COPY mart_dim_customers_behavior FROM 'C:/Users/GL65/Desktop/Worker/ECommercePlatform/ecommerce-data-platform/parquet-files/mart_models_1/mart_dim_customers_behavior.parquet' (FORMAT 'parquet');
COPY mart_dim_customers_product_purchase FROM 'C:/Users/GL65/Desktop/Worker/ECommercePlatform/ecommerce-data-platform/parquet-files/mart_models_1/mart_dim_customers_product_purchase.parquet' (FORMAT 'parquet');
COPY mart_dim_customers_recency FROM 'C:/Users/GL65/Desktop/Worker/ECommercePlatform/ecommerce-data-platform/parquet-files/mart_models_1/mart_dim_customers_recency.parquet' (FORMAT 'parquet');
COPY stg_fct_ecommerce_transactions_v1 FROM 'C:/Users/GL65/Desktop/Worker/ECommercePlatform/ecommerce-data-platform/parquet-files/mart_models_1/stg_fct_ecommerce_transactions_v_.parquet' (FORMAT 'parquet');
