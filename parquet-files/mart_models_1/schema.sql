


CREATE TABLE ecommerce_transactions(InvoiceNo VARCHAR, StockCode VARCHAR, Description VARCHAR, Quantity BIGINT, InvoiceDate TIMESTAMP, UnitPrice DOUBLE, CustomerID DOUBLE, Country VARCHAR);
CREATE TABLE mart_dim_customers_behavior(CustomerID DOUBLE, ADBP DOUBLE, FavoriteShoppingDay BIGINT, FavoriteShoppingHour BIGINT);
CREATE TABLE mart_dim_customers_product_purchase(CustomerID DOUBLE, total_transactions BIGINT, total_products_purchased HUGEINT, unique_products_purchased BIGINT, total_spending DOUBLE, average_transaction_value DOUBLE, cancellation_rate DOUBLE);
CREATE TABLE mart_dim_customers_recency(CustomerID DOUBLE, days_since_last_seen BIGINT, recency_category VARCHAR);
CREATE TABLE stg_fct_ecommerce_transactions_v1(InvoiceNo VARCHAR, StockCode VARCHAR, Description VARCHAR, Quantity BIGINT, UnitPrice DOUBLE, CustomerID DOUBLE, Country VARCHAR, InvoiceDate DATE, InvoiceDatetime TIMESTAMP, TransactionStatus VARCHAR);




