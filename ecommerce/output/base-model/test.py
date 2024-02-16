import duckdb
import dbt


with duckdb.connect("justin-company.db") as con:
    
    result = con.sql("SELECT * FROM ecommerce")
    print(result)