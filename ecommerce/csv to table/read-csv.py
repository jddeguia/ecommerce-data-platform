import duckdb

with duckdb.connect("ecommerce-data.db") as con:
    create_table = """
    CREATE TABLE ecommerce_table AS SELECT * FROM 'ecommerce.csv'
    """
    con.sql(create_table)
    con.table("ecommerce_table").show()
    