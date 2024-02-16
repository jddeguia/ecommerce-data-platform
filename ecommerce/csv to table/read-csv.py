import duckdb

with duckdb.connect("justin-company.db") as con:
    create_table = """
    CREATE TABLE ecommerce AS SELECT * FROM 'ecommerce.csv'
    """
    con.sql(create_table)
    con.table("ecommerce").show()
    