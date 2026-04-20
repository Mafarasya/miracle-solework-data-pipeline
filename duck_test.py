import duckdb

# con = duckdb.connect("miracle_solework.duckdb")
# # con.execute("CREATE TABLE test AS SELECT 1 as id, 'hello' AS val")
# con.sql("DROP TABLE orders")
# # tables = con.execute("SHOW TABLES").fetchall()
# # print(tables)

with duckdb.connect("miracle_solework.duckdb") as con:
    con.execute("DROP TABLE IF EXISTS orders")

    print(con.execute("SHOW TABLES").fetchall())