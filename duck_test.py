import duckdb

# con = duckdb.connect("miracle_solework.duckdb")
# # con.execute("CREATE TABLE test AS SELECT 1 as id, 'hello' AS val")
# con.sql("DROP TABLE orders")
# # tables = con.execute("SHOW TABLES").fetchall()
# # print(tables)

# with duckdb.connect("data/warehouse/miracle_solework.duckdb") as con:
#     con.execute("DROP TABLE IF EXISTS marts_customer_summary")

#     print(con.execute("SHOW TABLES").fetchall())


# drop table

# Connect to your database
con = duckdb.connect('data/warehouse/miracle_solework.duckdb')

# Fetch all table names from the main schema
tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchall()

# Drop each table
for table in tables:
    con.execute(f"DROP TABLE IF EXISTS {table[0]}")
    
print(f"Dropped {len(tables)} tables.")
