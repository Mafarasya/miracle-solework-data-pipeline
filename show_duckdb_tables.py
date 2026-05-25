import duckdb

# con = duckdb.connect("data/warehouse/miracle_solework.duckdb")


# print(con.execute("""
#     SELECT table_schema, table_name 
#     FROM information_schema.tables 
#     WHERE table_name IN ('raw_orders', 'raw_expenses')
# """).fetchall())


with duckdb.connect('data/warehouse/miracle_solework.duckdb', read_only=True) as con:
    print(con.execute("SHOW TABLES").fetchall())
