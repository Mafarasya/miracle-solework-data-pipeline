import duckdb

con = duckdb.connect("data/warehouse/miracle_solework.duckdb")

# Cek schema yang ada
con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()

# Atau cek langsung tables beserta schema-nya
print(con.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_name IN ('raw_orders', 'raw_expenses')
""").fetchall())

# print(con.execute("""
#     SELECT * FROM raw_orders LIMIT 5
#     """).fetchall())