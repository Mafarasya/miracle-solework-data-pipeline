import duckdb

con = duckdb.connect("../data/warehouse/miracle_solework.duckdb")
con.execute("INSTALL ui")
con.execute("LOAD ui")
con.execute("CALL start_ui()")

# how to run
# run the server
# duckdb miracle_solework.duckdb -ui
