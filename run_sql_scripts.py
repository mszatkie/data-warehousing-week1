import duckdb

# Connect to DuckDB
con = duckdb.connect(database=':memory:')

# Read and execute the create_tables.sql script
with open('sql_scripts/create_tables.sql', 'r') as f:
    sql_content = f.read()
    con.execute(sql_content)

print("Executed create_tables.sql")
