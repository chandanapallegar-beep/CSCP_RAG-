import duckdb
import os

CSV_PATH = "data/chemicals_in_cosmetics.csv"
DB_PATH  = "db/cscp.duckdb"

os.makedirs("db", exist_ok=True)

print("Connecting to DuckDB...")
con = duckdb.connect(DB_PATH)

print("Loading CSV...")
con.execute(f"""
    CREATE OR REPLACE TABLE products AS
    SELECT * FROM read_csv_auto('{CSV_PATH}', header=True)
""")

print(con.execute("DESCRIBE products").fetchdf().to_string())
print("Total rows:", con.execute("SELECT COUNT(*) FROM products").fetchone()[0])
con.close()
print("Done. Database saved to", DB_PATH)