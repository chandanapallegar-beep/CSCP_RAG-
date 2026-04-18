import duckdb
con = duckdb.connect('db/cscp.duckdb', read_only=True)

company = con.execute("SELECT COUNT(*) FROM products WHERE LOWER(CompanyName) ILIKE '%avon%'").fetchone()[0]
brand   = con.execute("SELECT COUNT(*) FROM products WHERE LOWER(BrandName) ILIKE '%avon%'").fetchone()[0]
either  = con.execute("SELECT COUNT(*) FROM products WHERE LOWER(CompanyName) ILIKE '%avon%' OR LOWER(BrandName) ILIKE '%avon%'").fetchone()[0]

print('CompanyName matches:', company)
print('BrandName matches  :', brand)
print('Either column      :', either)

chemicals = con.execute("""
    SELECT DISTINCT CompanyName 
    FROM products 
    WHERE LOWER(CompanyName) ILIKE '%avon%'
""").fetchdf()
print()
print('Avon company names in DB:')
print(chemicals)