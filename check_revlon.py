import duckdb
import pandas as pd

con = duckdb.connect('db/cscp.duckdb', read_only=True)

company = con.execute("SELECT COUNT(*) FROM products WHERE LOWER(CompanyName) ILIKE '%revlon%'").fetchone()[0]
brand   = con.execute("SELECT COUNT(*) FROM products WHERE LOWER(BrandName) ILIKE '%revlon%'").fetchone()[0]
any_col = con.execute("SELECT COUNT(*) FROM products WHERE LOWER(CompanyName) ILIKE '%revlon%' OR LOWER(BrandName) ILIKE '%revlon%'").fetchone()[0]

print('CompanyName matches:', company)
print('BrandName matches  :', brand)
print('Either column      :', any_col)

df = pd.read_csv('data/chemicals_in_cosmetics_clean.csv')

revlon_company = df['CompanyName'].str.lower().str.contains('revlon', na=False).sum()
revlon_brand   = df['BrandName'].str.lower().str.contains('revlon', na=False).sum()

any_column = df.apply(
    lambda row: row.astype(str).str.lower().str.contains('revlon').any(),
    axis=1
).sum()

print()
print('CSV CompanyName rows :', revlon_company)
print('CSV BrandName rows   :', revlon_brand)
print('CSV any column rows  :', any_column)
print()
print('Excel likely counted :', any_column, 'cells not unique rows')
print('Correct answer       :', company, 'unique product-chemical records')