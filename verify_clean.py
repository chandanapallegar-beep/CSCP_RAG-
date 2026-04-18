import pandas as pd
import duckdb

CLEAN_PATH = "data/chemicals_in_cosmetics_clean.csv"
DB_PATH    = "db/cscp.duckdb"

print("=" * 60)
print("CLEAN DATA VERIFICATION REPORT")
print("=" * 60)

# ── LOAD BOTH SOURCES ─────────────────────────────────────
df  = pd.read_csv(CLEAN_PATH, encoding="utf-8")
con = duckdb.connect(DB_PATH, read_only=True)
db_count = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]

print(f"\n[1] ROW COUNTS")
print(f"    Clean CSV rows : {len(df):,}")
print(f"    DuckDB rows    : {db_count:,}")
print(f"    Match          : {'YES' if len(df) == db_count else 'NO — MISMATCH'}")

# ── NULL CHECK ────────────────────────────────────────────
print(f"\n[2] NULL CHECK (critical columns)")
critical = ["CDPHId", "ProductName", "CompanyName",
            "ChemicalName", "BrandName"]
all_clean = True
for col in critical:
    nulls = df[col].isnull().sum()
    status = "OK" if nulls == 0 else f"PROBLEM — {nulls:,} nulls remain"
    print(f"    {col:<30} {status}")
    if nulls > 0:
        all_clean = False

print(f"\n    CasNumber nulls  : {df['CasNumber'].isnull().sum():,} (flagged, not dropped — OK)")
print(f"    cas_missing flag : {'EXISTS' if 'cas_missing' in df.columns else 'MISSING'}")

# ── DUPLICATE CHECK ───────────────────────────────────────
print(f"\n[3] DUPLICATE CHECK")
exact_dupes = df.duplicated().sum()
logical_dupes = df.duplicated(subset=["CDPHId", "ChemicalId"]).sum()
print(f"    Exact duplicates           : {exact_dupes:,} {'OK' if exact_dupes == 0 else 'PROBLEM'}")
print(f"    CDPHId+ChemicalId dupes    : {logical_dupes:,} {'OK' if logical_dupes == 0 else 'PROBLEM'}")

# ── HELPER COLUMNS CHECK ──────────────────────────────────
print(f"\n[4] HELPER COLUMNS")
for col in ["company_lower", "chemical_lower", "brand_lower", "is_active"]:
    exists = col in df.columns
    print(f"    {col:<30} {'EXISTS' if exists else 'MISSING'}")

# ── VALUE SANITY CHECK ────────────────────────────────────
print(f"\n[5] VALUE SANITY CHECK")
print(f"    Active products     : {df['is_active'].sum():,}")
print(f"    Discontinued        : {(~df['is_active']).sum():,}")
print(f"    Unique chemicals    : {df['ChemicalName'].nunique()}")
print(f"    Unique companies    : {df['CompanyName'].nunique()}")
print(f"    Unique CAS numbers  : {df['CasNumber'].nunique()}")
print(f"    Date range          : {df['InitialDateReported'].min()} → {df['InitialDateReported'].max()}")

# ── SQL ACCURACY TESTS ────────────────────────────────────
print(f"\n[6] SQL ACCURACY TESTS (CSV vs DuckDB must match)")
tests = [
    ("Titanium dioxide records",
     "SELECT COUNT(*) FROM products WHERE LOWER(ChemicalName) ILIKE '%titanium dioxide%'",
     len(df[df["ChemicalName"].str.lower().str.contains("titanium dioxide", na=False)])),

    ("Revlon records",
     "SELECT COUNT(*) FROM products WHERE LOWER(CompanyName) ILIKE '%revlon%'",
     len(df[df["CompanyName"].str.lower().str.contains("revlon", na=False)])),

    ("Discontinued records",
     "SELECT COUNT(*) FROM products WHERE DiscontinuedDate != 'active'",
     len(df[df["DiscontinuedDate"] != "active"])),

    ("Missing CAS records",
     "SELECT COUNT(*) FROM products WHERE cas_missing = true",
     len(df[df["cas_missing"] == True])),
]

all_match = True
for name, sql, csv_count in tests:
    db_result = con.execute(sql).fetchone()[0]
    match = csv_count == db_result
    if not match:
        all_match = False
    print(f"    {name}")
    print(f"      CSV: {csv_count:,}  DB: {db_result:,}  {'MATCH' if match else 'MISMATCH'}")

# ── FINAL VERDICT ─────────────────────────────────────────
print(f"\n{'=' * 60}")
if all_clean and exact_dupes == 0 and logical_dupes == 0 and all_match:
    print("VERDICT: CLEAN — data is accurate and ready to use")
else:
    print("VERDICT: ISSUES FOUND — review the problems above")
print("=" * 60)

con.close()