import pandas as pd
import duckdb
import re
import os

CSV_PATH   = "data/chemicals_in_cosmetics.csv"
CLEAN_PATH = "data/chemicals_in_cosmetics_clean.csv"
DB_PATH    = "db/cscp.duckdb"

print("=" * 55)
print("CSCP DATA CLEANING PIPELINE")
print("=" * 55)

# ── LOAD ──────────────────────────────────────────────────
print("\n[1/8] Loading raw data...")
df = pd.read_csv(CSV_PATH, encoding="utf-8", on_bad_lines="skip")
print(f"      Raw shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

# ── STEP 1: EXACT DUPLICATES ──────────────────────────────
print("\n[2/8] Removing exact duplicates...")
before = len(df)
df = df.drop_duplicates()
print(f"      Removed: {before - len(df):,} rows → {len(df):,} remaining")

# ── STEP 2: LOGICAL DUPLICATES ────────────────────────────
print("\n[3/8] Removing logical duplicates (CDPHId + ChemicalId)...")
before = len(df)
df["MostRecentDateReported"] = pd.to_datetime(
    df["MostRecentDateReported"], errors="coerce"
)
df = df.sort_values("MostRecentDateReported", ascending=False)
df = df.drop_duplicates(subset=["CDPHId", "ChemicalId"], keep="first")
print(f"      Removed: {before - len(df):,} rows → {len(df):,} remaining")

# ── STEP 3: NULL HANDLING ─────────────────────────────────
print("\n[4/8] Handling null values...")

# Date nulls — null means still active
df["DiscontinuedDate"]    = df["DiscontinuedDate"].fillna("active")
df["ChemicalDateRemoved"] = df["ChemicalDateRemoved"].fillna("active")

# Optional metadata — fill with defaults
df["CSF"]   = df["CSF"].fillna("unknown")
df["CSFId"] = df["CSFId"].fillna(0)

# BrandName — fall back to CompanyName
null_brands = df["BrandName"].isnull().sum()
df["BrandName"] = df["BrandName"].fillna(df["CompanyName"])
print(f"      BrandName nulls filled with CompanyName: {null_brands:,}")

# Flag missing CAS numbers before dropping
df["cas_missing"] = df["CasNumber"].isnull()
cas_missing_count = df["cas_missing"].sum()
print(f"      CAS number nulls flagged: {cas_missing_count:,}")

# Drop rows missing critical fields
before = len(df)
df = df.dropna(subset=["CDPHId", "ProductName", "CompanyName", "ChemicalName"])
print(f"      Dropped {before - len(df):,} rows missing critical fields")

# ── STEP 4: TEXT CLEANING ─────────────────────────────────
print("\n[5/8] Cleaning text columns...")

def clean_text(val):
    if not isinstance(val, str) or val.strip() == "":
        return val
    val = val.strip()
    val = re.sub(r"\s+", " ", val)
    val = re.sub(r"[^\x00-\x7F]+", " ", val)
    return val

text_cols = [
    "ProductName", "CompanyName", "BrandName",
    "ChemicalName", "PrimaryCategory", "SubCategory"
]
for col in text_cols:
    df[col] = df[col].apply(clean_text)
print(f"      Cleaned {len(text_cols)} text columns")

# ── STEP 5: HELPER COLUMNS ────────────────────────────────
print("\n[6/8] Adding helper columns...")
df["company_lower"]  = df["CompanyName"].str.lower().str.strip()
df["chemical_lower"] = df["ChemicalName"].str.lower().str.strip()
df["brand_lower"]    = df["BrandName"].str.lower().str.strip()
df["is_active"]      = df["DiscontinuedDate"] == "active"
print("      Added: company_lower, chemical_lower, brand_lower, is_active")

# ── STEP 6: SAVE CLEAN CSV ────────────────────────────────
print("\n[7/8] Saving clean CSV...")
df.to_csv(CLEAN_PATH, index=False)
print(f"      Saved → {CLEAN_PATH}")

# ── STEP 7: RELOAD INTO DUCKDB ────────────────────────────
print("\n[8/8] Reloading into DuckDB...")
os.makedirs("db", exist_ok=True)
con = duckdb.connect(DB_PATH)
con.execute("CREATE OR REPLACE TABLE products AS SELECT * FROM df")
con.close()
print(f"      Saved → {DB_PATH}")

# ── SUMMARY ───────────────────────────────────────────────
print(f"""
╔══════════════════════════════════════════════╗
║           CLEANING COMPLETE                  ║
╠══════════════════════════════════════════════╣
║  Final rows        : {len(df):<24,}║
║  Columns           : {len(df.columns):<24}║
║  CAS nulls flagged : {cas_missing_count:<24,}║
║  Active products   : {df['is_active'].sum():<24,}║
║  Discontinued      : {(~df['is_active']).sum():<24,}║
║  Unique chemicals  : {df['ChemicalName'].nunique():<24}║
║  Unique companies  : {df['CompanyName'].nunique():<24}║
║  Unique brands     : {df['BrandName'].nunique():<24}║
╚══════════════════════════════════════════════╝
""")