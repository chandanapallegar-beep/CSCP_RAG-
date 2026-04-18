import duckdb
import requests

con = duckdb.connect("db/cscp.duckdb", read_only=True)

def ask(question):
    r = requests.post(
        "http://127.0.0.1:8000/query",
        json={"question": question},
        timeout=60
    )
    return r.json()

def check(label, system_count, db_count, tolerance=5):
    diff   = abs(system_count - db_count)
    status = "PASS" if diff <= tolerance else "FAIL"
    print(f"  {status} — System: {system_count:,}  DB: {db_count:,}  Diff: {diff}")
    return status == "PASS"

print("=" * 60)
print("ANSWER VERIFICATION REPORT")
print("=" * 60)

all_pass = True

# ── TEST 1: LOOKUP ────────────────────────────────────────
print("\n[1] LOOKUP — Which products contain CAS 75-07-0?")
data       = ask("Which products contain CAS 75-07-0?")
sys_count  = len(data.get("records", []))
db_count   = con.execute(
    "SELECT COUNT(*) FROM products WHERE CasNumber = '75-07-0'"
).fetchone()[0]
passed = check("CAS 75-07-0", sys_count, db_count)
all_pass = all_pass and passed

# verify evidence IDs are real
evidence = data.get("evidence", [])[:5]
print(f"  Verifying {len(evidence)} evidence IDs...")
for eid in evidence:
    row = con.execute(
        f"SELECT CDPHId, CasNumber FROM products WHERE CDPHId = {eid}"
    ).fetchone()
    if row:
        print(f"    ID {eid} → CAS {row[1]} ✓")
    else:
        print(f"    ID {eid} → NOT FOUND IN DB ✗")
        all_pass = False

# ── TEST 2: LIST ──────────────────────────────────────────
print("\n[2] LIST — List all chemicals reported by Revlon")
data      = ask("List all chemicals reported by Revlon")
sys_count = len(data.get("records", []))
true_total = data.get("query_plan", {})
for step in data.get("query_plan", {}).get("steps", []):
    if step.get("agent") == "sql_agent":
        true_total = step.get("output", {}).get("true_total", 0)
db_count  = con.execute("""
    SELECT COUNT(*) FROM products
    WHERE LOWER(CompanyName) ILIKE '%revlon%'
    OR LOWER(BrandName) ILIKE '%revlon%'
""").fetchone()[0]
passed = check("Revlon records", true_total, db_count)
all_pass = all_pass and passed

# ── TEST 3: SUMMARIZE ─────────────────────────────────────
print("\n[3] SUMMARIZE — Summarize chemicals reported by New Avon LLC")
data      = ask("Summarize chemicals reported by New Avon LLC")
sys_count = len(data.get("records", []))
db_count  = con.execute("""
    SELECT COUNT(DISTINCT ChemicalName) FROM products
    WHERE LOWER(CompanyName) ILIKE '%avon%'
    OR LOWER(BrandName) ILIKE '%avon%'
""").fetchone()[0]
passed = check("Avon unique chemicals", sys_count, db_count)
all_pass = all_pass and passed

# ── TEST 4: TREND ─────────────────────────────────────────
print("\n[4] TREND — Show reporting trends since 2017")
data    = ask("Show reporting trends since 2017")
records = data.get("records", [])
print(f"  Years returned: {[r.get('year') for r in records]}")
for rec in records:
    year     = rec.get("year")
    sys_cnt  = rec.get("report_count", 0)
    db_cnt   = con.execute(f"""
        SELECT COUNT(*) FROM products
        WHERE SUBSTR(InitialDateReported, 7, 4) = '{year}'
    """).fetchone()[0]
    diff   = abs(sys_cnt - db_cnt)
    status = "PASS" if diff == 0 else "FAIL"
    print(f"  {status} — {year}: System {sys_cnt:,}  DB {db_cnt:,}  Diff {diff}")
    if status == "FAIL":
        all_pass = False

# ── TEST 5: QUALITY ───────────────────────────────────────
print("\n[5] QUALITY — Are there any products with missing CAS numbers?")
data      = ask("Are there any products with missing CAS numbers?")
for step in data.get("query_plan", {}).get("steps", []):
    if step.get("agent") == "sql_agent":
        true_total = step.get("output", {}).get("true_total", 0)
db_count  = con.execute(
    "SELECT COUNT(*) FROM products WHERE cas_missing = true"
).fetchone()[0]
passed = check("Missing CAS", true_total, db_count)
all_pass = all_pass and passed

# ── TEST 6: COMPARE ───────────────────────────────────────
print("\n[6] COMPARE — Compare Revlon and New Avon LLC")
data      = ask("Compare chemical disclosures between Revlon and New Avon LLC")
records   = data.get("records", [])
companies = set(r.get("CompanyName", "") for r in records)
print(f"  Companies in results: {companies}")
revlon_in = any("revlon" in c.lower() for c in companies)
avon_in   = any("avon" in c.lower() for c in companies)
print(f"  Revlon present : {'PASS' if revlon_in else 'FAIL'}")
print(f"  Avon present   : {'PASS' if avon_in else 'FAIL'}")
if not revlon_in or not avon_in:
    all_pass = False

# ── TEST 7: EVIDENCE CITATION CHECK ──────────────────────
print("\n[7] EVIDENCE — Verify CDPHIds are real records")
data     = ask("Which products contain CAS 75-07-0?")
evidence = data.get("evidence", [])
valid    = 0
invalid  = 0
for eid in evidence[:20]:
    row = con.execute(
        f"SELECT CDPHId FROM products WHERE CDPHId = {eid}"
    ).fetchone()
    if row:
        valid += 1
    else:
        invalid += 1
print(f"  Valid IDs  : {valid}")
print(f"  Invalid IDs: {invalid}")
if invalid > 0:
    all_pass = False
    print("  FAIL — some evidence IDs don't exist in DB")
else:
    print("  PASS — all evidence IDs verified")

# ── FINAL VERDICT ─────────────────────────────────────────
print(f"\n{'=' * 60}")
if all_pass:
    print("VERDICT: ALL TESTS PASSED — answers are accurate")
else:
    print("VERDICT: SOME TESTS FAILED — review issues above")
print("=" * 60)

con.close() 