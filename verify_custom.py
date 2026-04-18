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

def verify(question):
    print("=" * 60)
    print(f"QUESTION: {question}")
    print("=" * 60)

    data      = ask(question)
    records   = data.get("records", [])
    evidence  = data.get("evidence", [])
    warnings  = data.get("warnings", [])
    confidence = data.get("confidence", 0)

    # get intent and sql from query plan
    intent    = "unknown"
    sql_used  = ""
    true_total = 0
    for step in data.get("query_plan", {}).get("steps", []):
        if step.get("agent") == "intent_classifier":
            intent = step.get("output", {}).get("intent", "unknown")
        if step.get("agent") == "sql_agent":
            sql_used   = step.get("output", {}).get("sql", "")
            true_total = step.get("output", {}).get("true_total", 0)

    print(f"\nIntent     : {intent}")
    print(f"Confidence : {round(confidence * 100)}%")
    print(f"Records    : {len(records):,}")
    print(f"True total : {true_total:,}")

    if warnings:
        print(f"\nWarnings:")
        for w in warnings:
            print(f"  - {w}")

    # show answer
    print(f"\nAnswer preview:")
    for rec in records[:5]:
        if intent == "trend":
            print(f"  {rec.get('year')} — {rec.get('report_count'):,} reports")
        elif intent == "summarize":
            print(f"  {rec.get('ChemicalName')} — {rec.get('product_count')} products")
        elif intent == "compare":
            print(f"  {rec.get('CompanyName')} — {rec.get('ChemicalName')}")
        else:
            print(f"  [{rec.get('CDPHId')}] {rec.get('ProductName')} — {rec.get('ChemicalName')}")

    # verify evidence IDs are real
    print(f"\nVerifying evidence IDs...")
    valid   = 0
    invalid = 0
    for eid in evidence[:10]:
        row = con.execute(
            f"SELECT CDPHId FROM products WHERE CDPHId = {eid}"
        ).fetchone()
        if row:
            valid += 1
        else:
            invalid += 1
            print(f"  INVALID ID: {eid}")
    print(f"  Valid: {valid}  Invalid: {invalid}")

    # run the same SQL directly on DB to cross-check
    if sql_used:
        print(f"\nCross-checking with direct DB query...")
        try:
            df       = con.execute(sql_used).fetchdf()
            db_count = len(df)
            sys_count = len(records)
            match    = "MATCH" if db_count == sys_count else "MISMATCH"
            print(f"  System returned : {sys_count:,}")
            print(f"  DB direct query : {db_count:,}")
            print(f"  Result          : {match}")
        except Exception as e:
            print(f"  Could not run SQL: {e}")

    print(f"\nVERDICT: ", end="")
    if invalid == 0 and len(records) > 0:
        print("ACCURATE — evidence verified, records returned")
    elif len(records) == 0:
        print("NO RESULTS — check if entity name is correct")
    else:
        print("CHECK NEEDED — some evidence IDs invalid")

    print("=" * 60)


# ── ASK YOUR OWN QUESTION HERE ────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # run from command line: python verify_custom.py "your question"
        question = " ".join(sys.argv[1:])
    else:
        # or type it here
        question = input("Enter your question: ")

    verify(question)
    con.close()