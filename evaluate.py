import requests
import pandas as pd

df = pd.read_csv("data/chemicals_in_cosmetics_clean.csv")

TEST_CASES = [
    {
        "question": "Which products contain CAS 75-07-0?",
        "check_field": "CasNumber",
        "check_value": "75-07-0",
        "expected_min_count": 1
    },
    {
        "question": "List all chemicals reported by Revlon",
        "check_field": "CompanyName",
        "check_value": "Revlon",
        "expected_min_count": 1
    },
    {
        "question": "Are there any products with missing CAS numbers?",
        "check_field": "cas_missing",
        "check_value": True,
        "expected_min_count": 1
    },
]

print("=" * 60)
print("EVALUATION REPORT")
print("=" * 60)

for i, test in enumerate(TEST_CASES):
    question = test["question"]

    # Get actual answer from ground truth CSV
    field = test["check_field"]
    value = test["check_value"]
    if isinstance(value, str):
        gt_rows = df[df[field].str.contains(value, case=False, na=False)]
    else:
        gt_rows = df[df[field] == value]
    gt_count = len(gt_rows)
    gt_ids   = set(gt_rows["CDPHId"].tolist())

    # Get answer from your RAG system
    response = requests.post(
        "http://127.0.0.1:8000/query",
        json={"question": question},
        timeout=60
    ).json()

    rag_ids     = set(response.get("evidence", []))
    rag_count   = len(response.get("records", []))
    confidence  = response.get("confidence", 0)

    # Compare
    matched     = len(gt_ids & rag_ids)
    precision   = round(matched / rag_count * 100, 1) if rag_count > 0 else 0
    recall      = round(matched / gt_count * 100, 1)  if gt_count > 0 else 0

    status = "PASS" if rag_count >= test["expected_min_count"] else "FAIL"

    print(f"\nTest {i+1}: {question}")
    print(f"  Ground truth count : {gt_count}")
    print(f"  RAG returned count : {rag_count}")
    print(f"  Matched IDs        : {matched}")
    print(f"  Precision          : {precision}%")
    print(f"  Recall             : {recall}%")
    print(f"  Intent confidence  : {round(confidence*100)}%")
    print(f"  Status             : {status}")

print("\n" + "=" * 60)