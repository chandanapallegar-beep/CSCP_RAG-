from rapidfuzz import process, fuzz
import pandas as pd

df = pd.read_csv("data/chemicals_in_cosmetics_clean.csv")
companies = df["CompanyName"].unique().tolist()

print("Finding near-duplicate company names...")
groups = {}
matched = set()

for name in companies:
    if name in matched:
        continue
    results = process.extract(
        name, companies,
        scorer=fuzz.token_sort_ratio,
        limit=10
    )
    similar = [r[0] for r in results if 85 < r[1] < 100]
    if similar:
        groups[name] = similar
        matched.update(similar)

print(f"\nFound {len(groups)} groups of similar names:")
for canonical, dupes in list(groups.items())[:10]:
    print(f"  '{canonical}' ← {dupes}")

# Standardize — replace all variants with the canonical name
for canonical, dupes in groups.items():
    df.loc[df["CompanyName"].isin(dupes), "CompanyName"] = canonical

print(f"\nCompany names before: 606")
print(f"Company names after : {df['CompanyName'].nunique()}")
df.to_csv("data/chemicals_in_cosmetics_clean.csv", index=False)