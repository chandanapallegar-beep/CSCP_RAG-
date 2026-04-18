from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np

df = pd.read_csv("data/chemicals_in_cosmetics_clean.csv")

# Build numeric features for anomaly detection
le_company  = LabelEncoder()
le_chemical = LabelEncoder()

features = pd.DataFrame({
    "company_id":  le_company.fit_transform(df["CompanyName"]),
    "chemical_id": le_chemical.fit_transform(df["ChemicalName"]),
    "chem_count":  df["ChemicalCount"].fillna(0),
    "cas_missing": df["cas_missing"].astype(int),
})

# Train Isolation Forest
clf = IsolationForest(
    n_estimators=100,
    contamination=0.01,  # expect ~1% anomalies
    random_state=42
)
df["anomaly_score"] = clf.fit_predict(features)
df["is_anomaly"]    = df["anomaly_score"] == -1

anomalies = df[df["is_anomaly"]]
print(f"Anomalies detected: {len(anomalies):,}")
print(anomalies[["CDPHId", "ProductName", "CompanyName",
                  "ChemicalName", "ChemicalCount"]].head(10))

# Option 1 — just flag them
df.to_csv("data/chemicals_in_cosmetics_clean.csv", index=False)

# Option 2 — remove them (only if you're confident)
# df = df[~df["is_anomaly"]]  