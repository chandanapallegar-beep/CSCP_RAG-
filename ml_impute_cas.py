from sklearn.impute import KNNImputer
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np

df = pd.read_csv("data/chemicals_in_cosmetics_clean.csv")

# Encode ChemicalName as numbers for KNN
le = LabelEncoder()
df["chemical_encoded"] = le.fit_transform(df["ChemicalName"].fillna("unknown"))

# Encode CasNumber — treat nulls as NaN for imputer
cas_le = LabelEncoder()
known_cas = df[df["CasNumber"].notna()]["CasNumber"].unique()
cas_le.fit(known_cas)

df["cas_encoded"] = np.nan
mask = df["CasNumber"].notna()
df.loc[mask, "cas_encoded"] = cas_le.transform(df.loc[mask, "CasNumber"])

# Run KNN imputer
imputer = KNNImputer(n_neighbors=5)
features = df[["chemical_encoded", "cas_encoded"]].values
imputed  = imputer.fit_transform(features)

# Fill back missing CAS numbers
df["cas_encoded_imputed"] = imputed[:, 1]
df.loc[~mask, "cas_encoded"] = df.loc[~mask, "cas_encoded_imputed"].round()

# Decode back to CAS string
def decode_cas(val):
    try:
        idx = int(round(val))
        if 0 <= idx < len(cas_le.classes_):
            return cas_le.classes_[idx]
    except:
        pass
    return None

df["CasNumber_imputed"] = df["cas_encoded"].apply(decode_cas)
df.loc[~mask, "CasNumber"] = df.loc[~mask, "CasNumber_imputed"]

# Clean up temp columns
df = df.drop(columns=["chemical_encoded", "cas_encoded",
                       "cas_encoded_imputed", "CasNumber_imputed"])

print(f"CAS nulls before : 4034")
print(f"CAS nulls after  : {df['CasNumber'].isnull().sum()}")
df.to_csv("data/chemicals_in_cosmetics_clean.csv", index=False)
print("Saved.")