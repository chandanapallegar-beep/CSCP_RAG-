import duckdb
import faiss
import numpy as np
import pickle
from sentence_transformers import SentenceTransformer

DB_PATH    = "db/cscp.duckdb"
INDEX_PATH = "index/faiss.index"
META_PATH  = "index/faiss_meta.pkl"

model = SentenceTransformer("all-MiniLM-L6-v2")  # 80MB, CPU-friendly
con   = duckdb.connect(DB_PATH)

# Pull the unique entities you want to search semantically
rows = con.execute("""
    SELECT DISTINCT
        CDPHId,
        ChemicalName,
        CasNumber,
        ProductName,
        CompanyName,
        BrandName
    FROM products
    WHERE ChemicalName IS NOT NULL
""").fetchdf()

# Build a searchable text per row — what gets embedded
rows["search_text"] = (
    rows["ChemicalName"].fillna("") + " " +
    rows["CasNumber"].fillna("") + " " +
    rows["ProductName"].fillna("")
).str.strip()

texts = rows["search_text"].tolist()
print(f"Embedding {len(texts)} records...")

embeddings = model.encode(texts, batch_size=256, show_progress_bar=True)
embeddings = np.array(embeddings, dtype="float32")

# Normalize for cosine similarity
faiss.normalize_L2(embeddings)

index = faiss.IndexFlatIP(embeddings.shape[1])  # inner product = cosine after L2 norm
index.add(embeddings)

faiss.write_index(index, INDEX_PATH)

# Save metadata so you can map result IDs back to CDPHId
with open(META_PATH, "wb") as f:
    pickle.dump(rows.to_dict(orient="records"), f)

print(f"Index built: {index.ntotal} vectors")