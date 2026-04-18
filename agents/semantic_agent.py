import faiss
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

INDEX_PATH = "index/faiss.index"
META_PATH  = "index/faiss_meta.pkl"

class SemanticAgent:
    def __init__(self):
        print("Loading semantic agent...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.index = faiss.read_index(INDEX_PATH)
        with open(META_PATH, "rb") as f:
            self.meta = pickle.load(f)

    def search(self, query_text: str, top_k: int = 20) -> dict:
        vec = self.model.encode([query_text], convert_to_numpy=True).astype("float32")
        faiss.normalize_L2(vec)

        scores, indices = self.index.search(vec, top_k)
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx == -1:
                continue
            record = self.meta[idx].copy()
            record["similarity"] = round(float(score), 4)
            results.append(record)

        return {
            "records":   results,
            "count":     len(results),
            "query":     query_text,
            "threshold": 0.70,
            "confident": [r for r in results if r["similarity"] >= 0.70]
        }