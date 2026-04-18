import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
with open("index/intent_classifier.pkl", "rb") as f:
    clf = pickle.load(f)

questions = [
    "Which products contain CAS 75-07-0?",
    "List all chemicals reported by Revlon",
    "Compare Revlon and Avon",
    "Summarize chemicals for Avon",
    "Show trends since 2017",
    "Find missing CAS numbers",
    "Products containing titanium dioxide",
    "Are there products with missing CAS numbers?",
    "Show reporting trends since 2018",
    "Give me an overview of Avon chemicals",
]

embeddings  = model.encode(questions)
predictions = clf.predict(embeddings)
probs       = clf.predict_proba(embeddings)

print(f"{'Intent':<12} {'Confidence':<12} Question")
print("-" * 70)
for q, pred, prob in zip(questions, predictions, probs):
    confidence = max(prob)
    flag = "OK" if confidence >= 0.75 else "LOW"
    print(f"{pred:<12} {confidence:.0%}           {flag}  {q}")