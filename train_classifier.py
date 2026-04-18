import json
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.preprocessing import LabelEncoder
import pickle
import os

os.makedirs("index", exist_ok=True)

with open("training_data.json") as f:
    examples = json.load(f)

texts  = [e["text"]  for e in examples]
labels = [e["label"] for e in examples]

print(f"Training on {len(texts)} examples...")
print(f"Labels: {set(labels)}")

model      = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode(texts, show_progress_bar=True)

clf = LogisticRegression(max_iter=1000, C=2.0, class_weight="balanced")
clf.fit(embeddings, labels)

cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
scores = cross_val_score(clf, embeddings, labels, cv=cv, scoring="accuracy")
print(f"\nCross-validation accuracy: {scores.mean():.1%} ± {scores.std():.1%}")
print(f"Per fold: {[f'{s:.1%}' for s in scores]}")

with open("index/intent_classifier.pkl", "wb") as f:
    pickle.dump(clf, f)

print("\nModel saved to index/intent_classifier.pkl")
print("\nTest predictions:")
test_questions = [
    "Which products contain CAS 75-07-0?",
    "List all chemicals reported by Revlon",
    "Compare Revlon and Avon",
    "Summarize chemicals for Avon",
    "Show trends since 2017",
    "Find missing CAS numbers",
]
test_embeddings = model.encode(test_questions)
predictions     = clf.predict(test_embeddings)
probabilities   = clf.predict_proba(test_embeddings)

for q, pred, probs in zip(test_questions, predictions, probabilities):
    confidence = max(probs)
    print(f"  {pred:<12} {confidence:.0%}  '{q}'")