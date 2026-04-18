import pickle
import numpy as np
from sentence_transformers import SentenceTransformer
import os

MODEL_PATH = "index/intent_classifier.pkl"

class IntentClassifier:
    def __init__(self):
        print("Loading intent classifier...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

        if os.path.exists(MODEL_PATH):
            with open(MODEL_PATH, "rb") as f:
                self.clf = pickle.load(f)
            self.mode = "trained"
            print("Using trained classifier.")
        else:
            # fallback to zero-shot if model not trained yet
            from transformers import pipeline
            self.pipe = pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                device=-1
            )
            self.mode = "zero-shot"
            print("Using zero-shot classifier (run train_classifier.py for better accuracy).")

    def classify(self, question: str) -> dict:
        if self.mode == "trained":
            return self._classify_trained(question)
        else:
            return self._classify_zero_shot(question)

    def _classify_trained(self, question: str) -> dict:
        embedding  = self.model.encode([question])
        probs      = self.clf.predict_proba(embedding)[0]
        classes    = self.clf.classes_
        top_idx    = np.argmax(probs)
        top_intent = classes[top_idx]
        top_score  = float(probs[top_idx])

        return {
            "intent":     top_intent,
            "confidence": round(top_score, 3),
            "all_scores": {
                cls: round(float(p), 3)
                for cls, p in zip(classes, probs)
            },
            "mode": "trained"
        }

    def _classify_zero_shot(self, question: str) -> dict:
        INTENTS = [
            "find or lookup a specific product chemical or record using a CAS number product ID or exact name",
            "list or show all products chemicals or records that match a company brand or category filter",
            "compare or contrast chemical disclosures between two or more companies brands or categories",
            "summarize or give an overview of what chemicals a specific company or brand has reported",
            "show trend analysis reporting history or changes over time by year or date range",
            "check data quality find missing null empty or conflicting records in the dataset",
        ]
        INTENT_KEYS = ["lookup", "list", "compare", "summarize", "trend", "quality"]

        result    = self.pipe(question, candidate_labels=INTENTS, multi_label=False)
        top_idx   = INTENTS.index(result["labels"][0])
        top_score = result["scores"][0]

        return {
            "intent":     INTENT_KEYS[top_idx],
            "confidence": round(top_score, 3),
            "all_scores": dict(zip(INTENT_KEYS, result["scores"])),
            "mode":       "zero-shot"
        }