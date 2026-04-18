import duckdb
from agents.intent_classifier import IntentClassifier
from agents.entity_extractor  import extract, build_ruler
from agents.sql_agent         import SQLAgent
from agents.semantic_agent    import SemanticAgent
from agents.synthesizer       import Synthesizer
from agents.spell_corrector   import correct_spelling
from agents.fuzzy_matcher     import FuzzyMatcher

REPLAN_THRESHOLD = 0.5
MAX_REPLANS      = 2

STRONG_KEYWORDS = {
    "lookup":    ["which products contain", "what products have",
                  "products that contain", "products containing",
                  "find products with cas", "contains cas"],
    "list":      ["list all", "list every", "show all",
                  "give me all", "find all", "all products",
                  "all chemicals", "every product"],
    "compare":   ["compare", "vs", "versus",
                  "difference between", "side by side"],
    "summarize": ["summarize", "summarise", "summary",
                  "give me an overview", "give overview"],
    "trend":     ["trend", "over time", "by year",
                  "per year", "reporting history", "annually"],
    "quality":   ["missing cas", "data quality", "without cas",
                  "no cas number", "data issues", "conflicting dates"],
}

WEAK_KEYWORDS = {
    "lookup":    ["contain", "containing", "products with"],
    "list":      ["show me all", "all records"],
    "compare":   ["between"],
    "summarize": ["overview", "describe", "tell me about"],
    "trend":     ["since 20", "growth", "history"],
    "quality":   ["missing", "null", "incomplete"],
}


def apply_overrides(question: str, intent_result: dict) -> dict:
    q             = question.lower().strip()
    ml_intent     = intent_result["intent"]
    ml_confidence = intent_result["confidence"]

    # Check strong keywords first
    strong_match = None
    matched_kw   = None
    for intent, keywords in STRONG_KEYWORDS.items():
        for kw in keywords:
            if kw in q:
                strong_match = intent
                matched_kw   = kw
                break
        if strong_match:
            break

    if strong_match:
        if strong_match != ml_intent:
            # strong keyword DISAGREES with ML — override fully
            intent_result["intent"]          = strong_match
            intent_result["confidence"]      = 0.88
            intent_result["overridden"]      = True
            intent_result["override_reason"] = f"strong keyword: '{matched_kw}'"
        else:
            # strong keyword AGREES with ML — keep ML score exactly
            # only add a note, never change the confidence
            intent_result["override_reason"] = f"confirmed by keyword: '{matched_kw}'"
        return intent_result

    # Check weak keywords only if no strong match
    weak_match = None
    for intent, keywords in WEAK_KEYWORDS.items():
        for kw in keywords:
            if kw in q:
                weak_match = intent
                matched_kw = kw
                break
        if weak_match:
            break

    if weak_match and weak_match != ml_intent:
        # weak keyword disagrees — only override if ML is very uncertain
        if ml_confidence < 0.45:
            intent_result["intent"]          = weak_match
            intent_result["confidence"]      = 0.60
            intent_result["overridden"]      = True
            intent_result["override_reason"] = f"weak keyword: '{matched_kw}'"

    # everything else — return pure ML result untouched
    return intent_result

class Orchestrator:
    def __init__(self):
        print("Initialising orchestrator...")
        self.con          = duckdb.connect("db/cscp.duckdb", read_only=True)
        self.nlp          = build_ruler(self.con)
        self.classifier   = IntentClassifier()
        self.sql_agent    = SQLAgent()
        self.sem_agent    = SemanticAgent()
        self.synthesizer  = Synthesizer()
        self.fuzzy        = FuzzyMatcher(self.con)
        print("Orchestrator ready.")

    def run(self, question: str) -> dict:
        plan = {"steps": [], "warnings": [], "replans": 0}

        # ── STEP 1: SPELL CORRECTION ──────────────────────
        spell_result = correct_spelling(question)
        corrected_q  = spell_result["corrected"]

        if spell_result["was_corrected"]:
            plan["warnings"].append(
                f"Spelling corrected: {spell_result['corrections']}"
            )
        plan["steps"].append({
            "agent":  "spell_corrector",
            "output": spell_result
        })

        # ── STEP 2: CLASSIFY INTENT ───────────────────────
        intent_result = self.classifier.classify(corrected_q)
        intent_result = apply_overrides(corrected_q, intent_result)
        intent        = intent_result["intent"]

        plan["steps"].append({
            "agent":  "intent_classifier",
            "output": intent_result
        })

        # ── STEP 3: EXTRACT ENTITIES ──────────────────────
        entities = extract(corrected_q, self.nlp)

        # ── STEP 4: FUZZY RESOLVE ENTITIES ────────────────
        entities = self.fuzzy.resolve_entities(entities)

        if entities.get("fuzzy_corrections"):
            plan["warnings"].append(
                f"Fuzzy matched: {entities['fuzzy_corrections']}"
            )

        plan["steps"].append({
            "agent":  "entity_extractor",
            "output": entities
        })

        no_entities = (
            not entities["chemicals"] and
            not entities["cas_numbers"] and
            not entities["companies"] and
            not entities["brands"] and
            not entities["date_constraints"] and
            not entities["discontinued"]
        )

        if no_entities and intent not in ("trend", "quality"):
            plan["warnings"].append(
                "No specific entity found in your question. "
                "Please mention a company name, chemical name, or CAS number. "
                "Example: 'List all chemicals reported by Revlon' or "
                "'Which products contain CAS 75-07-0?'"
            )
            return {
                "answer":     "Please refine your question by mentioning a specific company, chemical, or CAS number.",
                "evidence":   [],
                "records":    [],
                "query_plan": plan,
                "confidence": intent_result["confidence"],
                "warnings":   plan["warnings"],
            }

        # ── STEP 5: DECIDE ROUTING ────────────────────────
        needs_semantic = (
            not entities["chemicals"] and
            not entities["cas_numbers"] and
            intent in ("lookup", "list", "summarize")
        )

        sql_results = {"records": [], "count": 0, "true_total": 0}
        sem_results = {"confident": []}

        if needs_semantic:
            sem_results = self.sem_agent.search(corrected_q)
            plan["steps"].append({
                "agent":  "semantic_agent",
                "output": {
                    "count":     sem_results["count"],
                    "confident": len(sem_results["confident"])
                }
            })
            if sem_results["confident"]:
                entities["chemicals"] = list(dict.fromkeys([
                    r["ChemicalName"]
                    for r in sem_results["confident"][:3]
                    if r.get("ChemicalName")
                ]))

        # ── STEP 6: RUN SQL ───────────────────────────────
        sql_results = self.sql_agent.run(intent, entities)
        plan["steps"].append({
            "agent":  "sql_agent",
            "output": {
                "count":      sql_results["count"],
                "true_total": sql_results.get("true_total", 0),
                "sql":        sql_results.get("sql_used", "")
            }
        })

        # ── STEP 7: RE-PLAN IF EMPTY ──────────────────────
        replans = 0
        while sql_results["count"] == 0 and replans < MAX_REPLANS:
            replans += 1
            plan["replans"] += 1

            if replans == 1 and entities["date_constraints"]:
                plan["warnings"].append(
                    f"Re-plan {replans}: relaxing date filter."
                )
                entities["date_constraints"] = {}

            elif replans == 1 and entities["companies"]:
                plan["warnings"].append(
                    f"Re-plan {replans}: relaxing company filter."
                )
                entities["companies"] = []

            elif replans == 2 and entities["chemicals"]:
                plan["warnings"].append(
                    f"Re-plan {replans}: relaxing chemical filter."
                )
                entities["chemicals"] = []

            sql_results = self.sql_agent.run(intent, entities)

        if sql_results["count"] == 0:
            plan["warnings"].append(
                "No results found after re-planning. "
                "Try rephrasing or check the company or chemical name."
            )

        # ── STEP 8: SYNTHESIZE ────────────────────────────
        answer = self.synthesizer.synthesize(
            intent=intent,
            question=corrected_q,
            entities=entities,
            sql_results=sql_results,
            sem_results=sem_results,
        )
        plan["steps"].append({"agent": "synthesizer"})

        return {
            "answer":     answer["text"],
            "evidence":   answer["evidence"],
            "records":    answer.get("records", []),
            "query_plan": plan,
            "confidence": intent_result["confidence"],
            "warnings":   plan["warnings"],
        }