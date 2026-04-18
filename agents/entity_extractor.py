import re
import spacy
import dateparser
import duckdb

COMPANY_ALIASES = {
    "avon":      "new avon llc",
    "l'oreal":   "loreal",
    "loreal":    "loreal",
    "colgate":   "colgate-palmolive company",
    "maybelline":"maybelline llc",
    "revlon":    "revlon consumer product corporation",
}

CAS_RE = re.compile(r"\b\d{2,7}-\d{2}-\d\b")

def build_ruler(con):
    nlp = spacy.load("en_core_web_sm")
    ruler = nlp.add_pipe("entity_ruler", before="ner")

    companies = con.execute(
        "SELECT DISTINCT LOWER(CompanyName) as n FROM products WHERE CompanyName IS NOT NULL"
    ).fetchdf()["n"].tolist()

    brands = con.execute(
        "SELECT DISTINCT LOWER(BrandName) as n FROM products WHERE BrandName IS NOT NULL"
    ).fetchdf()["n"].tolist()

    chemicals = con.execute(
        "SELECT DISTINCT LOWER(ChemicalName) as n FROM products WHERE ChemicalName IS NOT NULL"
    ).fetchdf()["n"].tolist()

    patterns = (
        [{"label": "COMPANY",  "pattern": n} for n in companies]  +
        [{"label": "BRAND",    "pattern": n} for n in brands]     +
        [{"label": "CHEMICAL", "pattern": n} for n in chemicals]
    )
    ruler.add_patterns(patterns)
    return nlp


def extract(question: str, nlp) -> dict:
    cas_matches = CAS_RE.findall(question)
    doc = nlp(question.lower())

    entities = {
        "chemicals":        [],
        "companies":        [],
        "brands":           [],
        "cas_numbers":      cas_matches,
        "date_constraints": {},
        "categories":       [],
        "discontinued":     "discontinued" in question.lower()
                            or "removed" in question.lower(),
    }

    for ent in doc.ents:
        if ent.label_ == "CHEMICAL":
            entities["chemicals"].append(ent.text)
        elif ent.label_ == "COMPANY":
            entities["companies"].append(ent.text)
        elif ent.label_ == "BRAND":
            entities["brands"].append(ent.text)

    year_matches = re.findall(r"\b(20\d{2})\b", question)
    if year_matches:
        entities["date_constraints"]["years"] = [int(y) for y in year_matches]

    for phrase in ["since", "after", "before", "in", "during"]:
        pattern = rf"{phrase}\s+([\w\s,]+?)(?:\s|$|\.|\?)"
        m = re.search(pattern, question, re.I)
        if m:
            parsed = dateparser.parse(m.group(1))
            if parsed:
                entities["date_constraints"][phrase] = parsed.isoformat()

    # Resolve company aliases
    resolved_companies = []
    for company in entities["companies"]:
        alias = COMPANY_ALIASES.get(company.lower())
        resolved_companies.append(alias if alias else company)
    entities["companies"] = resolved_companies

    # Resolve brand aliases
    resolved_brands = []
    for brand in entities["brands"]:
        alias = COMPANY_ALIASES.get(brand.lower())
        resolved_brands.append(alias if alias else brand)
    entities["brands"] = resolved_brands

    return entities