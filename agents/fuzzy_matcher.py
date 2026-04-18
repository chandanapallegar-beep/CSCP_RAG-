from rapidfuzz import process, fuzz
import duckdb

DB_PATH = "db/cscp.duckdb"


class FuzzyMatcher:
    def __init__(self, con):
        print("Loading fuzzy matcher...")
        self.companies = con.execute(
            "SELECT DISTINCT LOWER(CompanyName) FROM products WHERE CompanyName IS NOT NULL"
        ).fetchdf().iloc[:, 0].tolist()

        self.brands = con.execute(
            "SELECT DISTINCT LOWER(BrandName) FROM products WHERE BrandName IS NOT NULL"
        ).fetchdf().iloc[:, 0].tolist()

        self.chemicals = con.execute(
            "SELECT DISTINCT LOWER(ChemicalName) FROM products WHERE ChemicalName IS NOT NULL"
        ).fetchdf().iloc[:, 0].tolist()

        self.cas_numbers = con.execute(
            "SELECT DISTINCT CasNumber FROM products WHERE CasNumber IS NOT NULL"
        ).fetchdf().iloc[:, 0].tolist()

    def find_best_company(self, query: str, threshold: int = 75) -> dict:
        result = process.extractOne(
            query.lower(),
            self.companies,
            scorer=fuzz.token_sort_ratio
        )
        if result and result[1] >= threshold:
            return {"match": result[0], "score": result[1], "found": True}
        return {"match": None, "score": 0, "found": False}

    def find_best_chemical(self, query: str, threshold: int = 75) -> dict:
        result = process.extractOne(
            query.lower(),
            self.chemicals,
            scorer=fuzz.token_sort_ratio
        )
        if result and result[1] >= threshold:
            return {"match": result[0], "score": result[1], "found": True}
        return {"match": None, "score": 0, "found": False}

    def find_best_brand(self, query: str, threshold: int = 75) -> dict:
        result = process.extractOne(
            query.lower(),
            self.brands,
            scorer=fuzz.token_sort_ratio
        )
        if result and result[1] >= threshold:
            return {"match": result[0], "score": result[1], "found": True}
        return {"match": None, "score": 0, "found": False}

    def resolve_entities(self, entities: dict) -> dict:
        resolved = entities.copy()
        fuzzy_corrections = {}

        # Resolve companies
        resolved_companies = []
        for company in entities.get("companies", []):
            match = self.find_best_company(company)
            if match["found"]:
                resolved_companies.append(match["match"])
                if match["match"] != company.lower():
                    fuzzy_corrections[company] = match["match"]
            else:
                resolved_companies.append(company)
        resolved["companies"] = resolved_companies

        # Resolve chemicals
        resolved_chemicals = []
        for chemical in entities.get("chemicals", []):
            match = self.find_best_chemical(chemical)
            if match["found"]:
                resolved_chemicals.append(match["match"])
                if match["match"] != chemical.lower():
                    fuzzy_corrections[chemical] = match["match"]
            else:
                resolved_chemicals.append(chemical)
        resolved["chemicals"] = resolved_chemicals

        # Resolve brands
        resolved_brands = []
        for brand in entities.get("brands", []):
            match = self.find_best_brand(brand)
            if match["found"]:
                resolved_brands.append(match["match"])
                if match["match"] != brand.lower():
                    fuzzy_corrections[brand] = match["match"]
            else:
                resolved_brands.append(brand)
        resolved["brands"] = resolved_brands

        resolved["fuzzy_corrections"] = fuzzy_corrections
        return resolved