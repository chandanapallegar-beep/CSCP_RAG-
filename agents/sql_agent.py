import duckdb

DB_PATH = "db/cscp.duckdb"

TEMPLATES = {
    "lookup": """
        SELECT CDPHId, ProductName, CompanyName, BrandName,
               ChemicalName, CasNumber, InitialDateReported, DiscontinuedDate
        FROM products
        WHERE 1=1
          {cas_filter}
          {chemical_filter}
          {company_filter}
        ORDER BY InitialDateReported DESC
        LIMIT 1000
    """,

    "list": """
        SELECT CDPHId, ProductName, ChemicalName, CasNumber,
               CompanyName, BrandName, InitialDateReported
        FROM products
        WHERE 1=1
          {company_filter}
          {chemical_filter}
          {discontinued_filter}
        ORDER BY InitialDateReported DESC
        LIMIT 1000
    """,

    "trend": """
        SELECT SUBSTR(InitialDateReported, 7, 4) AS year,
               COUNT(*) AS report_count,
               COUNT(DISTINCT ChemicalName) AS unique_chemicals,
               COUNT(DISTINCT CompanyName) AS unique_companies
        FROM products
        WHERE InitialDateReported IS NOT NULL
          {company_filter}
          {chemical_filter}
          {year_filter}
        GROUP BY year
        ORDER BY year
    """,

    "summarize": """
        SELECT DISTINCT
               ChemicalName, CasNumber,
               COUNT(DISTINCT ProductName) AS product_count,
               COUNT(DISTINCT CompanyName) AS company_count,
               MIN(InitialDateReported) AS first_reported
        FROM products
        WHERE 1=1
          {company_filter}
        GROUP BY ChemicalName, CasNumber
        ORDER BY product_count DESC
        LIMIT 50
    """,

    "compare": """
        SELECT DISTINCT
               CompanyName, ChemicalName, CasNumber,
               COUNT(DISTINCT ProductName) AS product_count,
               MIN(InitialDateReported) AS first_reported
        FROM products
        WHERE 1=1
          {company_filter}
          {chemical_filter}
        GROUP BY CompanyName, ChemicalName, CasNumber
        ORDER BY CompanyName, product_count DESC
        LIMIT 200
    """,

    "quality": """
        SELECT DISTINCT
               CDPHId, ProductName, CompanyName, ChemicalName,
               CasNumber, InitialDateReported, DiscontinuedDate
        FROM products
        WHERE cas_missing = true
        ORDER BY CompanyName
        LIMIT 500
    """
}

COUNT_TEMPLATE = """
    SELECT COUNT(*) as total
    FROM products
    WHERE 1=1
      {cas_filter}
      {chemical_filter}
      {company_filter}
"""


def build_filters(entities: dict, intent: str = "") -> dict:
    filters = {k: "" for k in
               ["cas_filter", "chemical_filter", "company_filter",
                "brand_filter", "discontinued_filter", "year_filter"]}

    if entities.get("cas_numbers"):
        cas_list = ", ".join(f"'{c}'" for c in entities["cas_numbers"])
        filters["cas_filter"] = f"AND CasNumber IN ({cas_list})"

    if entities.get("chemicals"):
        filters["chemical_filter"] = \
            f"AND LOWER(ChemicalName) ILIKE '%{entities['chemicals'][0]}%'"

    if intent == "compare":
        # collect all companies and brands for comparison
        search_terms = []
        for c in entities.get("companies", []):
            search_terms.append(c)
        for b in entities.get("brands", []):
            if b not in search_terms:
                search_terms.append(b)

        if search_terms:
            conditions = " OR ".join([
                f"(LOWER(CompanyName) ILIKE '%{t}%' "
                f"OR LOWER(BrandName) ILIKE '%{t}%')"
                for t in search_terms
            ])
            filters["company_filter"] = f"AND ({conditions})"
    else:
        # single entity search — companies take priority over brands
        search_term = None
        if entities.get("companies"):
            search_term = entities["companies"][0]
        elif entities.get("brands"):
            search_term = entities["brands"][0]

        if search_term:
            filters["company_filter"] = (
                f"AND (LOWER(CompanyName) ILIKE '%{search_term}%' "
                f"OR LOWER(BrandName) ILIKE '%{search_term}%')"
            )

    if entities.get("discontinued"):
        filters["discontinued_filter"] = "AND DiscontinuedDate != 'active'"

    years = entities.get("date_constraints", {}).get("years", [])
    if years:
        min_year = min(years)
        filters["year_filter"] = \
            f"AND SUBSTR(InitialDateReported, 7, 4) >= '{min_year}'"

    return filters


def has_any_filter(filters: dict) -> bool:
    return any([
        filters["cas_filter"],
        filters["chemical_filter"],
        filters["company_filter"],
        filters["discontinued_filter"],
        filters["year_filter"],
    ])


class SQLAgent:
    def __init__(self):
        self.con = duckdb.connect(DB_PATH, read_only=True)

    def run(self, intent: str, entities: dict) -> dict:
        template = TEMPLATES.get(intent, TEMPLATES["list"])
        filters  = build_filters(entities, intent=intent)
        sql      = template.format(**filters)

        # guard — no filters = too broad
        if not has_any_filter(filters) and intent not in ("trend", "quality"):
            return {
                "records":    [],
                "count":      0,
                "true_total": 0,
                "sql_used":   sql,
                "warning":    "No filters extracted — query too broad."
            }

        # get true total count
        true_total = 0
        if intent in ("lookup", "list", "quality"):
            try:
                if intent == "quality":
                    count_sql = "SELECT COUNT(*) FROM products WHERE cas_missing = true"
                else:
                    count_sql = COUNT_TEMPLATE.format(
                        cas_filter=filters["cas_filter"],
                        chemical_filter=filters["chemical_filter"],
                        company_filter=filters["company_filter"],
                    )
                true_total = self.con.execute(count_sql).fetchone()[0]
            except Exception as e:
                print("Count query error:", e)
                true_total = 0

        try:
            df = self.con.execute(sql).fetchdf()
            returned_count = len(df)
            return {
                "records":    df.to_dict(orient="records"),
                "count":      returned_count,
                "true_total": true_total if true_total > 0 else returned_count,
                "sql_used":   sql.strip(),
                "ids":        df["CDPHId"].tolist() if "CDPHId" in df.columns else []
            }
        except Exception as e:
            return {
                "records":    [],
                "count":      0,
                "true_total": 0,
                "error":      str(e),
                "sql_used":   sql
            }