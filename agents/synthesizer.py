from jinja2 import Environment, FileSystemLoader

env = Environment(loader=FileSystemLoader("templates"))

class Synthesizer:
    def synthesize(self, intent, question, entities, sql_results, sem_results) -> dict:
        try:
            template = env.get_template(f"{intent}.j2")
        except Exception:
            template = env.get_template("list.j2")

        records = sql_results.get("records", [])
        ids     = sql_results.get("ids", [])

        text = template.render(
            question=question,
            entities=entities,
            records=records,
            count=sql_results.get("count", 0),
            sql=sql_results.get("sql_used", ""),
        )

        return {
            "text":     text.strip(),
            "evidence": ids,           # all IDs, no cap
            "records":  records,       # pass full records list
        }