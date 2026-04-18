import streamlit as st
import requests

st.set_page_config(page_title="CSCP RAG", page_icon="🧴", layout="wide")

st.title("🧴 CSCP Chemical Disclosure Assistant")
st.caption("Ask questions about chemicals in cosmetics — powered by the California Safe Cosmetics Program dataset.")

if "history" not in st.session_state:
    st.session_state.history = []
if "page_map" not in st.session_state:
    st.session_state.page_map = {}

with st.form("query_form", clear_on_submit=True):
    question  = st.text_input("Ask a question", placeholder='e.g. "Which products contain CAS 75-07-0?"')
    submitted = st.form_submit_button("Ask")

SAMPLE_QUESTIONS = [
    "Which products contain CAS 75-07-0?",
    "List all chemicals reported by Revlon",
    "Summarize chemicals reported by New Avon LLC",
    "Show reporting trends since 2017",
    "Are there any products with missing CAS numbers?",
    "Compare chemical disclosures between Revlon and New Avon LLC",
    "Products that contain titanium dioxide",
]

with st.sidebar:
    st.markdown("### Sample questions")
    for q in SAMPLE_QUESTIONS:
        if st.button(q, use_container_width=True):
            question  = q
            submitted = True
    st.divider()
    if st.button("Clear history", use_container_width=True):
        st.session_state.history  = []
        st.session_state.page_map = {}
        st.rerun()
    st.divider()
    st.markdown("### About")
    st.caption("This tool queries 58,600 cleaned cosmetic product records from the California Department of Public Health.")

if submitted and question:
    with st.spinner("Thinking..."):
        try:
            response = requests.post(
                "http://127.0.0.1:8000/query",
                json={"question": question},
                timeout=60
            )
            data = response.json()
            st.session_state.history.insert(0, {"question": question, "data": data})
            st.session_state.page_map[question] = 1
        except Exception as e:
            st.error(f"Could not reach the API: {e}. Make sure uvicorn is running.")


PAGE_SIZE = 10

for item in st.session_state.history:
    q    = item["question"]
    data = item["data"]

    with st.container():
        st.markdown(f"#### Q: {q}")

        # extract intent and counts before columns
        steps      = data.get("query_plan", {}).get("steps", [])
        intent     = "unknown"
        sql_count  = 0
        true_total = 0
        for step in steps:
            if step.get("agent") == "intent_classifier":
                intent = step.get("output", {}).get("intent", "unknown")
            if step.get("agent") == "sql_agent":
                sql_count  = step.get("output", {}).get("count", 0)
                true_total = step.get("output", {}).get("true_total", 0)

        col1, col2 = st.columns([2, 1])

        with col1:
            records = data.get("records", [])
            total   = len(records)

            if records:
                current_page = st.session_state.page_map.get(q, 1)
                total_pages  = max(1, -(-total // PAGE_SIZE))
                start        = (current_page - 1) * PAGE_SIZE
                end          = min(start + PAGE_SIZE, total)
                page_records = records[start:end]

                if true_total > total:
                    st.markdown(
                        f"**Found {true_total:,} record(s) — "
                        f"showing {start+1}–{end} "
                        f"(page {current_page} of {total_pages})**"
                    )
                    st.info(
                        f"There are {true_total:,} total matching records. "
                        f"Displaying the most recent {total}. "
                        f"Refine your query to narrow results."
                    )
                else:
                    st.markdown(
                        f"**Found {total:,} record(s) — "
                        f"showing {start+1}–{end} "
                        f"(page {current_page} of {total_pages})**"
                    )

                # render records based on intent
                for rec in page_records:
                    if intent == "trend":
                        year  = rec.get("year", "unknown")
                        count = rec.get("report_count", 0)
                        chems = rec.get("unique_chemicals", 0)
                        comps = rec.get("unique_companies", 0)
                        st.markdown(
                            f"**{year}** — {count:,} reports · "
                            f"{chems} unique chemicals · "
                            f"{comps} companies"
                        )
                    elif intent == "compare":
                        company = rec.get("CompanyName", "")
                        chem    = rec.get("ChemicalName", "")
                        cas     = rec.get("CasNumber", "")
                        prods   = rec.get("product_count", 0)
                        first   = rec.get("first_reported", "")
                        st.markdown(
                            f"**{company}** — {chem}"
                            f"{f' [CAS {cas}]' if cas else ''} · "
                            f"{prods} product(s) · "
                            f"first reported {first}"
                        )
                    else:
                        parts = []
                        if rec.get("ProductName"):
                            parts.append(f"**{rec['ProductName']}**")
                        if rec.get("CompanyName"):
                            parts.append(f"({rec['CompanyName']})")
                        if rec.get("BrandName") and rec.get("BrandName") != rec.get("CompanyName"):
                            parts.append(f"/ {rec['BrandName']}")
                        if rec.get("ChemicalName"):
                            parts.append(f"— {rec['ChemicalName']}")
                        if rec.get("CasNumber"):
                            parts.append(f"[CAS {rec['CasNumber']}]")
                        disc = str(rec.get("DiscontinuedDate", ""))
                        if disc and disc not in ("None", "NaT", "nan", "active", ""):
                            parts.append(f"· discontinued {disc}")
                        else:
                            parts.append("· active")
                        if rec.get("CDPHId"):
                            parts.append(f"· ID: {rec['CDPHId']}")
                        st.markdown(" ".join(str(p) for p in parts))

                # pagination
                st.markdown("")
                if total_pages > 1:
                    pcol1, pcol2, pcol3 = st.columns([1, 2, 1])
                    with pcol1:
                        if current_page > 1:
                            if st.button("← Previous", key=f"prev_{q}_{current_page}"):
                                st.session_state.page_map[q] = current_page - 1
                                st.rerun()
                    with pcol2:
                        st.markdown(
                            f"<p style='text-align:center;color:gray;font-size:13px;'>"
                            f"Page {current_page} of {total_pages}</p>",
                            unsafe_allow_html=True
                        )
                    with pcol3:
                        if current_page < total_pages:
                            if st.button("Next →", key=f"next_{q}_{current_page}"):
                                st.session_state.page_map[q] = current_page + 1
                                st.rerun()

                # jump to page
                if total_pages > 5:
                    st.markdown("")
                    jcol1, jcol2 = st.columns([1, 3])
                    with jcol1:
                        jump = st.number_input(
                            "Go to page",
                            min_value=1,
                            max_value=total_pages,
                            value=current_page,
                            step=1,
                            key=f"jump_{q}"
                        )
                        if jump != current_page:
                            st.session_state.page_map[q] = jump
                            st.rerun()
            else:
                st.markdown(data.get("answer", "No answer returned."))

        with col2:
            confidence = data.get("confidence", 0)
            st.metric("Intent",          intent)
            st.metric("Confidence",      f"{round(confidence * 100)}%")
            st.metric("Records fetched", f"{sql_count:,}")
            if true_total > sql_count:
                st.metric("True total",  f"{true_total:,}")

        plan_warnings = data.get("warnings", [])
        if plan_warnings:
            for w in plan_warnings:
                if "Spelling corrected" in w:
                    st.info(f"Auto-corrected: {w.replace('Spelling corrected: ', '')}")
                elif "Fuzzy matched" in w:
                    st.info(f"Fuzzy matched: {w.replace('Fuzzy matched: ', '')}")
                else:
                    st.warning(w)

        with st.expander("Evidence (cited record IDs)"):
            evidence = data.get("evidence", [])
            if evidence:
                st.caption(f"{len(evidence)} record IDs cited")
                cols = st.columns(5)
                for i, eid in enumerate(evidence):
                    cols[i % 5].code(str(eid))
            else:
                st.caption("No evidence returned.")

        with st.expander("Query plan"):
            plan = data.get("query_plan", {})
            for step in plan.get("steps", []):
                agent  = step.get("agent", "")
                output = step.get("output", {})
                st.markdown(f"**{agent}**")
                if output:
                    st.json(output)
            if plan.get("replans", 0) > 0:
                st.info(f"Re-planned {plan['replans']} time(s)")

        with st.expander("SQL used"):
            for step in data.get("query_plan", {}).get("steps", []):
                if step.get("agent") == "sql_agent":
                    sql = step.get("output", {}).get("sql", "")
                    if sql:
                        st.code(sql, language="sql")

        st.divider()