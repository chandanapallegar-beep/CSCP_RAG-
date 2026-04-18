from fastapi import FastAPI
from pydantic import BaseModel
from agents.orchestrator import Orchestrator

app          = FastAPI(title="CSCP RAG")
orchestrator = Orchestrator()

class Query(BaseModel):
    question: str

@app.post("/query")
def query(q: Query):
    return orchestrator.run(q.question)

@app.get("/health")
def health():
    return {"status": "ok"}