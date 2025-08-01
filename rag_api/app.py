from fastapi import FastAPI
from query_model import QueryRequest
from retriever import get_relevant_documents

app = FastAPI()

@app.post("/query")
def query_docs(request: QueryRequest):
    results = get_relevant_documents(request.query)
    return {"results": results}
