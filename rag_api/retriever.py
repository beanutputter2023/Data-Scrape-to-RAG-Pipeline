from langchain.vectorstores import Chroma
from langchain.embeddings import HuggingFaceEmbeddings
import os

# Load the same embedding model you used in embeddings.py
embedding_function = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

# Update path to the correct Chroma directory
CHROMA_DB_DIR = "chroma_db"

def get_relevant_documents(query: str):
    vectordb = Chroma(collection_name="articles", persist_directory=CHROMA_DB_DIR, embedding_function=embedding_function)
    docs = vectordb.similarity_search(query, k=3)
    return [doc.page_content for doc in docs]
