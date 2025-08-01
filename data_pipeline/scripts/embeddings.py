from minio import Minio
import tempfile
import duckdb
from io import BytesIO
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings

def generate_embeddings():
    # MinIO client setup
    minio_client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )

    # Download gold.parquet from MinIO
    gold_obj = minio_client.get_object("gold", "gold_articles.parquet")
    gold_data = gold_obj.read()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_gold:
        tmp_gold.write(gold_data)
        gold_path = tmp_gold.name

    # Connect to DuckDB and load gold table
    con = duckdb.connect()
    con.execute(f"CREATE OR REPLACE TABLE gold AS SELECT * FROM read_parquet('{gold_path}')")

    # Fetch necessary columns
    df = con.execute("SELECT id, title, url, extracted_text FROM gold").fetchdf()

    # Initialize embedding model
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Prepare texts for embedding (full article text)
    texts = df['extracted_text'].fillna('').tolist()
    embeddings = model.encode(texts, show_progress_bar=True)

    # Persistent Chroma client (data will be saved under ./chroma_db/)
    client = chromadb.PersistentClient(path="./chroma_db", settings=Settings(anonymized_telemetry=False))

    # Create or get collection
    collection_name = "articles"
    try:
        collection = client.get_collection(name=collection_name)
    except:
        collection = client.create_collection(name=collection_name)

    # Add embeddings and metadata to collection
    collection.add(
        documents=texts,
        embeddings=embeddings.tolist(),
        metadatas=df[['id', 'title', 'url']].to_dict('records'),
        ids=df['id'].tolist()
    )

    print(f"Embeddings generated and stored in persistent Chroma collection: {collection_name}")
    con.close()

if __name__ == "__main__":
    generate_embeddings()
