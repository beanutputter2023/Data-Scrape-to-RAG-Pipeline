import duckdb
from minio import Minio
from io import BytesIO
import tempfile
import pandas as pd

def transform_to_gold():
    # MinIO Setup
    minio_client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )

    # Download silver.parquet
    silver_obj = minio_client.get_object("silver", "silver_articles.parquet")
    silver_data = silver_obj.read()

    # Write to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_silver:
        tmp_silver.write(silver_data)
        silver_path = tmp_silver.name

    # DuckDB Connection
    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE TABLE silver AS 
        SELECT * FROM read_parquet('{silver_path}');
    """)

    # Create gold table: per article enriched + date parts
    con.execute("""
        CREATE OR REPLACE TABLE gold AS
        SELECT
            file_id AS id,
            TRIM(raw_filename) AS filename,
            title,
            description,
            url,
            publish_ts,
            DATE_TRUNC('year', publish_ts) AS publish_year,
            DATE_TRUNC('month', publish_ts) AS publish_month,
            DATE_TRUNC('day', publish_ts) AS publish_day,
            TRIM(extracted_text) AS extracted_text,
            word_count,
            has_description,
            processed_at,
            'gold' AS processing_stage
        FROM silver
        WHERE publish_ts IS NOT NULL
        ORDER BY publish_ts DESC
    """)

    # Export to parquet
    gold_df = con.execute("SELECT * FROM gold").fetchdf()
    buffer = BytesIO()
    gold_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name="gold",
        object_name="gold_articles.parquet",
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print("gold_articles.parquet uploaded to MinIO 'gold' bucket.")
    con.close()

if __name__ == "__main__":
    transform_to_gold()
