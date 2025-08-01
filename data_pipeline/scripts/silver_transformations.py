import duckdb
from minio import Minio
from io import BytesIO
import tempfile
import pandas as pd

def transform_to_silver():
    # Setup MinIO client
    minio_client = Minio(
        "minio:9000",
        access_key="admin",
        secret_key="password123",
        secure=False
    )

    # Load bronze.parquet from MinIO
    bronze_obj = minio_client.get_object("bronze", "bronze_articles.parquet")
    bronze_data = bronze_obj.read()

    # Write to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp_bronze:
        tmp_bronze.write(bronze_data)
        bronze_path = tmp_bronze.name

    # DuckDB: read, clean, and transform
    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE TABLE bronze AS 
        SELECT * FROM read_parquet('{bronze_path}');
    """)

    con.execute("""
        CREATE OR REPLACE TABLE silver AS
        SELECT DISTINCT
            file_id,
            raw_filename,
            TRIM(REGEXP_REPLACE(title, '\\s+', ' ', 'g')) AS title,
            TRIM(REGEXP_REPLACE(description, '\\s+', ' ', 'g')) AS description,
            url,
            TRY_CAST(publish_date AS TIMESTAMP) AS publish_ts,
            extracted_text,
            text_length,
            word_count,
            ROUND(word_count / 200.0, 2) AS reading_time_min,
            CASE WHEN description IS NOT NULL AND LENGTH(description) > 0 THEN TRUE ELSE FALSE END AS has_description,
            processed_at,
            'silver' AS processing_stage
        FROM bronze
        WHERE extracted_text IS NOT NULL 
          AND LENGTH(extracted_text) > 100
          AND title IS NOT NULL;
    """)

    # Export to parquet
    silver_df = con.execute("SELECT * FROM silver").fetchdf()
    buffer = BytesIO()
    silver_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name="silver",
        object_name="silver_articles.parquet",
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print("silver_articles.parquet uploaded to MinIO 'silver' bucket.")
    con.close()

if __name__ == "__main__":
    transform_to_silver()
