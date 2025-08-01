from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Make sure Airflow can import your scripts
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

# Import functions from each script
from scripts.scraper import run_scraper
from scripts.bronze_transformations import process_raw_to_bronze
from scripts.silver_transformations import transform_to_silver
from scripts.gold_transformations import transform_to_gold
from scripts.embeddings import generate_embeddings

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}

with DAG(
    dag_id="news_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 7, 30),
    schedule_interval=None,
    catchup=False,
    description="ETL DAG: scrape, transform, embed news articles using MinIO + ChromaDB"
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_rss_and_html",
        python_callable=run_scraper
    )

    bronze_task = PythonOperator(
        task_id="transform_to_bronze",
        python_callable=process_raw_to_bronze
    )

    silver_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver
    )

    gold_task = PythonOperator(
        task_id="transform_to_gold",
        python_callable=transform_to_gold
    )

    embedding_task = PythonOperator(
        task_id="generate_article_embeddings",
        python_callable=generate_embeddings
    )

    # Set the task order
    scrape_task >> bronze_task >> silver_task >> gold_task >> embedding_task
