# blackbox_task — End-to-End Data-Scrape-to-RAG Pipeline

## Brief Description

This project is a lightweight, production-style **Retrieval-Augmented Generation (RAG)** pipeline built from scratch. It scrapes news articles (BBC Tech RSS), ingests and transforms them through a medallion-style Bronze → Silver → Gold flow using **DuckDB** and Python, generates semantic embeddings with `sentence-transformers`, stores those vectors in **ChromaDB**, and exposes a semantic search / Q&A interface via a **FastAPI** RAG API. Orchestration, scheduling, and execution are handled with **Apache Airflow** DAG, and all data/artifacts are persisted in **MinIO** (S3-compatible) object storage. The entire stack is containerized and run locally with **Docker Compose**.

## Tools Used

- **Python** – Core language for ETL, embedding, and API logic  
- **DuckDB** – Fast SQL transformations for Silver/Gold layers  
- **BeautifulSoup / RSS parsing** – Scraping and HTML extraction  
- **MinIO** – Local object storage for raw/bronze/silver/gold parquet data  
- **ChromaDB** – Vector store for embeddings and semantic retrieval  
- **SentenceTransformers** (`all-MiniLM-L6-v2`) – Embedding model  
- **FastAPI** – RAG API to accept queries and return answers with sources  
- **Apache Airflow** – DAG-based orchestration of scrape → transform → embed → serve  
- **Docker & Docker Compose** – Containerization of Airflow, MinIO, and the RAG API  
- **Git** – Version control for submission  

## Architecture

![Architecture Diagram]<img width="411" height="871" alt="image" src="https://github.com/user-attachments/assets/5e58af49-bc5d-4800-83e2-de32412169b1" />


*(The diagram shows: scraping → raw in MinIO → bronze/silver/gold via DuckDB transforms → embedding generation into ChromaDB → FastAPI RAG API querying ChromaDB, all orchestrated by Airflow and containerized with Docker Compose.)*

## Demo Video

Watch the end-to-end demo walkthrough here: **<https://drive.google.com/file/d/1tzEH1b4Vn6zwg1EM0H0cKHE0glonjRuA/view?usp=sharing>**

---
