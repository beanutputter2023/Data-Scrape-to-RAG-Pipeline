FROM apache/airflow:2.9.1-python3.10

USER root

# Install system dependencies for duckdb
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy requirements and install Python dependencies
COPY requirements.txt requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# RUN pip install --upgrade pip && \
#     pip install --no-cache-dir -r requirements.txt

RUN pip install duckdb --prefer-binary
