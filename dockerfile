# Custom Airflow image with all document intelligence dependencies
# Base: official Apache Airflow 2.9.3
FROM apache/airflow:2.9.3

# Switch to root to install system dependencies
USER root

# Install system packages needed by PyMuPDF
RUN apt-get update && apt-get install -y \
    libmupdf-dev \
    mupdf-tools \
    gcc \
    g++ \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Install Python dependencies in correct order
# numpy first to avoid binary incompatibility
RUN pip install --no-cache-dir "numpy==1.26.4"

# Document extraction
RUN pip install --no-cache-dir \
    pymupdf==1.23.8 \
    python-docx==1.1.0

# Embeddings — install torch first then sentence-transformers
RUN pip install --no-cache-dir \
    "torch==2.2.0" --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir \
    sentence-transformers==2.3.1

# Vector DB + utils
RUN pip install --no-cache-dir \
    "pinecone==5.0.0" \
    python-dotenv==1.0.0 \
    requests==2.31.0