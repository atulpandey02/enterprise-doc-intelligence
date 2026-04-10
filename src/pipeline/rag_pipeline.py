"""
RAG Pipeline — Enterprise Document Intelligence
================================================
Full ingestion pipeline:
  1. Scan documents/ folder for new/changed files
  2. Extract text (PDF / DOCX / TXT)
  3. Chunk into 500-char overlapping pieces
  4. Embed with sentence-transformers all-MiniLM-L6-v2 (free, local)
  5. Upsert vectors into Pinecone
  6. Delete vectors for removed documents
  7. Update hash store

Run:
  Manual:  python src/pipeline/rag_pipeline.py
  Airflow: called by document_ingestion_dag.py
"""

import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# ── Load .env ─────────────────────────────────────────────────────────────────
_this_file = Path(os.path.abspath(__file__))
for _parent in [_this_file.parent, _this_file.parent.parent,
                _this_file.parent.parent.parent]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.document_processor import process_document
from ingestion.chunker             import chunk_text, prepare_chunks_for_embedding
from ingestion.hash_tracker        import (
    detect_changes, update_hash_store, scan_documents_folder
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PINECONE_API_KEY   = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX     = os.getenv("PINECONE_INDEX_NAME", "doc-intelligence")
DOCUMENTS_FOLDER   = os.getenv("DOCUMENTS_FOLDER",   "documents")
HASH_STORE_PATH    = os.getenv("HASH_STORE_PATH",     "hash_store.json")
EMBEDDING_MODEL    = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM      = 384
BATCH_SIZE         = 50   # vectors per Pinecone upsert call
SEPARATOR          = "=" * 55


def log_section(title: str):
    logger.info(SEPARATOR)
    logger.info(f"  {title}")
    logger.info(SEPARATOR)


# ── Step 1: Embedding ─────────────────────────────────────────────────────────

def get_embeddings(texts: list) -> list:
    """
    Generate embeddings locally using sentence-transformers.
    Free, no API key, runs on CPU.
    Model: all-MiniLM-L6-v2 — 384 dimensions.
    """
    from sentence_transformers import SentenceTransformer

    model      = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = model.encode(texts, show_progress_bar=False)
    return embeddings.tolist()


# ── Step 2: Pinecone ──────────────────────────────────────────────────────────

def get_pinecone_index():
    """Connect to Pinecone, create index if it doesn't exist."""
    from pinecone import Pinecone, ServerlessSpec

    pc    = Pinecone(api_key=PINECONE_API_KEY)
    names = [idx.name for idx in pc.list_indexes()]

    if PINECONE_INDEX not in names:
        logger.info(f"Creating Pinecone index: {PINECONE_INDEX}")
        pc.create_index(
            name      = PINECONE_INDEX,
            dimension = EMBEDDING_DIM,
            metric    = "cosine",
            spec      = ServerlessSpec(cloud="aws", region="us-east-1")
        )
        time.sleep(30)
        logger.info("Index ready ✓")

    return pc.Index(PINECONE_INDEX)


def upsert_to_pinecone(index, chunks: list):
    """Embed chunks and upsert into Pinecone in batches."""
    if not chunks:
        return

    for i in range(0, len(chunks), BATCH_SIZE):
        batch  = chunks[i:i + BATCH_SIZE]
        texts  = [c["text"] for c in batch]

        logger.info(f"  Embedding batch {i // BATCH_SIZE + 1} "
                    f"({len(batch)} chunks)...")

        embeddings = get_embeddings(texts)

        vectors = [
            {
                "id":     c["id"],
                "values": emb,
                "metadata": {
                    **c["metadata"],
                    "text": c["text"]
                },
            }
            for c, emb in zip(batch, embeddings)
        ]

        index.upsert(vectors=vectors)
        logger.info(f"  ✓ Upserted {len(vectors)} vectors")


def delete_from_pinecone(index, file_path: str):
    """
    Delete all vectors for a given file.
    Pinecone filter delete — removes all chunks from this document.
    """
    try:
        file_name = Path(file_path).name
        # Fetch IDs matching this file then delete
        results = index.query(
            vector    = [0.0] * EMBEDDING_DIM,
            top_k     = 10000,
            filter    = {"file_name": file_name},
            include_metadata = False,
        )
        ids = [m["id"] for m in results.get("matches", [])]
        if ids:
            index.delete(ids=ids)
            logger.info(f"  Deleted {len(ids)} vectors for {file_name}")
    except Exception as e:
        logger.warning(f"  Could not delete vectors for {file_path}: {e}")


# ── Step 3: Process single document ──────────────────────────────────────────

def ingest_document(file_path: str, index) -> int:
    """
    Full ingestion for one document:
      extract → chunk → embed → upsert

    Returns number of chunks ingested, 0 on failure.
    """
    logger.info(f"  Ingesting: {Path(file_path).name}")

    # Extract text
    doc = process_document(file_path)
    if not doc:
        logger.warning(f"  Skipping — could not extract text")
        return 0

    # Chunk
    chunks = chunk_text(doc["text"])
    if not chunks:
        logger.warning(f"  Skipping — no chunks produced")
        return 0

    logger.info(f"  {doc['char_count']:,} chars → {len(chunks)} chunks")

    # Prepare with metadata
    prepared = prepare_chunks_for_embedding(doc, chunks)

    # Delete old vectors first (handles document updates)
    delete_from_pinecone(index, file_path)

    # Upsert new vectors
    upsert_to_pinecone(index, prepared)

    return len(prepared)


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(**kwargs):
    """
    Full pipeline entry point.
    Callable from both CLI and Airflow PythonOperator.
    """
    log_section("ENTERPRISE DOCUMENT INTELLIGENCE PIPELINE")

    if not PINECONE_API_KEY:
        raise ValueError("PINECONE_API_KEY not set in .env")

    # ── Detect changes ────────────────────────────────────────────────────────
    log_section("DETECTING DOCUMENT CHANGES")
    new_files, changed_files, deleted_files = detect_changes(
        DOCUMENTS_FOLDER, HASH_STORE_PATH
    )

    files_to_process = new_files + changed_files
    logger.info(f"  To ingest : {len(files_to_process)} files")
    logger.info(f"  To delete : {len(deleted_files)} files")

    if not files_to_process and not deleted_files:
        logger.info("  No changes detected — nothing to do ✓")
        return {"ingested": 0, "deleted": 0, "chunks": 0}

    # ── Connect to Pinecone ───────────────────────────────────────────────────
    log_section("CONNECTING TO PINECONE")
    index = get_pinecone_index()

    # ── Delete removed documents ──────────────────────────────────────────────
    if deleted_files:
        log_section("DELETING REMOVED DOCUMENTS")
        for file_path in deleted_files:
            delete_from_pinecone(index, file_path)

    # ── Ingest new + changed documents ───────────────────────────────────────
    if files_to_process:
        log_section("INGESTING DOCUMENTS")

    total_chunks = 0
    ingested     = 0
    failed       = 0

    for i, file_path in enumerate(files_to_process):
        logger.info(f"  [{i+1}/{len(files_to_process)}] "
                    f"{Path(file_path).name}")
        n = ingest_document(file_path, index)
        if n > 0:
            total_chunks += n
            ingested     += 1
        else:
            failed += 1

    # ── Update hash store ─────────────────────────────────────────────────────
    update_hash_store(DOCUMENTS_FOLDER, HASH_STORE_PATH)

    # ── Summary ───────────────────────────────────────────────────────────────
    stats = index.describe_index_stats()

    log_section("PIPELINE COMPLETE")
    logger.info(f"  Documents ingested : {ingested}")
    logger.info(f"  Documents failed   : {failed}")
    logger.info(f"  Documents deleted  : {len(deleted_files)}")
    logger.info(f"  Chunks added       : {total_chunks}")
    logger.info(f"  Total vectors now  : {stats.total_vector_count}")

    return {
        "ingested": ingested,
        "deleted":  len(deleted_files),
        "chunks":   total_chunks,
        "total_vectors": stats.total_vector_count,
    }


if __name__ == "__main__":
    result = run_pipeline()
    print(result)