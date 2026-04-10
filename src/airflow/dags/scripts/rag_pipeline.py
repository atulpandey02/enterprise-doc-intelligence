"""
RAG Pipeline — Enterprise Document Intelligence (Airflow/Docker version)
Flat imports — all files in same scripts/ folder inside Docker
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
                _this_file.parent.parent.parent, Path("/opt/airflow")]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

# ── Add scripts dir to path for flat imports ──────────────────────────────────
_scripts_dir = str(Path(__file__).parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

# ── Imports — flat (Docker) with fallback to nested (local) ───────────────────
try:
    from document_processor import process_document
    from chunker             import chunk_text, prepare_chunks_for_embedding
    from hash_tracker        import detect_changes, update_hash_store, scan_documents_folder
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from ingestion.document_processor import process_document
    from ingestion.chunker             import chunk_text, prepare_chunks_for_embedding
    from ingestion.hash_tracker        import detect_changes, update_hash_store, scan_documents_folder

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX   = os.getenv("PINECONE_INDEX_NAME", "doc-intelligence")
DOCUMENTS_FOLDER = os.getenv("DOCUMENTS_FOLDER",   "documents")
HASH_STORE_PATH  = os.getenv("HASH_STORE_PATH",     "hash_store.json")
EMBEDDING_MODEL  = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM    = 384
BATCH_SIZE       = 50
SEPARATOR        = "=" * 55


def log_section(title):
    logger.info(SEPARATOR)
    logger.info(f"  {title}")
    logger.info(SEPARATOR)


def get_embeddings(texts):
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBEDDING_MODEL)
    return model.encode(texts, show_progress_bar=False).tolist()


def get_pinecone_index():
    from pinecone import Pinecone, ServerlessSpec
    pc    = Pinecone(api_key=PINECONE_API_KEY)
    names = [idx.name for idx in pc.list_indexes()]
    if PINECONE_INDEX not in names:
        logger.info(f"Creating Pinecone index: {PINECONE_INDEX}")
        pc.create_index(
            name=PINECONE_INDEX, dimension=EMBEDDING_DIM, metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1")
        )
        time.sleep(30)
    return pc.Index(PINECONE_INDEX)


def upsert_to_pinecone(index, chunks):
    if not chunks:
        return
    for i in range(0, len(chunks), BATCH_SIZE):
        batch      = chunks[i:i + BATCH_SIZE]
        texts      = [c["text"] for c in batch]
        embeddings = get_embeddings(texts)
        vectors    = [
            {
                "id":     c["id"],
                "values": emb,
                "metadata": {**c["metadata"], "text": c["text"]},
            }
            for c, emb in zip(batch, embeddings)
        ]
        index.upsert(vectors=vectors)
        logger.info(f"  ✓ Upserted {len(vectors)} vectors")


def delete_from_pinecone(index, file_path):
    try:
        file_name = Path(file_path).name
        results   = index.query(
            vector=[0.0] * EMBEDDING_DIM, top_k=10000,
            filter={"file_name": {"$eq": file_name}},
            include_metadata=False,
        )
        ids = [m["id"] for m in results.get("matches", [])]
        if ids:
            index.delete(ids=ids)
            logger.info(f"  Deleted {len(ids)} vectors for {file_name}")
    except Exception as e:
        logger.warning(f"  Could not delete vectors: {e}")


def ingest_document(file_path, index):
    logger.info(f"  Ingesting: {Path(file_path).name}")
    doc = process_document(file_path)
    if not doc:
        return 0
    chunks = chunk_text(doc["text"])
    if not chunks:
        return 0
    logger.info(f"  {doc['char_count']:,} chars → {len(chunks)} chunks")
    prepared = prepare_chunks_for_embedding(doc, chunks)
    delete_from_pinecone(index, file_path)
    upsert_to_pinecone(index, prepared)
    return len(prepared)


def run_pipeline(**kwargs):
    log_section("ENTERPRISE DOCUMENT INTELLIGENCE PIPELINE")
    if not PINECONE_API_KEY:
        raise ValueError("PINECONE_API_KEY not set")

    log_section("DETECTING DOCUMENT CHANGES")
    new_files, changed_files, deleted_files = detect_changes(DOCUMENTS_FOLDER, HASH_STORE_PATH)
    files_to_process = new_files + changed_files

    logger.info(f"  To ingest : {len(files_to_process)} files")
    logger.info(f"  To delete : {len(deleted_files)} files")

    if not files_to_process and not deleted_files:
        logger.info("  No changes detected — nothing to do ✓")
        return {"ingested": 0, "deleted": 0, "chunks": 0, "total_vectors": 0}

    log_section("CONNECTING TO PINECONE")
    index = get_pinecone_index()

    if deleted_files:
        log_section("DELETING REMOVED DOCUMENTS")
        for f in deleted_files:
            delete_from_pinecone(index, f)

    total_chunks = ingested = failed = 0
    if files_to_process:
        log_section("INGESTING DOCUMENTS")
    for i, file_path in enumerate(files_to_process):
        logger.info(f"  [{i+1}/{len(files_to_process)}] {Path(file_path).name}")
        n = ingest_document(file_path, index)
        if n > 0:
            total_chunks += n
            ingested     += 1
        else:
            failed += 1

    update_hash_store(DOCUMENTS_FOLDER, HASH_STORE_PATH)

    stats = index.describe_index_stats()
    log_section("PIPELINE COMPLETE")
    logger.info(f"  Documents ingested : {ingested}")
    logger.info(f"  Documents failed   : {failed}")
    logger.info(f"  Documents deleted  : {len(deleted_files)}")
    logger.info(f"  Chunks added       : {total_chunks}")
    logger.info(f"  Total vectors now  : {stats.total_vector_count}")

    return {
        "ingested": ingested, "deleted": len(deleted_files),
        "chunks": total_chunks, "total_vectors": stats.total_vector_count,
    }


if __name__ == "__main__":
    print(run_pipeline())
