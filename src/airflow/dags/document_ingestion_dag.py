"""
Document Intelligence — Airflow DAG
=====================================
Schedule: every 6 hours
Tasks:
  1. scan_for_changes     detect new/changed/deleted files
  2. ingest_documents     extract → chunk → embed → Pinecone
  3. pipeline_summary     log results

Features:
  - Custom MinIO-style folder sensor checks documents/ before ingesting
  - on_failure_callback logs detailed error on any task failure
  - Skips ingestion cleanly if no changes detected
  - Retries once on failure with 3-minute delay
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Add scripts folder to Python path ─────────────────────────────────────────
# All ingestion scripts are copied here so Airflow can import them
SCRIPTS_DIR = "/opt/airflow/dags/scripts"
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

logger = logging.getLogger(__name__)

DOCUMENTS_FOLDER = os.getenv("DOCUMENTS_FOLDER", "/opt/airflow/documents")
HASH_STORE_PATH  = os.getenv("HASH_STORE_PATH",  "/opt/airflow/hash_store.json")


# ── Failure callback ───────────────────────────────────────────────────────────

def on_task_failure(context):
    """
    Called automatically when any task fails.
    Logs a clear summary of what failed and why.
    In production you would send a Slack/email alert here.
    """
    task_id   = context["task_instance"].task_id
    dag_id    = context["dag"].dag_id
    error     = context.get("exception", "Unknown error")
    exec_date = context["execution_date"]

    logger.error("=" * 55)
    logger.error("  TASK FAILURE ALERT")
    logger.error("=" * 55)
    logger.error(f"  DAG      : {dag_id}")
    logger.error(f"  Task     : {task_id}")
    logger.error(f"  Run date : {exec_date}")
    logger.error(f"  Error    : {error}")
    logger.error("=" * 55)
    logger.error("  Action: Check task logs in Airflow UI")
    logger.error(f"  Path:   http://localhost:8080/log?"
                 f"dag_id={dag_id}&task_id={task_id}")
    logger.error("=" * 55)


# ── Task 1: Scan for changes ──────────────────────────────────────────────────

def scan_for_changes(**context):
    """
    Scan the documents/ folder and detect what changed.
    Pushes results to XCom so task 2 knows what to process.

    Uses MD5 hash comparison:
      - New file    → hash not in store
      - Changed file → hash different from stored
      - Deleted file → was in store but file no longer exists
    """
    from hash_tracker import detect_changes

    logger.info("=" * 55)
    logger.info("  TASK 1: Scanning for document changes")
    logger.info("=" * 55)
    logger.info(f"  Watching: {DOCUMENTS_FOLDER}")

    # Check folder exists
    if not os.path.exists(DOCUMENTS_FOLDER):
        logger.warning(f"  Documents folder not found: {DOCUMENTS_FOLDER}")
        logger.warning("  Creating empty folder and skipping ingestion")
        os.makedirs(DOCUMENTS_FOLDER, exist_ok=True)
        context["ti"].xcom_push(key="files_to_process", value=[])
        context["ti"].xcom_push(key="deleted_files",    value=[])
        context["ti"].xcom_push(key="has_changes",      value=False)
        return

    new_files, changed_files, deleted_files = detect_changes(
        DOCUMENTS_FOLDER, HASH_STORE_PATH
    )

    files_to_process = new_files + changed_files
    has_changes      = bool(files_to_process or deleted_files)

    logger.info(f"  New files     : {len(new_files)}")
    logger.info(f"  Changed files : {len(changed_files)}")
    logger.info(f"  Deleted files : {len(deleted_files)}")
    logger.info(f"  Has changes   : {has_changes}")

    if not has_changes:
        logger.info("  No changes detected — ingestion will be skipped")
    else:
        for f in files_to_process:
            logger.info(f"  → Will ingest: {Path(f).name}")
        for f in deleted_files:
            logger.info(f"  → Will delete: {Path(f).name}")

    # Push to XCom for task 2
    context["ti"].xcom_push(key="files_to_process", value=files_to_process)
    context["ti"].xcom_push(key="deleted_files",    value=deleted_files)
    context["ti"].xcom_push(key="has_changes",      value=has_changes)


# ── Task 2: Ingest documents ──────────────────────────────────────────────────

def ingest_documents(**context):
    """
    Run the full ingestion pipeline for changed files only.
    Skips cleanly if task 1 found no changes.

    Flow per file:
      extract text → chunk → embed → upsert to Pinecone
    Also deletes vectors for removed files.
    """
    from rag_pipeline import run_pipeline

    has_changes = context["ti"].xcom_pull(
        task_ids="scan_for_changes",
        key="has_changes"
    )

    if not has_changes:
        logger.info("=" * 55)
        logger.info("  TASK 2: No changes — skipping ingestion")
        logger.info("=" * 55)
        return

    logger.info("=" * 55)
    logger.info("  TASK 2: Running ingestion pipeline")
    logger.info("=" * 55)

    result = run_pipeline()

    logger.info(f"  Documents ingested : {result.get('ingested', 0)}")
    logger.info(f"  Documents deleted  : {result.get('deleted', 0)}")
    logger.info(f"  Chunks added       : {result.get('chunks', 0)}")
    logger.info(f"  Total vectors now  : {result.get('total_vectors', 0)}")

    # Push results to XCom for summary task
    context["ti"].xcom_push(key="ingestion_result", value=result)

    logger.info("  ✓ Ingestion complete")


# ── Task 3: Pipeline summary ──────────────────────────────────────────────────

def pipeline_summary(**context):
    """
    Log a clean summary of the full DAG run.
    Shows what was processed and current Pinecone state.
    """
    import os
    from pinecone import Pinecone

    has_changes = context["ti"].xcom_pull(
        task_ids="scan_for_changes",
        key="has_changes"
    )

    result = context["ti"].xcom_pull(
        task_ids="ingest_documents",
        key="ingestion_result"
    ) or {}

    # Get live Pinecone stats
    try:
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "doc-intelligence"))
        stats = index.describe_index_stats()
        total_vectors = stats.total_vector_count
    except Exception as e:
        total_vectors = f"Error fetching: {e}"

    logger.info("=" * 55)
    logger.info("  PIPELINE COMPLETE ✅")
    logger.info("=" * 55)
    logger.info(f"  Changes detected   : {has_changes}")
    logger.info(f"  Documents ingested : {result.get('ingested', 0)}")
    logger.info(f"  Documents deleted  : {result.get('deleted', 0)}")
    logger.info(f"  Chunks added       : {result.get('chunks', 0)}")
    logger.info(f"  Total vectors now  : {total_vectors}")
    logger.info(f"  Index name         : "
                f"{os.getenv('PINECONE_INDEX_NAME', 'doc-intelligence')}")
    logger.info("=" * 55)
    logger.info("  Next steps:")
    logger.info("  → Add documents to documents/ folder to ingest more")
    logger.info("  → Ask questions at http://localhost:8501")
    logger.info("=" * 55)


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "Atul",
    "depends_on_past":  False,
    "email_on_failure": False,   # set to True + add email to get email alerts
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "on_failure_callback": on_task_failure,  # ← fires on any task failure
}

with DAG(
    dag_id="document_intelligence_pipeline",
    default_args=default_args,
    description="Watch documents/ folder and ingest new/changed files into Pinecone",
    schedule_interval="0 */6 * * *",  # every 6 hours — adjust as needed
    start_date=datetime(2025, 9, 15),
    catchup=False,
    max_active_runs=1,
    tags=["rag", "documents", "pinecone"],
) as dag:

    t1_scan = PythonOperator(
        task_id="scan_for_changes",
        python_callable=scan_for_changes,
    )

    t2_ingest = PythonOperator(
        task_id="ingest_documents",
        python_callable=ingest_documents,
        execution_timeout=timedelta(minutes=30),
    )

    t3_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=pipeline_summary,
    )

    # ── Flow ──────────────────────────────────────────────────────────────────
    t1_scan >> t2_ingest >> t3_summary