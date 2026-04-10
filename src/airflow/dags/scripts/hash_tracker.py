"""
Hash Tracker
============
Detects when documents change using MD5 file hashing.

How it works:
  On every Airflow run, compute MD5 of each file in watched folder.
  Compare against stored hashes from previous run (saved in hash_store.json).
  Only re-embed files whose hash changed or are new.
  This avoids redundant embedding API calls for unchanged documents.

Why MD5 not mtime (modification time)?
  mtime changes when a file is copied or moved even if content is identical.
  MD5 only changes when the actual content changes.
  More reliable for detecting genuine document updates.
"""

import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

# Where hashes are persisted between Airflow runs
HASH_STORE_PATH = os.getenv("HASH_STORE_PATH", "hash_store.json")

# Supported file extensions
SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".txt"}


def compute_file_hash(file_path: str) -> str:
    """
    Compute MD5 hash of file content.
    Reads in 8KB chunks to handle large files efficiently.
    """
    md5  = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)
    return md5.hexdigest()


def load_hash_store(store_path: str = HASH_STORE_PATH) -> Dict[str, str]:
    """Load previously stored hashes from JSON file."""
    if os.path.exists(store_path):
        try:
            with open(store_path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load hash store: {e} — starting fresh")
    return {}


def save_hash_store(hashes: Dict[str, str], store_path: str = HASH_STORE_PATH):
    """Persist current hashes to JSON file."""
    with open(store_path, "w") as f:
        json.dump(hashes, f, indent=2)
    logger.info(f"Hash store saved: {len(hashes)} files tracked")


def scan_documents_folder(folder_path: str) -> List[str]:
    """
    Scan folder for all supported documents.
    Returns sorted list of absolute file paths.
    """
    folder = Path(folder_path)
    if not folder.exists():
        logger.warning(f"Documents folder not found: {folder_path}")
        return []

    files = [
        str(f.absolute())
        for f in folder.rglob("*")
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    logger.info(f"Found {len(files)} documents in {folder_path}")
    return sorted(files)


def detect_changes(
    folder_path: str,
    store_path:  str = HASH_STORE_PATH,
) -> Tuple[List[str], List[str], List[str]]:
    """
    Compare current folder contents against stored hashes.

    Returns:
        new_files     — files that didn't exist before
        changed_files — files whose content changed
        deleted_files — files that no longer exist
    """
    current_files  = scan_documents_folder(folder_path)
    stored_hashes  = load_hash_store(store_path)

    current_hashes = {}
    new_files      = []
    changed_files  = []

    for file_path in current_files:
        current_hash = compute_file_hash(file_path)
        current_hashes[file_path] = current_hash

        if file_path not in stored_hashes:
            new_files.append(file_path)
            logger.info(f"NEW: {Path(file_path).name}")
        elif stored_hashes[file_path] != current_hash:
            changed_files.append(file_path)
            logger.info(f"CHANGED: {Path(file_path).name}")

    # Detect deletions
    deleted_files = [
        f for f in stored_hashes
        if f not in current_hashes
    ]
    for f in deleted_files:
        logger.info(f"DELETED: {Path(f).name}")

    logger.info(
        f"Change detection: {len(new_files)} new, "
        f"{len(changed_files)} changed, "
        f"{len(deleted_files)} deleted"
    )

    return new_files, changed_files, deleted_files


def update_hash_store(
    folder_path: str,
    store_path:  str = HASH_STORE_PATH,
):
    """
    Recompute and save hashes for all current files.
    Called after successful ingestion to mark files as processed.
    """
    current_files = scan_documents_folder(folder_path)
    hashes = {f: compute_file_hash(f) for f in current_files}
    save_hash_store(hashes, store_path)
    return hashes