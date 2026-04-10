"""
Text Chunker
============
Splits extracted document text into overlapping chunks
for embedding. Uses recursive character splitting —
tries to break on paragraphs, then sentences, then words.
"""

import logging
from typing import List

logger = logging.getLogger(__name__)

CHUNK_SIZE    = 500   # characters per chunk
CHUNK_OVERLAP = 50    # overlap between consecutive chunks

# Split priority — tries each separator in order
SEPARATORS = ["\n\n", "\n", ". ", "? ", "! ", " ", ""]


def chunk_text(
    text:      str,
    chunk_size:    int = CHUNK_SIZE,
    chunk_overlap: int = CHUNK_OVERLAP,
) -> List[str]:
    """
    Recursively split text into chunks.

    Strategy:
      1. Try to split on double newlines (paragraph breaks)
      2. If chunks still too large, split on single newlines
      3. Then sentences, then words, then characters

    Returns list of non-empty chunks, each <= chunk_size chars.
    """
    if not text or not text.strip():
        return []

    # Short text — return as single chunk
    if len(text) <= chunk_size:
        return [text.strip()]

    chunks = _recursive_split(text, chunk_size, chunk_overlap, SEPARATORS)

    # Filter empty, strip whitespace
    chunks = [c.strip() for c in chunks if c.strip()]

    logger.info(f"Chunked into {len(chunks)} pieces "
                f"(size={chunk_size}, overlap={chunk_overlap})")
    return chunks


def _recursive_split(
    text:      str,
    chunk_size:    int,
    chunk_overlap: int,
    separators:    List[str],
) -> List[str]:
    """Recursively split using the best separator available."""

    if not separators:
        # No separators left — force split by character count
        return _split_by_size(text, chunk_size, chunk_overlap)

    separator = separators[0]
    remaining = separators[1:]

    if separator not in text:
        # Try next separator
        return _recursive_split(text, chunk_size, chunk_overlap, remaining)

    splits = text.split(separator)
    chunks = []
    current = ""

    for split in splits:
        candidate = current + separator + split if current else split

        if len(candidate) <= chunk_size:
            current = candidate
        else:
            if current:
                chunks.append(current)
                # Overlap — keep tail of current chunk for context
                overlap_text = current[-chunk_overlap:] if chunk_overlap else ""
                current = overlap_text + separator + split if overlap_text else split
            else:
                # Single split is too large — recurse
                sub_chunks = _recursive_split(split, chunk_size, chunk_overlap, remaining)
                chunks.extend(sub_chunks)
                current = ""

    if current:
        chunks.append(current)

    return chunks


def _split_by_size(text: str, chunk_size: int, chunk_overlap: int) -> List[str]:
    """Last resort — split by character count with overlap."""
    chunks = []
    start  = 0

    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - chunk_overlap

    return chunks


def prepare_chunks_for_embedding(
    document: dict,
    chunks:   List[str],
) -> List[dict]:
    """
    Attach document metadata to each chunk.
    Each chunk becomes a Pinecone vector with full context.

    Returns list of:
        {
            text:      str  — the chunk text
            metadata:  dict — file_name, file_type, chunk_index,
                              total_chunks, char_count
        }
    """
    import hashlib

    total = len(chunks)
    result = []

    for i, chunk in enumerate(chunks):
        # Unique ID from file path + chunk index
        chunk_id = hashlib.md5(
            f"{document['file_path']}-{i}".encode()
        ).hexdigest()

        result.append({
            "id":   chunk_id,
            "text": chunk,
            "metadata": {
                "file_name":   document["file_name"],
                "file_path":   document["file_path"],
                "file_type":   document["file_type"],
                "chunk_index": i,
                "total_chunks":total,
                "char_count":  len(chunk),
            },
        })

    return result