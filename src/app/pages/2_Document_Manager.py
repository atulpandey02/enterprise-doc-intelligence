"""
Page 2 — Document Manager
Pure Python Streamlit — no HTML
"""

import os
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

_this_file = Path(os.path.abspath(__file__))
for _parent in [_this_file.parent, _this_file.parent.parent,
                _this_file.parent.parent.parent,
                _this_file.parent.parent.parent.parent]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

st.set_page_config(
    page_title="DocIntel — Manager",
    page_icon="📂",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── Data fetching ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def get_index_stats():
    try:
        from pinecone import Pinecone
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "doc-intelligence"))
        stats = index.describe_index_stats()
        return {
            "total_vectors": stats.total_vector_count or 0,
            "dimension":     stats.dimension or 384,
            "status":        "Connected ✓",
        }
    except Exception as e:
        return {"total_vectors": 0, "dimension": 384, "status": f"Error: {e}"}


@st.cache_data(ttl=30)
def get_document_breakdown():
    try:
        from pinecone import Pinecone
        from sentence_transformers import SentenceTransformer
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "doc-intelligence"))
        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        vec   = model.encode(["document"])[0].tolist()
        results = index.query(vector=vec, top_k=10000, include_metadata=True)
        doc_chunks = {}
        for m in results.get("matches", []):
            meta      = m.get("metadata", {})
            file_name = meta.get("file_name", "Unknown")
            file_type = meta.get("file_type", "")
            if file_name not in doc_chunks:
                doc_chunks[file_name] = {"chunks": 0, "type": file_type}
            doc_chunks[file_name]["chunks"] += 1
        return doc_chunks
    except Exception:
        return {}


def get_local_files():
    documents_folder = os.getenv("DOCUMENTS_FOLDER", "documents")
    if not os.path.exists(documents_folder):
        return [], documents_folder
    files = [
        f for f in Path(documents_folder).rglob("*")
        if f.is_file() and f.suffix.lower() in {".pdf", ".docx", ".txt"}
    ]
    return sorted(files), documents_folder


# ── Fetch data ────────────────────────────────────────────────────────────────

stats         = get_index_stats()
docs          = get_document_breakdown()
files, folder = get_local_files()


# ── Header ────────────────────────────────────────────────────────────────────

st.title("📂 Document Manager")
st.caption("Pinecone index stats · Ingested documents · Local file watcher")
st.divider()


# ── KPI row ───────────────────────────────────────────────────────────────────

k1, k2, k3, k4 = st.columns(4)
k1.metric("Vectors Indexed",   f"{stats['total_vectors']:,}")
k2.metric("Documents in Index", f"{len(docs)}")
k3.metric("Local Files",        f"{len(files)}")
k4.metric("Index Status",       stats["status"])

st.divider()


# ── Indexed documents ─────────────────────────────────────────────────────────

st.subheader("📋 Indexed Documents")

if docs:
    import pandas as pd
    rows = [
        {
            "Document":     name,
            "Type":         info["type"].upper(),
            "Chunks":       info["chunks"],
        }
        for name, info in sorted(docs.items())
    ]
    df = pd.DataFrame(rows)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Chunks": st.column_config.ProgressColumn(
                "Chunks",
                min_value=0,
                max_value=max(r["Chunks"] for r in rows),
                format="%d",
            )
        }
    )
else:
    st.warning(
        "No documents indexed yet.\n\n"
        "Drop files into the `documents/` folder and run the pipeline."
    )

st.divider()


# ── Local documents folder ────────────────────────────────────────────────────

st.subheader("📁 Local Documents Folder")
st.caption(f"Watching: `{folder}`")

if files:
    for f in files:
        col_name, col_size, col_type = st.columns([4, 1, 1])
        col_name.write(f"📄 {f.name}")
        col_size.caption(f"{f.stat().st_size / 1024:.1f} KB")
        col_type.caption(f.suffix.upper().lstrip("."))
    st.caption(f"{len(files)} file(s) found")
else:
    st.info(
        f"No supported files found in `{folder}`\n\n"
        "Drop PDF, Word (.docx), or text (.txt) files here."
    )

st.divider()


# ── Actions ───────────────────────────────────────────────────────────────────

st.subheader("⚙️ Actions")
st.caption("Run the ingestion pipeline manually without Airflow")

col_run, col_refresh, col_empty = st.columns([1, 1, 2])

with col_run:
    if st.button("▶ Run Pipeline Now", use_container_width=True):
        with st.spinner("Running ingestion pipeline..."):
            try:
                import sys
                sys.path.insert(
                    0, str(Path(__file__).parent.parent.parent / "pipeline")
                )
                from rag_pipeline import run_pipeline
                result = run_pipeline()
                st.success(
                    f"✓ Complete\n\n"
                    f"Documents ingested: {result['ingested']}\n\n"
                    f"Chunks added: {result['chunks']}\n\n"
                    f"Total vectors: {result['total_vectors']}"
                )
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Pipeline error: {e}")

with col_refresh:
    if st.button("↺ Refresh Stats", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()


# ── How it works ──────────────────────────────────────────────────────────────

st.subheader("ℹ️ How Change Detection Works")

col_a, col_b = st.columns(2)

with col_a:
    st.markdown("""
**MD5 Hash Tracking**

On every pipeline run:
1. Compute MD5 hash of each file in `documents/`
2. Compare against stored hashes from last run
3. Only re-embed files whose hash changed
4. Delete Pinecone vectors for removed files
5. Save new hashes for next comparison

This means if you have 100 documents and change 1,
only that 1 document gets re-processed.
""")

with col_b:
    st.markdown("""
**Why MD5 and not file modification time?**

File modification time (`mtime`) changes when:
- You copy a file (even if content is identical)
- You move a file to a different folder
- Your OS updates metadata

MD5 hash only changes when:
- The actual content of the file changes

This makes change detection reliable and prevents
unnecessary re-embedding of unchanged documents.
""")