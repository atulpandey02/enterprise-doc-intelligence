"""
Enterprise Document Intelligence — Landing Page
Pure Python Streamlit — no HTML
"""
import os
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

_this_file = Path(os.path.abspath(__file__))
for _parent in [_this_file.parent, _this_file.parent.parent,
                _this_file.parent.parent.parent]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

st.set_page_config(
    page_title="DocIntel",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── Pinecone stats ────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def get_stats():
    try:
        from pinecone import Pinecone
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "doc-intelligence"))
        stats = index.describe_index_stats()
        results = index.query(vector=[0.1] * 384, top_k=100, include_metadata=True)
        docs = len({
            m["metadata"].get("file_name", "")
            for m in results.get("matches", [])
            if m.get("metadata", {}).get("file_name")
        })
        return stats.total_vector_count or 0, docs
    except Exception:
        return 0, 0


total_vectors, total_docs = get_stats()

# ── Hero ──────────────────────────────────────────────────────────────────────

st.title("📄 Enterprise Document Intelligence")
st.caption("Drop any document. Ask any question. Get grounded answers with citations.")
st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────

k1, k2, k3 = st.columns(3)
k1.metric("Vectors Indexed",   f"{total_vectors:,}", "Pinecone")
k2.metric("Documents Indexed", f"{total_docs}",      "In index")
k3.metric("Embedding Dim",     "384",                "all-MiniLM-L6-v2")

st.divider()

# ── Feature cards ─────────────────────────────────────────────────────────────

st.subheader("Navigate")

c1, c2, c3 = st.columns(3)

with c1:
    st.markdown("#### 💬 Document Chat")
    st.caption(
        "Ask natural language questions about any ingested document. "
        "Pinecone retrieves the most relevant passages. "
        "Groq Llama3 generates a grounded answer with source citations."
    )
    st.page_link("pages/1_Document_Chat.py", label="Open Chat →", icon="💬")

with c2:
    st.markdown("#### 📂 Document Manager")
    st.caption(
        "View all indexed documents and their chunk counts. "
        "Check Pinecone index stats. "
        "Trigger manual ingestion without Airflow."
    )
    st.page_link("pages/2_Document_Manager.py", label="Open Manager →", icon="📂")

with c3:
    st.markdown("#### ⚙️ How It Works")
    st.caption(
        "1. Drop PDF/Word/TXT into `documents/` folder\n\n"
        "2. Airflow detects changes via MD5 hash comparison\n\n"
        "3. Text extracted → chunked → embedded → stored in Pinecone\n\n"
        "4. Ask a question → semantic search → Groq answers"
    )

st.divider()

# ── Tech stack ────────────────────────────────────────────────────────────────

st.subheader("Tech Stack")

t1, t2, t3, t4, t5, t6 = st.columns(6)
t1.info("**Pinecone**\nVector DB")
t2.info("**Groq Llama3**\nLLM")
t3.info("**sentence-transformers**\nEmbeddings")
t4.info("**Apache Airflow**\nOrchestration")
t5.info("**PyMuPDF**\nPDF Extraction")
t6.info("**Docker**\nInfrastructure")

st.divider()
st.caption(
    "Supports PDF · Word (.docx) · Text (.txt)  |  "
    "Change detection via MD5 hashing  |  "
    "500-char recursive chunking with 50-char overlap"
)