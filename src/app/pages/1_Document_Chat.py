"""
Page 1 — Document Chat
Pure Python Streamlit — no HTML
"""

import os
import logging
import requests
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

_this_file = Path(os.path.abspath(__file__))
for _parent in [_this_file.parent, _this_file.parent.parent,
                _this_file.parent.parent.parent,
                _this_file.parent.parent.parent.parent]:
    _env = _parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True)
        break

st.set_page_config(
    page_title="DocIntel — Chat",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def safe_str(val, default="") -> str:
    return str(val) if val is not None else default


# ── Embedding model ───────────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading embedding model...")
def load_embedding_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


# ── Available documents ───────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def get_available_documents() -> list:
    try:
        from pinecone import Pinecone
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
        index = pc.Index(os.getenv("PINECONE_INDEX_NAME", "doc-intelligence"))
        model = load_embedding_model()
        vec   = model.encode(["document"])[0].tolist()
        results = index.query(vector=vec, top_k=100, include_metadata=True)
        names = list({
            m["metadata"].get("file_name", "")
            for m in results.get("matches", [])
            if m.get("metadata", {}).get("file_name")
        })
        return sorted(names)
    except Exception:
        return []


# ── Pinecone retrieval ────────────────────────────────────────────────────────

def retrieve_chunks(question: str, file_filter: str = None, top_k: int = 5) -> list:
    from pinecone import Pinecone

    model     = load_embedding_model()
    query_vec = model.encode([question])[0].tolist()
    pc        = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index     = pc.Index(os.getenv("PINECONE_INDEX_NAME", "doc-intelligence"))

    params = {"vector": query_vec, "top_k": top_k, "include_metadata": True}
    if file_filter and file_filter != "All Documents":
        params["filter"] = {"file_name": {"$eq": file_filter}}

    results = index.query(**params)
    chunks  = []

    for m in results.get("matches", []):
        meta = m.get("metadata") or {}
        text = meta.get("text", "")
        if text:
            chunks.append({
                "text":         text,
                "file_name":    meta.get("file_name",    "Unknown"),
                "file_type":    meta.get("file_type",    ""),
                "chunk_index":  meta.get("chunk_index",  0),
                "total_chunks": meta.get("total_chunks", 0),
                "score":        round(float(m.get("score", 0)), 3),
            })
    return chunks


# ── Groq answer generation ────────────────────────────────────────────────────

def generate_answer(question: str, chunks: list) -> str:
    groq_key = os.getenv("GROQ_API_KEY")
    if not groq_key:
        return "⚠️ GROQ_API_KEY missing — check your .env file."
    if not chunks:
        return "⚠️ No relevant content found. Make sure documents are ingested."

    context = "\n\n---\n\n".join([
        f"[Source: {c['file_name']} | Chunk {c['chunk_index']+1}/{c['total_chunks']}]\n{c['text']}"
        for c in chunks
    ])

    try:
        response = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {groq_key}",
                "Content-Type":  "application/json",
            },
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are a precise document analyst. "
                            "Answer ONLY using the provided document excerpts. "
                            "Always cite the source filename in your answer. "
                            "If the answer is not in the documents, say so clearly. "
                            "Be direct and concise."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Document excerpts:\n{context}\n\n"
                            f"Question: {question}\n\nAnswer:"
                        ),
                    },
                ],
                "temperature": 0.1,
                "max_tokens":  600,
            },
            timeout=30,
        )
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"].strip()
        return f"⚠️ Groq error {response.status_code}: {response.text[:200]}"
    except requests.exceptions.Timeout:
        return "⚠️ Request timed out — try again."
    except Exception as e:
        return f"⚠️ Error: {e}"


# ── Render source cards ───────────────────────────────────────────────────────

def render_sources(chunks: list):
    if not chunks:
        return
    with st.expander(f"📄 {len(chunks)} sources retrieved", expanded=False):
        for i, src in enumerate(chunks, 1):
            col_info, col_score = st.columns([4, 1])
            with col_info:
                st.markdown(f"**[{i}] {src['file_name']}**")
                st.caption(
                    f"{src['file_type'].upper()}  ·  "
                    f"Chunk {src['chunk_index']+1} of {src['total_chunks']}"
                )
                st.text(
                    src["text"][:220] + "..."
                    if len(src["text"]) > 220
                    else src["text"]
                )
            with col_score:
                st.metric("Score", src["score"])
            if i < len(chunks):
                st.divider()


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 📄 DocIntel")
    st.divider()

    st.markdown("**Filter by Document**")
    docs         = ["All Documents"] + get_available_documents()
    selected_doc = st.selectbox("", docs, label_visibility="collapsed")

    st.divider()
    st.markdown("**Quick Questions**")
    quick_questions = [
        "What are the key topics in this document?",
        "What are the main conclusions?",
        "What data or evidence is presented?",
        "What are the main recommendations?",
        "What risks or challenges are mentioned?",
    ]
    for q in quick_questions:
        if st.button(q, key=f"q_{q}", use_container_width=True):
            st.session_state["quick_q"] = q
            st.rerun()

    st.divider()
    st.markdown("**Pipeline Info**")
    st.caption("📰 Source: documents/ folder")
    st.caption("🔢 Embeddings: all-MiniLM-L6-v2")
    st.caption("🗄️ Vector DB: Pinecone")
    st.caption("🧠 LLM: Groq llama-3.1-8b-instant")
    st.caption("📦 Chunks: 500 chars, 50 overlap")

    st.divider()
    if st.button("🗑️ Clear conversation", use_container_width=True):
        st.session_state["messages"] = []
        st.rerun()


# ── Header ────────────────────────────────────────────────────────────────────

st.title("💬 Document Chat")
st.caption(
    f"Asking about: **{selected_doc}**  ·  "
    "Pinecone semantic search  ·  Groq llama-3.1-8b-instant"
)
st.divider()


# ── Session state ─────────────────────────────────────────────────────────────

if "messages" not in st.session_state:
    st.session_state["messages"] = []
if "quick_q" not in st.session_state:
    st.session_state["quick_q"] = None


# ── Chat history ──────────────────────────────────────────────────────────────

for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"],
                         avatar="🧑" if msg["role"] == "user" else "📄"):
        st.write(msg["content"])
        if msg["role"] == "assistant" and msg.get("sources"):
            render_sources(msg["sources"])


# ── Chat input ────────────────────────────────────────────────────────────────

user_input = st.chat_input("Ask anything about your documents...")

if st.session_state.get("quick_q") and not user_input:
    user_input = st.session_state["quick_q"]
    st.session_state["quick_q"] = None


# ── Process input ─────────────────────────────────────────────────────────────

if user_input and user_input.strip():
    question = user_input.strip()

    with st.chat_message("user", avatar="🧑"):
        st.write(question)

    st.session_state["messages"].append({
        "role":    "user",
        "content": question,
    })

    with st.chat_message("assistant", avatar="📄"):
        with st.spinner("Searching documents · Generating answer..."):
            try:
                doc_filter = selected_doc if selected_doc != "All Documents" else None
                chunks     = retrieve_chunks(question, file_filter=doc_filter)
                answer     = generate_answer(question, chunks)
            except Exception as e:
                chunks = []
                answer = f"⚠️ Error: {e}"

        st.write(answer)
        render_sources(chunks)

    st.session_state["messages"].append({
        "role":    "assistant",
        "content": answer,
        "sources": chunks,
    })


# ── Empty state ───────────────────────────────────────────────────────────────

if not st.session_state["messages"]:
    st.info(
        "**Getting started:**\n\n"
        "1. Drop PDF, Word, or text files into the `documents/` folder\n"
        "2. Run the pipeline to ingest them\n"
        "3. Select a document in the sidebar (or ask across all)\n"
        "4. Ask any question below"
    )
    st.markdown("**Try these questions:**")
    c1, c2, c3 = st.columns(3)
    if c1.button("What are the key findings?",   key="ex1", use_container_width=True):
        st.session_state["quick_q"] = "What are the key findings?"
        st.rerun()
    if c2.button("What risks are mentioned?",    key="ex2", use_container_width=True):
        st.session_state["quick_q"] = "What risks are mentioned?"
        st.rerun()
    if c3.button("Summarise the conclusions.",   key="ex3", use_container_width=True):
        st.session_state["quick_q"] = "Summarise the conclusions."
        st.rerun()