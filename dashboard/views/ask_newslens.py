"""Ask NewsLens view — semantic search + RAG-powered Q&A over news articles."""

import logging

import streamlit as st
import weaviate
from sentence_transformers import SentenceTransformer

from dashboard.config import DashboardConfig
from dashboard.weaviate_helper import search_by_vector

logger = logging.getLogger(__name__)

# Cache the embedding model across reruns
@st.cache_resource
def _load_embedding_model(model_name: str) -> SentenceTransformer:
    return SentenceTransformer(model_name)


def _build_rag_context(articles: list[dict]) -> str:
    """Format retrieved articles into a context block for the LLM."""
    parts = []
    for i, a in enumerate(articles, 1):
        parts.append(
            f"[{i}] {a.get('headline', 'Untitled')}\n"
            f"    Summary: {a.get('summary', 'N/A')}\n"
            f"    Sentiment: {a.get('sentiment', 'N/A')} | Tag: {a.get('domain_tag', 'N/A')}\n"
            f"    Entities: {', '.join(a.get('entities', []))}\n"
            f"    Source: {a.get('source_url', 'N/A')}"
        )
    return "\n\n".join(parts)


def _ask_ollama(question: str, context: str, config: DashboardConfig) -> str:
    """Send a RAG prompt to Ollama and return the response."""
    import requests

    prompt = (
        "You are NewsLens, an AI news analyst. Answer the user's question "
        "based ONLY on the following news articles. Cite sources using [1], [2], etc. "
        "If the articles don't contain enough information, say so.\n\n"
        f"=== NEWS ARTICLES ===\n{context}\n\n"
        f"=== QUESTION ===\n{question}\n\n"
        "=== ANSWER ==="
    )

    try:
        resp = requests.post(
            f"{config.ollama_base_url}/api/generate",
            json={
                "model": config.ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2},
            },
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "No response from LLM.")
    except requests.exceptions.ConnectionError:
        return "Could not connect to Ollama. Make sure the LLM service is running."
    except Exception as e:
        return f"LLM error: {e}"


def render(client: weaviate.WeaviateClient, config: DashboardConfig):
    """Render the Ask NewsLens Q&A page."""
    st.header("\U0001f50d Ask NewsLens")
    st.caption("Ask a question — I'll search the news and synthesize an answer.")

    question = st.text_input(
        "Your question",
        placeholder="e.g. What are the latest developments in AI regulation?",
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        top_k = st.slider("Number of sources", min_value=1, max_value=10, value=5)
    with col2:
        use_llm = st.checkbox("Generate AI answer", value=True)

    if not question:
        st.info("Type a question above to search the news articles.")
        return

    with st.spinner("Embedding your query..."):
        model = _load_embedding_model(config.embedding_model)
        query_vector = model.encode(question).tolist()

    with st.spinner("Searching Weaviate..."):
        results = search_by_vector(client, config.collection_name, query_vector, limit=top_k)

    if not results:
        st.warning("No matching articles found. Try a different question.")
        return

    # Show retrieved articles
    st.subheader(f"\U0001f4da {len(results)} Relevant Articles")
    for i, article in enumerate(results, 1):
        distance = article.get("distance", 0)
        similarity = max(0, 1 - distance)
        headline = article.get("headline", "Untitled")
        summary = article.get("summary", "")
        source_url = article.get("source_url", "")
        sentiment = article.get("sentiment", "")
        tag = article.get("domain_tag", "")

        with st.expander(f"[{i}] {headline} ({similarity:.0%} match)", expanded=(i == 1)):
            st.markdown(f"**Summary:** {summary}")
            cols = st.columns(3)
            with cols[0]:
                st.caption(f"Sentiment: {sentiment}")
            with cols[1]:
                st.caption(f"Tag: {tag}")
            with cols[2]:
                if source_url:
                    st.markdown(f"[\U0001f517 Source]({source_url})")

    # RAG answer
    if use_llm:
        st.subheader("\U0001f916 AI Answer")
        context = _build_rag_context(results)
        with st.spinner("Generating answer with Ollama..."):
            answer = _ask_ollama(question, context, config)
        st.markdown(answer)
