"""Live Feed view — displays the latest enriched articles from Weaviate."""

import streamlit as st
import weaviate

from dashboard.config import DashboardConfig
from dashboard.weaviate_helper import fetch_latest_articles, count_articles

# Sentiment → color mapping
SENTIMENT_COLORS = {
    "Positive": "#28a745",
    "Negative": "#dc3545",
    "Neutral": "#6c757d",
}

# Domain tag → emoji mapping
TAG_EMOJI = {
    "AI": "\U0001f916",
    "Crypto": "\U0001f4b0",
    "Regulation": "\u2696\ufe0f",
    "Geopolitics": "\U0001f30d",
    "Earnings": "\U0001f4c8",
    "Climate": "\U0001f30e",
    "Cybersecurity": "\U0001f512",
    "Other": "\U0001f4cc",
}


def render(client: weaviate.WeaviateClient, config: DashboardConfig):
    """Render the Live Feed page."""
    st.header("\U0001f4f0 Live Feed")

    total = count_articles(client, config.collection_name)
    st.caption(f"{total} articles in Weaviate")

    articles = fetch_latest_articles(client, config.collection_name, limit=50)

    if not articles:
        st.info("No articles ingested yet. Wait for the pipeline to process some news.")
        return

    # Filters in sidebar
    all_tags = sorted({a.get("domain_tag", "Other") for a in articles})
    all_sentiments = sorted({a.get("sentiment", "Neutral") for a in articles})

    selected_tags = st.sidebar.multiselect("Filter by Domain Tag", all_tags, default=all_tags)
    selected_sentiments = st.sidebar.multiselect("Filter by Sentiment", all_sentiments, default=all_sentiments)

    filtered = [
        a for a in articles
        if a.get("domain_tag", "Other") in selected_tags
        and a.get("sentiment", "Neutral") in selected_sentiments
    ]

    st.caption(f"Showing {len(filtered)} of {len(articles)} articles")

    for article in filtered:
        _render_article_card(article)


def _render_article_card(article: dict):
    """Render a single article as an expander card."""
    headline = article.get("headline", "Untitled")
    sentiment = article.get("sentiment", "Neutral")
    domain_tag = article.get("domain_tag", "Other")
    section = article.get("section", "")
    published = article.get("published_at", "")
    source_url = article.get("source_url", "")
    summary = article.get("summary", "No summary available.")
    entities = article.get("entities", [])
    sentiment_reason = article.get("sentiment_reason", "")

    color = SENTIMENT_COLORS.get(sentiment, "#6c757d")
    emoji = TAG_EMOJI.get(domain_tag, "\U0001f4cc")

    with st.expander(f"{emoji} **{headline}**", expanded=False):
        cols = st.columns([2, 1, 1])
        with cols[0]:
            st.markdown(f"**Section:** {section}")
        with cols[1]:
            st.markdown(
                f"**Sentiment:** <span style='color:{color};font-weight:bold'>"
                f"{sentiment}</span>",
                unsafe_allow_html=True,
            )
        with cols[2]:
            st.markdown(f"**Tag:** {emoji} {domain_tag}")

        st.markdown(f"**Summary:** {summary}")

        if sentiment_reason:
            st.caption(f"\U0001f4ac {sentiment_reason}")

        if entities:
            entity_chips = " ".join(f"`{e}`" for e in entities)
            st.markdown(f"**Entities:** {entity_chips}")

        meta_cols = st.columns([1, 1])
        with meta_cols[0]:
            if published:
                pub_str = str(published)[:19] if published else ""
                st.caption(f"\U0001f4c5 {pub_str}")
        with meta_cols[1]:
            if source_url:
                st.markdown(f"[\U0001f517 Source]({source_url})")
