"""Pipeline Metrics view — ingestion stats, sentiment distribution, domain tags."""

import logging
from datetime import datetime, timedelta, timezone

import streamlit as st
import weaviate
from weaviate.classes.query import Filter

from dashboard.config import DashboardConfig
from dashboard.weaviate_helper import count_articles, aggregate_by_property

logger = logging.getLogger(__name__)

# Chart colors
SENTIMENT_COLORS = {"Positive": "#28a745", "Negative": "#dc3545", "Neutral": "#6c757d"}
TAG_COLORS = {
    "AI": "#667eea", "Crypto": "#f6ad55", "Regulation": "#fc8181",
    "Geopolitics": "#68d391", "Earnings": "#63b3ed", "Climate": "#48bb78",
    "Cybersecurity": "#e53e3e", "Other": "#a0aec0",
}


def _count_recent(
    client: weaviate.WeaviateClient, collection_name: str, hours: int,
) -> int:
    """Count articles ingested in the last N hours."""
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    collection = client.collections.get(collection_name)
    result = collection.aggregate.over_all(
        filters=Filter.by_property("published_at").greater_than(since),
        total_count=True,
    )
    return result.total_count or 0


def render(client: weaviate.WeaviateClient, config: DashboardConfig):
    """Render the Pipeline Metrics page."""
    st.header("\U0001f4ca Pipeline Metrics")

    # Top-level KPI row
    total = count_articles(client, config.collection_name)
    last_24h = _count_recent(client, config.collection_name, 24)
    last_1h = _count_recent(client, config.collection_name, 1)

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("Total Articles", total)
    kpi2.metric("Last 24 Hours", last_24h)
    kpi3.metric("Last Hour", last_1h)

    st.markdown("---")

    # Two-column layout for charts
    col_left, col_right = st.columns(2)

    # Sentiment distribution
    with col_left:
        st.subheader("Sentiment Distribution")
        sentiment_counts = aggregate_by_property(client, config.collection_name, "sentiment")
        if sentiment_counts:
            import pandas as pd
            df_sent = pd.DataFrame(
                [{"Sentiment": k, "Count": v} for k, v in sentiment_counts.items()]
            )
            st.bar_chart(df_sent.set_index("Sentiment"))
        else:
            st.info("No sentiment data yet.")

    # Domain tag breakdown
    with col_right:
        st.subheader("Domain Tag Breakdown")
        tag_counts = aggregate_by_property(client, config.collection_name, "domain_tag")
        if tag_counts:
            import pandas as pd
            df_tags = pd.DataFrame(
                [{"Tag": k, "Count": v} for k, v in tag_counts.items()]
            )
            st.bar_chart(df_tags.set_index("Tag"))
        else:
            st.info("No domain tag data yet.")

    st.markdown("---")

    # Section breakdown
    st.subheader("Articles by Section")
    section_counts = aggregate_by_property(client, config.collection_name, "section")
    if section_counts:
        import pandas as pd
        df_sec = pd.DataFrame(
            [{"Section": k, "Count": v} for k, v in section_counts.items()]
        )
        df_sec = df_sec.sort_values("Count", ascending=False)
        st.bar_chart(df_sec.set_index("Section"))

    # Kafka topic stats (read-only, best-effort)
    st.markdown("---")
    st.subheader("\U0001f4e1 Pipeline Health")
    st.caption("Check service UIs for deeper stats:")
    link_cols = st.columns(4)
    with link_cols[0]:
        st.markdown("[\U0001f4e8 Kafka UI](http://localhost:8080)")
    with link_cols[1]:
        st.markdown("[\u2699\ufe0f Flink Dashboard](http://localhost:8081)")
    with link_cols[2]:
        st.markdown("[\U0001f552 Airflow](http://localhost:8082)")
    with link_cols[3]:
        st.markdown("[\U0001f50d Marquez Lineage](http://localhost:3000)")
