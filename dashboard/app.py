"""NewsLens Dashboard — Streamlit entry point with sidebar navigation."""

import logging
import sys

import streamlit as st
from dotenv import load_dotenv

from dashboard.config import DashboardConfig
from dashboard.weaviate_helper import get_client

load_dotenv()
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

st.set_page_config(
    page_title="NewsLens Dashboard",
    page_icon="\U0001f4f0",
    layout="wide",
    initial_sidebar_state="expanded",
)

PAGES = {
    "\U0001f4f0 Live Feed": "live_feed",
    "\U0001f50d Ask NewsLens": "ask_newslens",
    "\U0001f4ca Pipeline Metrics": "metrics",
}


def main():
    st.sidebar.title("\U0001f4f0 NewsLens")
    st.sidebar.markdown("---")
    page = st.sidebar.radio("Navigate", list(PAGES.keys()), label_visibility="collapsed")

    config = DashboardConfig.from_env()

    try:
        client = get_client(config)
    except Exception as exc:
        st.error(f"Cannot connect to Weaviate: {exc}")
        st.info("Make sure the pipeline is running (`docker compose up -d`).")
        return

    try:
        view_name = PAGES[page]
        if view_name == "live_feed":
            from dashboard.views.live_feed import render
            render(client, config)
        elif view_name == "ask_newslens":
            st.header("\U0001f50d Ask NewsLens")
            st.info("Coming in Stage 9b — semantic search + RAG Q&A")
        elif view_name == "metrics":
            st.header("\U0001f4ca Pipeline Metrics")
            st.info("Coming in Stage 9c — ingestion stats, sentiment charts, pass rates")
    finally:
        client.close()

    # Auto-refresh every 2 minutes on Live Feed
    if PAGES.get(page) == "live_feed":
        st.sidebar.markdown("---")
        if st.sidebar.button("\U0001f504 Refresh Now"):
            st.rerun()


if __name__ == "__main__":
    main()
