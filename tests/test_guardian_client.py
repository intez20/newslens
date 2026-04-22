"""Integration tests for GuardianClient — uses REAL Guardian API calls.

Requires GUARDIAN_API_KEY env var or .env file.
"""

import os
import sys

import pytest
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from producer.guardian_client import GuardianClient
from producer.models import ArticleEvent


# Skip all tests if no API key available
pytestmark = pytest.mark.skipif(
    not os.getenv("GUARDIAN_API_KEY"),
    reason="GUARDIAN_API_KEY not set",
)


class TestGuardianClientReal:
    """Tests that hit the real Guardian API."""

    def test_fetch_returns_article_events(self):
        client = GuardianClient(sections=("technology",), page_size=5)
        articles = client.fetch()
        assert len(articles) > 0
        assert all(isinstance(a, ArticleEvent) for a in articles)

    def test_article_fields_populated(self):
        client = GuardianClient(sections=("technology",), page_size=2)
        articles = client.fetch()
        article = articles[0]
        assert article.article_id.startswith("technology/")
        assert len(article.headline) > 0
        assert len(article.body) > 0
        assert article.section == "technology"
        assert "theguardian.com" in str(article.source_url)

    def test_multiple_sections(self):
        client = GuardianClient(
            sections=("technology", "science"), page_size=2
        )
        articles = client.fetch()
        sections = {a.section for a in articles}
        assert "technology" in sections
        assert "science" in sections

    def test_serialization_roundtrip(self):
        client = GuardianClient(sections=("business",), page_size=1)
        articles = client.fetch()
        article = articles[0]
        dumped = article.model_dump()
        restored = ArticleEvent(**dumped)
        assert restored.article_id == article.article_id
        assert restored.headline == article.headline

    def test_source_name(self):
        client = GuardianClient(sections=("technology",))
        assert client.source_name == "guardian"

    def test_invalid_key_raises(self):
        with pytest.raises(ValueError, match="GUARDIAN_API_KEY"):
            GuardianClient(api_key="", sections=("technology",))
