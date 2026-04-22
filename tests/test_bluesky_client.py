"""Integration tests for BlueskyClient — uses REAL Bluesky public API calls."""

import pytest

from producer.bluesky_client import BlueskyClient
from producer.models import ArticleEvent


class TestBlueskyClientReal:
    """Tests that hit the real Bluesky getAuthorFeed API."""

    def test_fetch_returns_article_events(self):
        client = BlueskyClient(
            accounts={"theverge.com": "technology"}, limit=3
        )
        articles = client.fetch()
        assert len(articles) > 0
        assert all(isinstance(a, ArticleEvent) for a in articles)

    def test_article_fields_populated(self):
        client = BlueskyClient(
            accounts={"theverge.com": "technology"}, limit=2
        )
        articles = client.fetch()
        article = articles[0]
        assert article.article_id.startswith("bsky/")
        assert len(article.headline) > 0
        assert article.section == "technology"
        assert str(article.source_url).startswith("http")

    def test_multiple_accounts(self):
        client = BlueskyClient(
            accounts={
                "techcrunch.com": "technology",
                "reuters.com": "world",
            },
            limit=2,
        )
        articles = client.fetch()
        sections = {a.section for a in articles}
        assert len(sections) >= 1

    def test_source_name(self):
        client = BlueskyClient()
        assert client.source_name == "bluesky"

    def test_article_ids_are_unique(self):
        client = BlueskyClient(
            accounts={"theverge.com": "technology"}, limit=5
        )
        articles = client.fetch()
        ids = [a.article_id for a in articles]
        assert len(ids) == len(set(ids)), "Duplicate article IDs found"
