"""Integration tests for RSSClient — uses REAL RSS feed requests."""

import pytest

from producer.rss_client import RSSClient
from producer.models import ArticleEvent


class TestRSSClientReal:
    """Tests that hit real RSS feeds."""

    def test_fetch_returns_article_events(self):
        # Use a single reliable feed to keep test fast
        client = RSSClient(
            feeds={"http://feeds.bbci.co.uk/news/technology/rss.xml": "technology"}
        )
        articles = client.fetch()
        assert len(articles) > 0
        assert all(isinstance(a, ArticleEvent) for a in articles)

    def test_article_fields_populated(self):
        client = RSSClient(
            feeds={"http://feeds.bbci.co.uk/news/technology/rss.xml": "technology"}
        )
        articles = client.fetch()
        article = articles[0]
        assert article.article_id.startswith("rss/")
        assert len(article.headline) > 0
        assert article.section == "technology"
        assert str(article.source_url).startswith("http")

    def test_multiple_feeds(self):
        client = RSSClient(
            feeds={
                "http://feeds.bbci.co.uk/news/technology/rss.xml": "technology",
                "http://feeds.bbci.co.uk/news/business/rss.xml": "business",
            }
        )
        articles = client.fetch()
        sections = {a.section for a in articles}
        assert "technology" in sections
        assert "business" in sections

    def test_source_name(self):
        client = RSSClient()
        assert client.source_name == "rss"

    def test_article_ids_are_unique(self):
        client = RSSClient(
            feeds={"http://feeds.bbci.co.uk/news/technology/rss.xml": "technology"}
        )
        articles = client.fetch()
        ids = [a.article_id for a in articles]
        assert len(ids) == len(set(ids)), "Duplicate article IDs found"
