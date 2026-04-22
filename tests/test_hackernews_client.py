"""Integration tests for HackerNewsClient — uses REAL HN API calls."""

import pytest

from producer.hackernews_client import HackerNewsClient
from producer.models import ArticleEvent


class TestHackerNewsClientReal:
    """Tests that hit the real Hacker News Firebase API."""

    def test_fetch_returns_article_events(self):
        client = HackerNewsClient(limit=5)
        articles = client.fetch()
        assert len(articles) > 0
        assert all(isinstance(a, ArticleEvent) for a in articles)

    def test_article_fields_populated(self):
        client = HackerNewsClient(limit=3)
        articles = client.fetch()
        article = articles[0]
        assert article.article_id.startswith("hn/")
        assert len(article.headline) > 0
        assert article.section in {"technology", "business", "science", "world", "money"}
        assert str(article.source_url).startswith("http")

    def test_published_at_is_valid(self):
        client = HackerNewsClient(limit=2)
        articles = client.fetch()
        for article in articles:
            assert article.published_at.year >= 2020

    def test_source_name(self):
        client = HackerNewsClient()
        assert client.source_name == "hackernews"

    def test_limit_respected(self):
        client = HackerNewsClient(limit=3)
        articles = client.fetch()
        assert len(articles) <= 3

    def test_article_ids_are_unique(self):
        client = HackerNewsClient(limit=5)
        articles = client.fetch()
        ids = [a.article_id for a in articles]
        assert len(ids) == len(set(ids)), "Duplicate article IDs found"
