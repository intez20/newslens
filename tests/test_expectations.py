"""Unit tests for quality_gate.expectations.ArticleExpectationSuite."""

from datetime import datetime, timedelta, timezone

import pytest

from quality_gate.config import QualityGateConfig
from quality_gate.expectations import ArticleExpectationSuite


@pytest.fixture
def config():
    return QualityGateConfig(
        bootstrap_servers="localhost:9092",
        topic_input="enriched-news",
        topic_output="validated-news",
        topic_failed="news-failed",
        consumer_group_id="quality-gate",
        embedding_model="all-MiniLM-L6-v2",
        embedding_dimension=384,
        max_recency_days=7,
        min_summary_length=50,
        max_summary_length=500,
    )


@pytest.fixture
def suite(config):
    return ArticleExpectationSuite(config)


def _valid_article(**overrides) -> dict:
    base = {
        "summary": (
            "The EU has proposed a sweeping AI regulation framework "
            "that would affect major technology companies worldwide. "
            "Analysts expect the regulation to pass by mid-2025."
        ),
        "sentiment": "Neutral",
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


class TestArticleExpectationSuite:
    """Tests for the GX expectation suite."""

    def test_valid_article_passes(self, suite):
        passed, failures = suite.validate(_valid_article())
        assert passed is True
        assert failures == []

    def test_short_summary_fails(self, suite):
        passed, failures = suite.validate(_valid_article(summary="Too short"))
        assert passed is False
        assert any("summary_length" in f for f in failures)

    def test_long_summary_fails(self, suite):
        passed, failures = suite.validate(_valid_article(summary="A" * 501))
        assert passed is False
        assert any("summary_length" in f for f in failures)

    def test_exact_min_summary_passes(self, suite):
        passed, _ = suite.validate(_valid_article(summary="A" * 50))
        assert passed is True

    def test_exact_max_summary_passes(self, suite):
        passed, _ = suite.validate(_valid_article(summary="A" * 500))
        assert passed is True

    def test_invalid_sentiment_fails(self, suite):
        passed, failures = suite.validate(
            _valid_article(sentiment="Unknown")
        )
        assert passed is False
        assert any("sentiment" in f for f in failures)

    def test_valid_sentiments_pass(self, suite):
        for sentiment in ("Positive", "Negative", "Neutral"):
            passed, _ = suite.validate(_valid_article(sentiment=sentiment))
            assert passed is True

    def test_stale_article_fails(self, suite):
        stale_dt = (
            datetime.now(timezone.utc) - timedelta(days=10)
        ).isoformat()
        passed, failures = suite.validate(
            _valid_article(published_at=stale_dt)
        )
        assert passed is False
        assert any("recency" in f for f in failures)

    def test_recent_article_passes(self, suite):
        recent_dt = (
            datetime.now(timezone.utc) - timedelta(days=3)
        ).isoformat()
        passed, _ = suite.validate(_valid_article(published_at=recent_dt))
        assert passed is True

    def test_multiple_failures_reported(self, suite):
        passed, failures = suite.validate(
            _valid_article(
                summary="short",
                sentiment="Bad",
                published_at=(
                    datetime.now(timezone.utc) - timedelta(days=10)
                ).isoformat(),
            )
        )
        assert passed is False
        assert len(failures) == 3

    def test_missing_published_at_passes_recency(self, suite):
        article = _valid_article()
        del article["published_at"]
        passed, failures = suite.validate(article)
        assert passed is True
        assert not any("recency" in f for f in failures)
