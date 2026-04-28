"""Unit tests for quality_gate.gate_worker.QualityGateWorker."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from quality_gate.config import QualityGateConfig
from quality_gate.expectations import ArticleExpectationSuite
from quality_gate.models import EMBEDDING_DIMENSION


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
def expectations(config):
    return ArticleExpectationSuite(config)


@pytest.fixture
def mock_embedder():
    embedder = MagicMock()
    embedder.embed.return_value = [0.1] * EMBEDDING_DIMENSION
    return embedder


def _valid_enriched(**overrides) -> dict:
    base = {
        "article_id": "technology/2024/jan/15/test-article",
        "headline": "EU proposes comprehensive AI regulation framework",
        "body": "Full article body text with enough words to be meaningful.",
        "section": "technology",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "source_url": "https://example.com/eu-ai-regulation",
        "summary": (
            "The EU has proposed a sweeping AI regulation framework "
            "that would affect major technology companies worldwide. "
            "Analysts expect the regulation to pass by mid-2025."
        ),
        "entities": ["European Commission", "Microsoft", "United States"],
        "sentiment": "Neutral",
        "sentiment_reason": "The article presents regulatory developments objectively.",
        "domain_tag": "Regulation",
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


class TestGateWorkerProcess:
    """Tests for QualityGateWorker.process_article (mocked Kafka + embedder)."""

    def _make_worker(self, config, expectations, mock_embedder):
        with patch("quality_gate.gate_worker.KafkaConsumer"), \
             patch("quality_gate.gate_worker.KafkaProducer"):
            from quality_gate.gate_worker import QualityGateWorker
            return QualityGateWorker(config, expectations, mock_embedder)

    def test_process_article_success(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        result = worker.process_article(_valid_enriched())
        assert result["article_id"] == "technology/2024/jan/15/test-article"
        assert "embedding" in result
        assert "validated_at" in result

    def test_embedding_attached(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        result = worker.process_article(_valid_enriched())
        assert len(result["embedding"]) == EMBEDDING_DIMENSION

    def test_validated_at_set(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        result = worker.process_article(_valid_enriched())
        assert result["validated_at"] is not None
        # Should be a valid ISO timestamp
        datetime.fromisoformat(result["validated_at"])

    def test_gx_failure_raises(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        with pytest.raises(ValueError, match="GX validation failed"):
            worker.process_article(_valid_enriched(summary="short"))

    def test_stale_article_rejected(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        stale_dt = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        with pytest.raises(ValueError, match="recency"):
            worker.process_article(_valid_enriched(published_at=stale_dt))

    def test_invalid_sentiment_rejected(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        with pytest.raises(ValueError, match="sentiment"):
            worker.process_article(_valid_enriched(sentiment="Unknown"))

    def test_missing_article_id_rejected(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        with pytest.raises(ValueError, match="missing required field"):
            worker.process_article(_valid_enriched(article_id=""))

    def test_dead_letter_includes_stage(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        event = _valid_enriched()
        worker._send_to_dead_letter(event, "test error")
        assert event["_failed_stage"] == "quality-gate"

    def test_dead_letter_includes_error(self, config, expectations, mock_embedder):
        worker = self._make_worker(config, expectations, mock_embedder)
        event = _valid_enriched()
        worker._send_to_dead_letter(event, "something broke")
        assert event["_error"] == "something broke"
