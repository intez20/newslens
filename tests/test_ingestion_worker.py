"""Unit tests for IngestionWorker."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from weaviate_store.config import WeaviateStoreConfig
from weaviate_store.ingestion_worker import IngestionWorker, WEAVIATE_FIELDS


def _make_config(**overrides) -> WeaviateStoreConfig:
    defaults = {
        "weaviate_url": "http://localhost:8080",
        "weaviate_grpc_url": "localhost:50051",
        "collection_name": "TestArticle",
        "bootstrap_servers": "localhost:29092",
        "topic_input": "validated-news",
        "topic_failed": "news-failed",
        "consumer_group_id": "test-group",
    }
    defaults.update(overrides)
    return WeaviateStoreConfig(**defaults)


def _make_article(**overrides) -> dict:
    base = {
        "article_id": "test/2026/apr/28/sample-article",
        "headline": "Sample headline",
        "body": "Full body text here",
        "summary": "A brief 3-sentence summary of the article.",
        "entities": ["Entity1", "Entity2"],
        "sentiment": "Positive",
        "sentiment_reason": "Good news",
        "domain_tag": "AI",
        "section": "technology",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "source_url": "https://example.com/article",
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "embedding": [0.1] * 384,
    }
    base.update(overrides)
    return base


class TestWeaviateFields:
    """Test the field mapping constant."""

    def test_expected_fields(self):
        assert "article_id" in WEAVIATE_FIELDS
        assert "headline" in WEAVIATE_FIELDS
        assert "summary" in WEAVIATE_FIELDS
        assert "entities" in WEAVIATE_FIELDS
        assert "sentiment" in WEAVIATE_FIELDS
        assert "domain_tag" in WEAVIATE_FIELDS
        assert "section" in WEAVIATE_FIELDS
        assert "published_at" in WEAVIATE_FIELDS
        assert "source_url" in WEAVIATE_FIELDS
        assert "sentiment_reason" in WEAVIATE_FIELDS

    def test_excluded_fields(self):
        """body, enriched_at, validated_at, embedding are NOT in Weaviate."""
        assert "body" not in WEAVIATE_FIELDS
        assert "enriched_at" not in WEAVIATE_FIELDS
        assert "validated_at" not in WEAVIATE_FIELDS
        assert "embedding" not in WEAVIATE_FIELDS


class TestIngestionWorker:
    """Tests for IngestionWorker._process()."""

    @patch("weaviate_store.ingestion_worker.KafkaProducer")
    @patch("weaviate_store.ingestion_worker.KafkaConsumer")
    def test_process_new_article(self, mock_consumer_cls, mock_producer_cls):
        """New article is upserted into Weaviate."""
        mock_weaviate = MagicMock()
        mock_weaviate.exists.return_value = False
        mock_weaviate.upsert_article.return_value = "uuid-123"

        worker = IngestionWorker(_make_config(), mock_weaviate)
        article = _make_article()
        worker._process(article)

        mock_weaviate.exists.assert_called_once_with(article["article_id"])
        call_args = mock_weaviate.upsert_article.call_args
        props = call_args[0][0]
        vector = call_args[0][1]

        # Only WEAVIATE_FIELDS should be in properties
        assert set(props.keys()).issubset(WEAVIATE_FIELDS)
        assert "body" not in props
        assert "embedding" not in props
        assert len(vector) == 384

    @patch("weaviate_store.ingestion_worker.KafkaProducer")
    @patch("weaviate_store.ingestion_worker.KafkaConsumer")
    def test_process_duplicate_skipped(self, mock_consumer_cls, mock_producer_cls):
        """Duplicate article is skipped (not upserted)."""
        mock_weaviate = MagicMock()
        mock_weaviate.exists.return_value = True

        worker = IngestionWorker(_make_config(), mock_weaviate)
        worker._process(_make_article())

        mock_weaviate.upsert_article.assert_not_called()

    @patch("weaviate_store.ingestion_worker.KafkaProducer")
    @patch("weaviate_store.ingestion_worker.KafkaConsumer")
    def test_process_missing_article_id(self, mock_consumer_cls, mock_producer_cls):
        """Missing article_id raises ValueError."""
        mock_weaviate = MagicMock()
        worker = IngestionWorker(_make_config(), mock_weaviate)

        with pytest.raises(ValueError, match="article_id"):
            worker._process({"headline": "no id"})

    @patch("weaviate_store.ingestion_worker.KafkaProducer")
    @patch("weaviate_store.ingestion_worker.KafkaConsumer")
    def test_process_missing_embedding(self, mock_consumer_cls, mock_producer_cls):
        """Missing embedding raises ValueError."""
        mock_weaviate = MagicMock()
        mock_weaviate.exists.return_value = False
        worker = IngestionWorker(_make_config(), mock_weaviate)

        article = _make_article()
        del article["embedding"]
        with pytest.raises(ValueError, match="embedding"):
            worker._process(article)

    @patch("weaviate_store.ingestion_worker.KafkaProducer")
    @patch("weaviate_store.ingestion_worker.KafkaConsumer")
    def test_dead_letter(self, mock_consumer_cls, mock_producer_cls):
        """Failed events go to dead-letter topic."""
        mock_weaviate = MagicMock()
        mock_producer = MagicMock()
        mock_producer_cls.return_value = mock_producer

        worker = IngestionWorker(_make_config(), mock_weaviate)
        event = {"article_id": "test-123"}
        worker._send_to_dead_letter(event, "test error")

        mock_producer.send.assert_called_once()
        call_args = mock_producer.send.call_args
        assert call_args[0][0] == "news-failed"
        sent_event = call_args[1]["value"]
        assert sent_event["_error"] == "test error"
        assert sent_event["_failed_stage"] == "weaviate-ingestion"
