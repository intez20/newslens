"""Unit tests for enrichment.enrichment_worker.EnrichmentWorker."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from enrichment.enrichment_worker import BODY_TRUNCATE_CHARS, EnrichmentWorker
from enrichment.config import EnrichmentConfig


def _make_config() -> EnrichmentConfig:
    return EnrichmentConfig(
        bootstrap_servers="localhost:29092",
        topics_input=("tech-news", "finance-news", "world-news"),
        topic_failed="news-failed",
        llm_backend="ollama",
        ollama_base_url="http://localhost:11434",
        ollama_model="mistral",
        groq_api_key="",
        groq_model="llama3-8b-8192",
        llm_timeout_seconds=30,
        consumer_group_id="enrichment-worker",
    )


def _raw_article(**overrides) -> dict:
    """Return a valid raw article dict with optional overrides."""
    base = {
        "article_id": "technology/2024/jan/15/test-article",
        "headline": "OpenAI announces new reasoning model",
        "body": (
            "The European Commission has unveiled a sweeping new regulatory "
            "framework for artificial intelligence that would impose strict "
            "requirements on high-risk AI systems. The proposed rules target "
            "applications in critical infrastructure, law enforcement, and "
            "hiring processes. Industry leaders have expressed both support "
            "and concern about compliance costs. "
        )
        * 5,  # ~300 words
        "section": "technology",
        "published_at": "2024-01-15T10:30:00Z",
        "source_url": "https://theguardian.com/technology/2024/jan/15/test",
    }
    base.update(overrides)
    return base


def _mock_chains_result() -> dict:
    """Return a valid enrichment chains result."""
    return {
        "summary": (
            "The EU proposed AI regulation for high-risk systems. "
            "Industry reacted with mixed sentiments. "
            "Compliance costs concern major tech firms."
        ),
        "entities": ["European Commission", "EU"],
        "sentiment": {"sentiment": "Neutral", "reason": "Balanced reporting."},
        "domain_tag": "Regulation",
    }


class TestProcessArticle:
    """Tests for EnrichmentWorker.process_article (no Kafka needed)."""

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_process_article_success(self, mock_consumer, mock_producer):
        config = _make_config()
        mock_chains = MagicMock()
        mock_chains.enrich.return_value = _mock_chains_result()

        worker = EnrichmentWorker(config, mock_chains)
        result = worker.process_article(_raw_article())

        assert result["article_id"] == "technology/2024/jan/15/test-article"
        assert result["sentiment"] == "Neutral"
        assert result["domain_tag"] == "Regulation"
        assert "summary" in result
        assert "entities" in result
        assert "enriched_at" in result

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_process_article_adds_enriched_at(self, mock_consumer, mock_producer):
        config = _make_config()
        mock_chains = MagicMock()
        mock_chains.enrich.return_value = _mock_chains_result()

        worker = EnrichmentWorker(config, mock_chains)
        result = worker.process_article(_raw_article())

        # enriched_at should be a valid ISO datetime string
        enriched_at = datetime.fromisoformat(result["enriched_at"])
        assert enriched_at.year >= 2024

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_llm_timeout_raises(self, mock_consumer, mock_producer):
        config = _make_config()
        mock_chains = MagicMock()
        mock_chains.enrich.side_effect = TimeoutError("LLM timeout")

        worker = EnrichmentWorker(config, mock_chains)
        with pytest.raises(TimeoutError, match="LLM timeout"):
            worker.process_article(_raw_article())

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_validation_error_on_bad_sentiment(self, mock_consumer, mock_producer):
        config = _make_config()
        mock_chains = MagicMock()
        bad_result = _mock_chains_result()
        bad_result["sentiment"] = {"sentiment": "Unknown", "reason": "Bad."}
        mock_chains.enrich.return_value = bad_result

        worker = EnrichmentWorker(config, mock_chains)
        with pytest.raises(ValidationError):
            worker.process_article(_raw_article())


class TestBuildContext:
    """Tests for context building and truncation."""

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_context_contains_headline(self, mock_consumer, mock_producer):
        config = _make_config()
        mock_chains = MagicMock()
        worker = EnrichmentWorker(config, mock_chains)

        context = worker._build_context(_raw_article())
        assert "OpenAI announces new reasoning model" in context

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_context_truncates_long_body(self, mock_consumer, mock_producer):
        config = _make_config()
        mock_chains = MagicMock()
        worker = EnrichmentWorker(config, mock_chains)

        long_body = "A" * 3000
        raw = _raw_article(body=long_body)
        context = worker._build_context(raw)

        # headline + \n\n + truncated body
        headline = raw["headline"]
        expected_max = len(headline) + 2 + BODY_TRUNCATE_CHARS
        assert len(context) <= expected_max


class TestDeadLetter:
    """Tests for dead-letter routing."""

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_dead_letter_includes_error_metadata(
        self, mock_consumer, mock_producer_cls
    ):
        config = _make_config()
        mock_chains = MagicMock()
        worker = EnrichmentWorker(config, mock_chains)

        raw = _raw_article()
        worker._send_to_dead_letter(raw, "LLM timeout")

        assert raw["_error"] == "LLM timeout"
        assert "_failed_at" in raw

    @patch("enrichment.enrichment_worker.KafkaProducer")
    @patch("enrichment.enrichment_worker.KafkaConsumer")
    def test_dead_letter_sends_to_failed_topic(
        self, mock_consumer, mock_producer_cls
    ):
        config = _make_config()
        mock_chains = MagicMock()
        worker = EnrichmentWorker(config, mock_chains)

        raw = _raw_article()
        worker._send_to_dead_letter(raw, "parse error")

        worker.failed_producer.send.assert_called_once_with(
            "news-failed", value=raw
        )
