"""Unit tests for quality_gate.config.QualityGateConfig."""

import pytest

from quality_gate.config import QualityGateConfig


class TestQualityGateConfig:
    """Tests for the QualityGateConfig frozen dataclass."""

    def test_defaults(self, monkeypatch):
        for var in (
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC_ENRICHED",
            "KAFKA_TOPIC_VALIDATED",
            "KAFKA_TOPIC_FAILED",
            "QUALITY_GATE_CONSUMER_GROUP",
            "EMBEDDING_MODEL",
            "EMBEDDING_DIMENSION",
            "MAX_RECENCY_DAYS",
            "MIN_SUMMARY_LENGTH",
            "MAX_SUMMARY_LENGTH",
        ):
            monkeypatch.delenv(var, raising=False)

        config = QualityGateConfig.from_env()

        assert config.bootstrap_servers == "kafka:9092"
        assert config.topic_input == "enriched-news"
        assert config.topic_output == "validated-news"
        assert config.topic_failed == "news-failed"
        assert config.consumer_group_id == "quality-gate"
        assert config.embedding_model == "all-MiniLM-L6-v2"
        assert config.embedding_dimension == 384
        assert config.max_recency_days == 7
        assert config.min_summary_length == 50
        assert config.max_summary_length == 500

    def test_topic_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_TOPIC_ENRICHED", "my-enriched")
        config = QualityGateConfig.from_env()
        assert config.topic_input == "my-enriched"

    def test_embedding_dimension_override(self, monkeypatch):
        monkeypatch.setenv("EMBEDDING_DIMENSION", "768")
        config = QualityGateConfig.from_env()
        assert config.embedding_dimension == 768

    def test_recency_override(self, monkeypatch):
        monkeypatch.setenv("MAX_RECENCY_DAYS", "14")
        config = QualityGateConfig.from_env()
        assert config.max_recency_days == 14

    def test_frozen(self, monkeypatch):
        config = QualityGateConfig.from_env()
        with pytest.raises(AttributeError):
            config.topic_input = "other"
