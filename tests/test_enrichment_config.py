"""Unit tests for enrichment.config.EnrichmentConfig."""

import pytest

from enrichment.config import EnrichmentConfig


class TestEnrichmentConfig:
    """Tests for the EnrichmentConfig frozen dataclass."""

    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        monkeypatch.delenv("LLM_BACKEND", raising=False)
        monkeypatch.delenv("OLLAMA_BASE_URL", raising=False)
        monkeypatch.delenv("OLLAMA_MODEL", raising=False)
        monkeypatch.delenv("GROQ_API_KEY", raising=False)
        monkeypatch.delenv("GROQ_MODEL", raising=False)
        monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
        monkeypatch.delenv("ENRICHMENT_CONSUMER_GROUP", raising=False)
        monkeypatch.delenv("KAFKA_TOPIC_TECH", raising=False)
        monkeypatch.delenv("KAFKA_TOPIC_FINANCE", raising=False)
        monkeypatch.delenv("KAFKA_TOPIC_WORLD", raising=False)
        monkeypatch.delenv("KAFKA_TOPIC_FAILED", raising=False)

        config = EnrichmentConfig.from_env()

        assert config.bootstrap_servers == "kafka:9092"
        assert config.topics_input == ("tech-news", "finance-news", "world-news")
        assert config.topic_failed == "news-failed"
        assert config.llm_backend == "ollama"
        assert config.ollama_base_url == "http://ollama:11434"
        assert config.ollama_model == "mistral"
        assert config.groq_api_key == ""
        assert config.groq_model == "llama3-8b-8192"
        assert config.llm_timeout_seconds == 30
        assert config.consumer_group_id == "enrichment-worker"

    def test_llm_backend_override(self, monkeypatch):
        monkeypatch.setenv("LLM_BACKEND", "groq")
        config = EnrichmentConfig.from_env()
        assert config.llm_backend == "groq"

    def test_groq_model_override(self, monkeypatch):
        monkeypatch.setenv("GROQ_MODEL", "mixtral-8x7b-32768")
        config = EnrichmentConfig.from_env()
        assert config.groq_model == "mixtral-8x7b-32768"

    def test_timeout_override(self, monkeypatch):
        monkeypatch.setenv("LLM_TIMEOUT_SECONDS", "60")
        config = EnrichmentConfig.from_env()
        assert config.llm_timeout_seconds == 60

    def test_bootstrap_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        config = EnrichmentConfig.from_env()
        assert config.bootstrap_servers == "localhost:29092"

    def test_frozen_immutability(self):
        config = EnrichmentConfig.from_env()
        with pytest.raises(AttributeError):
            config.llm_backend = "groq"
