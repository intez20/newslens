"""Unit tests for processor.config."""

import os

import pytest

from processor.config import ProcessorConfig


class TestProcessorConfigDefaults:
    """Verify defaults when no env vars are set."""

    def test_from_env_defaults(self, monkeypatch):
        # Clear any existing env vars
        for key in [
            "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC_RAW", "KAFKA_TOPIC_TECH",
            "KAFKA_TOPIC_FINANCE", "KAFKA_TOPIC_WORLD", "FLINK_CHECKPOINT_DIR",
            "FLINK_PARALLELISM", "FLINK_CHECKPOINT_INTERVAL_MS",
        ]:
            monkeypatch.delenv(key, raising=False)

        config = ProcessorConfig.from_env()

        assert config.bootstrap_servers == "kafka:9092"
        assert config.topic_raw == "raw-news"
        assert config.topic_tech == "tech-news"
        assert config.topic_finance == "finance-news"
        assert config.topic_world == "world-news"
        assert config.checkpoint_dir == "/tmp/flink-checkpoints"
        assert config.parallelism == 1
        assert config.checkpoint_interval_ms == 60_000


class TestProcessorConfigOverrides:
    """Verify env vars override defaults."""

    def test_bootstrap_servers_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        config = ProcessorConfig.from_env()
        assert config.bootstrap_servers == "localhost:29092"

    def test_parallelism_override(self, monkeypatch):
        monkeypatch.setenv("FLINK_PARALLELISM", "4")
        config = ProcessorConfig.from_env()
        assert config.parallelism == 4

    def test_checkpoint_interval_override(self, monkeypatch):
        monkeypatch.setenv("FLINK_CHECKPOINT_INTERVAL_MS", "30000")
        config = ProcessorConfig.from_env()
        assert config.checkpoint_interval_ms == 30_000


class TestProcessorConfigFrozen:
    """Config is immutable once created."""

    def test_config_is_frozen(self):
        config = ProcessorConfig.from_env()
        with pytest.raises(AttributeError):
            config.parallelism = 99
