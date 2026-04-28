"""Unit tests for WeaviateStoreConfig."""

import os
from unittest.mock import patch

from weaviate_store.config import WeaviateStoreConfig


class TestWeaviateStoreConfig:
    """Tests for WeaviateStoreConfig.from_env()."""

    def test_defaults(self):
        """Default values when no env vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            cfg = WeaviateStoreConfig.from_env()
        assert cfg.weaviate_url == "http://weaviate:8080"
        assert cfg.weaviate_grpc_url == "weaviate:50051"
        assert cfg.collection_name == "NewsArticle"
        assert cfg.bootstrap_servers == "kafka:9092"
        assert cfg.topic_input == "validated-news"
        assert cfg.topic_failed == "news-failed"
        assert cfg.consumer_group_id == "weaviate-ingestion"

    def test_custom_env(self):
        """Custom values from environment."""
        env = {
            "WEAVIATE_URL": "http://localhost:8080",
            "WEAVIATE_GRPC_URL": "localhost:50051",
            "WEAVIATE_COLLECTION": "TestArticle",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
            "KAFKA_TOPIC_VALIDATED": "test-validated",
            "KAFKA_TOPIC_FAILED": "test-failed",
            "WEAVIATE_INGESTION_CONSUMER_GROUP": "test-group",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = WeaviateStoreConfig.from_env()
        assert cfg.weaviate_url == "http://localhost:8080"
        assert cfg.weaviate_grpc_url == "localhost:50051"
        assert cfg.collection_name == "TestArticle"
        assert cfg.consumer_group_id == "test-group"

    def test_frozen(self):
        """Config is immutable."""
        cfg = WeaviateStoreConfig(
            weaviate_url="http://weaviate:8080",
            weaviate_grpc_url="weaviate:50051",
            collection_name="NewsArticle",
            bootstrap_servers="kafka:9092",
            topic_input="validated-news",
            topic_failed="news-failed",
            consumer_group_id="weaviate-ingestion",
        )
        try:
            cfg.collection_name = "Other"
            assert False, "Should have raised"
        except AttributeError:
            pass
