"""Unit tests for WeaviateNewsClient (mocked Weaviate)."""

from unittest.mock import MagicMock, patch

from weaviate_store.client import WeaviateNewsClient
from weaviate_store.config import WeaviateStoreConfig


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


class TestWeaviateNewsClient:
    """Tests for WeaviateNewsClient with mocked Weaviate connection."""

    @patch("weaviate_store.client.ensure_collection")
    @patch("weaviate_store.client.weaviate.connect_to_custom")
    def test_connect_and_ensure_collection(self, mock_connect, mock_ensure):
        """Client connects and ensures collection on init."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        config = _make_config()
        client = WeaviateNewsClient(config)

        mock_connect.assert_called_once()
        mock_ensure.assert_called_once_with(mock_client, "TestArticle")
        mock_client.collections.get.assert_called_once_with("TestArticle")

    @patch("weaviate_store.client.ensure_collection")
    @patch("weaviate_store.client.weaviate.connect_to_custom")
    def test_exists_true(self, mock_connect, mock_ensure):
        """exists() returns True when article found."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        mock_collection = MagicMock()
        mock_client.collections.get.return_value = mock_collection
        mock_result = MagicMock()
        mock_result.objects = [MagicMock()]
        mock_collection.query.fetch_objects.return_value = mock_result

        client = WeaviateNewsClient(_make_config())
        assert client.exists("abc-123") is True

    @patch("weaviate_store.client.ensure_collection")
    @patch("weaviate_store.client.weaviate.connect_to_custom")
    def test_exists_false(self, mock_connect, mock_ensure):
        """exists() returns False when article not found."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        mock_collection = MagicMock()
        mock_client.collections.get.return_value = mock_collection
        mock_result = MagicMock()
        mock_result.objects = []
        mock_collection.query.fetch_objects.return_value = mock_result

        client = WeaviateNewsClient(_make_config())
        assert client.exists("abc-123") is False

    @patch("weaviate_store.client.ensure_collection")
    @patch("weaviate_store.client.weaviate.connect_to_custom")
    def test_upsert_article(self, mock_connect, mock_ensure):
        """upsert_article() inserts with properties and vector."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client
        mock_collection = MagicMock()
        mock_client.collections.get.return_value = mock_collection
        mock_collection.data.insert.return_value = "uuid-abc"

        client = WeaviateNewsClient(_make_config())
        props = {"headline": "Test", "article_id": "a1"}
        vector = [0.1] * 384
        result = client.upsert_article(props, vector)

        assert result == "uuid-abc"
        mock_collection.data.insert.assert_called_once_with(
            properties=props, vector=vector
        )

    @patch("weaviate_store.client.ensure_collection")
    @patch("weaviate_store.client.weaviate.connect_to_custom")
    def test_close(self, mock_connect, mock_ensure):
        """close() closes the underlying client."""
        mock_client = MagicMock()
        mock_connect.return_value = mock_client

        client = WeaviateNewsClient(_make_config())
        client.close()
        mock_client.close.assert_called_once()
