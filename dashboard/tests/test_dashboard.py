"""Tests for dashboard.weaviate_helper functions."""

from unittest.mock import MagicMock, patch
import pytest

from dashboard.config import DashboardConfig


@pytest.fixture
def config():
    return DashboardConfig(
        weaviate_http_host="localhost",
        weaviate_http_port=8085,
        weaviate_grpc_host="localhost",
        weaviate_grpc_port=50051,
        collection_name="NewsArticle",
        ollama_base_url="http://localhost:11434",
        ollama_model="mistral",
        embedding_model="all-MiniLM-L6-v2",
        embedding_dim=384,
    )


@pytest.fixture
def mock_client():
    return MagicMock()


class TestCountArticles:
    def test_returns_total(self, mock_client):
        from dashboard.weaviate_helper import count_articles

        collection = MagicMock()
        mock_client.collections.get.return_value = collection
        collection.aggregate.over_all.return_value = MagicMock(total_count=42)

        assert count_articles(mock_client, "NewsArticle") == 42
        mock_client.collections.get.assert_called_once_with("NewsArticle")

    def test_returns_zero_when_none(self, mock_client):
        from dashboard.weaviate_helper import count_articles

        collection = MagicMock()
        mock_client.collections.get.return_value = collection
        collection.aggregate.over_all.return_value = MagicMock(total_count=None)

        assert count_articles(mock_client, "NewsArticle") == 0


class TestFetchLatestArticles:
    def test_returns_article_list(self, mock_client):
        from dashboard.weaviate_helper import fetch_latest_articles

        collection = MagicMock()
        mock_client.collections.get.return_value = collection

        mock_obj = MagicMock()
        mock_obj.properties = {"title": "Test Article", "sentiment": "Positive"}
        collection.query.fetch_objects.return_value = MagicMock(objects=[mock_obj])

        result = fetch_latest_articles(mock_client, "NewsArticle", limit=10)
        assert len(result) == 1
        assert result[0].properties["title"] == "Test Article"


class TestAggregateByProperty:
    def test_returns_dict(self, mock_client):
        from dashboard.weaviate_helper import aggregate_by_property

        collection = MagicMock()
        mock_client.collections.get.return_value = collection

        group1 = MagicMock()
        group1.grouped_by.value = "Positive"
        group1.total_count = 10
        group2 = MagicMock()
        group2.grouped_by.value = "Negative"
        group2.total_count = 5

        collection.aggregate.over_all.return_value = [group1, group2]

        result = aggregate_by_property(mock_client, "NewsArticle", "sentiment")
        assert result == {"Positive": 10, "Negative": 5}


class TestSearchByVector:
    def test_returns_results(self, mock_client):
        from dashboard.weaviate_helper import search_by_vector

        collection = MagicMock()
        mock_client.collections.get.return_value = collection

        mock_obj = MagicMock()
        mock_obj.properties = {"title": "Vector Result"}
        mock_obj.metadata = MagicMock(distance=0.15)
        collection.query.near_vector.return_value = MagicMock(objects=[mock_obj])

        vector = [0.1] * 384
        result = search_by_vector(mock_client, "NewsArticle", vector, limit=5)
        assert len(result) == 1
        assert result[0].properties["title"] == "Vector Result"


class TestAskNewslens:
    @patch("dashboard.views.ask_newslens.requests.post")
    def test_ask_ollama(self, mock_post, config):
        from dashboard.views.ask_newslens import _ask_ollama

        mock_post.return_value = MagicMock(
            status_code=200,
            json=MagicMock(return_value={"response": "AI says hello"}),
        )

        answer = _ask_ollama("What is AI?", "Context here", config)
        assert answer == "AI says hello"
        mock_post.assert_called_once()

    @patch("dashboard.views.ask_newslens.requests.post")
    def test_ask_ollama_error(self, mock_post, config):
        from dashboard.views.ask_newslens import _ask_ollama
        from requests.exceptions import HTTPError

        mock_resp = MagicMock(status_code=500, text="Server Error")
        mock_resp.raise_for_status.side_effect = HTTPError("500 Server Error")
        mock_post.return_value = mock_resp

        answer = _ask_ollama("What?", "ctx", config)
        assert "error" in answer.lower() or "LLM" in answer
