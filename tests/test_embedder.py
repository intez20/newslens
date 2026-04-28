"""Unit tests for quality_gate.embedder.ArticleEmbedder."""

import pytest

from quality_gate.config import QualityGateConfig
from quality_gate.embedder import ArticleEmbedder


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
def embedder(config):
    return ArticleEmbedder(config)


class TestArticleEmbedder:
    """Tests for the ArticleEmbedder class."""

    def test_embed_returns_384_dims(self, embedder):
        result = embedder.embed(
            "EU proposes AI regulation",
            "The European Union has proposed sweeping AI regulation.",
        )
        assert len(result) == 384

    def test_embed_values_are_floats(self, embedder):
        result = embedder.embed(
            "AI breakthrough announced",
            "Researchers have made a significant breakthrough in AI.",
        )
        assert all(isinstance(v, float) for v in result)

    def test_embed_different_texts_different_vectors(self, embedder):
        v1 = embedder.embed(
            "EU proposes AI regulation",
            "The EU has proposed sweeping AI regulation framework.",
        )
        v2 = embedder.embed(
            "Bitcoin hits new all-time high",
            "Bitcoin surged past $100k driven by institutional demand.",
        )
        assert v1 != v2

    def test_embed_same_text_same_vector(self, embedder):
        v1 = embedder.embed("Same headline", "Same summary text here.")
        v2 = embedder.embed("Same headline", "Same summary text here.")
        assert v1 == v2

    def test_embed_empty_headline(self, embedder):
        result = embedder.embed(
            "",
            "The EU has proposed sweeping AI regulation framework.",
        )
        assert len(result) == 384

    def test_embed_empty_summary(self, embedder):
        result = embedder.embed("EU proposes AI regulation", "")
        assert len(result) == 384
