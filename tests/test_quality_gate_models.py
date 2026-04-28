"""Unit tests for quality_gate.models.ValidatedArticle."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from quality_gate.models import EMBEDDING_DIMENSION, ValidatedArticle


def _valid_payload(**overrides) -> dict:
    """Return a valid ValidatedArticle payload, with optional overrides."""
    base = {
        "article_id": "technology/2024/jan/15/test-article",
        "headline": "EU proposes comprehensive AI regulation framework",
        "body": "Full article body text with enough words to be meaningful.",
        "section": "technology",
        "published_at": "2024-01-15T10:30:00Z",
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
        "embedding": [0.1] * EMBEDDING_DIMENSION,
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


class TestValidatedArticle:
    """Tests for the ValidatedArticle Pydantic model."""

    def test_valid_article(self):
        article = ValidatedArticle(**_valid_payload())
        assert article.article_id == "technology/2024/jan/15/test-article"
        assert article.sentiment == "Neutral"
        assert article.domain_tag == "Regulation"
        assert len(article.embedding) == EMBEDDING_DIMENSION

    def test_embedding_correct_dimension(self):
        article = ValidatedArticle(**_valid_payload())
        assert len(article.embedding) == 384

    def test_embedding_wrong_dimension_rejected(self):
        with pytest.raises(ValidationError, match="384 dims"):
            ValidatedArticle(**_valid_payload(embedding=[0.1] * 100))

    def test_embedding_empty_rejected(self):
        with pytest.raises(ValidationError, match="384 dims"):
            ValidatedArticle(**_valid_payload(embedding=[]))

    def test_validated_at_present(self):
        article = ValidatedArticle(**_valid_payload())
        assert article.validated_at is not None

    def test_validated_at_accepts_iso_string(self):
        article = ValidatedArticle(
            **_valid_payload(validated_at="2024-06-15T12:00:00Z")
        )
        assert article.validated_at.year == 2024

    def test_invalid_sentiment_rejected(self):
        with pytest.raises(ValidationError):
            ValidatedArticle(**_valid_payload(sentiment="Unknown"))

    def test_invalid_domain_tag_rejected(self):
        with pytest.raises(ValidationError):
            ValidatedArticle(**_valid_payload(domain_tag="Sports"))

    def test_invalid_section_rejected(self):
        with pytest.raises(ValidationError):
            ValidatedArticle(**_valid_payload(section="entertainment"))

    def test_all_fields_preserved(self):
        payload = _valid_payload()
        article = ValidatedArticle(**payload)
        assert article.headline == payload["headline"]
        assert article.summary == payload["summary"]
        assert article.entities == payload["entities"]
        assert str(article.source_url).rstrip("/") == payload["source_url"].rstrip("/")
