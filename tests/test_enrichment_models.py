"""Unit tests for enrichment.models.EnrichedArticleEvent."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from enrichment.models import EnrichedArticleEvent


def _valid_payload(**overrides) -> dict:
    """Return a valid EnrichedArticleEvent payload, with optional overrides."""
    base = {
        "article_id": "technology/2024/jan/15/test-article",
        "headline": "OpenAI announces new reasoning model",
        "body": "Full article body text with enough words to be meaningful.",
        "section": "technology",
        "published_at": "2024-01-15T10:30:00Z",
        "source_url": "https://theguardian.com/technology/2024/jan/15/test",
        "summary": (
            "OpenAI released a new reasoning model called o3. "
            "It represents a significant leap in multi-step logic. "
            "Analysts expect competitors to respond within months."
        ),
        "entities": ["OpenAI", "Sam Altman", "United States"],
        "sentiment": "Positive",
        "sentiment_reason": "The announcement signals major progress in AI.",
        "domain_tag": "AI",
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


class TestEnrichedArticleEvent:
    """Tests for the EnrichedArticleEvent Pydantic model."""

    def test_valid_enriched_event(self):
        event = EnrichedArticleEvent(**_valid_payload())
        assert event.article_id == "technology/2024/jan/15/test-article"
        assert event.sentiment == "Positive"
        assert event.domain_tag == "AI"
        assert len(event.entities) == 3

    def test_empty_summary_rejected(self):
        with pytest.raises(ValidationError, match="summary must be at least 50"):
            EnrichedArticleEvent(**_valid_payload(summary=""))

    def test_short_summary_rejected(self):
        with pytest.raises(ValidationError, match="summary must be at least 50"):
            EnrichedArticleEvent(**_valid_payload(summary="Too short."))

    def test_long_summary_rejected(self):
        long = "A" * 501
        with pytest.raises(ValidationError, match="summary must be at most 500"):
            EnrichedArticleEvent(**_valid_payload(summary=long))

    def test_entities_max_five(self):
        six = ["A", "B", "C", "D", "E", "F"]
        with pytest.raises(ValidationError, match="at most 5 items"):
            EnrichedArticleEvent(**_valid_payload(entities=six))

    def test_entities_empty_string_rejected(self):
        with pytest.raises(ValidationError, match="non-empty string"):
            EnrichedArticleEvent(**_valid_payload(entities=["OpenAI", ""]))

    def test_entities_empty_list_allowed(self):
        event = EnrichedArticleEvent(**_valid_payload(entities=[]))
        assert event.entities == []

    def test_invalid_sentiment_rejected(self):
        with pytest.raises(ValidationError):
            EnrichedArticleEvent(**_valid_payload(sentiment="Unknown"))

    def test_invalid_domain_tag_rejected(self):
        with pytest.raises(ValidationError):
            EnrichedArticleEvent(**_valid_payload(domain_tag="Sports"))

    def test_enriched_at_accepts_iso_string(self):
        event = EnrichedArticleEvent(
            **_valid_payload(enriched_at="2024-01-15T10:30:42Z")
        )
        assert event.enriched_at.year == 2024

    def test_empty_article_id_rejected(self):
        with pytest.raises(ValidationError, match="article_id must not be empty"):
            EnrichedArticleEvent(**_valid_payload(article_id="   "))

    def test_empty_headline_rejected(self):
        with pytest.raises(ValidationError, match="headline must not be empty"):
            EnrichedArticleEvent(**_valid_payload(headline=""))

    def test_empty_sentiment_reason_rejected(self):
        with pytest.raises(ValidationError, match="sentiment_reason must not be"):
            EnrichedArticleEvent(**_valid_payload(sentiment_reason="  "))
