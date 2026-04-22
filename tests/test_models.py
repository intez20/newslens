"""Unit tests for producer.models — ArticleEvent Pydantic schema."""

import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from producer.models import ArticleEvent, VALID_SECTIONS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_article_data() -> dict:
    """Return a minimal valid article payload."""
    return {
        "article_id": "technology/2024/jan/15/openai-new-model",
        "headline": "OpenAI announces new reasoning model",
        "body": " ".join(["word"] * 250),  # 250 words — above 200 minimum
        "section": "technology",
        "published_at": "2024-01-15T10:30:00Z",
        "source_url": "https://www.theguardian.com/technology/2024/jan/15/openai-new-model",
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestArticleEventValid:
    """Tests for valid ArticleEvent creation."""

    def test_creates_from_valid_dict(self, valid_article_data):
        event = ArticleEvent(**valid_article_data)
        assert event.article_id == valid_article_data["article_id"]
        assert event.headline == valid_article_data["headline"]
        assert event.section == "technology"

    def test_published_at_parsed_as_datetime(self, valid_article_data):
        event = ArticleEvent(**valid_article_data)
        assert isinstance(event.published_at, datetime)
        assert event.published_at.year == 2024

    def test_source_url_parsed(self, valid_article_data):
        event = ArticleEvent(**valid_article_data)
        assert "theguardian.com" in str(event.source_url)

    def test_serialization_json(self, valid_article_data):
        event = ArticleEvent(**valid_article_data)
        json_str = event.model_dump_json()
        parsed = json.loads(json_str)
        assert parsed["article_id"] == valid_article_data["article_id"]
        assert parsed["headline"] == valid_article_data["headline"]
        assert parsed["section"] == "technology"
        # All 6 fields present
        assert set(parsed.keys()) == {
            "article_id", "headline", "body", "section",
            "published_at", "source_url",
        }

    def test_model_dump_roundtrip(self, valid_article_data):
        event = ArticleEvent(**valid_article_data)
        dumped = event.model_dump()
        reconstructed = ArticleEvent(**dumped)
        assert reconstructed.article_id == event.article_id

    @pytest.mark.parametrize("section", VALID_SECTIONS)
    def test_all_valid_sections_accepted(self, valid_article_data, section):
        valid_article_data["section"] = section
        event = ArticleEvent(**valid_article_data)
        assert event.section == section


# ---------------------------------------------------------------------------
# Validation failures
# ---------------------------------------------------------------------------

class TestArticleEventInvalid:
    """Tests for ArticleEvent validation errors."""

    def test_missing_article_id(self, valid_article_data):
        del valid_article_data["article_id"]
        with pytest.raises(ValidationError) as exc_info:
            ArticleEvent(**valid_article_data)
        assert "article_id" in str(exc_info.value)

    def test_missing_headline(self, valid_article_data):
        del valid_article_data["headline"]
        with pytest.raises(ValidationError) as exc_info:
            ArticleEvent(**valid_article_data)
        assert "headline" in str(exc_info.value)

    def test_missing_body(self, valid_article_data):
        del valid_article_data["body"]
        with pytest.raises(ValidationError) as exc_info:
            ArticleEvent(**valid_article_data)
        assert "body" in str(exc_info.value)

    def test_empty_article_id_rejected(self, valid_article_data):
        valid_article_data["article_id"] = "   "
        with pytest.raises(ValidationError) as exc_info:
            ArticleEvent(**valid_article_data)
        assert "article_id" in str(exc_info.value)

    def test_empty_headline_rejected(self, valid_article_data):
        valid_article_data["headline"] = ""
        with pytest.raises(ValidationError) as exc_info:
            ArticleEvent(**valid_article_data)
        assert "headline" in str(exc_info.value)

    def test_invalid_section_rejected(self, valid_article_data):
        valid_article_data["section"] = "sports"
        with pytest.raises(ValidationError) as exc_info:
            ArticleEvent(**valid_article_data)
        assert "section" in str(exc_info.value)

    def test_invalid_url_rejected(self, valid_article_data):
        valid_article_data["source_url"] = "not-a-valid-url"
        with pytest.raises(ValidationError):
            ArticleEvent(**valid_article_data)

    def test_invalid_datetime_rejected(self, valid_article_data):
        valid_article_data["published_at"] = "not-a-date"
        with pytest.raises(ValidationError):
            ArticleEvent(**valid_article_data)
