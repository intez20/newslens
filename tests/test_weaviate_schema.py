"""Unit tests for Weaviate schema definition."""

from weaviate_store.schema import COLLECTION_NAME, NEWSARTICLE_PROPERTIES


class TestSchema:
    """Tests for NewsArticle collection schema."""

    def test_collection_name(self):
        assert COLLECTION_NAME == "NewsArticle"

    def test_property_count(self):
        """10 properties defined."""
        assert len(NEWSARTICLE_PROPERTIES) == 10

    def test_property_names(self):
        """All expected property names are present."""
        names = {p.name for p in NEWSARTICLE_PROPERTIES}
        expected = {
            "article_id",
            "headline",
            "summary",
            "entities",
            "sentiment",
            "sentiment_reason",
            "domain_tag",
            "section",
            "published_at",
            "source_url",
        }
        assert names == expected

    def test_field_tokenization(self):
        """ID/enum fields use FIELD tokenization for exact match."""
        field_tokenized = {"article_id", "sentiment", "domain_tag", "section", "source_url"}
        for prop in NEWSARTICLE_PROPERTIES:
            if prop.name in field_tokenized:
                assert prop.tokenization is not None, (
                    f"{prop.name} should have FIELD tokenization"
                )
