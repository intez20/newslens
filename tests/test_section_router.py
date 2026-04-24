"""Unit tests for processor.section_router."""

import pytest

from processor.section_router import DEFAULT_TOPIC, SECTION_MAP, route_section


class TestRouteSection:
    """Verify each section maps to the correct downstream Kafka topic."""

    def test_technology_routes_to_tech_news(self):
        assert route_section("technology") == "tech-news"

    def test_business_routes_to_finance_news(self):
        assert route_section("business") == "finance-news"

    def test_money_routes_to_finance_news(self):
        assert route_section("money") == "finance-news"

    def test_world_routes_to_world_news(self):
        assert route_section("world") == "world-news"

    def test_science_routes_to_world_news(self):
        assert route_section("science") == "world-news"

    def test_unknown_section_defaults_to_world_news(self):
        assert route_section("sports") == DEFAULT_TOPIC
        assert route_section("") == DEFAULT_TOPIC

    def test_all_valid_sections_covered(self):
        """Every section in the model's Literal type has a mapping."""
        valid_sections = {"technology", "business", "money", "world", "science"}
        assert valid_sections == set(SECTION_MAP.keys())


class TestSectionMapConstants:
    """Verify the routing constants are consistent."""

    def test_default_topic_is_world_news(self):
        assert DEFAULT_TOPIC == "world-news"

    def test_all_targets_are_known_topics(self):
        expected_topics = {"tech-news", "finance-news", "world-news"}
        assert set(SECTION_MAP.values()) == expected_topics
