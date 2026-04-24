"""Section-based topic routing for the stream processor."""

SECTION_MAP: dict[str, str] = {
    "technology": "tech-news",
    "business": "finance-news",
    "money": "finance-news",
    "world": "world-news",
    "science": "world-news",
}

DEFAULT_TOPIC = "world-news"


def route_section(section: str) -> str:
    """Map an article section to its downstream Kafka topic."""
    return SECTION_MAP.get(section, DEFAULT_TOPIC)
