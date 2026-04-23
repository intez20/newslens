"""RSS feed client — fetches articles from multiple news RSS feeds."""

import hashlib
import logging
from datetime import datetime, timezone

import feedparser

from producer.base_client import BaseNewsClient
from producer.body_fetcher import fetch_body
from producer.models import ArticleEvent

logger = logging.getLogger(__name__)

# Map feed URLs → default section for that feed
DEFAULT_FEEDS: dict[str, str] = {
    "http://feeds.bbci.co.uk/news/technology/rss.xml": "technology",
    "http://feeds.bbci.co.uk/news/business/rss.xml": "business",
    "http://feeds.bbci.co.uk/news/world/rss.xml": "world",
    "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml": "science",
    "https://techcrunch.com/feed/": "technology",
    "https://feeds.arstechnica.com/arstechnica/index": "technology",
}


class RSSClient(BaseNewsClient):
    """Polls RSS feeds and converts items into ArticleEvent objects."""

    def __init__(self, feeds: dict[str, str] | None = None):
        self._feeds = feeds or DEFAULT_FEEDS

    @property
    def source_name(self) -> str:
        return "rss"

    def fetch(self) -> list[ArticleEvent]:
        """Fetch latest items from all configured RSS feeds."""
        articles: list[ArticleEvent] = []
        for feed_url, section in self._feeds.items():
            batch = self._fetch_feed(feed_url, section)
            articles.extend(batch)
        logger.info("RSS: fetched %d articles total", len(articles))
        return articles

    def _fetch_feed(self, url: str, section: str) -> list[ArticleEvent]:
        """Parse a single RSS feed."""
        try:
            feed = feedparser.parse(url)
            if feed.bozo and not feed.entries:
                logger.warning("RSS feed error for %s: %s", url, feed.bozo_exception)
                return []
            return self._parse_entries(feed.entries, section)
        except Exception as exc:
            logger.error("RSS fetch failed for %s: %s", url, exc)
            return []

    def _parse_entries(
        self, entries: list, section: str
    ) -> list[ArticleEvent]:
        """Convert feedparser entries to ArticleEvent objects."""
        events: list[ArticleEvent] = []
        for entry in entries:
            try:
                link = entry.get("link", "")
                article_id = f"rss/{self._hash_id(entry.get('id', link))}"

                published = entry.get("published_parsed") or entry.get("updated_parsed")
                if published:
                    pub_dt = datetime(*published[:6], tzinfo=timezone.utc)
                else:
                    pub_dt = datetime.now(timezone.utc)

                event = ArticleEvent(
                    article_id=article_id,
                    headline=entry.get("title", ""),
                    body=entry.get("summary", entry.get("description", "")),
                    section=section,
                    published_at=pub_dt,
                    source_url=link,
                )

                # Attempt to fetch full article body from source URL
                full_body = fetch_body(link)
                if full_body:
                    event = event.model_copy(update={"body": full_body})

                events.append(event)
            except Exception as exc:
                logger.warning("Skipping RSS entry: %s", exc)
        return events

    @staticmethod
    def _hash_id(raw_id: str) -> str:
        """Create a short deterministic hash from a feed item ID or URL."""
        return hashlib.sha256(raw_id.encode()).hexdigest()[:16]
