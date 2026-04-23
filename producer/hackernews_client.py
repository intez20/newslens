"""Hacker News API client — fetches top/new stories from the Firebase API."""

import logging
from datetime import datetime, timezone

import requests

from producer.base_client import BaseNewsClient
from producer.body_fetcher import fetch_body
from producer.models import ArticleEvent

logger = logging.getLogger(__name__)

HN_BASE_URL = "https://hacker-news.firebaseio.com/v0"
DEFAULT_LIMIT = 30

# Keywords used to map HN titles into NewsLens sections
SECTION_KEYWORDS: dict[str, list[str]] = {
    "business": ["startup", "funding", "vc", "valuation", "ipo", "revenue", "market"],
    "science": ["research", "study", "climate", "physics", "biology", "space", "nasa"],
    "world": ["government", "policy", "war", "election", "geopolit", "sanction"],
}


class HackerNewsClient(BaseNewsClient):
    """Fetches top + new stories from the Hacker News API (no auth needed)."""

    def __init__(self, limit: int = DEFAULT_LIMIT):
        self._limit = limit

    @property
    def source_name(self) -> str:
        return "hackernews"

    def fetch(self) -> list[ArticleEvent]:
        """Fetch top and new stories, deduplicate, return as ArticleEvents."""
        top_ids = self._fetch_story_ids("topstories")
        new_ids = self._fetch_story_ids("newstories")
        # Merge, deduplicate, cap at limit
        seen: set[int] = set()
        unique_ids: list[int] = []
        for sid in top_ids + new_ids:
            if sid not in seen and len(unique_ids) < self._limit:
                seen.add(sid)
                unique_ids.append(sid)

        articles: list[ArticleEvent] = []
        for sid in unique_ids:
            event = self._fetch_item(sid)
            if event:
                event = self._enrich_body(event)
                articles.append(event)

        logger.info("HackerNews: fetched %d articles", len(articles))
        return articles

    def _fetch_story_ids(self, endpoint: str) -> list[int]:
        """Fetch story IDs from topstories/newstories."""
        try:
            resp = requests.get(
                f"{HN_BASE_URL}/{endpoint}.json", timeout=(5, 10)
            )
            resp.raise_for_status()
            ids = resp.json()
            return ids[: self._limit]
        except requests.exceptions.RequestException as exc:
            logger.error("HN %s fetch failed: %s", endpoint, exc)
            return []

    def _fetch_item(self, item_id: int) -> ArticleEvent | None:
        """Fetch a single HN item and convert to ArticleEvent."""
        try:
            resp = requests.get(
                f"{HN_BASE_URL}/item/{item_id}.json", timeout=(5, 10)
            )
            resp.raise_for_status()
            item = resp.json()

            if not item or item.get("type") != "story":
                return None
            if item.get("dead") or item.get("deleted"):
                return None

            title = item.get("title", "")
            url = item.get("url", f"https://news.ycombinator.com/item?id={item_id}")
            body = item.get("text", "")  # Only present for Ask HN / Show HN
            pub_dt = datetime.fromtimestamp(item.get("time", 0), tz=timezone.utc)

            return ArticleEvent(
                article_id=f"hn/{item_id}",
                headline=title,
                body=body if body else title,
                section=self._classify_section(title),
                published_at=pub_dt,
                source_url=url,
            )
        except Exception as exc:
            logger.warning("Skipping HN item %d: %s", item_id, exc)
            return None

    def _enrich_body(self, event: ArticleEvent) -> ArticleEvent:
        """Attempt to fetch the full article body from the story URL."""
        full_body = fetch_body(str(event.source_url))
        if full_body:
            return event.model_copy(update={"body": full_body})
        return event

    @staticmethod
    def _classify_section(title: str) -> str:
        """Map an HN title to a NewsLens section using keyword matching."""
        lower = title.lower()
        for section, keywords in SECTION_KEYWORDS.items():
            if any(kw in lower for kw in keywords):
                return section
        return "technology"  # HN default
