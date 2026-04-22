"""Bluesky API client - fetches posts from curated news accounts via AT Protocol."""

import hashlib
import logging
from datetime import datetime, timezone

import requests

from producer.base_client import BaseNewsClient
from producer.models import ArticleEvent

logger = logging.getLogger(__name__)

BLUESKY_PUBLIC_API = "https://public.api.bsky.app/xrpc"
DEFAULT_LIMIT = 10

# Curated news accounts mapped to their primary NewsLens section.
# searchPosts requires auth, so we use getAuthorFeed (public, no auth).
DEFAULT_ACCOUNTS: dict[str, str] = {
    "theguardian.com": "world",
    "nytimes.com": "world",
    "reuters.com": "world",
    "washingtonpost.com": "world",
    "theverge.com": "technology",
    "techcrunch.com": "technology",
    "arstechnica.com": "technology",
    "wired.com": "technology",
}


class BlueskyClient(BaseNewsClient):
    """Fetches recent posts from curated Bluesky news accounts (no auth)."""

    def __init__(
        self,
        accounts: dict[str, str] | None = None,
        limit: int = DEFAULT_LIMIT,
    ):
        self._accounts = accounts or DEFAULT_ACCOUNTS
        self._limit = limit

    @property
    def source_name(self) -> str:
        return "bluesky"

    def fetch(self) -> list[ArticleEvent]:
        """Fetch latest posts from all configured accounts."""
        articles: list[ArticleEvent] = []
        for handle, section in self._accounts.items():
            batch = self._fetch_author_feed(handle, section)
            articles.extend(batch)
        logger.info("Bluesky: fetched %d posts total", len(articles))
        return articles

    def _fetch_author_feed(self, handle: str, section: str) -> list[ArticleEvent]:
        """Fetch recent posts from a single Bluesky account."""
        params = {"actor": handle, "limit": self._limit}
        try:
            resp = requests.get(
                f"{BLUESKY_PUBLIC_API}/app.bsky.feed.getAuthorFeed",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            feed_items = data.get("feed", [])
            return self._parse_feed(feed_items, section)
        except requests.exceptions.RequestException as exc:
            logger.error("Bluesky feed failed for @%s: %s", handle, exc)
            return []

    def _parse_feed(
        self, feed_items: list[dict], section: str
    ) -> list[ArticleEvent]:
        """Convert Bluesky feed items into ArticleEvent objects."""
        events: list[ArticleEvent] = []
        for item in feed_items:
            try:
                post = item.get("post", {})
                uri = post.get("uri", "")
                record = post.get("record", {})
                text = record.get("text", "")

                # Prefer embedded link title/URL if present
                embed = post.get("embed", {})
                external = embed.get("external", {})
                headline = external.get("title") or text[:120]
                source_url = external.get("uri") or self._post_url(uri)
                body = text
                if external.get("description"):
                    body = f"{text}\n\n{external['description']}"

                created_at = record.get("createdAt", "")

                event = ArticleEvent(
                    article_id=f"bsky/{self._hash_uri(uri)}",
                    headline=headline,
                    body=body if body else headline,
                    section=section,
                    published_at=created_at,
                    source_url=source_url,
                )
                events.append(event)
            except Exception as exc:
                logger.warning("Skipping Bluesky post: %s", exc)
        return events

    @staticmethod
    def _hash_uri(uri: str) -> str:
        """Create a short deterministic hash from a Bluesky AT URI."""
        return hashlib.sha256(uri.encode()).hexdigest()[:16]

    @staticmethod
    def _post_url(at_uri: str) -> str:
        """Convert an at:// URI to a bsky.app web URL."""
        # at://did:plc:abc/app.bsky.feed.post/xyz → https://bsky.app/profile/did:plc:abc/post/xyz
        parts = at_uri.replace("at://", "").split("/")
        if len(parts) >= 3:
            did = parts[0]
            rkey = parts[2]
            return f"https://bsky.app/profile/{did}/post/{rkey}"
        return f"https://bsky.app"
