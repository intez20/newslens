"""Guardian API client — fetches articles from the Guardian Content API."""

import logging
import os
import time
from datetime import datetime, timezone

import requests

from producer.base_client import BaseNewsClient
from producer.models import ArticleEvent

logger = logging.getLogger(__name__)

GUARDIAN_BASE_URL = "https://content.guardianapis.com/search"
GUARDIAN_SECTIONS = ("technology", "business", "money", "world", "science")
DEFAULT_PAGE_SIZE = 20
MAX_RETRIES = 5


class GuardianClient(BaseNewsClient):
    """Polls the Guardian /search endpoint for articles across 5 sections."""

    def __init__(
        self,
        api_key: str | None = None,
        sections: tuple[str, ...] = GUARDIAN_SECTIONS,
        page_size: int = DEFAULT_PAGE_SIZE,
    ):
        self._api_key = api_key if api_key is not None else os.getenv("GUARDIAN_API_KEY", "")
        if not self._api_key:
            raise ValueError("GUARDIAN_API_KEY is required")
        self._sections = sections
        self._page_size = page_size

    @property
    def source_name(self) -> str:
        return "guardian"

    def fetch(self) -> list[ArticleEvent]:
        """Fetch latest articles from all configured sections."""
        articles: list[ArticleEvent] = []
        for section in self._sections:
            batch = self._fetch_section(section)
            articles.extend(batch)
        logger.info("Guardian: fetched %d articles total", len(articles))
        return articles

    def _fetch_section(self, section: str) -> list[ArticleEvent]:
        """Fetch one section with retry logic."""
        params = {
            "api-key": self._api_key,
            "section": section,
            "page-size": self._page_size,
            "show-fields": "bodyText,wordcount,headline",
            "order-by": "newest",
        }

        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    GUARDIAN_BASE_URL, params=params, timeout=10
                )

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Guardian rate-limited (429), retrying in %ds", wait
                    )
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()
                results = data.get("response", {}).get("results", [])
                return self._parse_results(results, section)

            except requests.exceptions.Timeout:
                logger.warning(
                    "Guardian timeout for section=%s, attempt %d/%d",
                    section, attempt + 1, MAX_RETRIES,
                )
            except requests.exceptions.RequestException as exc:
                logger.error("Guardian request error: %s", exc)
                break

        return []

    def _parse_results(
        self, results: list[dict], section: str
    ) -> list[ArticleEvent]:
        """Convert raw Guardian API results into ArticleEvent objects."""
        events: list[ArticleEvent] = []
        for item in results:
            try:
                fields = item.get("fields", {})
                event = ArticleEvent(
                    article_id=item["id"],
                    headline=fields.get("headline", item.get("webTitle", "")),
                    body=fields.get("bodyText", ""),
                    section=section,
                    published_at=item["webPublicationDate"],
                    source_url=item["webUrl"],
                )
                events.append(event)
            except Exception as exc:
                logger.warning("Skipping article %s: %s", item.get("id"), exc)
        return events
