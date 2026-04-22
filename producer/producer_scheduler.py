"""ProducerScheduler — orchestrates multi-source polling, dedup, filtering, and publishing."""

import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from producer.base_client import BaseNewsClient
from producer.bluesky_client import BlueskyClient
from producer.guardian_client import GuardianClient
from producer.hackernews_client import HackerNewsClient
from producer.kafka_publisher import KafkaPublisher
from producer.rss_client import RSSClient

logger = logging.getLogger(__name__)


class ProducerScheduler:
    """Orchestrates multi-source news polling on a 60s interval.

    Wires together data source clients (Guardian, RSS, HN, Bluesky),
    deduplicates articles by article_id, filters short content, and
    publishes to Kafka through KafkaPublisher.
    """

    def __init__(
        self,
        clients: list[BaseNewsClient] | None = None,
        publisher: KafkaPublisher | None = None,
        interval_seconds: int | None = None,
        min_word_count: int | None = None,
    ):
        self.clients = clients if clients is not None else self._default_clients()
        self.publisher = publisher if publisher is not None else KafkaPublisher()
        self.interval_seconds = (
            interval_seconds
            if interval_seconds is not None
            else int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
        )
        self.min_word_count = (
            min_word_count
            if min_word_count is not None
            else int(os.getenv("MIN_WORD_COUNT", "200"))
        )
        self.seen_ids: set[str] = set()
        self._scheduler: BlockingScheduler | None = None

    # ------------------------------------------------------------------
    # Default client factory
    # ------------------------------------------------------------------

    @staticmethod
    def _default_clients() -> list[BaseNewsClient]:
        """Create all 4 data source clients with default configuration."""
        return [
            GuardianClient(),
            RSSClient(),
            HackerNewsClient(),
            BlueskyClient(),
        ]

    # ------------------------------------------------------------------
    # Core polling loop
    # ------------------------------------------------------------------

    def tick(self) -> dict:
        """Execute one polling cycle across all sources.

        Returns:
            dict with keys: fetched, duplicates, filtered, published, buffered
        """
        stats = {
            "fetched": 0,
            "duplicates": 0,
            "filtered": 0,
            "published": 0,
            "buffered": 0,
        }

        for client in self.clients:
            try:
                articles = client.fetch()
                logger.info(
                    "%s: fetched %d articles", client.source_name, len(articles)
                )
                stats["fetched"] += len(articles)
            except Exception:
                logger.exception("%s: fetch failed", client.source_name)
                continue

            for article in articles:
                # Dedup — skip articles we've already seen this session
                if article.article_id in self.seen_ids:
                    stats["duplicates"] += 1
                    continue

                # Word count filter — drop short articles
                word_count = len(article.body.split())
                if word_count < self.min_word_count:
                    logger.debug(
                        "Filtered %s — %d words (min %d)",
                        article.article_id,
                        word_count,
                        self.min_word_count,
                    )
                    stats["filtered"] += 1
                    continue

                # Mark as seen before publishing
                self.seen_ids.add(article.article_id)

                # Publish to Kafka (or buffer on failure)
                if self.publisher.publish(article):
                    stats["published"] += 1
                else:
                    stats["buffered"] += 1

        logger.info(
            "Tick complete: fetched=%d dupes=%d filtered=%d published=%d buffered=%d",
            stats["fetched"],
            stats["duplicates"],
            stats["filtered"],
            stats["published"],
            stats["buffered"],
        )
        return stats

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the APScheduler polling loop. Blocks until shutdown signal."""
        logger.info(
            "Starting ProducerScheduler — %d clients, %ds interval, min %d words",
            len(self.clients),
            self.interval_seconds,
            self.min_word_count,
        )

        # Run one tick immediately on startup
        self.tick()

        self._scheduler = BlockingScheduler()
        self._scheduler.add_job(
            self.tick,
            trigger=IntervalTrigger(seconds=self.interval_seconds),
            id="producer_tick",
            name="Poll all news sources",
        )
        self._scheduler.start()

    def stop(self) -> None:
        """Gracefully stop: flush publisher, shutdown scheduler."""
        logger.info("Stopping ProducerScheduler")
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
        self.publisher.close()
        logger.info("ProducerScheduler stopped — %d unique articles seen", len(self.seen_ids))
