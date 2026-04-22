"""Tests for ProducerScheduler — unit (mocked) + integration (real APIs)."""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from producer.base_client import BaseNewsClient
from producer.kafka_publisher import KafkaPublisher
from producer.models import ArticleEvent
from producer.producer_scheduler import ProducerScheduler


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def make_event(article_id: str, word_count: int = 250) -> ArticleEvent:
    """Create a test ArticleEvent with controllable body length."""
    return ArticleEvent(
        article_id=article_id,
        headline=f"Headline for {article_id}",
        body=" ".join(["word"] * word_count),
        section="technology",
        published_at=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
        source_url=f"https://example.com/{article_id}",
    )


def make_mock_client(
    name: str, events: list[ArticleEvent] | None = None
) -> MagicMock:
    """Create a mocked BaseNewsClient that returns controlled events."""
    client = MagicMock(spec=BaseNewsClient)
    client.source_name = name
    client.fetch.return_value = events if events is not None else []
    return client


# ------------------------------------------------------------------ #
# Fixtures                                                            #
# ------------------------------------------------------------------ #

@pytest.fixture
def mock_publisher() -> MagicMock:
    """A KafkaPublisher mock that records publish calls without Kafka."""
    pub = MagicMock(spec=KafkaPublisher)
    pub.publish.return_value = True
    return pub


@pytest.fixture
def two_clients() -> list[MagicMock]:
    """Two mock clients returning distinct articles."""
    client_a = make_mock_client("source_a", [
        make_event("a/1", 300),
        make_event("a/2", 300),
    ])
    client_b = make_mock_client("source_b", [
        make_event("b/1", 300),
        make_event("b/2", 300),
    ])
    return [client_a, client_b]


@pytest.fixture
def scheduler(two_clients, mock_publisher) -> ProducerScheduler:
    """ProducerScheduler with two mock clients and a mock publisher."""
    return ProducerScheduler(
        clients=two_clients,
        publisher=mock_publisher,
        interval_seconds=0,
        min_word_count=200,
    )


# ================================================================== #
# Unit Tests (mocked — no network, no Kafka)                         #
# ================================================================== #


class TestTickCallsAllClients:
    """Test 1: tick() calls fetch() on every client in the list."""

    def test_tick_calls_all_clients(self, scheduler, two_clients):
        scheduler.tick()

        for client in two_clients:
            client.fetch.assert_called_once()


class TestTickDedupSkipsSeenIds:
    """Test 2: Same article_id from two sources — only first is published."""

    def test_tick_dedup_skips_seen_ids(self, mock_publisher):
        duplicate_event = make_event("shared/1", 300)
        client_a = make_mock_client("source_a", [duplicate_event])
        client_b = make_mock_client("source_b", [duplicate_event])

        sched = ProducerScheduler(
            clients=[client_a, client_b],
            publisher=mock_publisher,
            min_word_count=200,
        )
        stats = sched.tick()

        assert stats["duplicates"] == 1
        assert mock_publisher.publish.call_count == 1


class TestTickFiltersShortArticles:
    """Test 3: Article with body < min_word_count is skipped."""

    def test_tick_filters_short_articles(self, mock_publisher):
        short = make_event("short/1", word_count=50)
        client = make_mock_client("source", [short])

        sched = ProducerScheduler(
            clients=[client],
            publisher=mock_publisher,
            min_word_count=200,
        )
        stats = sched.tick()

        assert stats["filtered"] == 1
        assert stats["published"] == 0
        mock_publisher.publish.assert_not_called()


class TestTickPassesLongArticles:
    """Test 4: Article with body >= min_word_count is published."""

    def test_tick_passes_long_articles(self, mock_publisher):
        long = make_event("long/1", word_count=500)
        client = make_mock_client("source", [long])

        sched = ProducerScheduler(
            clients=[client],
            publisher=mock_publisher,
            min_word_count=200,
        )
        stats = sched.tick()

        assert stats["published"] == 1
        mock_publisher.publish.assert_called_once_with(long)


class TestTickReturnsCorrectStats:
    """Test 5: Returned dict has correct counts for all categories."""

    def test_tick_returns_correct_stats(self, mock_publisher):
        events = [
            make_event("ok/1", 300),     # published
            make_event("ok/2", 300),     # published
            make_event("short/1", 50),   # filtered
        ]
        client = make_mock_client("source", events)

        sched = ProducerScheduler(
            clients=[client],
            publisher=mock_publisher,
            min_word_count=200,
        )

        # Pre-seed one ID to trigger a duplicate
        sched.seen_ids.add("ok/1")

        stats = sched.tick()

        assert stats["fetched"] == 3
        assert stats["duplicates"] == 1
        assert stats["filtered"] == 1
        assert stats["published"] == 1
        assert stats["buffered"] == 0


class TestTickSourceFailureContinues:
    """Test 6: If one client raises, other clients still run."""

    def test_tick_source_failure_continues(self, mock_publisher):
        failing_client = make_mock_client("failing")
        failing_client.fetch.side_effect = RuntimeError("API down")

        healthy_client = make_mock_client("healthy", [make_event("h/1", 300)])

        sched = ProducerScheduler(
            clients=[failing_client, healthy_client],
            publisher=mock_publisher,
            min_word_count=200,
        )
        stats = sched.tick()

        # The healthy client's article should still be published
        assert stats["published"] == 1
        healthy_client.fetch.assert_called_once()


class TestTickPublishesViaPublisher:
    """Test 7: publisher.publish() is called with correct ArticleEvent objects."""

    def test_tick_publishes_via_publisher(self, mock_publisher):
        event = make_event("pub/1", 300)
        client = make_mock_client("source", [event])

        sched = ProducerScheduler(
            clients=[client],
            publisher=mock_publisher,
            min_word_count=200,
        )
        sched.tick()

        mock_publisher.publish.assert_called_once_with(event)
        # Verify the actual object passed
        published_event = mock_publisher.publish.call_args[0][0]
        assert published_event.article_id == "pub/1"
        assert published_event.headline == "Headline for pub/1"


class TestDedupPersistsAcrossTicks:
    """Test 8: Article seen in tick 1 is skipped in tick 2."""

    def test_dedup_persists_across_ticks(self, mock_publisher):
        event = make_event("persist/1", 300)
        client = make_mock_client("source", [event])

        sched = ProducerScheduler(
            clients=[client],
            publisher=mock_publisher,
            min_word_count=200,
        )

        stats1 = sched.tick()
        assert stats1["published"] == 1

        stats2 = sched.tick()
        assert stats2["duplicates"] == 1
        assert stats2["published"] == 0


class TestDefaultClientsCreated:
    """Test 9: With clients=None, scheduler creates all 4 default clients."""

    @patch("producer.producer_scheduler.BlueskyClient")
    @patch("producer.producer_scheduler.HackerNewsClient")
    @patch("producer.producer_scheduler.RSSClient")
    @patch("producer.producer_scheduler.GuardianClient")
    @patch("producer.producer_scheduler.KafkaPublisher")
    def test_default_clients_created(
        self, mock_kafka, mock_guardian, mock_rss, mock_hn, mock_bsky
    ):
        sched = ProducerScheduler()

        assert len(sched.clients) == 4
        mock_guardian.assert_called_once()
        mock_rss.assert_called_once()
        mock_hn.assert_called_once()
        mock_bsky.assert_called_once()


class TestStopFlushesPublisher:
    """Test 10: stop() calls publisher.close()."""

    def test_stop_flushes_publisher(self, mock_publisher):
        sched = ProducerScheduler(
            clients=[],
            publisher=mock_publisher,
        )
        sched.stop()

        mock_publisher.close.assert_called_once()


# ================================================================== #
# Integration Test (real APIs, mocked publisher)                      #
# ================================================================== #


class TestFullTickRealSources:
    """Test 11: Full tick() with real Guardian/RSS/HN/Bluesky — mock publisher."""

    @pytest.mark.skipif(
        not os.getenv("GUARDIAN_API_KEY"),
        reason="GUARDIAN_API_KEY not set — skipping integration test",
    )
    @pytest.mark.timeout(60)
    def test_full_tick_real_sources(self):
        from producer.bluesky_client import BlueskyClient
        from producer.guardian_client import GuardianClient
        from producer.hackernews_client import HackerNewsClient
        from producer.rss_client import RSSClient

        clients = [
            GuardianClient(),
            RSSClient(),
            HackerNewsClient(limit=5),
            BlueskyClient(limit=3),
        ]

        mock_pub = MagicMock(spec=KafkaPublisher)
        mock_pub.publish.return_value = True

        sched = ProducerScheduler(
            clients=clients,
            publisher=mock_pub,
            min_word_count=50,  # lower threshold for integration test
        )
        stats = sched.tick()

        # We should get articles from at least some sources
        assert stats["fetched"] > 0, "Expected at least some articles from real sources"
        assert stats["published"] > 0, "Expected at least some articles to be published"

        # Verify publisher received real ArticleEvent objects
        for call in mock_pub.publish.call_args_list:
            event = call[0][0]
            assert isinstance(event, ArticleEvent)
            assert event.article_id
            assert event.headline
            assert event.body

        print(f"\n  Integration test stats: {stats}")
        print(f"  Publisher received {mock_pub.publish.call_count} events")
        print(f"  Seen IDs: {len(sched.seen_ids)}")
