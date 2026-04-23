"""Stage 03b — Kafka end-to-end smoke tests.

Prerequisites:
    docker compose up -d kafka kafka-init   # Kafka + topics must be running
    Host port 29092 must be reachable (PLAINTEXT_HOST listener).

Run:
    pytest tests/test_kafka_e2e.py -v --timeout=60
"""

import json
import uuid

import pytest
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

from producer.kafka_publisher import KafkaPublisher
from producer.models import ArticleEvent

BOOTSTRAP = "localhost:29092"
TOPIC = "raw-news"


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _kafka_available() -> bool:
    """Return True if a broker is reachable on localhost:29092."""
    try:
        p = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        p.close()
        return True
    except NoBrokersAvailable:
        return False


skip_no_kafka = pytest.mark.skipif(
    not _kafka_available(),
    reason="Kafka not reachable on localhost:29092 — start docker compose first",
)


def _make_event(**overrides) -> ArticleEvent:
    """Create a minimal valid ArticleEvent with unique article_id."""
    defaults = {
        "article_id": f"smoke-{uuid.uuid4().hex[:8]}",
        "headline": "Smoke-test headline",
        "body": " ".join(["word"] * 250),          # 250 words — passes min filter
        "section": "technology",
        "published_at": "2026-04-23T12:00:00Z",
        "source_url": "https://example.com/smoke-test",
    }
    defaults.update(overrides)
    return ArticleEvent(**defaults)


def _get_end_offsets(topic: str) -> dict:
    """Snapshot current end offsets for all partitions — call BEFORE publishing."""
    from kafka import TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return {}
    tps = [TopicPartition(topic, p) for p in partitions]
    end_offsets = consumer.end_offsets(tps)
    consumer.close()
    return {tp: off for tp, off in end_offsets.items()}


def _consume_from(topic: str, start_offsets: dict, timeout_s: float = 5.0) -> list[dict]:
    """Consume only messages published AFTER *start_offsets*."""
    from kafka import TopicPartition

    if not start_offsets:
        return []
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        consumer_timeout_ms=int(timeout_s * 1000),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    consumer.assign(list(start_offsets.keys()))
    for tp, offset in start_offsets.items():
        consumer.seek(tp, offset)
    messages = [msg.value for msg in consumer]
    consumer.close()
    return messages


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

@skip_no_kafka
class TestKafkaConnectivity:
    """Basic broker reachability."""

    def test_broker_reachable(self):
        """KafkaProducer can connect to localhost:29092."""
        p = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        p.close()

    def test_raw_news_topic_exists(self):
        """The raw-news topic was created by kafka-init."""
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
        topics = consumer.topics()
        consumer.close()
        assert "raw-news" in topics

    def test_all_five_topics_exist(self):
        """kafka-init creates exactly these 5 topics."""
        expected = {"raw-news", "tech-news", "finance-news", "world-news", "news-failed"}
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
        topics = consumer.topics()
        consumer.close()
        assert expected.issubset(topics)


@skip_no_kafka
class TestPublishConsume:
    """Round-trip: publish via KafkaPublisher, consume and verify."""

    def test_single_event_round_trip(self):
        """Publish one event and read it back from raw-news."""
        offsets = _get_end_offsets(TOPIC)
        event = _make_event()
        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        assert publisher.publish(event) is True
        publisher.flush()
        publisher.close()

        messages = _consume_from(TOPIC, offsets)
        ids = [m["article_id"] for m in messages]
        assert event.article_id in ids

    def test_event_fields_preserved(self):
        """All ArticleEvent fields survive the Kafka round-trip."""
        offsets = _get_end_offsets(TOPIC)
        event = _make_event(
            headline="Fields-check headline",
            section="business",
        )
        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        publisher.publish(event)
        publisher.flush()
        publisher.close()

        messages = _consume_from(TOPIC, offsets)
        match = [m for m in messages if m["article_id"] == event.article_id]
        assert len(match) == 1

        got = match[0]
        assert got["headline"] == "Fields-check headline"
        assert got["section"] == "business"
        assert got["source_url"] == "https://example.com/smoke-test"
        assert got["body"].startswith("word word")

    def test_batch_publish(self):
        """publish_batch sends multiple events; all appear in the topic."""
        offsets = _get_end_offsets(TOPIC)
        events = [_make_event() for _ in range(5)]
        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        result = publisher.publish_batch(events)
        publisher.flush()
        publisher.close()

        assert result["sent"] == 5
        assert result["buffered"] == 0

        messages = _consume_from(TOPIC, offsets)
        ids_in_kafka = {m["article_id"] for m in messages}
        for e in events:
            assert e.article_id in ids_in_kafka

    def test_message_key_is_article_id(self):
        """The Kafka record key should be the article_id bytes."""
        from kafka import TopicPartition

        offsets = _get_end_offsets(TOPIC)
        event = _make_event()
        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        publisher.publish(event)
        publisher.flush()
        publisher.close()

        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            consumer_timeout_ms=5_000,
        )
        consumer.assign(list(offsets.keys()))
        for tp, offset in offsets.items():
            consumer.seek(tp, offset)
        found = False
        for msg in consumer:
            if msg.key and msg.key.decode("utf-8") == event.article_id:
                found = True
                break
        consumer.close()
        assert found, f"No message with key={event.article_id}"


@skip_no_kafka
class TestEdgeCases:
    """Boundary / edge-case scenarios."""

    def test_large_body_publish(self):
        """An article with a very large body still publishes successfully."""
        big_body = " ".join(["paragraph"] * 5000)      # ~40 KB
        event = _make_event(body=big_body)
        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        assert publisher.publish(event) is True
        publisher.flush()
        publisher.close()

    def test_unicode_headline(self):
        """Headlines with unicode chars survive the round-trip."""
        offsets = _get_end_offsets(TOPIC)
        event = _make_event(headline="日本語テスト — émoji 🚀")
        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        publisher.publish(event)
        publisher.flush()
        publisher.close()

        messages = _consume_from(TOPIC, offsets)
        match = [m for m in messages if m["article_id"] == event.article_id]
        assert len(match) == 1
        assert match[0]["headline"] == "日本語テスト — émoji 🚀"

    def test_duplicate_article_id_both_stored(self):
        """Kafka doesn't deduplicate — two messages with the same ID both appear."""
        offsets = _get_end_offsets(TOPIC)
        shared_id = f"dupe-{uuid.uuid4().hex[:8]}"
        e1 = _make_event(article_id=shared_id, headline="First")
        e2 = _make_event(article_id=shared_id, headline="Second")

        publisher = KafkaPublisher(bootstrap_servers=BOOTSTRAP, topic=TOPIC)
        publisher.publish(e1)
        publisher.publish(e2)
        publisher.flush()
        publisher.close()

        messages = _consume_from(TOPIC, offsets)
        matches = [m for m in messages if m["article_id"] == shared_id]
        assert len(matches) >= 2
