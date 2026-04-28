"""Stage 06e — Quality gate end-to-end integration tests.

Prerequisites:
    docker compose up -d kafka quality-gate
    Host ports: 29092 (Kafka)

Run:
    pytest tests/test_quality_gate_e2e.py -v --timeout=60
"""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:29092"
TOPIC_ENRICHED = "enriched-news"
TOPIC_VALIDATED = "validated-news"
TOPIC_FAILED = "news-failed"


# ------------------------------------------------------------------
# Skip decorator
# ------------------------------------------------------------------

def _quality_gate_available() -> bool:
    """Check if Kafka is reachable and required topics exist."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
        topics = consumer.topics()
        consumer.close()
        return "enriched-news" in topics and "validated-news" in topics
    except NoBrokersAvailable:
        return False


skip_no_quality_gate = pytest.mark.skipif(
    not _quality_gate_available(),
    reason="Kafka + quality-gate required — run: docker compose up -d kafka quality-gate",
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _valid_enriched(**overrides) -> dict:
    """Create a valid enriched article event."""
    defaults = {
        "article_id": f"quality-test/{uuid.uuid4().hex[:8]}",
        "headline": "European Union proposes comprehensive AI regulation framework",
        "body": "The European Commission has unveiled a sweeping new regulatory framework.",
        "section": "technology",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "source_url": "https://example.com/eu-ai-regulation",
        "summary": (
            "The EU has proposed a sweeping AI regulation framework "
            "that would affect major technology companies worldwide. "
            "Analysts expect the regulation to pass by mid-2025."
        ),
        "entities": ["European Commission", "Microsoft", "United States"],
        "sentiment": "Neutral",
        "sentiment_reason": "The article presents regulatory developments objectively.",
        "domain_tag": "Regulation",
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
    defaults.update(overrides)
    return defaults


def _get_end_offsets(topic: str) -> dict:
    """Snapshot current end offsets for every partition of *topic*."""
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return {}
    tps = [TopicPartition(topic, p) for p in partitions]
    end_offsets = consumer.end_offsets(tps)
    consumer.close()
    return {tp: off for tp, off in end_offsets.items()}


def _consume_from(
    topic: str, start_offsets: dict, timeout_s: float = 3.0
) -> list[dict]:
    """Consume only messages published AFTER *start_offsets*."""
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


def _publish_to_topic(topic: str, events: list[dict]) -> None:
    """Publish event dicts directly to a topic."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for event in events:
        producer.send(topic, key=event["article_id"], value=event)
    producer.flush()
    producer.close()


def _wait_for_messages(
    topic: str,
    start_offsets: dict,
    expected: int = 1,
    timeout_s: float = 30.0,
    poll_interval: float = 1.0,
) -> list[dict]:
    """Poll a topic until expected number of new messages appear."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msgs = _consume_from(topic, start_offsets, timeout_s=2.0)
        if len(msgs) >= expected:
            return msgs
        time.sleep(poll_interval)
    return _consume_from(topic, start_offsets, timeout_s=2.0)


# ------------------------------------------------------------------
# E2E Tests — Valid articles
# ------------------------------------------------------------------

@skip_no_quality_gate
class TestQualityGateOutput:
    """Publish enriched article → expect validated output."""

    def test_valid_article_passes_gate(self):
        event = _valid_enriched()
        snap = _get_end_offsets(TOPIC_VALIDATED)
        _publish_to_topic(TOPIC_ENRICHED, [event])

        msgs = _wait_for_messages(TOPIC_VALIDATED, snap, expected=1)
        assert len(msgs) >= 1

        output = next(m for m in msgs if m["article_id"] == event["article_id"])
        assert output["headline"] == event["headline"]
        assert "embedding" in output
        assert "validated_at" in output

    def test_embedding_is_384_dims(self):
        event = _valid_enriched(
            article_id=f"quality-test/dim-{uuid.uuid4().hex[:8]}"
        )
        snap = _get_end_offsets(TOPIC_VALIDATED)
        _publish_to_topic(TOPIC_ENRICHED, [event])

        msgs = _wait_for_messages(TOPIC_VALIDATED, snap, expected=1)
        output = next(m for m in msgs if m["article_id"] == event["article_id"])
        assert len(output["embedding"]) == 384

    def test_original_fields_preserved(self):
        event = _valid_enriched(
            article_id=f"quality-test/fields-{uuid.uuid4().hex[:8]}"
        )
        snap = _get_end_offsets(TOPIC_VALIDATED)
        _publish_to_topic(TOPIC_ENRICHED, [event])

        msgs = _wait_for_messages(TOPIC_VALIDATED, snap, expected=1)
        output = next(m for m in msgs if m["article_id"] == event["article_id"])
        assert output["summary"] == event["summary"]
        assert output["entities"] == event["entities"]
        assert output["sentiment"] == event["sentiment"]
        assert output["domain_tag"] == event["domain_tag"]


# ------------------------------------------------------------------
# E2E Tests — Dead-lettered articles
# ------------------------------------------------------------------

@skip_no_quality_gate
class TestQualityGateDeadLetter:
    """Publish invalid enriched articles → expect dead-letter."""

    def test_stale_article_dead_lettered(self):
        stale_dt = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        event = _valid_enriched(
            article_id=f"quality-test/stale-{uuid.uuid4().hex[:8]}",
            published_at=stale_dt,
        )
        snap = _get_end_offsets(TOPIC_FAILED)
        _publish_to_topic(TOPIC_ENRICHED, [event])

        msgs = _wait_for_messages(TOPIC_FAILED, snap, expected=1)
        failed = next(
            (m for m in msgs if m.get("article_id") == event["article_id"]),
            None,
        )
        assert failed is not None
        assert "recency" in failed.get("_error", "").lower()

    def test_bad_sentiment_dead_lettered(self):
        event = _valid_enriched(
            article_id=f"quality-test/bad-sent-{uuid.uuid4().hex[:8]}",
            sentiment="Unknown",
        )
        snap = _get_end_offsets(TOPIC_FAILED)
        _publish_to_topic(TOPIC_ENRICHED, [event])

        msgs = _wait_for_messages(TOPIC_FAILED, snap, expected=1)
        failed = next(
            (m for m in msgs if m.get("article_id") == event["article_id"]),
            None,
        )
        assert failed is not None
        assert "sentiment" in failed.get("_error", "").lower()

    def test_short_summary_dead_lettered(self):
        event = _valid_enriched(
            article_id=f"quality-test/short-{uuid.uuid4().hex[:8]}",
            summary="Too short summary.",
        )
        snap = _get_end_offsets(TOPIC_FAILED)
        _publish_to_topic(TOPIC_ENRICHED, [event])

        msgs = _wait_for_messages(TOPIC_FAILED, snap, expected=1)
        failed = next(
            (m for m in msgs if m.get("article_id") == event["article_id"]),
            None,
        )
        assert failed is not None
        assert "summary" in failed.get("_error", "").lower()
