"""Stage 04c — Flink stream-processor end-to-end integration tests.

Prerequisites:
    docker compose up -d                     # full stack including Flink
    docker exec newslens-flink-jobmanager \
        flink run -pym processor -pyfs /opt/flink/jobs/  # submit the job
    Host ports: 29092 (Kafka), 8081 (Flink Web UI)

Run:
    pytest tests/test_flink_e2e.py -v --timeout=120
"""

import json
import time
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:29092"
TOPIC_RAW = "raw-news"
TOPIC_TECH = "tech-news"
TOPIC_FINANCE = "finance-news"
TOPIC_WORLD = "world-news"

# Long body (250+ words) — passes the 200-word filter
LONG_BODY = " ".join(["word"] * 250)
# Short body (50 words) — below the 200-word filter
SHORT_BODY = " ".join(["word"] * 50)


# ------------------------------------------------------------------
# Skip decorators
# ------------------------------------------------------------------

def _kafka_available() -> bool:
    try:
        p = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        p.close()
        return True
    except NoBrokersAvailable:
        return False


def _flink_job_running() -> bool:
    """Return True if at least one Flink job is RUNNING on localhost:8081."""
    try:
        resp = urlopen("http://localhost:8081/jobs/overview", timeout=5)
        data = json.loads(resp.read())
        return any(j["state"] == "RUNNING" for j in data.get("jobs", []))
    except (URLError, OSError, KeyError):
        return False


skip_no_flink = pytest.mark.skipif(
    not (_kafka_available() and _flink_job_running()),
    reason="Kafka + running Flink job required — start docker compose and submit the job",
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_event(
    section: str = "technology",
    body: str = LONG_BODY,
    **overrides,
) -> dict:
    """Create a minimal article event dict with a unique article_id."""
    defaults = {
        "article_id": f"flink-{uuid.uuid4().hex[:8]}",
        "headline": "Flink integration test headline",
        "body": body,
        "section": section,
        "published_at": "2026-05-01T12:00:00Z",
        "source_url": "https://example.com/flink-test",
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


def _consume_from(topic: str, start_offsets: dict, timeout_s: float = 3.0) -> list[dict]:
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


def _publish_to_raw(events: list[dict]) -> None:
    """Publish event dicts to raw-news topic."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for event in events:
        producer.send(TOPIC_RAW, key=event["article_id"], value=event)
    producer.flush()
    producer.close()


def _wait_for_messages(
    topic: str,
    start_offsets: dict,
    min_count: int = 1,
    timeout_s: float = 30.0,
) -> list[dict]:
    """Poll *topic* until at least *min_count* new messages appear or timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msgs = _consume_from(topic, start_offsets, timeout_s=2.0)
        if len(msgs) >= min_count:
            return msgs
        time.sleep(1.0)
    # Final attempt
    return _consume_from(topic, start_offsets, timeout_s=2.0)


# ------------------------------------------------------------------
# Tests — Section routing
# ------------------------------------------------------------------

@skip_no_flink
class TestFlinkRouting:
    """Verify that articles are routed to the correct downstream topic."""

    def test_technology_routes_to_tech_news(self):
        offsets = _get_end_offsets(TOPIC_TECH)
        event = _make_event(section="technology")
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_TECH, offsets)
        ids = [m["article_id"] for m in msgs]
        assert event["article_id"] in ids

    def test_business_routes_to_finance_news(self):
        offsets = _get_end_offsets(TOPIC_FINANCE)
        event = _make_event(section="business")
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_FINANCE, offsets)
        ids = [m["article_id"] for m in msgs]
        assert event["article_id"] in ids

    def test_money_routes_to_finance_news(self):
        offsets = _get_end_offsets(TOPIC_FINANCE)
        event = _make_event(section="money")
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_FINANCE, offsets)
        ids = [m["article_id"] for m in msgs]
        assert event["article_id"] in ids

    def test_world_routes_to_world_news(self):
        offsets = _get_end_offsets(TOPIC_WORLD)
        event = _make_event(section="world")
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_WORLD, offsets)
        ids = [m["article_id"] for m in msgs]
        assert event["article_id"] in ids

    def test_unknown_section_defaults_to_world_news(self):
        offsets = _get_end_offsets(TOPIC_WORLD)
        event = _make_event(section="entertainment")
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_WORLD, offsets)
        ids = [m["article_id"] for m in msgs]
        assert event["article_id"] in ids


# ------------------------------------------------------------------
# Tests — Deduplication
# ------------------------------------------------------------------

@skip_no_flink
class TestFlinkDedup:
    """Verify that duplicate article_ids are dropped by keyed state."""

    def test_duplicate_article_id_emitted_only_once(self):
        shared_id = f"dedup-{uuid.uuid4().hex[:8]}"

        # Snapshot ALL downstream topics before publishing
        offsets_tech = _get_end_offsets(TOPIC_TECH)
        offsets_finance = _get_end_offsets(TOPIC_FINANCE)
        offsets_world = _get_end_offsets(TOPIC_WORLD)

        # Publish the same article_id twice (technology → tech-news)
        e1 = _make_event(article_id=shared_id, section="technology", headline="First")
        e2 = _make_event(article_id=shared_id, section="technology", headline="Second")
        _publish_to_raw([e1, e2])

        # Wait long enough for Flink to process both
        msgs_tech = _wait_for_messages(TOPIC_TECH, offsets_tech, min_count=1, timeout_s=30.0)
        # Give extra time to ensure the second one (if any) also arrives
        time.sleep(5)
        msgs_tech = _consume_from(TOPIC_TECH, offsets_tech, timeout_s=3.0)

        matches = [m for m in msgs_tech if m["article_id"] == shared_id]
        assert len(matches) == 1, f"Expected 1 but got {len(matches)} messages for {shared_id}"


# ------------------------------------------------------------------
# Tests — Word-count filter
# ------------------------------------------------------------------

@skip_no_flink
class TestFlinkWordFilter:
    """Verify that articles below 200 words are dropped."""

    def test_short_article_dropped(self):
        """An article with <200 words should NOT appear on any downstream topic."""
        # First, publish a long "canary" article so we know Flink is processing
        canary = _make_event(section="technology", body=LONG_BODY)
        short_event = _make_event(section="technology", body=SHORT_BODY)

        offsets_tech = _get_end_offsets(TOPIC_TECH)

        # Publish short first, then canary — if canary arrives, Flink has
        # processed both and the short one was intentionally dropped.
        _publish_to_raw([short_event, canary])

        msgs = _wait_for_messages(TOPIC_TECH, offsets_tech, min_count=1, timeout_s=30.0)
        canary_found = any(m["article_id"] == canary["article_id"] for m in msgs)
        assert canary_found, "Canary article did not arrive — Flink may not be processing"

        short_found = any(m["article_id"] == short_event["article_id"] for m in msgs)
        assert not short_found, f"Short article should be filtered out but was found"

    def test_long_article_passes(self):
        """An article with 250 words should arrive on the correct topic."""
        offsets_tech = _get_end_offsets(TOPIC_TECH)
        event = _make_event(section="technology", body=LONG_BODY)
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_TECH, offsets_tech)
        ids = [m["article_id"] for m in msgs]
        assert event["article_id"] in ids


# ------------------------------------------------------------------
# Tests — Field preservation
# ------------------------------------------------------------------

@skip_no_flink
class TestFlinkFieldPreservation:
    """Verify that all article fields survive the Flink pipeline."""

    def test_all_fields_preserved(self):
        offsets_tech = _get_end_offsets(TOPIC_TECH)
        event = _make_event(
            section="technology",
            headline="Preservation — unicode 日本語 🚀",
            source_url="https://example.com/preservation-test",
        )
        _publish_to_raw([event])

        msgs = _wait_for_messages(TOPIC_TECH, offsets_tech)
        match = [m for m in msgs if m["article_id"] == event["article_id"]]
        assert len(match) == 1

        got = match[0]
        assert got["headline"] == event["headline"]
        assert got["body"] == event["body"]
        assert got["section"] == event["section"]
        assert got["published_at"] == event["published_at"]
        assert got["source_url"] == event["source_url"]
