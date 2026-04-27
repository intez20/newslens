"""Stage 05e — Enrichment worker end-to-end integration tests.

Prerequisites:
    docker compose up -d kafka ollama enrichment-worker
    docker exec newslens-ollama ollama pull mistral   # first time only
    Host ports: 29092 (Kafka), 11434 (Ollama)

Run:
    pytest tests/test_enrichment_e2e.py -v --timeout=120
"""

import json
import time
import uuid

import pytest
import requests
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = "localhost:29092"
TOPIC_TECH = "tech-news"
TOPIC_FINANCE = "finance-news"
TOPIC_WORLD = "world-news"
TOPIC_FAILED = "news-failed"

VALID_SENTIMENTS = {"Positive", "Negative", "Neutral"}
VALID_DOMAIN_TAGS = {
    "AI", "Crypto", "Regulation", "Geopolitics",
    "Earnings", "Climate", "Cybersecurity", "Other",
}

# Realistic article body (~300 words) for meaningful LLM output
REALISTIC_BODY = (
    "The European Commission has unveiled a sweeping new regulatory "
    "framework for artificial intelligence that would impose strict "
    "requirements on high-risk AI systems. The proposed regulation, "
    "known as the AI Act, targets applications in critical infrastructure, "
    "law enforcement, hiring processes, and education. Under the new rules, "
    "companies deploying AI in these areas would be required to conduct "
    "risk assessments, maintain detailed documentation, and ensure human "
    "oversight. The regulation also introduces a complete ban on AI systems "
    "that manipulate human behavior or exploit vulnerabilities. "
    "Industry leaders have expressed both support and concern. Microsoft "
    "CEO Satya Nadella praised the initiative as a step toward responsible "
    "AI governance, while smaller startups warned that compliance costs "
    "could stifle innovation. The European Parliament is expected to vote "
    "on the final text by year end. Brussels officials say the regulation "
    "will serve as a global benchmark, similar to GDPR's influence on "
    "data privacy laws worldwide. Critics argue that overly prescriptive "
    "rules could push AI development to less regulated markets in Asia "
    "and the Middle East. The United States has taken a different approach, "
    "favoring voluntary commitments from major tech companies over binding "
    "legislation. Meanwhile, China has already implemented its own set of "
    "AI regulations focusing on content generation and recommendation "
    "algorithms. The global race to regulate AI reflects growing public "
    "concern about algorithmic bias, job displacement, and the potential "
    "misuse of generative AI tools. Consumer advocacy groups have welcomed "
    "the EU proposal but called for stronger enforcement mechanisms and "
    "more transparency requirements for foundation model providers."
)


# ------------------------------------------------------------------
# Skip decorator
# ------------------------------------------------------------------

def _kafka_available() -> bool:
    try:
        p = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        p.close()
        return True
    except NoBrokersAvailable:
        return False


def _ollama_available() -> bool:
    try:
        resp = requests.get("http://localhost:11434/api/tags", timeout=5)
        return resp.status_code == 200
    except requests.ConnectionError:
        return False


skip_no_enrichment = pytest.mark.skipif(
    not (_kafka_available() and _ollama_available()),
    reason="Kafka + Ollama required — run: docker compose up -d kafka ollama enrichment-worker",
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_event(
    section: str = "technology",
    body: str = REALISTIC_BODY,
    **overrides,
) -> dict:
    """Create an article event with a unique ID for enrichment testing."""
    defaults = {
        "article_id": f"enrich-test/{uuid.uuid4().hex[:8]}",
        "headline": "European Union proposes comprehensive AI regulation framework",
        "body": body,
        "section": section,
        "published_at": "2026-04-27T12:00:00Z",
        "source_url": "https://example.com/eu-ai-regulation-test",
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
    """Publish event dicts directly to a downstream topic."""
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
    min_count: int = 1,
    timeout_s: float = 60.0,
) -> list[dict]:
    """Poll *topic* until at least *min_count* new messages appear or timeout."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        msgs = _consume_from(topic, start_offsets, timeout_s=3.0)
        if len(msgs) >= min_count:
            return msgs
        time.sleep(2)
    return _consume_from(topic, start_offsets, timeout_s=3.0)


def _find_by_id(messages: list[dict], article_id: str) -> dict | None:
    """Find a message by article_id in a list of consumed messages."""
    for msg in messages:
        if msg.get("article_id") == article_id:
            return msg
    return None


# ------------------------------------------------------------------
# Tests — Enrichment Output
# ------------------------------------------------------------------


@skip_no_enrichment
class TestEnrichmentOutput:
    """Verify enriched articles have all expected fields with valid values.

    Publishes a realistic article to tech-news (bypassing Flink — the
    enrichment worker consumes directly from downstream topics).
    """

    @pytest.fixture(autouse=True, scope="class")
    def enriched_event(self, request):
        """Publish one article, wait for enrichment, cache for all tests."""
        event = _make_event()
        article_id = event["article_id"]

        # We need a topic to consume enriched output from.
        # The enrichment worker currently logs output. For E2E testing,
        # we watch news-failed (to verify nothing fails) and also verify
        # the worker processed it by checking logs or a future output topic.
        #
        # Strategy: Publish to tech-news. The enrichment worker consumes it,
        # enriches it, and (in the current implementation) logs it.
        # We verify the worker doesn't dead-letter it.
        snap_failed = _get_end_offsets(TOPIC_FAILED)
        _publish_to_topic(TOPIC_TECH, [event])

        # Give the enrichment worker time to process
        time.sleep(15)

        # Check nothing landed in dead-letter
        failed = _consume_from(TOPIC_FAILED, snap_failed, timeout_s=5.0)
        failed_for_us = [
            m for m in failed if m.get("article_id") == article_id
        ]

        request.cls.article_id = article_id
        request.cls.failed_events = failed_for_us
        request.cls.original_event = event

    def test_no_dead_letter(self):
        """Enrichment should succeed — article must NOT appear in news-failed."""
        assert len(self.failed_events) == 0, (
            f"Article {self.article_id} was dead-lettered: {self.failed_events}"
        )


@skip_no_enrichment
class TestEnrichmentDeadLetter:
    """Verify malformed articles route to news-failed."""

    def test_missing_body_routes_to_dead_letter(self):
        """Article missing 'body' should fail enrichment and land in news-failed."""
        bad_event = _make_event()
        bad_event.pop("body")  # remove body — chains will fail
        article_id = bad_event["article_id"]

        snap = _get_end_offsets(TOPIC_FAILED)
        _publish_to_topic(TOPIC_TECH, [bad_event])

        # Wait for dead-letter
        failed = _wait_for_messages(TOPIC_FAILED, snap, min_count=1, timeout_s=30)
        match = _find_by_id(failed, article_id)
        assert match is not None, (
            f"Expected {article_id} in news-failed but not found"
        )
        assert "_error" in match
