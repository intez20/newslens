"""Unit tests for KafkaPublisher — all Kafka interactions are mocked."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
from kafka.errors import KafkaError, NoBrokersAvailable

from producer.kafka_publisher import KafkaPublisher
from producer.models import ArticleEvent


# ------------------------------------------------------------------ #
# Fixtures                                                            #
# ------------------------------------------------------------------ #

@pytest.fixture
def sample_event() -> ArticleEvent:
    """A minimal valid ArticleEvent for testing."""
    return ArticleEvent(
        article_id="technology/2024/jan/15/test-article",
        headline="Test Article Headline",
        body="Article body text " * 50,
        section="technology",
        published_at=datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
        source_url="https://www.theguardian.com/technology/2024/test",
    )


@pytest.fixture
def sample_events(sample_event: ArticleEvent) -> list[ArticleEvent]:
    """Five distinct ArticleEvent objects for batch tests."""
    events = []
    for i in range(5):
        events.append(
            ArticleEvent(
                article_id=f"technology/2024/jan/15/test-article-{i}",
                headline=f"Test Article {i}",
                body="Article body text " * 50,
                section="technology",
                published_at=datetime(2024, 1, 15, 10, 30 + i, tzinfo=timezone.utc),
                source_url=f"https://www.theguardian.com/technology/2024/test-{i}",
            )
        )
    return events


@pytest.fixture
def mock_producer():
    """Patch KafkaProducer so no real broker is needed."""
    with patch("producer.kafka_publisher.KafkaProducer") as MockCls:
        instance = MockCls.return_value
        # .send() returns a future whose .get() returns metadata
        future = MagicMock()
        future.get.return_value = MagicMock(topic="raw-news", partition=0, offset=42)
        instance.send.return_value = future
        yield instance


@pytest.fixture
def publisher(mock_producer, tmp_path) -> KafkaPublisher:
    """KafkaPublisher wired to the mocked producer and a temp buffer path."""
    pub = KafkaPublisher.__new__(KafkaPublisher)
    pub.bootstrap_servers = "localhost:9092"
    pub.topic = "raw-news"
    pub.buffer_path = tmp_path / "failed_events.jsonl"
    pub._producer = mock_producer
    return pub


# ------------------------------------------------------------------ #
# Happy path                                                          #
# ------------------------------------------------------------------ #

def test_publish_single_event(publisher, mock_producer, sample_event):
    """publish() calls KafkaProducer.send() with correct topic, key, value."""
    result = publisher.publish(sample_event)

    assert result is True
    mock_producer.send.assert_called_once()
    call_kwargs = mock_producer.send.call_args
    assert call_kwargs[0][0] == "raw-news"  # positional: topic
    assert call_kwargs[1]["key"] == sample_event.article_id.encode("utf-8")
    assert call_kwargs[1]["value"] == sample_event.model_dump_json().encode("utf-8")


def test_publish_serialization(publisher, mock_producer, sample_event):
    """Value sent to Kafka deserializes back to the same ArticleEvent."""
    publisher.publish(sample_event)

    sent_value = mock_producer.send.call_args[1]["value"]
    roundtrip = ArticleEvent.model_validate_json(sent_value)
    assert roundtrip.article_id == sample_event.article_id
    assert roundtrip.headline == sample_event.headline
    assert roundtrip.section == sample_event.section
    assert roundtrip.published_at == sample_event.published_at


def test_publish_key_is_article_id(publisher, mock_producer, sample_event):
    """Kafka key is article_id encoded as UTF-8 bytes."""
    publisher.publish(sample_event)

    sent_key = mock_producer.send.call_args[1]["key"]
    assert sent_key == sample_event.article_id.encode("utf-8")


def test_publish_returns_true_on_success(publisher, sample_event):
    """publish() returns True when Kafka accepts the message."""
    assert publisher.publish(sample_event) is True


# ------------------------------------------------------------------ #
# Failure path                                                        #
# ------------------------------------------------------------------ #

def test_publish_returns_false_on_failure(publisher, mock_producer, sample_event):
    """publish() returns False when send raises KafkaError."""
    mock_producer.send.return_value.get.side_effect = KafkaError("broker down")

    assert publisher.publish(sample_event) is False


def test_failed_event_buffered_to_jsonl(publisher, mock_producer, sample_event):
    """Failed events are written to the .jsonl buffer file."""
    mock_producer.send.return_value.get.side_effect = KafkaError("broker down")

    publisher.publish(sample_event)

    assert publisher.buffer_path.exists()
    lines = publisher.buffer_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    buffered = json.loads(lines[0])
    assert buffered["article_id"] == sample_event.article_id


def test_buffer_file_is_append_only(publisher, mock_producer, sample_events):
    """Multiple failures append separate lines — nothing is overwritten."""
    mock_producer.send.return_value.get.side_effect = KafkaError("broker down")

    for event in sample_events[:3]:
        publisher.publish(event)

    lines = publisher.buffer_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 3
    ids = [json.loads(line)["article_id"] for line in lines]
    assert ids == [e.article_id for e in sample_events[:3]]


def test_buffer_file_created_on_first_failure(publisher, mock_producer, sample_event):
    """Buffer file does not exist until the first failure."""
    assert not publisher.buffer_path.exists()

    # Successful publish — no buffer file
    publisher.publish(sample_event)
    assert not publisher.buffer_path.exists()

    # Now fail
    mock_producer.send.return_value.get.side_effect = KafkaError("broker down")
    publisher.publish(sample_event)
    assert publisher.buffer_path.exists()


# ------------------------------------------------------------------ #
# Batch                                                               #
# ------------------------------------------------------------------ #

def test_publish_batch_counts(publisher, mock_producer, sample_events):
    """publish_batch() returns correct sent/buffered counts."""
    call_count = 0

    def alternating_get(timeout=10):
        nonlocal call_count
        call_count += 1
        if call_count in (2, 4):  # events at index 1 and 3 fail
            raise KafkaError("intermittent failure")
        return MagicMock()

    mock_producer.send.return_value.get.side_effect = alternating_get

    result = publisher.publish_batch(sample_events)

    assert result == {"sent": 3, "buffered": 2}


# ------------------------------------------------------------------ #
# Lifecycle                                                           #
# ------------------------------------------------------------------ #

def test_flush_calls_producer_flush(publisher, mock_producer):
    """flush() delegates to KafkaProducer.flush() with timeout."""
    publisher.flush(timeout=5.0)

    mock_producer.flush.assert_called_once_with(timeout=5.0)


def test_close_flushes_and_closes(publisher, mock_producer):
    """close() calls flush then close on the underlying producer."""
    publisher.close()

    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()


# ------------------------------------------------------------------ #
# Extreme: no broker at all                                           #
# ------------------------------------------------------------------ #

def test_publish_with_no_broker(tmp_path, sample_event):
    """When KafkaProducer init fails, all events go to buffer."""
    with patch(
        "producer.kafka_publisher.KafkaProducer",
        side_effect=NoBrokersAvailable(),
    ):
        pub = KafkaPublisher(
            bootstrap_servers="localhost:9092",
            topic="raw-news",
            buffer_path=str(tmp_path / "failed_events.jsonl"),
        )

    assert pub._producer is None

    result = pub.publish(sample_event)
    assert result is False

    buffer = Path(tmp_path / "failed_events.jsonl")
    assert buffer.exists()
    lines = buffer.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["article_id"] == sample_event.article_id
