"""KafkaPublisher — publishes ArticleEvent objects to Kafka with .jsonl failure buffer."""

import json
import logging
import os
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from producer.models import ArticleEvent

logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Publishes ArticleEvent objects to Kafka with failure buffering.

    When Kafka is unreachable, events are appended to a local .jsonl file
    so they can be replayed later — ensuring zero data loss.
    """

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        buffer_path: str | None = None,
    ):
        self.bootstrap_servers = (
            bootstrap_servers
            if bootstrap_servers is not None
            else os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        )
        self.topic = (
            topic if topic is not None else os.getenv("KAFKA_TOPIC", "raw-news")
        )
        self.buffer_path = Path(
            buffer_path
            if buffer_path is not None
            else os.getenv("KAFKA_BUFFER_PATH", "buffer/failed_events.jsonl")
        )

        self._producer: KafkaProducer | None = None
        self._connect()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Try to create a KafkaProducer. If broker is unreachable, log and
        continue — all publishes will go to the buffer file."""
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # We serialize manually via Pydantic, so value/key are raw bytes
                value_serializer=None,
                key_serializer=None,
            )
            logger.info("KafkaProducer connected to %s", self.bootstrap_servers)
        except NoBrokersAvailable:
            logger.warning(
                "No Kafka brokers available at %s — all events will be buffered to %s",
                self.bootstrap_servers,
                self.buffer_path,
            )
            self._producer = None

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def publish(self, event: ArticleEvent) -> bool:
        """Publish a single event to Kafka.

        Returns True if Kafka accepted the message, False if it was buffered.
        """
        key = event.article_id.encode("utf-8")
        value = event.model_dump_json().encode("utf-8")

        if self._producer is None:
            self._buffer_event(event)
            return False

        try:
            future = self._producer.send(self.topic, key=key, value=value)
            # Block briefly to confirm delivery (synchronous for reliability)
            future.get(timeout=10)
            logger.debug(
                "Published %s to %s", event.article_id, self.topic
            )
            return True
        except KafkaError as exc:
            logger.error(
                "Failed to publish %s: %s — buffering to %s",
                event.article_id,
                exc,
                self.buffer_path,
            )
            self._buffer_event(event)
            return False

    def publish_batch(self, events: list[ArticleEvent]) -> dict:
        """Publish a list of events. Returns {"sent": N, "buffered": N}."""
        sent = 0
        buffered = 0
        for event in events:
            if self.publish(event):
                sent += 1
            else:
                buffered += 1
        return {"sent": sent, "buffered": buffered}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def flush(self, timeout: float = 10.0) -> None:
        """Block until all in-flight messages are delivered or timeout."""
        if self._producer is not None:
            self._producer.flush(timeout=timeout)

    def close(self) -> None:
        """Flush remaining messages and close the producer."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            logger.info("KafkaProducer closed")

    # ------------------------------------------------------------------
    # Failure buffer
    # ------------------------------------------------------------------

    def _buffer_event(self, event: ArticleEvent) -> None:
        """Append a failed event to the .jsonl buffer file."""
        self.buffer_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.buffer_path, "a", encoding="utf-8") as f:
            f.write(event.model_dump_json() + "\n")
        logger.warning("Buffered %s to %s", event.article_id, self.buffer_path)
