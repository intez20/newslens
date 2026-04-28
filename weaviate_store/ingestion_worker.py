"""Ingestion worker — consumes validated articles from Kafka, upserts to Weaviate."""

import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from weaviate_store.client import WeaviateNewsClient
from weaviate_store.config import WeaviateStoreConfig

logger = logging.getLogger(__name__)

# Fields to store in Weaviate (excludes body, enriched_at, validated_at, embedding)
WEAVIATE_FIELDS = {
    "article_id",
    "headline",
    "summary",
    "entities",
    "sentiment",
    "sentiment_reason",
    "domain_tag",
    "section",
    "published_at",
    "source_url",
}


class IngestionWorker:
    """Kafka consumer loop that upserts validated articles into Weaviate.

    For each article from validated-news:
    1. Check if article_id already exists in Weaviate (dedup)
    2. Extract embedding vector and Weaviate-mapped properties
    3. Upsert into NewsArticle collection
    4. Dead-letter on failure
    """

    def __init__(
        self,
        config: WeaviateStoreConfig,
        weaviate_client: WeaviateNewsClient,
    ):
        self.config = config
        self.weaviate_client = weaviate_client
        self.consumer = KafkaConsumer(
            config.topic_input,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.consumer_group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.failed_producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def run(self):
        """Main consumer loop — runs until interrupted."""
        logger.info(
            "ingestion worker started — consuming %s", self.config.topic_input
        )
        try:
            for message in self.consumer:
                try:
                    self._process(message.value)
                except Exception as exc:
                    self._send_to_dead_letter(message.value, str(exc))
        finally:
            self.consumer.close()
            self.failed_producer.close()
            self.weaviate_client.close()

    def _process(self, article: dict) -> None:
        """Validate, dedup, and upsert a single article."""
        article_id = article.get("article_id")
        if not article_id:
            raise ValueError("missing required field: article_id")

        # Dedup: skip if already in Weaviate
        if self.weaviate_client.exists(article_id):
            logger.debug("skip duplicate %s", article_id)
            return

        # Extract embedding vector
        embedding = article.get("embedding")
        if not embedding or not isinstance(embedding, list):
            raise ValueError("missing or invalid embedding")

        # Map to Weaviate properties (only the fields we store)
        properties = {
            k: v for k, v in article.items() if k in WEAVIATE_FIELDS
        }

        uuid = self.weaviate_client.upsert_article(properties, embedding)
        logger.info(
            "ingested %s | section=%s | tag=%s | uuid=%s",
            article_id,
            properties.get("section", "?"),
            properties.get("domain_tag", "?"),
            uuid,
        )

    def _send_to_dead_letter(self, event: dict, error: str):
        """Route a failed event to the news-failed topic."""
        event["_error"] = error
        event["_failed_at"] = datetime.now(timezone.utc).isoformat()
        event["_failed_stage"] = "weaviate-ingestion"
        self.failed_producer.send(self.config.topic_failed, value=event)
        logger.error(
            "dead-letter %s: %s", event.get("article_id", "unknown"), error
        )
