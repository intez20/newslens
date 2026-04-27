"""Enrichment worker — consumes articles, runs LLM chains, routes failures."""

import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from enrichment.chains import EnrichmentChains
from enrichment.config import EnrichmentConfig
from enrichment.models import EnrichedArticleEvent

logger = logging.getLogger(__name__)

# Maximum body characters sent to the LLM (keeps within context window)
BODY_TRUNCATE_CHARS = 1500


class EnrichmentWorker:
    """Kafka consumer loop that enriches articles via LangChain chains.

    Consumes from tech-news, finance-news, world-news. For each article:
    1. Builds context string (headline + truncated body)
    2. Runs 4 LLM chains in parallel via EnrichmentChains
    3. Merges enrichment into original event
    4. Validates via EnrichedArticleEvent Pydantic model
    5. On failure → routes to news-failed dead-letter topic
    """

    def __init__(self, config: EnrichmentConfig, chains: EnrichmentChains):
        self.config = config
        self.chains = chains
        self.consumer = KafkaConsumer(
            *config.topics_input,
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
            "enrichment worker started — consuming %s", self.config.topics_input
        )
        try:
            for message in self.consumer:
                try:
                    enriched = self.process_article(message.value)
                    logger.info(
                        "enriched %s | section=%s | sentiment=%s | tag=%s",
                        enriched["article_id"],
                        enriched["section"],
                        enriched["sentiment"],
                        enriched["domain_tag"],
                    )
                except Exception as exc:
                    self._send_to_dead_letter(message.value, str(exc))
        finally:
            self.consumer.close()
            self.failed_producer.close()

    def process_article(self, raw: dict) -> dict:
        """Enrich a single article and return validated dict.

        Args:
            raw: Deserialized article event from Kafka.

        Returns:
            dict matching EnrichedArticleEvent schema.

        Raises:
            Exception: On LLM failure, JSON parse error, or validation error.
        """
        context = self._build_context(raw)
        result = self.chains.enrich(context)

        # Merge enrichment into original event
        raw["summary"] = result["summary"]
        raw["entities"] = result["entities"]
        raw["sentiment"] = result["sentiment"]["sentiment"]
        raw["sentiment_reason"] = result["sentiment"]["reason"]
        raw["domain_tag"] = result["domain_tag"]
        raw["enriched_at"] = datetime.now(timezone.utc).isoformat()

        # Validate via Pydantic — raises ValidationError on bad data
        validated = EnrichedArticleEvent(**raw)
        return validated.model_dump(mode="json")

    def _build_context(self, raw: dict) -> str:
        """Build the LLM context string: headline + truncated body."""
        headline = raw.get("headline", "")
        body = raw.get("body", "")[:BODY_TRUNCATE_CHARS]
        return f"{headline}\n\n{body}"

    def _send_to_dead_letter(self, event: dict, error: str):
        """Route a failed event to the news-failed topic."""
        event["_error"] = error
        event["_failed_at"] = datetime.now(timezone.utc).isoformat()
        self.failed_producer.send(self.config.topic_failed, value=event)
        logger.error(
            "dead-letter %s: %s", event.get("article_id", "unknown"), error
        )
