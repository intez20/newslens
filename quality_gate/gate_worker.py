"""Quality gate worker — validates enriched articles and generates embeddings."""

import json
import logging
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from quality_gate.config import QualityGateConfig
from quality_gate.embedder import ArticleEmbedder
from quality_gate.expectations import ArticleExpectationSuite
from quality_gate.models import ValidatedArticle

logger = logging.getLogger(__name__)


class QualityGateWorker:
    """Kafka consumer loop that validates enriched articles.

    Consumes from enriched-news. For each article:
    1. Runs GX expectation suite (summary, sentiment, recency)
    2. Generates 384-dim embedding via MiniLM-L6-v2
    3. Validates via ValidatedArticle Pydantic model
    4. Publishes to validated-news or dead-letters to news-failed
    """

    def __init__(
        self,
        config: QualityGateConfig,
        expectations: ArticleExpectationSuite,
        embedder: ArticleEmbedder,
    ):
        self.config = config
        self.expectations = expectations
        self.embedder = embedder
        self.consumer = KafkaConsumer(
            config.topic_input,
            bootstrap_servers=config.bootstrap_servers,
            group_id=config.consumer_group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        self.output_producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.failed_producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def run(self):
        """Main consumer loop — runs until interrupted."""
        logger.info(
            "quality gate worker started — consuming %s",
            self.config.topic_input,
        )
        try:
            for message in self.consumer:
                try:
                    validated = self.process_article(message.value)
                    self.output_producer.send(
                        self.config.topic_output, value=validated
                    )
                    logger.info(
                        "validated %s | sentiment=%s | tag=%s",
                        validated["article_id"],
                        validated["sentiment"],
                        validated["domain_tag"],
                    )
                except Exception as exc:
                    self._send_to_dead_letter(message.value, str(exc))
        finally:
            self.consumer.close()
            self.output_producer.close()
            self.failed_producer.close()

    def process_article(self, enriched: dict) -> dict:
        """Validate, embed, and return a validated article dict.

        Args:
            enriched: Deserialized enriched article from Kafka.

        Returns:
            dict matching ValidatedArticle schema.

        Raises:
            ValueError: On GX validation failure or missing fields.
        """
        # Fail fast on missing required fields
        for field in ("article_id", "headline", "summary"):
            if field not in enriched or not enriched[field]:
                raise ValueError(f"missing required field: {field}")

        # Phase 1: GX validation on enriched fields
        passed, failures = self.expectations.validate(enriched)
        if not passed:
            raise ValueError(f"GX validation failed: {'; '.join(failures)}")

        # Phase 2: Generate embedding
        headline = enriched.get("headline", "")
        summary = enriched.get("summary", "")
        embedding = self.embedder.embed(headline, summary)

        # Phase 3: Attach embedding + metadata
        enriched["embedding"] = embedding
        enriched["validated_at"] = datetime.now(timezone.utc).isoformat()

        # Phase 4: Final Pydantic validation
        validated = ValidatedArticle(**enriched)
        return validated.model_dump(mode="json")

    def _send_to_dead_letter(self, event: dict, error: str):
        """Route a failed event to the news-failed topic."""
        event["_error"] = error
        event["_failed_at"] = datetime.now(timezone.utc).isoformat()
        event["_failed_stage"] = "quality-gate"
        self.failed_producer.send(self.config.topic_failed, value=event)
        logger.error(
            "dead-letter %s: %s", event.get("article_id", "unknown"), error
        )
