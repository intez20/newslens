"""Environment-based configuration for the quality gate worker."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class QualityGateConfig:
    bootstrap_servers: str
    topic_input: str
    topic_output: str
    topic_failed: str
    consumer_group_id: str
    embedding_model: str
    embedding_dimension: int
    max_recency_days: int
    min_summary_length: int
    max_summary_length: int

    @classmethod
    def from_env(cls) -> "QualityGateConfig":
        return cls(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
            ),
            topic_input=os.environ.get(
                "KAFKA_TOPIC_ENRICHED", "enriched-news"
            ),
            topic_output=os.environ.get(
                "KAFKA_TOPIC_VALIDATED", "validated-news"
            ),
            topic_failed=os.environ.get("KAFKA_TOPIC_FAILED", "news-failed"),
            consumer_group_id=os.environ.get(
                "QUALITY_GATE_CONSUMER_GROUP", "quality-gate"
            ),
            embedding_model=os.environ.get(
                "EMBEDDING_MODEL", "all-MiniLM-L6-v2"
            ),
            embedding_dimension=int(
                os.environ.get("EMBEDDING_DIMENSION", "384")
            ),
            max_recency_days=int(
                os.environ.get("MAX_RECENCY_DAYS", "7")
            ),
            min_summary_length=int(
                os.environ.get("MIN_SUMMARY_LENGTH", "50")
            ),
            max_summary_length=int(
                os.environ.get("MAX_SUMMARY_LENGTH", "500")
            ),
        )
