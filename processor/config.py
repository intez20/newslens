"""Environment-based configuration for the PyFlink stream processor."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ProcessorConfig:
    bootstrap_servers: str
    topic_raw: str
    topic_tech: str
    topic_finance: str
    topic_world: str
    checkpoint_dir: str
    parallelism: int
    checkpoint_interval_ms: int

    @classmethod
    def from_env(cls) -> "ProcessorConfig":
        return cls(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic_raw=os.environ.get("KAFKA_TOPIC_RAW", "raw-news"),
            topic_tech=os.environ.get("KAFKA_TOPIC_TECH", "tech-news"),
            topic_finance=os.environ.get("KAFKA_TOPIC_FINANCE", "finance-news"),
            topic_world=os.environ.get("KAFKA_TOPIC_WORLD", "world-news"),
            checkpoint_dir=os.environ.get(
                "FLINK_CHECKPOINT_DIR", "/tmp/flink-checkpoints"
            ),
            parallelism=int(os.environ.get("FLINK_PARALLELISM", "1")),
            checkpoint_interval_ms=int(
                os.environ.get("FLINK_CHECKPOINT_INTERVAL_MS", "60000")
            ),
        )
