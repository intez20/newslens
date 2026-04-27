"""Environment-based configuration for the enrichment worker."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class EnrichmentConfig:
    bootstrap_servers: str
    topics_input: tuple[str, ...]
    topic_failed: str
    llm_backend: str
    ollama_base_url: str
    ollama_model: str
    groq_api_key: str
    groq_model: str
    llm_timeout_seconds: int
    consumer_group_id: str

    @classmethod
    def from_env(cls) -> "EnrichmentConfig":
        return cls(
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
            ),
            topics_input=(
                os.environ.get("KAFKA_TOPIC_TECH", "tech-news"),
                os.environ.get("KAFKA_TOPIC_FINANCE", "finance-news"),
                os.environ.get("KAFKA_TOPIC_WORLD", "world-news"),
            ),
            topic_failed=os.environ.get("KAFKA_TOPIC_FAILED", "news-failed"),
            llm_backend=os.environ.get("LLM_BACKEND", "ollama"),
            ollama_base_url=os.environ.get(
                "OLLAMA_BASE_URL", "http://ollama:11434"
            ),
            ollama_model=os.environ.get("OLLAMA_MODEL", "mistral"),
            groq_api_key=os.environ.get("GROQ_API_KEY", ""),
            groq_model=os.environ.get("GROQ_MODEL", "llama3-8b-8192"),
            llm_timeout_seconds=int(
                os.environ.get("LLM_TIMEOUT_SECONDS", "30")
            ),
            consumer_group_id=os.environ.get(
                "ENRICHMENT_CONSUMER_GROUP", "enrichment-worker"
            ),
        )
