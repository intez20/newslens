"""Environment-based configuration for the Weaviate ingestion worker."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WeaviateStoreConfig:
    weaviate_url: str
    weaviate_grpc_url: str
    collection_name: str
    bootstrap_servers: str
    topic_input: str
    topic_failed: str
    consumer_group_id: str

    @classmethod
    def from_env(cls) -> "WeaviateStoreConfig":
        return cls(
            weaviate_url=os.environ.get(
                "WEAVIATE_URL", "http://weaviate:8080"
            ),
            weaviate_grpc_url=os.environ.get(
                "WEAVIATE_GRPC_URL", "weaviate:50051"
            ),
            collection_name=os.environ.get(
                "WEAVIATE_COLLECTION", "NewsArticle"
            ),
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
            ),
            topic_input=os.environ.get(
                "KAFKA_TOPIC_VALIDATED", "validated-news"
            ),
            topic_failed=os.environ.get("KAFKA_TOPIC_FAILED", "news-failed"),
            consumer_group_id=os.environ.get(
                "WEAVIATE_INGESTION_CONSUMER_GROUP", "weaviate-ingestion"
            ),
        )
