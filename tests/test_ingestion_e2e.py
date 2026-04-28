"""E2E integration test — validated article → Kafka → IngestionWorker → Weaviate.

Requires running Kafka (localhost:9092) and Weaviate (localhost:8085).
Skip with: pytest -m "not integration"
"""

import json
import os
import time
import uuid

import pytest
import weaviate

from kafka import KafkaProducer, KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WEAVIATE_REST = os.getenv("WEAVIATE_URL_HOST", "http://localhost:8085")
TOPIC = "validated-news"
COLLECTION = "NewsArticle"


def _kafka_available():
    try:
        p = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, request_timeout_ms=3000)
        p.close(timeout=3)
        return True
    except Exception:
        return False


def _weaviate_available():
    try:
        client = weaviate.connect_to_local(host="localhost", port=8085, grpc_port=50051)
        client.close()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(
    not (_kafka_available() and _weaviate_available()),
    reason="Kafka and/or Weaviate not available",
)


def _make_article(article_id: str) -> dict:
    return {
        "article_id": article_id,
        "headline": f"Integration Test Article {article_id[:8]}",
        "summary": "This is a test summary for integration testing.",
        "body": "Full body text — should NOT appear in Weaviate.",
        "entities": ["TestOrg", "TestPerson"],
        "sentiment": "neutral",
        "sentiment_reason": "Integration test placeholder",
        "domain_tag": "tech",
        "section": "testing",
        "published_at": "2026-01-15T12:00:00Z",
        "source_url": f"https://example.com/test/{article_id}",
        "enriched_at": "2026-01-15T12:01:00Z",
        "validated_at": "2026-01-15T12:02:00Z",
        "embedding": [0.1] * 384,
    }


@requires_infra
class TestIngestionE2E:
    """End-to-end: publish validated article → ingestion worker → Weaviate."""

    @pytest.fixture(autouse=True)
    def _setup_weaviate(self):
        """Connect to Weaviate and ensure collection exists."""
        from weaviate_store.schema import ensure_collection
        self.weaviate_client = weaviate.connect_to_local(
            host="localhost", port=8085, grpc_port=50051
        )
        ensure_collection(self.weaviate_client, COLLECTION)
        yield
        self.weaviate_client.close()

    def test_produce_ingest_query(self):
        """Publish an article to Kafka, let ingestion worker consume it, query from Weaviate."""
        article_id = str(uuid.uuid4())
        article = _make_article(article_id)

        # 1. Produce to validated-news
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(TOPIC, value=article)
        producer.flush()
        producer.close()

        # 2. Wait for ingestion (worker must be running via docker compose)
        time.sleep(5)

        # 3. Query Weaviate for the article
        collection = self.weaviate_client.collections.get(COLLECTION)
        result = collection.query.fetch_objects(
            filters=weaviate.classes.query.Filter.by_property("article_id").equal(article_id),
            limit=1,
        )

        assert len(result.objects) == 1, f"Expected 1 object, got {len(result.objects)}"
        obj = result.objects[0]
        assert obj.properties["headline"] == article["headline"]
        assert obj.properties["domain_tag"] == "tech"
        assert "body" not in obj.properties  # body excluded

    def test_duplicate_idempotent(self):
        """Sending same article twice should result in single Weaviate object."""
        article_id = str(uuid.uuid4())
        article = _make_article(article_id)

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(TOPIC, value=article)
        producer.send(TOPIC, value=article)
        producer.flush()
        producer.close()

        time.sleep(6)

        collection = self.weaviate_client.collections.get(COLLECTION)
        result = collection.query.fetch_objects(
            filters=weaviate.classes.query.Filter.by_property("article_id").equal(article_id),
            limit=5,
        )

        assert len(result.objects) == 1, f"Expected 1 (dedup), got {len(result.objects)}"

    def test_vector_search_returns_ingested(self):
        """After ingestion, near-vector search should find the article."""
        article_id = str(uuid.uuid4())
        article = _make_article(article_id)

        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send(TOPIC, value=article)
        producer.flush()
        producer.close()

        time.sleep(5)

        collection = self.weaviate_client.collections.get(COLLECTION)
        results = collection.query.near_vector(
            near_vector=[0.1] * 384,
            limit=5,
        )

        found_ids = [o.properties.get("article_id") for o in results.objects]
        assert article_id in found_ids, f"Article {article_id} not found in vector search"
