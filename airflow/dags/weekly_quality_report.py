"""DAG: weekly_quality_report — aggregate quality gate pass/fail rates."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "newslens",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="weekly_quality_report",
    default_args=DEFAULT_ARGS,
    description="Aggregate quality gate pass/fail rates for the past week",
    schedule="0 9 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["quality", "reporting"],
)
def weekly_quality_report():

    @task()
    def count_validated(**context):
        """Count articles in Weaviate from last 7 days."""
        import os
        import weaviate
        from weaviate.classes.query import Filter
        from weaviate.classes.aggregate import GroupByAggregate
        from urllib.parse import urlparse

        url = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
        grpc_url = os.getenv("WEAVIATE_GRPC_URL", "weaviate:50051")
        collection_name = os.getenv("WEAVIATE_COLLECTION", "NewsArticle")

        parsed = urlparse(url)
        host = parsed.hostname or "weaviate"
        port = parsed.port or 8080
        grpc_host, grpc_port_str = grpc_url.split(":")
        grpc_port = int(grpc_port_str)

        since = datetime.now(timezone.utc) - timedelta(days=7)

        client = weaviate.connect_to_custom(
            http_host=host, http_port=port, http_secure=False,
            grpc_host=grpc_host, grpc_port=grpc_port, grpc_secure=False,
        )
        try:
            collection = client.collections.get(collection_name)
            result = collection.aggregate.over_all(
                filters=Filter.by_property("published_at").greater_than(since.isoformat()),
                total_count=True,
            )
            count = result.total_count or 0
            logger.info("Validated articles (last 7d): %d", count)
            return {"validated_count": count}
        finally:
            client.close()

    @task()
    def count_dead_letters(**context):
        """Count messages in news-failed topic from last 7 days."""
        import os
        from kafka import KafkaConsumer

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        topic = os.getenv("KAFKA_TOPIC_FAILED", "news-failed")

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=10000,
            group_id=None,  # no commit, just count
        )
        count = 0
        cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
        for msg in consumer:
            if msg.timestamp and msg.timestamp >= cutoff_ms:
                count += 1
        consumer.close()

        logger.info("Dead-letter messages (last 7d): %d", count)
        return {"dead_letter_count": count}

    @task()
    def compute_pass_rate(validated: dict, dead_letters: dict):
        """Calculate pass rate: validated / (validated + failed)."""
        v = validated.get("validated_count", 0)
        f = dead_letters.get("dead_letter_count", 0)
        total = v + f
        rate = (v / total * 100) if total > 0 else 0.0

        logger.info("Quality pass rate: %.1f%% (%d/%d)", rate, v, total)
        return {"pass_rate": round(rate, 1), "validated": v, "failed": f, "total": total}

    @task()
    def log_report(stats: dict):
        """Log the weekly quality report summary."""
        logger.info(
            "=== Weekly Quality Report ===\n"
            "  Validated articles: %d\n"
            "  Dead-letter count:  %d\n"
            "  Total processed:    %d\n"
            "  Pass rate:          %.1f%%\n"
            "=============================",
            stats.get("validated", 0),
            stats.get("failed", 0),
            stats.get("total", 0),
            stats.get("pass_rate", 0.0),
        )

    v = count_validated()
    d = count_dead_letters()
    rate = compute_pass_rate(v, d)
    log_report(rate)


weekly_quality_report()
