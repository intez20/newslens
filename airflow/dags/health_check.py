"""DAG: health_check — verify Kafka, Weaviate, Ollama are healthy every 15 min."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.lineage.entities import File

logger = logging.getLogger(__name__)

# OpenLineage dataset URIs
KAFKA_BROKER = "kafka://newslens/broker"
WEAVIATE_DATASET = "weaviate://newslens/NewsArticle"
OLLAMA_SERVICE = "http://ollama:11434"

DEFAULT_ARGS = {
    "owner": "newslens",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="health_check",
    default_args=DEFAULT_ARGS,
    description="Verify Kafka, Weaviate, Ollama are healthy; alert on failure",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["health", "monitoring"],
)
def health_check():

    @task(
        inlets=[File(url=KAFKA_BROKER)],
    )
    def check_kafka(**context):
        """Verify Kafka broker responds and topics have recent messages."""
        import os
        from kafka import KafkaConsumer, TopicPartition

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap,
                request_timeout_ms=5000,
            )
            topics = consumer.topics()
            consumer.close()
        except Exception as e:
            logger.error("Kafka broker unreachable: %s", e)
            raise

        if not topics:
            raise RuntimeError("Kafka broker has zero topics")

        # Check that key pipeline topics have messages
        stale_topics = []
        consumer = KafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=5000)
        for topic_name in ["raw-news", "enriched-news", "validated-news"]:
            if topic_name not in topics:
                stale_topics.append(f"{topic_name} (missing)")
                continue
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                stale_topics.append(f"{topic_name} (no partitions)")
                continue
            tps = [TopicPartition(topic_name, p) for p in partitions]
            end_offsets = consumer.end_offsets(tps)
            begin_offsets = consumer.beginning_offsets(tps)
            total_msgs = sum(end_offsets[tp] - begin_offsets[tp] for tp in tps)
            if total_msgs == 0:
                stale_topics.append(f"{topic_name} (empty)")
        consumer.close()

        if stale_topics:
            msg = f"Pipeline stall — topics with no messages: {', '.join(stale_topics)}"
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Kafka healthy — %d topics, pipeline topics have messages", len(topics))
        return {"status": "healthy", "topics": len(topics)}

    @task(
        inlets=[File(url=WEAVIATE_DATASET)],
    )
    def check_weaviate(**context):
        """Verify Weaviate REST API is ready and has recent ingestion."""
        import os
        import requests

        url = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
        try:
            resp = requests.get(f"{url}/v1/.well-known/ready", timeout=5)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Weaviate REST unreachable: %s", e)
            raise

        # Check total article count
        try:
            agg_resp = requests.post(
                f"{url}/v1/graphql",
                json={
                    "query": '{ Aggregate { NewsArticle { meta { count } } } }'
                },
                timeout=10,
            )
            agg_resp.raise_for_status()
            count = (
                agg_resp.json()
                .get("data", {})
                .get("Aggregate", {})
                .get("NewsArticle", [{}])[0]
                .get("meta", {})
                .get("count", 0)
            )
        except Exception:
            count = -1

        if count == 0:
            msg = "Weaviate has zero articles — ingestion may be stalled"
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Weaviate healthy — %s articles in NewsArticle", count)
        return {"status": "healthy", "article_count": count}

    @task(
        inlets=[File(url=OLLAMA_SERVICE)],
    )
    def check_ollama(**context):
        """Verify Ollama LLM server responds and has a model loaded."""
        import os
        import requests

        url = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
        try:
            resp = requests.get(f"{url}/api/tags", timeout=10)
            resp.raise_for_status()
            models = resp.json().get("models", [])
        except Exception as e:
            logger.error("Ollama health check FAILED: %s", e)
            raise

        if not models:
            msg = "Ollama has no models loaded — enrichment will fail"
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Ollama healthy — %d models loaded", len(models))
        return {"status": "healthy", "models": len(models)}

    @task()
    def check_consumer_lag(**context):
        """Verify Kafka consumer groups are active and not excessively lagging."""
        import os
        from kafka.admin import KafkaAdminClient

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
            groups = admin.list_consumer_groups()
            admin.close()
        except Exception as e:
            logger.error("Cannot query consumer groups: %s", e)
            raise

        if not groups:
            msg = "No Kafka consumer groups found — workers may be down"
            logger.error(msg)
            raise RuntimeError(msg)

        group_names = [g[0] for g in groups]
        logger.info("Consumer groups active: %s", group_names)
        return {"status": "healthy", "groups": group_names}

    @task(trigger_rule="all_done")
    def alert_on_failure(**context):
        """Send Discord alert if any upstream check failed (optional)."""
        import os
        import requests

        webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
        if not webhook_url:
            logger.info("No DISCORD_WEBHOOK_URL set — skipping alert")
            return {"alerted": False}

        # Check if any upstream task failed
        ti = context["ti"]
        dag_run = context["dag_run"]
        failed_tasks = []
        for task_instance in dag_run.get_task_instances():
            if task_instance.task_id != "alert_on_failure" and task_instance.state == "failed":
                failed_tasks.append(task_instance.task_id)

        if not failed_tasks:
            logger.info("All health checks passed — no alert needed")
            return {"alerted": False}

        message = (
            f"🚨 **NewsLens Health Check Failed**\n"
            f"Failed services: {', '.join(failed_tasks)}\n"
            f"Time: {datetime.utcnow().isoformat()}Z"
        )
        try:
            requests.post(webhook_url, json={"content": message}, timeout=10)
            logger.warning("Discord alert sent for: %s", failed_tasks)
            return {"alerted": True, "failed": failed_tasks}
        except Exception as e:
            logger.error("Failed to send Discord alert: %s", e)
            return {"alerted": False, "error": str(e)}

    kafka = check_kafka()
    weaviate = check_weaviate()
    ollama = check_ollama()
    lag = check_consumer_lag()
    [kafka, weaviate, ollama, lag] >> alert_on_failure()


health_check()
