"""DAG: health_check — verify Kafka, Weaviate, Ollama are healthy every 15 min."""

import logging
from datetime import datetime, timedelta

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
        """Verify Kafka broker responds."""
        import os
        from kafka import KafkaConsumer

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap,
                request_timeout_ms=5000,
            )
            topics = consumer.topics()
            consumer.close()
            logger.info("Kafka healthy — %d topics found", len(topics))
            return {"status": "healthy", "topics": len(topics)}
        except Exception as e:
            logger.error("Kafka health check FAILED: %s", e)
            raise

    @task(
        inlets=[File(url=WEAVIATE_DATASET)],
    )
    def check_weaviate(**context):
        """Verify Weaviate REST API is ready."""
        import os
        import requests

        url = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
        try:
            resp = requests.get(f"{url}/v1/.well-known/ready", timeout=5)
            resp.raise_for_status()
            logger.info("Weaviate healthy — ready endpoint returned 200")
            return {"status": "healthy"}
        except Exception as e:
            logger.error("Weaviate health check FAILED: %s", e)
            raise

    @task(
        inlets=[File(url=OLLAMA_SERVICE)],
    )
    def check_ollama(**context):
        """Verify Ollama LLM server responds."""
        import os
        import requests

        url = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
        try:
            resp = requests.get(f"{url}/api/tags", timeout=10)
            resp.raise_for_status()
            models = resp.json().get("models", [])
            logger.info("Ollama healthy — %d models loaded", len(models))
            return {"status": "healthy", "models": len(models)}
        except Exception as e:
            logger.error("Ollama health check FAILED: %s", e)
            raise

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
    [kafka, weaviate, ollama] >> alert_on_failure()


health_check()
