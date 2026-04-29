"""DAG: daily_housekeeping — prune old Weaviate articles (>30 days)."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.lineage.entities import File

logger = logging.getLogger(__name__)

# OpenLineage dataset URIs
WEAVIATE_DATASET = "weaviate://newslens/NewsArticle"

DEFAULT_ARGS = {
    "owner": "newslens",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="daily_housekeeping",
    default_args=DEFAULT_ARGS,
    description="Prune old Weaviate articles older than 30 days",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["housekeeping", "weaviate"],
)
def daily_housekeeping():

    @task(
        inlets=[File(url=WEAVIATE_DATASET)],
        outlets=[File(url=WEAVIATE_DATASET)],
    )
    def prune_old_articles(**context):
        """Delete Weaviate articles where published_at < 30 days ago."""
        import os
        import weaviate
        from weaviate.classes.query import Filter

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        logger.info("Pruning articles older than %s", cutoff.isoformat())

        url = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
        grpc_url = os.getenv("WEAVIATE_GRPC_URL", "weaviate:50051")
        collection_name = os.getenv("WEAVIATE_COLLECTION", "NewsArticle")

        # Parse host/port from URLs
        from urllib.parse import urlparse
        parsed = urlparse(url)
        host = parsed.hostname or "weaviate"
        port = parsed.port or 8080
        grpc_host, grpc_port_str = grpc_url.split(":")
        grpc_port = int(grpc_port_str)

        client = weaviate.connect_to_custom(
            http_host=host, http_port=port, http_secure=False,
            grpc_host=grpc_host, grpc_port=grpc_port, grpc_secure=False,
        )
        try:
            collection = client.collections.get(collection_name)

            # Fetch old articles
            old_articles = collection.query.fetch_objects(
                filters=Filter.by_property("published_at").less_than(cutoff.isoformat()),
                limit=1000,
            )
            deleted = 0
            for obj in old_articles.objects:
                collection.data.delete_by_id(obj.uuid)
                deleted += 1

            logger.info("Pruned %d articles older than %s", deleted, cutoff.date())
            return {"deleted_count": deleted, "cutoff": cutoff.isoformat()}
        finally:
            client.close()

    @task()
    def log_prune_stats(stats: dict):
        """Log pruning results."""
        logger.info(
            "Housekeeping complete: deleted %d articles (cutoff: %s)",
            stats.get("deleted_count", 0),
            stats.get("cutoff", "unknown"),
        )

    stats = prune_old_articles()
    log_prune_stats(stats)


daily_housekeeping()
