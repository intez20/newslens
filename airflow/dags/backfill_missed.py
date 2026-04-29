"""DAG: backfill_missed — re-fetch Guardian articles for gaps in the last 48h."""

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.lineage.entities import File

logger = logging.getLogger(__name__)

# OpenLineage dataset URIs
WEAVIATE_DATASET = "weaviate://newslens/NewsArticle"
KAFKA_RAW = "kafka://newslens/raw-news"
GUARDIAN_API = "https://content.guardianapis.com"

DEFAULT_ARGS = {
    "owner": "newslens",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": lambda ctx: _discord_alert(ctx),
}


def _discord_alert(context):
    """Send Discord webhook on task failure."""
    import os
    import requests as req

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not webhook_url:
        return
    task_id = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    dag_id = context.get("dag", {}).dag_id if context.get("dag") else "unknown"
    msg = (
        f"\U0001f6a8 **NewsLens DAG Failure**\n"
        f"DAG: `{dag_id}` | Task: `{task_id}`\n"
        f"Time: {datetime.now(timezone.utc).isoformat()}Z"
    )
    try:
        req.post(webhook_url, json={"content": msg}, timeout=10)
    except Exception:
        logger.exception("Discord alert failed")


@dag(
    dag_id="backfill_missed",
    default_args=DEFAULT_ARGS,
    description="Re-fetch Guardian articles for any gaps in the last 48h",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["backfill", "guardian"],
)
def backfill_missed():

    @task(
        inlets=[File(url=WEAVIATE_DATASET)],
    )
    def check_guardian_gaps(**context):
        """Query Weaviate for date coverage in last 48h, identify gaps."""
        import os
        import weaviate
        from weaviate.classes.query import Filter
        from urllib.parse import urlparse

        url = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
        grpc_url = os.getenv("WEAVIATE_GRPC_URL", "weaviate:50051")
        collection_name = os.getenv("WEAVIATE_COLLECTION", "NewsArticle")

        parsed = urlparse(url)
        host = parsed.hostname or "weaviate"
        port = parsed.port or 8080
        grpc_host, grpc_port_str = grpc_url.split(":")
        grpc_port = int(grpc_port_str)

        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=48)

        client = weaviate.connect_to_custom(
            http_host=host, http_port=port, http_secure=False,
            grpc_host=grpc_host, grpc_port=grpc_port, grpc_secure=False,
        )
        try:
            collection = client.collections.get(collection_name)
            results = collection.query.fetch_objects(
                filters=Filter.by_property("published_at").greater_than(since.isoformat()),
                limit=5000,
            )

            # Group by date to find gaps
            covered_dates = set()
            for obj in results.objects:
                pub = obj.properties.get("published_at")
                if pub:
                    if isinstance(pub, str):
                        dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    else:
                        dt = pub
                    covered_dates.add(dt.strftime("%Y-%m-%d"))

            # Check which dates in the window have no articles
            missing_dates = []
            check_date = since.date()
            while check_date <= now.date():
                if check_date.isoformat() not in covered_dates:
                    missing_dates.append(check_date.isoformat())
                check_date += timedelta(days=1)

            logger.info("Covered dates: %s, Missing: %s", covered_dates, missing_dates)
            return {"missing_dates": missing_dates}
        finally:
            client.close()

    @task(
        inlets=[File(url=GUARDIAN_API)],
    )
    def fetch_missing_articles(gaps: dict):
        """Call Guardian API for each missing date."""
        import os
        import requests

        missing = gaps.get("missing_dates", [])
        if not missing:
            logger.info("No gaps found — nothing to backfill")
            return {"fetched_articles": []}

        api_key = os.getenv("GUARDIAN_API_KEY", "")
        sections = os.getenv("GUARDIAN_SECTIONS", "technology,business,money,world,science")

        all_articles = []
        for date_str in missing:
            logger.info("Fetching Guardian articles for %s", date_str)
            for section in sections.split(","):
                url = (
                    f"https://content.guardianapis.com/search"
                    f"?api-key={api_key}&section={section.strip()}"
                    f"&from-date={date_str}&to-date={date_str}"
                    f"&show-fields=bodyText,headline"
                    f"&page-size=50"
                )
                try:
                    resp = requests.get(url, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                    results = data.get("response", {}).get("results", [])
                    for r in results:
                        all_articles.append({
                            "article_id": r.get("id", ""),
                            "headline": r.get("webTitle", ""),
                            "body": r.get("fields", {}).get("bodyText", ""),
                            "section": r.get("sectionId", ""),
                            "source_url": r.get("webUrl", ""),
                            "published_at": r.get("webPublicationDate", ""),
                            "source": "guardian-backfill",
                        })
                except requests.RequestException as e:
                    logger.warning("Guardian fetch failed for %s/%s: %s", date_str, section, e)

        logger.info("Fetched %d articles for backfill", len(all_articles))
        return {"fetched_articles": all_articles}

    @task(
        outlets=[File(url=KAFKA_RAW)],
    )
    def publish_to_kafka(fetch_result: dict):
        """Publish re-fetched articles to raw-news topic."""
        import json
        import os
        from kafka import KafkaProducer

        articles = fetch_result.get("fetched_articles", [])
        if not articles:
            logger.info("No articles to publish")
            return {"published": 0}

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        topic = os.getenv("KAFKA_TOPIC_RAW", "raw-news")

        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        count = 0
        for article in articles:
            producer.send(topic, value=article)
            count += 1
        producer.flush()
        producer.close()

        logger.info("Published %d backfill articles to %s", count, topic)
        return {"published": count}

    gaps = check_guardian_gaps()
    fetched = fetch_missing_articles(gaps)
    publish_to_kafka(fetched)


backfill_missed()
