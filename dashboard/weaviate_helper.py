"""Weaviate helper — shared query functions for the dashboard views."""

import logging
from datetime import datetime, timezone

import weaviate
from weaviate.classes.aggregate import GroupByAggregate
from weaviate.classes.query import Filter, MetadataQuery, Sort

from dashboard.config import DashboardConfig

logger = logging.getLogger(__name__)


def get_client(config: DashboardConfig) -> weaviate.WeaviateClient:
    """Return a connected Weaviate v4 client."""
    return weaviate.connect_to_custom(
        http_host=config.weaviate_http_host,
        http_port=config.weaviate_http_port,
        http_secure=False,
        grpc_host=config.weaviate_grpc_host,
        grpc_port=config.weaviate_grpc_port,
        grpc_secure=False,
    )


def fetch_latest_articles(
    client: weaviate.WeaviateClient,
    collection_name: str,
    limit: int = 50,
) -> list[dict]:
    """Fetch the most recent articles sorted by published_at descending."""
    collection = client.collections.get(collection_name)
    result = collection.query.fetch_objects(
        limit=limit,
        sort=Sort.by_property("published_at", ascending=False),
        return_metadata=MetadataQuery(creation_time=True),
    )
    return [
        {**obj.properties, "uuid": str(obj.uuid)}
        for obj in result.objects
    ]


def count_articles(
    client: weaviate.WeaviateClient,
    collection_name: str,
) -> int:
    """Return total article count in the collection."""
    collection = client.collections.get(collection_name)
    result = collection.aggregate.over_all(total_count=True)
    return result.total_count or 0


def search_by_vector(
    client: weaviate.WeaviateClient,
    collection_name: str,
    vector: list[float],
    limit: int = 5,
) -> list[dict]:
    """Semantic vector search — returns articles ranked by cosine similarity."""
    collection = client.collections.get(collection_name)
    result = collection.query.near_vector(
        near_vector=vector,
        limit=limit,
        return_metadata=MetadataQuery(distance=True),
    )
    return [
        {
            **obj.properties,
            "uuid": str(obj.uuid),
            "distance": obj.metadata.distance,
        }
        for obj in result.objects
    ]


def aggregate_by_property(
    client: weaviate.WeaviateClient,
    collection_name: str,
    property_name: str,
) -> dict[str, int]:
    """Group-by aggregation — returns {value: count} dict."""
    collection = client.collections.get(collection_name)
    result = collection.aggregate.over_all(
        group_by=GroupByAggregate(prop=property_name),
        total_count=True,
    )
    counts = {}
    for group in result.groups:
        key = group.grouped_by.value
        counts[key] = group.total_count or 0
    return counts
