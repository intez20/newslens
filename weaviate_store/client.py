"""Weaviate client wrapper for NewsArticle operations."""

import logging

import weaviate
from weaviate.classes.query import Filter

from weaviate_store.config import WeaviateStoreConfig
from weaviate_store.schema import ensure_collection

logger = logging.getLogger(__name__)


class WeaviateNewsClient:
    """Wraps the Weaviate v4 client for NewsArticle CRUD + search."""

    def __init__(self, config: WeaviateStoreConfig):
        self.config = config
        self._client = weaviate.connect_to_custom(
            http_host=config.weaviate_url.replace("http://", "").split(":")[0],
            http_port=int(
                config.weaviate_url.replace("http://", "").split(":")[1]
            ),
            http_secure=False,
            grpc_host=config.weaviate_grpc_url.split(":")[0],
            grpc_port=int(config.weaviate_grpc_url.split(":")[1]),
            grpc_secure=False,
        )
        ensure_collection(self._client, config.collection_name)
        self._collection = self._client.collections.get(
            config.collection_name
        )
        logger.info(
            "connected to Weaviate — collection %s", config.collection_name
        )

    def exists(self, article_id: str) -> bool:
        """Check if an article with this ID already exists."""
        result = self._collection.query.fetch_objects(
            filters=Filter.by_property("article_id").equal(article_id),
            limit=1,
        )
        return len(result.objects) > 0

    def upsert_article(self, properties: dict, vector: list[float]) -> str:
        """Insert an article with its vector. Returns the Weaviate UUID.

        Args:
            properties: Dict of article fields (no 'embedding' key).
            vector: 384-dim float vector from Stage 06.

        Returns:
            str UUID assigned by Weaviate.
        """
        result = self._collection.data.insert(
            properties=properties,
            vector=vector,
        )
        return str(result)

    def search_by_vector(
        self, vector: list[float], limit: int = 10
    ) -> list[dict]:
        """Semantic similarity search using a query vector.

        Returns:
            List of article dicts ordered by cosine similarity.
        """
        result = self._collection.query.near_vector(
            near_vector=vector,
            limit=limit,
        )
        return [
            {**obj.properties, "uuid": str(obj.uuid)} for obj in result.objects
        ]

    def close(self) -> None:
        """Close the Weaviate connection."""
        self._client.close()
