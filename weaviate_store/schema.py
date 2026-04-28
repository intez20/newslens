"""NewsArticle collection schema for Weaviate."""

import weaviate
from weaviate.classes.config import (
    Configure,
    DataType,
    Property,
    Tokenization,
    VectorDistances,
)

COLLECTION_NAME = "NewsArticle"

NEWSARTICLE_PROPERTIES = [
    Property(
        name="article_id",
        data_type=DataType.TEXT,
        tokenization=Tokenization.FIELD,
    ),
    Property(name="headline", data_type=DataType.TEXT),
    Property(name="summary", data_type=DataType.TEXT),
    Property(name="entities", data_type=DataType.TEXT_ARRAY),
    Property(
        name="sentiment",
        data_type=DataType.TEXT,
        tokenization=Tokenization.FIELD,
    ),
    Property(name="sentiment_reason", data_type=DataType.TEXT),
    Property(
        name="domain_tag",
        data_type=DataType.TEXT,
        tokenization=Tokenization.FIELD,
    ),
    Property(
        name="section",
        data_type=DataType.TEXT,
        tokenization=Tokenization.FIELD,
    ),
    Property(name="published_at", data_type=DataType.DATE),
    Property(
        name="source_url",
        data_type=DataType.TEXT,
        tokenization=Tokenization.FIELD,
    ),
]


def ensure_collection(client: weaviate.WeaviateClient, name: str) -> None:
    """Create the NewsArticle collection if it does not already exist.

    Args:
        client: Connected Weaviate v4 client.
        name: Collection name (default 'NewsArticle').
    """
    if client.collections.exists(name):
        return

    client.collections.create(
        name=name,
        properties=NEWSARTICLE_PROPERTIES,
        vectorizer_config=Configure.Vectorizer.none(),
        vector_index_config=Configure.VectorIndex.hnsw(
            distance_metric=VectorDistances.COSINE,
            ef=128,
            ef_construction=128,
            max_connections=64,
        ),
    )
