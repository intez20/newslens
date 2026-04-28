"""Pydantic model for articles that passed the quality gate."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, HttpUrl, field_validator

VALID_SENTIMENTS = {"Positive", "Negative", "Neutral"}
VALID_DOMAIN_TAGS = {
    "AI",
    "Crypto",
    "Regulation",
    "Geopolitics",
    "Earnings",
    "Climate",
    "Cybersecurity",
    "Other",
}
EMBEDDING_DIMENSION = 384


class ValidatedArticle(BaseModel):
    """Schema for an article that passed quality gate + has embedding."""

    # ---- original fields ----
    article_id: str
    headline: str
    body: str
    section: Literal["technology", "business", "money", "world", "science"]
    published_at: datetime
    source_url: HttpUrl

    # ---- enrichment fields (from Stage 05) ----
    summary: str
    entities: list[str]
    sentiment: Literal["Positive", "Negative", "Neutral"]
    sentiment_reason: str
    domain_tag: Literal[
        "AI",
        "Crypto",
        "Regulation",
        "Geopolitics",
        "Earnings",
        "Climate",
        "Cybersecurity",
        "Other",
    ]
    enriched_at: datetime

    # ---- quality gate fields (added by Stage 06) ----
    embedding: list[float]
    validated_at: datetime

    @field_validator("embedding")
    @classmethod
    def embedding_dimension(cls, v: list[float]) -> list[float]:
        if len(v) != EMBEDDING_DIMENSION:
            raise ValueError(
                f"embedding must be {EMBEDDING_DIMENSION} dims, got {len(v)}"
            )
        return v
