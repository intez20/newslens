"""Pydantic models for enriched NewsLens article events.

Defines the schema for articles after LLM enrichment —
original fields plus summary, entities, sentiment, domain tag.
"""

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


class EnrichedArticleEvent(BaseModel):
    """Schema for an article after LLM enrichment.

    Contains the 6 original fields from ArticleEvent plus 5 enrichment
    fields added by the LangChain chains, and an enriched_at timestamp.
    """

    # ---- original fields (same contract as ArticleEvent) ----
    article_id: str
    headline: str
    body: str
    section: Literal["technology", "business", "money", "world", "science"]
    published_at: datetime
    source_url: HttpUrl

    # ---- enrichment fields (added by LLM chains) ----
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

    @field_validator("article_id")
    @classmethod
    def article_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("article_id must not be empty or blank")
        return v

    @field_validator("headline")
    @classmethod
    def headline_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("headline must not be empty or blank")
        return v

    @field_validator("summary")
    @classmethod
    def summary_length(cls, v: str) -> str:
        stripped = v.strip()
        if len(stripped) < 50:
            raise ValueError(
                f"summary must be at least 50 characters, got {len(stripped)}"
            )
        if len(stripped) > 500:
            raise ValueError(
                f"summary must be at most 500 characters, got {len(stripped)}"
            )
        return stripped

    @field_validator("entities")
    @classmethod
    def entities_max_five(cls, v: list[str]) -> list[str]:
        if len(v) > 5:
            raise ValueError(f"entities must have at most 5 items, got {len(v)}")
        for entity in v:
            if not entity.strip():
                raise ValueError("each entity must be a non-empty string")
        return v

    @field_validator("sentiment_reason")
    @classmethod
    def sentiment_reason_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("sentiment_reason must not be empty or blank")
        return v
