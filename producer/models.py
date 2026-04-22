"""Pydantic models for NewsLens article events.

These models define the data contracts used across the pipeline -
from Guardian API ingestion through to Kafka publishing.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, HttpUrl, field_validator


# The 5 Guardian sections NewsLens monitors
VALID_SECTIONS = {"technology", "business", "money", "world", "science"}


class ArticleEvent(BaseModel):
    """Schema for a raw article event published to Kafka `raw-news` topic.

    Fields mirror the Guardian API response structure, flattened into
    a single event object for downstream consumption.
    """

    article_id: str
    headline: str
    body: str
    section: Literal["technology", "business", "money", "world", "science"]
    published_at: datetime
    source_url: HttpUrl

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
