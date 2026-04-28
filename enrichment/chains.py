"""Enrichment chains — unified single LLM call for all 4 enrichment tasks."""

import json
import logging
import re

from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.runnables import RunnableParallel

from enrichment.prompts import (
    DOMAIN_TAG_PROMPT,
    ENTITY_PROMPT,
    SENTIMENT_PROMPT,
    SUMMARY_PROMPT,
    UNIFIED_PROMPT,
)

logger = logging.getLogger(__name__)


class EnrichmentChains:
    """Builds the LLM enrichment chain.

    Default mode uses a single unified prompt (1 LLM call instead of 4).
    Falls back to parallel mode if unified parsing fails.

    Usage:
        chains = EnrichmentChains(llm)
        result = chains.enrich("headline\\n\\nbody text...")
        # result = {"summary": "...", "entities": [...],
        #           "sentiment": {...}, "domain_tag": "..."}
    """

    def __init__(self, llm):
        # Unified chain — single LLM call
        self.unified_chain = UNIFIED_PROMPT | llm | StrOutputParser()

        # Legacy parallel chains — used as fallback
        self.summary_chain = SUMMARY_PROMPT | llm | StrOutputParser()
        self.entity_chain = ENTITY_PROMPT | llm | JsonOutputParser()
        self.sentiment_chain = SENTIMENT_PROMPT | llm | JsonOutputParser()
        self.tag_chain = DOMAIN_TAG_PROMPT | llm | StrOutputParser()

        self.parallel = RunnableParallel(
            {
                "summary": self.summary_chain,
                "entities": self.entity_chain,
                "sentiment": self.sentiment_chain,
                "domain_tag": self.tag_chain,
            }
        )

    def enrich(self, article_context: str) -> dict:
        """Run unified single-call enrichment. Falls back to parallel on
        parse failure.

        Args:
            article_context: headline + truncated body text.

        Returns:
            dict with keys: summary, entities, sentiment, domain_tag.
        """
        try:
            raw_text = self.unified_chain.invoke({"article": article_context})
            parsed = self._parse_unified(raw_text)
            if parsed is not None:
                return parsed
            logger.warning("unified parse failed, falling back to parallel")
        except Exception as exc:
            logger.warning("unified chain error: %s — falling back", exc)

        # Fallback: 4 separate calls (slower but more robust)
        raw = self.parallel.invoke({"article": article_context})
        return {
            "summary": raw["summary"].strip(),
            "entities": raw["entities"],
            "sentiment": raw["sentiment"],
            "domain_tag": raw["domain_tag"].strip(),
        }

    @staticmethod
    def _parse_unified(raw_text: str) -> dict | None:
        """Parse the unified LLM response into the expected dict format.

        Returns None if parsing fails so caller can fall back.
        """
        text = raw_text.strip()
        # Strip markdown code fences if present
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None

        if not isinstance(data, dict):
            return None

        # Require at least summary and sentiment
        if "summary" not in data or "sentiment" not in data:
            return None

        # Normalize into the format the worker expects
        sentiment_val = data.get("sentiment", "Neutral")
        reason = data.get("sentiment_reason", "")

        return {
            "summary": str(data.get("summary", "")).strip(),
            "entities": data.get("entities", []),
            "sentiment": {"sentiment": str(sentiment_val), "reason": str(reason)},
            "domain_tag": str(data.get("domain_tag", "Other")).strip(),
        }
