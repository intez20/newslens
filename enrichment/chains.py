"""Enrichment chains — 4 LCEL chains composed via RunnableParallel."""

import logging

from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.runnables import RunnableParallel

from enrichment.prompts import (
    DOMAIN_TAG_PROMPT,
    ENTITY_PROMPT,
    SENTIMENT_PROMPT,
    SUMMARY_PROMPT,
)

logger = logging.getLogger(__name__)


class EnrichmentChains:
    """Builds and caches the 4 LLM enrichment chains.

    Usage:
        chains = EnrichmentChains(llm)
        result = chains.enrich("headline\\n\\nbody text...")
        # result = {"summary": "...", "entities": [...],
        #           "sentiment": {...}, "domain_tag": "..."}
    """

    def __init__(self, llm):
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
        """Run all 4 chains concurrently and return cleaned results.

        Args:
            article_context: headline + truncated body text.

        Returns:
            dict with keys: summary, entities, sentiment, domain_tag.
        """
        raw = self.parallel.invoke({"article": article_context})

        return {
            "summary": raw["summary"].strip(),
            "entities": raw["entities"],
            "sentiment": raw["sentiment"],
            "domain_tag": raw["domain_tag"].strip(),
        }
