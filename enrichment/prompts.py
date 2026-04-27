"""Prompt templates for the 4 LLM enrichment chains."""

from langchain_core.prompts import PromptTemplate


SUMMARY_PROMPT = PromptTemplate.from_template(
    "You are a news summarizer. Summarize the following article in exactly "
    "3 concise sentences. Output ONLY the 3 sentences, nothing else.\n\n"
    "Article:\n{article}"
)

ENTITY_PROMPT = PromptTemplate.from_template(
    "Extract up to 5 named entities (people, organizations, countries) from "
    "the following article. Return a JSON array of strings. If fewer than 5 "
    "entities are found, return only those found. If none are found, return "
    "an empty array []. Output ONLY the JSON array.\n\n"
    "Article:\n{article}"
)

SENTIMENT_PROMPT = PromptTemplate.from_template(
    "Classify the sentiment of the following article as exactly one of: "
    "Positive, Negative, or Neutral.\n\n"
    "Return a JSON object with two keys:\n"
    '- "sentiment": one of "Positive", "Negative", "Neutral"\n'
    '- "reason": a single sentence explaining why\n\n'
    "Output ONLY the JSON object.\n\n"
    "Article:\n{article}"
)

DOMAIN_TAG_PROMPT = PromptTemplate.from_template(
    "Classify the following article into exactly one domain tag from this "
    "list: AI, Crypto, Regulation, Geopolitics, Earnings, Climate, "
    "Cybersecurity, Other\n\n"
    "Output ONLY the tag word, nothing else.\n\n"
    "Article:\n{article}"
)
