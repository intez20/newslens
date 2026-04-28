"""Prompt templates for the LLM enrichment chains."""

from langchain_core.prompts import PromptTemplate


# ── Legacy individual prompts (kept for reference / fallback) ─────
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

# ── Unified single-call prompt (4x faster — one LLM call instead of 4) ──
UNIFIED_PROMPT = PromptTemplate.from_template(
    "You are a news analysis assistant. Analyze the following article and "
    "return a JSON object with exactly these keys:\n\n"
    '1. "summary": exactly 3 concise sentences summarizing the article\n'
    '2. "entities": an array of up to 5 named entities '
    "(people, organizations, countries). Empty array if none found.\n"
    '3. "sentiment": exactly one of "Positive", "Negative", "Neutral"\n'
    '4. "sentiment_reason": one sentence explaining the sentiment\n'
    '5. "domain_tag": exactly one of: AI, Crypto, Regulation, '
    "Geopolitics, Earnings, Climate, Cybersecurity, Other\n\n"
    "Output ONLY the JSON object, no markdown fences, no extra text.\n\n"
    "Article:\n{article}"
)
