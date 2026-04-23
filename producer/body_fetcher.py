"""Shared utility to fetch full article body text from a URL."""

import logging

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_TIMEOUT = (5, 10)  # (connect, read) in seconds
_MAX_BODY_BYTES = 1_000_000  # 1 MB cap to avoid downloading huge pages
_USER_AGENT = "NewsLens/1.0 (article body fetcher)"

# Tags that typically hold the main article text
_ARTICLE_TAGS = ["article", "main", "[role='main']"]

# Tags to remove before extracting text (nav, ads, sidebars, scripts)
_STRIP_TAGS = [
    "nav", "aside", "footer", "header", "script", "style",
    "noscript", "iframe", "form", "button", "svg",
    "figure", "figcaption",
]


def fetch_body(url: str) -> str | None:
    """Fetch a URL and extract the main article text.

    Returns the extracted text, or None if the fetch/parse fails.
    Caller should fall back to its original body on None.
    """
    if not url or url.startswith("https://news.ycombinator.com"):
        return None  # HN self-posts have no external article

    try:
        resp = requests.get(
            url,
            timeout=_TIMEOUT,
            headers={"User-Agent": _USER_AGENT},
            stream=True,
        )
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        if "text/html" not in content_type:
            logger.debug("Skipping non-HTML content at %s", url)
            return None

        # Read up to _MAX_BODY_BYTES to avoid memory issues
        html = resp.content[:_MAX_BODY_BYTES]
        return _extract_text(html)

    except requests.exceptions.RequestException as exc:
        logger.debug("Body fetch failed for %s: %s", url, exc)
        return None


def _extract_text(html: bytes) -> str | None:
    """Parse HTML and extract the main article body text."""
    soup = BeautifulSoup(html, "lxml")

    # Remove noise elements
    for tag_name in _STRIP_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Try to find the article container
    article_el = None
    for selector in _ARTICLE_TAGS:
        if selector.startswith("["):
            article_el = soup.select_one(selector)
        else:
            article_el = soup.find(selector)
        if article_el:
            break

    # Fall back to <body> if no article container found
    target = article_el or soup.body
    if not target:
        return None

    # Extract text from <p> tags for cleaner output
    paragraphs = target.find_all("p")
    if paragraphs:
        text = "\n\n".join(p.get_text(strip=True) for p in paragraphs)
    else:
        text = target.get_text(separator="\n", strip=True)

    text = text.strip()
    return text if len(text) > 50 else None  # Ignore trivially short extractions
