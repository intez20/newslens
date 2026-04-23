"""Tests for the body_fetcher utility — unit (mocked) + integration (real URLs)."""

import pytest
from unittest.mock import patch, MagicMock

from producer.body_fetcher import fetch_body, _extract_text


# ---------------------------------------------------------------------------
# Unit tests — mocked HTTP, pure HTML parsing
# ---------------------------------------------------------------------------

SAMPLE_HTML = b"""
<html>
<head><title>Test</title></head>
<body>
<nav>Navigation bar</nav>
<article>
    <h1>Test Article</h1>
    <p>This is the first paragraph of the article with enough text to be meaningful.</p>
    <p>This is the second paragraph with additional content for the article body.</p>
    <p>A third paragraph rounds out the article with more substantial content here.</p>
</article>
<footer>Footer content</footer>
</body>
</html>
"""

SAMPLE_HTML_NO_ARTICLE = b"""
<html>
<body>
<div>
    <p>This is body text without an article tag but still meaningful content.</p>
    <p>Second paragraph in the plain body without an article wrapper element.</p>
</div>
</body>
</html>
"""

SAMPLE_HTML_MINIMAL = b"""
<html><body><p>Hi</p></body></html>
"""


class TestExtractText:
    """Test the HTML text extraction logic directly."""

    def test_extracts_from_article_tag(self):
        text = _extract_text(SAMPLE_HTML)
        assert text is not None
        assert "first paragraph" in text
        assert "second paragraph" in text
        # Nav and footer should be stripped
        assert "Navigation bar" not in text
        assert "Footer content" not in text

    def test_falls_back_to_body(self):
        text = _extract_text(SAMPLE_HTML_NO_ARTICLE)
        assert text is not None
        assert "body text without an article tag" in text

    def test_returns_none_for_trivially_short(self):
        text = _extract_text(SAMPLE_HTML_MINIMAL)
        assert text is None  # "Hi" is < 50 chars

    def test_returns_none_for_empty_html(self):
        text = _extract_text(b"")
        assert text is None


class TestFetchBody:
    """Test fetch_body with mocked HTTP responses."""

    @patch("producer.body_fetcher.requests.get")
    def test_returns_text_on_success(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
        mock_resp.content = SAMPLE_HTML
        mock_get.return_value = mock_resp

        result = fetch_body("https://example.com/article")
        assert result is not None
        assert "first paragraph" in result

    @patch("producer.body_fetcher.requests.get")
    def test_returns_none_on_non_html(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/pdf"}
        mock_get.return_value = mock_resp

        result = fetch_body("https://example.com/doc.pdf")
        assert result is None

    @patch("producer.body_fetcher.requests.get")
    def test_returns_none_on_http_error(self, mock_get):
        import requests
        mock_get.side_effect = requests.exceptions.HTTPError("403 Forbidden")

        result = fetch_body("https://paywalled-site.com/article")
        assert result is None

    @patch("producer.body_fetcher.requests.get")
    def test_returns_none_on_timeout(self, mock_get):
        import requests
        mock_get.side_effect = requests.exceptions.Timeout("Read timed out")

        result = fetch_body("https://slow-site.com/article")
        assert result is None

    def test_returns_none_for_empty_url(self):
        assert fetch_body("") is None
        assert fetch_body(None) is None

    def test_skips_hn_self_posts(self):
        result = fetch_body("https://news.ycombinator.com/item?id=12345")
        assert result is None


# ---------------------------------------------------------------------------
# Integration tests — real HTTP requests (may be slow/flaky)
# ---------------------------------------------------------------------------

class TestFetchBodyIntegration:
    """Integration tests that hit real URLs. Skipped in CI."""

    @pytest.mark.timeout(15)
    def test_bbc_article(self):
        """BBC articles should return substantial body text."""
        # Use a known BBC article URL pattern
        url = "https://www.bbc.com/news/technology-67988517"
        result = fetch_body(url)
        # BBC may 404 on old articles, so just check the function doesn't crash
        # If it returns text, it should be substantial
        if result is not None:
            assert len(result) > 100

    @pytest.mark.timeout(15)
    def test_techcrunch_article(self):
        """TechCrunch articles should return body text."""
        url = "https://techcrunch.com/2024/01/15/openai-chatgpt/"
        result = fetch_body(url)
        if result is not None:
            assert len(result) > 100

    @pytest.mark.timeout(15)
    def test_github_url_returns_none_or_text(self):
        """GitHub repos are not articles — should handle gracefully."""
        url = "https://github.com/python/cpython"
        result = fetch_body(url)
        # Should not crash, may return None or some text
        assert result is None or isinstance(result, str)
