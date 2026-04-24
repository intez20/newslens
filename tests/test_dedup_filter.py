"""Unit tests for processor.dedup_filter — pure logic only.

The ``should_emit`` function is the testable core of the dedup filter.
Flink state behavior (KeyedProcessFunction) is tested in Stage 04c E2E tests.
"""

import pytest

from processor.dedup_filter import MIN_WORD_COUNT, should_emit


class TestShouldEmit:
    """Verify the word-count gate works correctly."""

    def test_body_with_enough_words_passes(self):
        body = " ".join(["word"] * 250)
        assert should_emit(body) is True

    def test_body_at_exact_threshold_passes(self):
        body = " ".join(["word"] * MIN_WORD_COUNT)
        assert should_emit(body) is True

    def test_body_below_threshold_rejected(self):
        body = " ".join(["word"] * 50)
        assert should_emit(body) is False

    def test_body_one_below_threshold_rejected(self):
        body = " ".join(["word"] * (MIN_WORD_COUNT - 1))
        assert should_emit(body) is False

    def test_empty_body_rejected(self):
        assert should_emit("") is False

    def test_whitespace_only_body_rejected(self):
        assert should_emit("   ") is False

    def test_custom_min_words(self):
        body = " ".join(["word"] * 10)
        assert should_emit(body, min_words=5) is True
        assert should_emit(body, min_words=15) is False

    def test_realistic_article_body(self):
        """A realistic article body with mixed-length words passes."""
        words = [
            "The", "government", "announced", "new", "regulations",
            "affecting", "cryptocurrency", "trading", "platforms",
        ]
        # Repeat to exceed threshold
        body = " ".join(words * 25)  # 225 words
        assert should_emit(body) is True


class TestMinWordCountConstant:

    def test_default_is_200(self):
        assert MIN_WORD_COUNT == 200
