"""Great Expectations-style validation for enriched articles."""

from datetime import datetime, timedelta, timezone

from quality_gate.models import VALID_SENTIMENTS

VALID_SENTIMENTS_SET = VALID_SENTIMENTS


class ArticleExpectationSuite:
    """Defines and runs quality checks on enriched articles.

    Four checks:
    1. Summary length within [min, max]
    2. Sentiment in {Positive, Negative, Neutral}
    3. Recency: published_at ≤ N days ago
    4. Embedding dimension = 384 (run externally after embedding step)
    """

    def __init__(self, config):
        self.config = config

    def validate(self, article: dict) -> tuple[bool, list[str]]:
        """Validate an enriched article against all expectations.

        Returns:
            (passed: bool, failures: list[str])
        """
        failures = []

        # Check 1: Summary length
        summary_len = len(article.get("summary", ""))
        if not (
            self.config.min_summary_length
            <= summary_len
            <= self.config.max_summary_length
        ):
            failures.append(
                f"summary_length: {summary_len} not in "
                f"[{self.config.min_summary_length}, {self.config.max_summary_length}]"
            )

        # Check 2: Sentiment value
        sentiment = article.get("sentiment", "")
        if sentiment not in VALID_SENTIMENTS_SET:
            failures.append(
                f"sentiment: '{sentiment}' not in {VALID_SENTIMENTS_SET}"
            )

        # Check 3: Recency (published_at ≤ N days ago)
        published_at = article.get("published_at")
        if published_at:
            cutoff = datetime.now(timezone.utc) - timedelta(
                days=self.config.max_recency_days
            )
            if isinstance(published_at, str):
                pub_dt = datetime.fromisoformat(published_at)
            else:
                pub_dt = published_at
            # Ensure timezone-aware
            if pub_dt.tzinfo is None:
                pub_dt = pub_dt.replace(tzinfo=timezone.utc)
            if pub_dt < cutoff:
                failures.append(
                    f"recency: published_at {pub_dt.isoformat()} "
                    f"older than {self.config.max_recency_days} days"
                )

        passed = len(failures) == 0
        return passed, failures
