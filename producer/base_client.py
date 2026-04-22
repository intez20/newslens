"""Abstract base class for all NewsLens data source clients."""

from abc import ABC, abstractmethod

from producer.models import ArticleEvent


class BaseNewsClient(ABC):
    """Interface that every news source client must implement."""

    @abstractmethod
    def fetch(self) -> list[ArticleEvent]:
        """Fetch latest articles and return them as ArticleEvent objects."""
        ...

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable name for this source (e.g. 'guardian', 'rss')."""
        ...
