"""Article embedder using sentence-transformers MiniLM-L6-v2."""

from sentence_transformers import SentenceTransformer

from quality_gate.config import QualityGateConfig


class ArticleEmbedder:
    """Generates dense vector embeddings for articles using MiniLM-L6-v2.

    The model is loaded once at initialization (~80 MB, ~2s on CPU).
    Each encode() call takes ~5-10ms on CPU.
    """

    def __init__(self, config: QualityGateConfig):
        self.model = SentenceTransformer(config.embedding_model)
        self.expected_dim = config.embedding_dimension

    def embed(self, headline: str, summary: str) -> list[float]:
        """Generate a 384-dim embedding from headline + summary.

        Args:
            headline: Article headline.
            summary: 3-sentence summary from LLM enrichment.

        Returns:
            list[float] of length 384.

        Raises:
            ValueError: If output dimension doesn't match expected.
        """
        text = f"{headline} {summary}"
        vector = self.model.encode(text).tolist()

        if len(vector) != self.expected_dim:
            raise ValueError(
                f"Embedding dimension mismatch: expected {self.expected_dim}, "
                f"got {len(vector)}"
            )
        return vector
