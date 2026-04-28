"""Entry point: python -m quality_gate"""

import logging

from quality_gate.config import QualityGateConfig
from quality_gate.embedder import ArticleEmbedder
from quality_gate.expectations import ArticleExpectationSuite
from quality_gate.gate_worker import QualityGateWorker


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    config = QualityGateConfig.from_env()
    expectations = ArticleExpectationSuite(config)
    embedder = ArticleEmbedder(config)
    worker = QualityGateWorker(config, expectations, embedder)
    worker.run()


if __name__ == "__main__":
    main()
