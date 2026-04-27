"""Entry point: python -m enrichment"""

import logging

from enrichment.config import EnrichmentConfig
from enrichment.chains import EnrichmentChains
from enrichment.enrichment_worker import EnrichmentWorker
from enrichment.llm_factory import create_llm


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    config = EnrichmentConfig.from_env()
    llm = create_llm(config)
    chains = EnrichmentChains(llm)
    worker = EnrichmentWorker(config, chains)
    worker.run()


if __name__ == "__main__":
    main()
