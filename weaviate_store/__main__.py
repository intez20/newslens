"""Entry point: python -m weaviate_store"""

import logging

from weaviate_store.client import WeaviateNewsClient
from weaviate_store.config import WeaviateStoreConfig
from weaviate_store.ingestion_worker import IngestionWorker


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    config = WeaviateStoreConfig.from_env()
    weaviate_client = WeaviateNewsClient(config)
    worker = IngestionWorker(config, weaviate_client)
    worker.run()


if __name__ == "__main__":
    main()
