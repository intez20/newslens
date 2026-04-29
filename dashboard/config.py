"""Dashboard configuration — reads Weaviate and LLM settings from environment."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardConfig:
    weaviate_http_host: str
    weaviate_http_port: int
    weaviate_grpc_host: str
    weaviate_grpc_port: int
    collection_name: str
    ollama_base_url: str
    ollama_model: str
    embedding_model: str
    embedding_dim: int

    @classmethod
    def from_env(cls) -> "DashboardConfig":
        weaviate_url = os.getenv("WEAVIATE_URL", "http://weaviate:8080")
        grpc_url = os.getenv("WEAVIATE_GRPC_URL", "weaviate:50051")

        http_host = weaviate_url.replace("http://", "").replace("https://", "").split(":")[0]
        http_port = int(weaviate_url.replace("http://", "").replace("https://", "").split(":")[1])
        grpc_host, grpc_port_str = grpc_url.split(":")

        return cls(
            weaviate_http_host=http_host,
            weaviate_http_port=http_port,
            weaviate_grpc_host=grpc_host,
            weaviate_grpc_port=int(grpc_port_str),
            collection_name=os.getenv("WEAVIATE_COLLECTION", "NewsArticle"),
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://ollama:11434"),
            ollama_model=os.getenv("OLLAMA_MODEL", "mistral"),
            embedding_model=os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2"),
            embedding_dim=int(os.getenv("EMBEDDING_DIMENSION", "384")),
        )
