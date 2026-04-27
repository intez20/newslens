"""LLM client factory — returns Ollama or Groq LLM based on config."""

from enrichment.config import EnrichmentConfig


def create_llm(config: EnrichmentConfig):
    """Return a LangChain LLM instance based on LLM_BACKEND setting.

    Imports are deferred so unit tests that mock the LLM don't need
    langchain-community or network access.
    """
    if config.llm_backend == "ollama":
        from langchain_community.llms import Ollama

        return Ollama(
            base_url=config.ollama_base_url,
            model=config.ollama_model,
            temperature=0.0,
        )
    elif config.llm_backend == "groq":
        from langchain_community.chat_models import ChatGroq

        return ChatGroq(
            api_key=config.groq_api_key,
            model_name=config.groq_model,
            temperature=0.0,
        )
    else:
        raise ValueError(f"Unknown LLM backend: {config.llm_backend}")
