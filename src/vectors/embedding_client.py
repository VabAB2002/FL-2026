"""OpenAI embedding client with batching and retry logic."""

from __future__ import annotations

import time
from typing import Any

import openai

from ..infrastructure.logger import get_logger

logger = get_logger("finloom.vectors.embeddings")


class EmbeddingClient:
    """Wrapper for OpenAI embeddings API with batching and retry."""

    def __init__(self, api_key: str | None = None, model: str = "text-embedding-3-large"):
        """
        Initialize OpenAI client.

        Args:
            api_key: OpenAI API key (or None to use env var)
            model: Embedding model (text-embedding-3-large = 3072 dims)
        """
        self.client = openai.OpenAI(api_key=api_key)
        self.model = model
        self.dimension = 3072 if "large" in model else 1536
        logger.info(f"Embedding client initialized: {model} ({self.dimension}D)")

    def embed_texts(
        self, texts: list[str], max_retries: int = 3, retry_delay: float = 1.0
    ) -> list[list[float]]:
        """
        Generate embeddings for batch of texts with retry logic.

        Args:
            texts: List of text strings to embed
            max_retries: Maximum retry attempts on failure
            retry_delay: Delay between retries (seconds)

        Returns:
            List of embedding vectors

        Raises:
            Exception: If all retries fail
        """
        if not texts:
            return []

        for attempt in range(max_retries):
            try:
                response = self.client.embeddings.create(input=texts, model=self.model)

                embeddings = [item.embedding for item in response.data]
                logger.debug(f"Generated {len(embeddings)} embeddings")
                return embeddings

            except openai.RateLimitError as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)  # Exponential backoff
                    logger.warning(f"Rate limit hit, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Rate limit exceeded after {max_retries} retries")
                    raise

            except openai.APIError as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2**attempt)
                    logger.warning(f"API error, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API error after {max_retries} retries")
                    raise

            except Exception as e:
                logger.error(f"Unexpected error generating embeddings: {e}")
                raise

        return []  # Should never reach here

    def embed_single(self, text: str) -> list[float]:
        """
        Generate embedding for single text.

        Args:
            text: Text string to embed

        Returns:
            Embedding vector
        """
        embeddings = self.embed_texts([text])
        return embeddings[0] if embeddings else []

    def estimate_cost(self, total_tokens: int) -> dict[str, Any]:
        """
        Estimate API cost for given token count.

        Args:
            total_tokens: Total tokens to embed

        Returns:
            Dict with cost breakdown
        """
        # text-embedding-3-large: $0.13 per 1M tokens
        cost_per_million = 0.13
        cost = (total_tokens / 1_000_000) * cost_per_million

        return {
            "model": self.model,
            "total_tokens": total_tokens,
            "cost_per_million": cost_per_million,
            "estimated_cost_usd": round(cost, 2),
        }
