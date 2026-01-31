"""
Generate pseudo follow-up questions per chunk using DeepSeek LLM.

Uses OpenAI-compatible SDK with DeepSeek API endpoint.
Generates 3 follow-up questions per chunk for passage graph edges.
"""

from __future__ import annotations

import os
import time

import openai

from src.infrastructure.logger import get_logger

logger = get_logger("finloom.retrieval.pseudo_query")

SYSTEM_PROMPT = (
    "You are an SEC financial filing analyst. Given a chunk of text from an "
    "SEC 10-K filing, generate exactly 3 follow-up questions that an analyst "
    "would ask to find logically related information.\n\n"
    "Rules:\n"
    "- Questions should require information from OTHER parts of the filing "
    "or OTHER filings\n"
    "- Focus on: comparative metrics, causal explanations, risk factors, "
    "year-over-year changes, related entities\n"
    "- Be specific to the content, not generic\n"
    "- Return ONLY the 3 questions, one per line, no numbering or bullets"
)

# Max characters of chunk text to send
MAX_CHUNK_CHARS = 1500


class PseudoQueryGenerator:
    """Generate follow-up questions from chunk text using DeepSeek."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
        max_retries: int = 3,
        requests_per_minute: int = 200,
    ):
        """
        Initialize DeepSeek client via OpenAI SDK.

        Args:
            api_key: DeepSeek API key (or from DEEPSEEK_API_KEY env var)
            base_url: API base URL (swap to OpenAI if needed)
            model: Model name
            max_retries: Retry attempts on failure
            requests_per_minute: Rate limit
        """
        self.client = openai.OpenAI(
            api_key=api_key or os.getenv("DEEPSEEK_API_KEY"),
            base_url=base_url,
        )
        self.model = model
        self.max_retries = max_retries
        self._min_interval = 60.0 / requests_per_minute
        self._last_request_time = 0.0

    def generate_questions(
        self, chunk_text: str, context_prefix: str = ""
    ) -> list[str]:
        """
        Generate 3 follow-up questions for a chunk.

        Args:
            chunk_text: The chunk text content (truncated to 1500 chars)
            context_prefix: Filing context string

        Returns:
            List of question strings (typically 3, fewer on parse issues)
        """
        self._rate_limit()

        user_content = chunk_text[:MAX_CHUNK_CHARS]
        if context_prefix:
            user_content = f"[{context_prefix}]\n\n{user_content}"

        for attempt in range(self.max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": user_content},
                    ],
                    temperature=0.7,
                    max_tokens=200,
                )
                text = response.choices[0].message.content.strip()
                questions = [q.strip() for q in text.split("\n") if q.strip()]
                # Filter out empty or very short lines
                questions = [q for q in questions if len(q) > 10]
                return questions[:3]

            except (openai.RateLimitError, openai.APIConnectionError) as e:
                wait = 2 ** (attempt + 1)
                logger.warning(f"API error (attempt {attempt + 1}): {e}. Retrying in {wait}s")
                time.sleep(wait)
            except Exception as e:
                logger.error(f"Unexpected error generating questions: {e}")
                return []

        logger.warning("Max retries exceeded for question generation")
        return []

    def _rate_limit(self) -> None:
        """Enforce minimum interval between requests."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    @staticmethod
    def estimate_cost(
        num_chunks: int,
        avg_input_tokens: int = 450,
        avg_output_tokens: int = 80,
        input_cost_per_m: float = 0.14,
        output_cost_per_m: float = 0.28,
    ) -> dict:
        """
        Estimate DeepSeek API cost for pseudo-query generation.

        Returns:
            Dict with token counts and estimated USD cost.
        """
        total_input = num_chunks * avg_input_tokens
        total_output = num_chunks * avg_output_tokens
        cost = (total_input / 1_000_000 * input_cost_per_m) + (
            total_output / 1_000_000 * output_cost_per_m
        )
        return {
            "num_chunks": num_chunks,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "estimated_cost_usd": round(cost, 2),
        }
