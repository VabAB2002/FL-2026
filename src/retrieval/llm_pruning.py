"""
LLM-based relevance pruning for multi-hop retrieval.

At each hop, evaluates neighbor chunks and decides which are relevant
to the query, preventing irrelevant drift during graph expansion.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

import openai

from src.infrastructure.logger import get_logger

logger = get_logger("finloom.retrieval.llm_pruning")

PRUNING_SYSTEM_PROMPT = (
    "You evaluate whether candidate passages from SEC filings are relevant "
    "for answering a financial analysis query.\n\n"
    "Given the query and already-retrieved context, evaluate each candidate. "
    "For each, decide KEEP or PRUNE.\n\n"
    "KEEP if the passage:\n"
    "- Provides new information relevant to the query\n"
    "- Connects causally or temporally to the current context\n"
    "- Contains comparative data, risk factors, or metrics mentioned in the query\n\n"
    "PRUNE if the passage:\n"
    "- Repeats information already in context\n"
    "- Is about an unrelated topic, company, or time period\n"
    "- Is boilerplate/generic SEC language\n\n"
    "Respond with JSON:\n"
    '{"decisions": [{"id": "0", "action": "keep", "reason": "brief reason"}, ...]}'
)


@dataclass
class PruningResult:
    kept_chunk_ids: list[str] = field(default_factory=list)
    pruned_chunk_ids: list[str] = field(default_factory=list)


class LLMPruner:
    """Evaluate neighbor chunk relevance using DeepSeek LLM."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
        max_retries: int = 3,
        batch_size: int = 15,
    ):
        self._api_key = api_key
        self._base_url = base_url
        self._client: openai.OpenAI | None = None
        self.model = model
        self.max_retries = max_retries
        self.batch_size = batch_size

    def _get_client(self) -> openai.OpenAI:
        """Lazy-init LLM client."""
        if self._client is None:
            self._client = openai.OpenAI(
                api_key=self._api_key or os.getenv("DEEPSEEK_API_KEY"),
                base_url=self._base_url,
            )
        return self._client

    def prune(
        self,
        query: str,
        current_context: list[dict],
        candidates: list[dict],
        max_keep: int = 5,
    ) -> PruningResult:
        """
        Evaluate candidate passages and keep the most relevant ones.

        Args:
            query: The user's search query.
            current_context: Already-retrieved passages from previous hops.
            candidates: Neighbor passages to evaluate. Each has
                "content", "score", "metadata" (with chunk_id, ticker, etc).
            max_keep: Maximum passages to keep from this batch.

        Returns:
            PruningResult with kept and pruned chunk_ids.
        """
        if not candidates:
            return PruningResult()

        context_summary = self._summarize_context(current_context)
        result = PruningResult()

        # Process in batches
        for i in range(0, len(candidates), self.batch_size):
            batch = candidates[i : i + self.batch_size]
            batch_result = self._prune_batch(query, context_summary, batch)
            result.kept_chunk_ids.extend(batch_result.kept_chunk_ids)
            result.pruned_chunk_ids.extend(batch_result.pruned_chunk_ids)

        # Enforce max_keep across all batches
        if len(result.kept_chunk_ids) > max_keep:
            overflow = result.kept_chunk_ids[max_keep:]
            result.kept_chunk_ids = result.kept_chunk_ids[:max_keep]
            result.pruned_chunk_ids.extend(overflow)

        logger.info(
            f"Pruning: {len(result.kept_chunk_ids)} kept, "
            f"{len(result.pruned_chunk_ids)} pruned"
        )
        return result

    def _prune_batch(
        self,
        query: str,
        context_summary: str,
        candidates: list[dict],
    ) -> PruningResult:
        """Prune a single batch of candidates via LLM."""
        # Build candidate descriptions
        candidate_lines = []
        chunk_id_map: dict[str, str] = {}  # index -> chunk_id
        for idx, cand in enumerate(candidates):
            meta = cand.get("metadata", {})
            ticker = meta.get("ticker", "?")
            section = meta.get("section_title", "?")
            filing_date = meta.get("filing_date", "?")
            preview = cand.get("content", "")[:200]
            candidate_lines.append(
                f"[{idx}] [{ticker} | {section} | {filing_date}] {preview}"
            )
            chunk_id_map[str(idx)] = meta.get("chunk_id", f"unknown_{idx}")

        user_prompt = (
            f"Query: {query}\n\n"
            f"Current context summary:\n{context_summary}\n\n"
            f"Candidate passages:\n" + "\n".join(candidate_lines)
        )

        for attempt in range(self.max_retries):
            try:
                response = self._get_client().chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": PRUNING_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=0.0,
                    max_tokens=800,
                    response_format={"type": "json_object"},
                )
                text = response.choices[0].message.content.strip()
                return self._parse_response(text, chunk_id_map)

            except (openai.RateLimitError, openai.APIConnectionError) as e:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    f"Pruning API error (attempt {attempt + 1}): {e}. "
                    f"Retrying in {wait}s"
                )
                time.sleep(wait)
            except Exception as e:
                logger.error(f"Pruning failed: {e}")
                break

        # On failure, keep all candidates (don't lose data)
        logger.warning("Pruning fallback: keeping all candidates")
        return PruningResult(
            kept_chunk_ids=[
                cand.get("metadata", {}).get("chunk_id", "")
                for cand in candidates
                if cand.get("metadata", {}).get("chunk_id")
            ]
        )

    def _parse_response(
        self,
        response_text: str,
        chunk_id_map: dict[str, str],
    ) -> PruningResult:
        """Parse LLM JSON response into PruningResult."""
        result = PruningResult()

        try:
            data = json.loads(response_text)
            decisions = data.get("decisions", [])

            for d in decisions:
                idx = str(d.get("id", ""))
                action = d.get("action", "prune").lower()
                chunk_id = chunk_id_map.get(idx, "")

                if not chunk_id:
                    continue

                if action == "keep":
                    result.kept_chunk_ids.append(chunk_id)
                else:
                    result.pruned_chunk_ids.append(chunk_id)

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse pruning response: {e}")
            # Keep all on parse failure
            result.kept_chunk_ids = list(chunk_id_map.values())

        return result

    def _summarize_context(self, context: list[dict], max_chars: int = 500) -> str:
        """Build a brief summary of already-retrieved context."""
        if not context:
            return "No context retrieved yet."

        parts = []
        total = 0
        for doc in context[:5]:
            meta = doc.get("metadata", {})
            ticker = meta.get("ticker", "?")
            section = meta.get("section_title", "?")
            snippet = doc.get("content", "")[:80]
            line = f"- {ticker} ({section}): {snippet}"
            if total + len(line) > max_chars:
                break
            parts.append(line)
            total += len(line)

        return "\n".join(parts) if parts else "No context retrieved yet."
