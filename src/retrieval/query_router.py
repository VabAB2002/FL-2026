"""
Route queries to appropriate retrieval strategy based on complexity.

Uses rule-based heuristics first (fast, free), falls back to DeepSeek
LLM for ambiguous queries.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from enum import Enum

import openai

from src.infrastructure.logger import get_logger

logger = get_logger("finloom.retrieval.query_router")

_TICKER_RE = re.compile(r"\b[A-Z]{2,5}\b")

# Company names for detection (subset from passage_graph)
_COMPANY_NAMES: dict[str, list[str]] = {
    "AMD": ["AMD", "Advanced Micro Devices"],
    "AAPL": ["Apple"],
    "AMZN": ["Amazon"],
    "BAC": ["Bank of America"],
    "CSCO": ["Cisco"],
    "DIS": ["Disney", "Walt Disney"],
    "GOOG": ["Google", "Alphabet"],
    "GS": ["Goldman Sachs"],
    "HD": ["Home Depot"],
    "IBM": ["IBM"],
    "INTC": ["Intel"],
    "JPM": ["JPMorgan", "JP Morgan"],
    "META": ["Meta", "Facebook"],
    "MSFT": ["Microsoft"],
    "NVDA": ["NVIDIA"],
    "ORCL": ["Oracle"],
    "TSLA": ["Tesla"],
    "WFC": ["Wells Fargo"],
    "WMT": ["Walmart"],
    "BRKA": ["Berkshire Hathaway", "Berkshire"],
}

_CROSS_FILING_WORDS = re.compile(
    r"\b(compare|comparison|versus|vs\.?|difference between|relative to|"
    r"industry|sector|peers?|competitors?|against|benchmark)\b",
    re.IGNORECASE,
)

_COMPLEX_WORDS = re.compile(
    r"\b(trend|over time|year-over-year|yoy|changed|growth|decline|"
    r"historically|evolution|trajectory|why|because|impact|effect|"
    r"caused by|led to|resulted in|driven by|attributed to|"
    r"relationship between|correlation|how did .+ affect)\b",
    re.IGNORECASE,
)

_SIMPLE_PATTERNS = re.compile(
    r"\b(what is|what was|who is|who was|when did|how much|how many|"
    r"name the|list the|define)\b",
    re.IGNORECASE,
)

ROUTER_SYSTEM_PROMPT = (
    "You classify SEC filing analysis queries into three categories.\n\n"
    "SIMPLE_FACT: Single fact from one filing section. "
    'Examples: "What was Apple\'s revenue in 2023?", "Who is NVIDIA\'s CEO?"\n\n'
    "COMPLEX_ANALYSIS: Requires connecting multiple pieces of information "
    "within or across sections of the same company's filings. "
    'Examples: "How did AMD\'s R&D spending relate to their revenue growth?", '
    '"What risk factors affected Tesla\'s operating margins?"\n\n'
    "CROSS_FILING: Requires comparing information across multiple companies "
    "or analyzing industry-wide patterns. "
    'Examples: "Compare Intel and AMD\'s semiconductor strategies", '
    '"Which tech companies mentioned AI risks in their 10-K?"\n\n'
    'Respond with JSON: {"type": "simple_fact"|"complex_analysis"|"cross_filing", '
    '"reasoning": "brief explanation"}'
)


class QueryType(Enum):
    SIMPLE_FACT = "simple_fact"
    COMPLEX_ANALYSIS = "complex_analysis"
    CROSS_FILING = "cross_filing"


@dataclass
class RoutingDecision:
    query_type: QueryType
    max_hops: int
    confidence: float
    reasoning: str


def detect_companies(query: str) -> set[str]:
    """Detect distinct company tickers mentioned in query."""
    found = set()
    query_upper = query.upper()
    for ticker, names in _COMPANY_NAMES.items():
        if ticker in query_upper:
            found.add(ticker)
            continue
        for name in names:
            if name.lower() in query.lower():
                found.add(ticker)
                break
    return found


def _count_companies(query: str) -> int:
    """Count distinct companies mentioned in query."""
    return len(detect_companies(query))


class QueryRouter:
    """Route queries to retrieval strategies based on complexity."""

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-chat",
    ):
        self._api_key = api_key
        self._base_url = base_url
        self._client: openai.OpenAI | None = None
        self.model = model

    def _get_client(self) -> openai.OpenAI:
        """Lazy-init LLM client (only needed for ambiguous queries)."""
        if self._client is None:
            self._client = openai.OpenAI(
                api_key=self._api_key or os.getenv("DEEPSEEK_API_KEY"),
                base_url=self._base_url,
            )
        return self._client

    def route(self, query: str) -> RoutingDecision:
        """Classify query: try rules first, fall back to LLM."""
        decision = self._rule_based_classify(query)
        if decision is not None:
            logger.info(
                f"Query routed (rules): {decision.query_type.value} "
                f"(confidence={decision.confidence:.2f})"
            )
            return decision

        decision = self._llm_classify(query)
        logger.info(
            f"Query routed (LLM): {decision.query_type.value} "
            f"(confidence={decision.confidence:.2f})"
        )
        return decision

    def _rule_based_classify(self, query: str) -> RoutingDecision | None:
        """Fast heuristic classification. Returns None if uncertain."""
        num_companies = _count_companies(query)
        has_cross = bool(_CROSS_FILING_WORDS.search(query))
        has_complex = bool(_COMPLEX_WORDS.search(query))
        has_simple = bool(_SIMPLE_PATTERNS.search(query))
        word_count = len(query.split())

        # Cross-filing: multiple companies or comparison language with a company
        if num_companies >= 2 or (num_companies >= 1 and has_cross):
            return RoutingDecision(
                query_type=QueryType.CROSS_FILING,
                max_hops=3,
                confidence=0.9 if num_companies >= 2 else 0.75,
                reasoning=f"{num_companies} companies detected, cross-filing signals",
            )

        # Complex: temporal or causal language
        if has_complex and not has_simple:
            return RoutingDecision(
                query_type=QueryType.COMPLEX_ANALYSIS,
                max_hops=2,
                confidence=0.8,
                reasoning="Temporal/causal analysis signals detected",
            )

        # Simple: short factual query with one entity
        if has_simple and word_count <= 12 and not has_complex:
            return RoutingDecision(
                query_type=QueryType.SIMPLE_FACT,
                max_hops=0,
                confidence=0.85,
                reasoning="Short factual query pattern",
            )

        # Uncertain — fall through to LLM
        return None

    def _llm_classify(self, query: str) -> RoutingDecision:
        """DeepSeek classification for ambiguous queries."""
        try:
            response = self._get_client().chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
                    {"role": "user", "content": query},
                ],
                temperature=0.0,
                max_tokens=100,
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content.strip()
            data = json.loads(text)

            type_str = data.get("type", "complex_analysis")
            reasoning = data.get("reasoning", "LLM classification")

            type_map = {
                "simple_fact": (QueryType.SIMPLE_FACT, 0),
                "complex_analysis": (QueryType.COMPLEX_ANALYSIS, 2),
                "cross_filing": (QueryType.CROSS_FILING, 3),
            }
            query_type, max_hops = type_map.get(
                type_str, (QueryType.COMPLEX_ANALYSIS, 2)
            )

            return RoutingDecision(
                query_type=query_type,
                max_hops=max_hops,
                confidence=0.7,
                reasoning=reasoning,
            )
        except Exception as e:
            logger.warning(f"LLM routing failed: {e}, defaulting to COMPLEX_ANALYSIS")
            return RoutingDecision(
                query_type=QueryType.COMPLEX_ANALYSIS,
                max_hops=2,
                confidence=0.5,
                reasoning=f"LLM fallback failed: {e}",
            )
