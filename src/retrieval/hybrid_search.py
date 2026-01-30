"""Hybrid search orchestrator combining vector, keyword, and graph search."""

import os
import re
from typing import Any

from src.infrastructure.logger import get_logger
from src.retrieval.graph_search import GraphSearch
from src.retrieval.keyword_search import KeywordSearch
from src.retrieval.reranker import Reranker
from src.retrieval.vector_search import VectorSearch

logger = get_logger(__name__)

# Simple pattern to detect company names / tickers in queries
_TICKER_RE = re.compile(r"\b[A-Z]{2,5}\b")


class HybridRetriever:
    """
    Hybrid retrieval combining multiple search strategies.

    Orchestrates vector (semantic), keyword (exact), and optional graph search,
    then reranks results using Cohere.
    """

    def __init__(
        self,
        openai_api_key: str | None = None,
        cohere_api_key: str | None = None,
        qdrant_host: str = "localhost",
        qdrant_port: int = 6333,
        meilisearch_host: str = "http://localhost:7700",
        neo4j_uri: str = "bolt://localhost:7687",
        neo4j_user: str = "neo4j",
        neo4j_password: str | None = None,
        use_reranking: bool = True,
        use_graph: bool = True,
    ):
        """
        Initialize hybrid retriever.

        Args:
            openai_api_key: OpenAI API key for embeddings
            cohere_api_key: Cohere API key for reranking
            qdrant_host: Qdrant host
            qdrant_port: Qdrant port
            meilisearch_host: Meilisearch host URL
            neo4j_uri: Neo4j bolt URI
            neo4j_user: Neo4j username
            neo4j_password: Neo4j password
            use_reranking: Whether to use Cohere reranking
            use_graph: Whether to use Neo4j graph search
        """
        # Get API keys / secrets from env if not provided
        openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        cohere_api_key = cohere_api_key or os.getenv("COHERE_API_KEY")
        neo4j_password = neo4j_password or os.getenv("NEO4J_PASSWORD", "finloom123")

        # Initialize search clients
        self.vector_search = VectorSearch(
            host=qdrant_host, port=qdrant_port, embedding_api_key=openai_api_key
        )
        self.keyword_search = KeywordSearch(host=meilisearch_host)

        # Initialize graph search if enabled
        self.graph_search = None
        if use_graph:
            try:
                self.graph_search = GraphSearch(
                    uri=neo4j_uri, user=neo4j_user, password=neo4j_password
                )
            except Exception as e:
                logger.warning(f"Graph search unavailable: {e}")

        # Build company name -> ticker lookup for filtering
        self._company_lookup = self._build_company_lookup()

        # Initialize reranker if enabled
        self.use_reranking = use_reranking and cohere_api_key
        if self.use_reranking:
            self.reranker = Reranker(api_key=cohere_api_key)
            logger.info("Hybrid retriever initialized with reranking")
        else:
            self.reranker = None
            logger.info("Hybrid retriever initialized without reranking")

    def _build_company_lookup(self) -> dict[str, str]:
        """
        Build a lowercase company-name -> ticker lookup from Meilisearch.

        Scans indexed documents to extract unique company_name -> ticker
        mappings so that natural-language mentions like "Apple" can be
        resolved to "AAPL" for filtering.
        """
        lookup: dict[str, str] = {}
        try:
            index = self.keyword_search.client.get_index(self.keyword_search.index_name)
            offset = 0
            seen_tickers: set[str] = set()
            while True:
                batch = index.get_documents(
                    {"limit": 100, "offset": offset, "fields": ["ticker", "company_name"]}
                )
                results = batch.results
                if not results:
                    break
                for doc in results:
                    ticker = getattr(doc, "ticker", "") or ""
                    company_name = getattr(doc, "company_name", "") or ""
                    if ticker and ticker not in seen_tickers:
                        seen_tickers.add(ticker)
                        lookup[ticker.lower()] = ticker
                        if company_name:
                            lookup[company_name.lower()] = ticker
                            first_word = company_name.split()[0].lower()
                            if len(first_word) > 2:
                                lookup[first_word] = ticker
                offset += 100
                if len(seen_tickers) > 50:
                    break
        except Exception as e:
            logger.warning(f"Could not build company lookup: {e}")
        logger.info(f"Company lookup built: {len(lookup)} entries, {len(set(lookup.values()))} companies")
        return lookup

    def _resolve_ticker(self, query: str) -> str | None:
        """
        Resolve a query to a company ticker if a specific company is mentioned.

        Returns:
            Ticker string (e.g., "AAPL") or None if no company detected.
        """
        query_lower = query.lower()

        # Check each known company name / ticker against the query
        for name, ticker in self._company_lookup.items():
            if name in query_lower:
                logger.debug(f"Resolved company: '{name}' -> {ticker}")
                return ticker
        return None

    def extract_entities(self, query: str) -> list[str]:
        """
        Extract potential entity names from a query.

        Uses simple heuristics: uppercase words that look like tickers,
        and known company name patterns (e.g., "Apple", "Microsoft").

        Args:
            query: Search query text

        Returns:
            List of detected entity names
        """
        entities = []

        # Detect ticker-like patterns (2-5 uppercase letters)
        tickers = _TICKER_RE.findall(query)
        # Filter common English words that look like tickers
        stop_words = {"THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL",
                       "CAN", "HAS", "HER", "WAS", "ONE", "OUR", "OUT", "HIS",
                       "HOW", "ITS", "MAY", "NEW", "NOW", "OLD", "SEE", "WAY",
                       "WHO", "DID", "GET", "LET", "SAY", "SHE", "TOO", "USE",
                       "WHAT", "WITH", "FROM", "THIS", "THAT", "HAVE", "BEEN"}
        for t in tickers:
            if t not in stop_words:
                entities.append(t)

        # Detect capitalised multi-word names (e.g., "Apple Inc", "Goldman Sachs")
        name_pattern = re.findall(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", query)
        for name in name_pattern:
            if len(name) > 3:
                entities.append(name)

        return entities

    def retrieve(
        self,
        query: str,
        top_k: int = 20,
        vector_weight: float = 0.7,
        keyword_weight: float = 0.3,
        keyword_boost_threshold: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Retrieve relevant documents using hybrid search.

        Args:
            query: Search query
            top_k: Number of results to return
            vector_weight: Weight for vector search results
            keyword_weight: Weight for keyword search results
            keyword_boost_threshold: Boost keyword results if query has N+ words

        Returns:
            Ranked list of documents with content, score, and metadata
        """
        logger.info(f"Hybrid search: '{query}' (top_k={top_k})")

        # Detect company ticker for scoped filtering
        ticker = self._resolve_ticker(query)
        if ticker:
            logger.info(f"Company filter active: {ticker}")

        # 1. Vector search (semantic similarity), scoped to company if detected
        vector_filters = {"ticker": ticker} if ticker else None
        vector_results = self.vector_search.search(
            query, top_k=top_k * 2, filters=vector_filters
        )
        logger.debug(f"Vector search: {len(vector_results)} results")

        # 2. Keyword search (exact matching), scoped to company if detected
        keyword_filter_str = f"ticker = '{ticker}'" if ticker else None
        word_count = len(query.split())
        if word_count >= keyword_boost_threshold:
            keyword_limit = max(10, top_k // 2)
        else:
            keyword_limit = min(5, top_k // 3)

        keyword_results = self.keyword_search.search(
            query, top_k=keyword_limit, filters=keyword_filter_str
        )
        logger.debug(f"Keyword search: {len(keyword_results)} results")

        # 3. Graph search (if entities detected and graph is available)
        graph_results: list[dict[str, Any]] = []
        if self.graph_search:
            entities = self.extract_entities(query)
            if entities:
                logger.debug(f"Detected entities: {entities}")
                for entity in entities[:2]:  # Limit to first 2 entities
                    try:
                        results = self.graph_search.search_by_entity(
                            entity_name=entity, top_k=5
                        )
                        graph_results.extend(results)
                    except Exception as e:
                        logger.warning(f"Graph search failed for '{entity}': {e}")
                logger.debug(f"Graph search: {len(graph_results)} results")

        # 4. Merge and deduplicate results
        merged = self._merge_results(
            vector_results,
            keyword_results,
            graph_results,
            vector_weight=vector_weight,
            keyword_weight=keyword_weight,
        )
        logger.debug(f"Merged: {len(merged)} unique results")

        # 5. Rerank if enabled
        if self.use_reranking and self.reranker:
            reranked = self.reranker.rerank(query, merged, top_k=top_k)
            logger.debug(f"Reranked: {len(reranked)} results")
            return reranked
        else:
            # Sort by combined score and return top-k
            sorted_results = sorted(merged, key=lambda x: x["score"], reverse=True)
            return sorted_results[:top_k]

    def _merge_results(
        self,
        vector_results: list[dict],
        keyword_results: list[dict],
        graph_results: list[dict] | None = None,
        vector_weight: float = 0.7,
        keyword_weight: float = 0.3,
        graph_weight: float = 0.5,
    ) -> list[dict]:
        """
        Merge and deduplicate results from multiple sources.

        Uses chunk_id to deduplicate. For duplicates, combines scores.
        Graph results without chunk_id are appended directly.

        Args:
            vector_results: Results from vector search
            keyword_results: Results from keyword search
            graph_results: Results from graph search (optional)
            vector_weight: Weight for vector scores
            keyword_weight: Weight for keyword scores
            graph_weight: Weight for graph scores

        Returns:
            Deduplicated and scored results
        """
        seen: dict[str, dict] = {}

        # Add vector results
        for result in vector_results:
            chunk_id = result["metadata"]["chunk_id"]
            result["score"] = result["score"] * vector_weight
            result["metadata"]["sources"] = ["vector"]
            seen[chunk_id] = result

        # Add keyword results (merge if duplicate)
        for result in keyword_results:
            chunk_id = result["metadata"]["chunk_id"]
            weighted_score = result["score"] * keyword_weight

            if chunk_id in seen:
                seen[chunk_id]["score"] += weighted_score
                seen[chunk_id]["metadata"]["sources"].append("keyword")
            else:
                result["score"] = weighted_score
                result["metadata"]["sources"] = ["keyword"]
                seen[chunk_id] = result

        # Add graph results (may lack chunk_id)
        if graph_results:
            for result in graph_results:
                chunk_id = result["metadata"].get("chunk_id")
                weighted_score = result["score"] * graph_weight

                if chunk_id and chunk_id in seen:
                    seen[chunk_id]["score"] += weighted_score
                    seen[chunk_id]["metadata"]["sources"].append("graph")
                else:
                    result["score"] = weighted_score
                    result["metadata"]["sources"] = ["graph"]
                    # Use a unique key for graph results without chunk_id
                    key = chunk_id or f"graph_{id(result)}"
                    seen[key] = result

        return list(seen.values())

    def close(self):
        """Close all search client connections."""
        self.vector_search.close()
        if self.graph_search:
            self.graph_search.close()
        logger.info("Hybrid retriever closed")
