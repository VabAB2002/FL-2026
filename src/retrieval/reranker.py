"""Reranking using Cohere's rerank API."""

import cohere

from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


class Reranker:
    """
    Rerank search results using Cohere's rerank model.

    Improves result quality by scoring results against the original query.
    """

    def __init__(self, api_key: str, model: str = "rerank-v3.5"):
        """
        Initialize reranker.

        Args:
            api_key: Cohere API key
            model: Cohere rerank model name
        """
        self.client = cohere.ClientV2(api_key=api_key)
        self.model = model
        logger.info(f"Reranker initialized: {model}")

    def rerank(self, query: str, documents: list[dict], top_k: int = 20) -> list[dict]:
        """
        Rerank documents based on relevance to query.

        Args:
            query: Original search query
            documents: List of documents with 'content' and 'metadata'
            top_k: Number of top results to return

        Returns:
            Reranked list of documents with updated scores
        """
        if not documents:
            return []

        # Extract texts for reranking
        texts = [doc["content"] for doc in documents]

        try:
            # Call Cohere rerank API
            response = self.client.rerank(
                model=self.model,
                query=query,
                documents=texts,
                top_n=top_k,
            )

            # Map reranked results back to original documents
            reranked = []
            for result in response.results:
                idx = result.index
                doc = documents[idx].copy()
                doc["score"] = result.relevance_score  # Cohere's relevance score
                doc["metadata"]["rerank_score"] = result.relevance_score
                reranked.append(doc)

            logger.debug(f"Reranked {len(documents)} → {len(reranked)} documents")
            return reranked

        except Exception as e:
            logger.warning(f"Reranking failed: {e}. Returning original results.")
            return documents[:top_k]
