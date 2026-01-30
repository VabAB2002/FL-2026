"""Vector search client wrapping Qdrant."""

from src.infrastructure.logger import get_logger
from src.vectors import EmbeddingClient, VectorStore

logger = get_logger(__name__)


class VectorSearch:
    """
    Vector similarity search using Qdrant.

    Thin wrapper around existing VectorStore with standardized interface.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6333,
        embedding_api_key: str | None = None,
    ):
        """
        Initialize vector search client.

        Args:
            host: Qdrant host
            port: Qdrant port
            embedding_api_key: OpenAI API key for query embedding
        """
        self.embedding_client = EmbeddingClient(api_key=embedding_api_key)
        self.vector_store = VectorStore(host=host, port=port)
        logger.info(f"Vector search initialized: {host}:{port}")

    def search(
        self, query: str, top_k: int = 20, filters: dict[str, str] | None = None
    ) -> list[dict]:
        """
        Search for documents similar to query.

        Args:
            query: Search query text
            top_k: Number of results to return
            filters: Optional payload filters (e.g., {"ticker": "AAPL"})

        Returns:
            List of search results with score, content, and metadata
        """
        # Generate query embedding
        query_vector = self.embedding_client.embed_single(query)

        # Search Qdrant
        results = self.vector_store.search(
            query_vector, limit=top_k, score_threshold=None, payload_filter=filters
        )

        # Standardize format
        return [
            {
                "content": r["payload"]["content"],
                "score": r["score"],
                "metadata": {
                    "chunk_id": r["payload"]["chunk_id"],
                    "ticker": r["payload"]["ticker"],
                    "company_name": r["payload"]["company_name"],
                    "section_item": r["payload"]["section_item"],
                    "section_title": r["payload"]["section_title"],
                    "filing_date": r["payload"]["filing_date"],
                    "source": "vector",
                },
            }
            for r in results
        ]

    def close(self):
        """Close vector store connection."""
        self.vector_store.close()
