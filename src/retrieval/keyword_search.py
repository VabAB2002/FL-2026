"""Keyword search client using Meilisearch."""

from typing import Any

import meilisearch

from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


class KeywordSearch:
    """
    Fast keyword/full-text search using Meilisearch.

    Complements vector search with exact matching and BM25 ranking.
    """

    def __init__(self, host: str = "http://localhost:7700", api_key: str | None = None):
        """
        Initialize keyword search client.

        Args:
            host: Meilisearch host URL
            api_key: Meilisearch master key (optional in dev mode)
        """
        self.client = meilisearch.Client(host, api_key)
        self.index_name = "sec_filings"
        logger.info(f"Keyword search initialized: {host}")

    def create_index(self) -> None:
        """Create Meilisearch index with configuration."""
        try:
            index = self.client.get_index(self.index_name)
            logger.info(f"Index '{self.index_name}' already exists")
        except meilisearch.errors.MeilisearchApiError:
            # Create index
            self.client.create_index(self.index_name, {"primaryKey": "chunk_id"})
            logger.info(f"Created index '{self.index_name}'")

            # Configure searchable attributes
            index = self.client.get_index(self.index_name)
            index.update_searchable_attributes(
                ["content", "section_title", "company_name", "ticker"]
            )

            # Configure filterable attributes
            index.update_filterable_attributes(
                ["ticker", "section_item", "filing_date", "company_name"]
            )

            logger.info("Index configuration complete")

    def index_documents(self, documents: list[dict[str, Any]], batch_size: int = 1000) -> None:
        """
        Index documents in batches.

        Args:
            documents: List of documents with chunk_id, content, and metadata
            batch_size: Number of documents per batch
        """
        index = self.client.get_index(self.index_name)

        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            task = index.add_documents(batch)
            logger.debug(f"Indexed batch {i//batch_size + 1}: task {task.task_uid}")

        logger.info(f"Indexed {len(documents)} documents in '{self.index_name}'")

    def search(self, query: str, top_k: int = 10, filters: str | None = None) -> list[dict]:
        """
        Search for documents matching query.

        Args:
            query: Search query text
            top_k: Number of results to return
            filters: Optional Meilisearch filter string (e.g., "ticker = AAPL")

        Returns:
            List of search results with score, content, and metadata
        """
        index = self.client.get_index(self.index_name)

        results = index.search(
            query,
            {
                "limit": top_k,
                "filter": filters,
                "showRankingScore": True,
                "attributesToRetrieve": [
                    "chunk_id",
                    "content",
                    "ticker",
                    "company_name",
                    "section_item",
                    "section_title",
                    "filing_date",
                ],
            },
        )

        # Standardize format using Meilisearch's ranking score
        return [
            {
                "content": hit["content"],
                "score": hit.get("_rankingScore", 0.5),
                "metadata": {
                    "chunk_id": hit["chunk_id"],
                    "ticker": hit.get("ticker", ""),
                    "company_name": hit.get("company_name", ""),
                    "section_item": hit.get("section_item", ""),
                    "section_title": hit.get("section_title", ""),
                    "filing_date": hit.get("filing_date", ""),
                    "source": "keyword",
                },
            }
            for hit in results["hits"]
        ]

    def get_stats(self) -> dict[str, Any]:
        """Get index statistics."""
        index = self.client.get_index(self.index_name)
        stats = index.get_stats()
        return {
            "number_of_documents": stats.number_of_documents,
            "is_indexing": stats.is_indexing,
            "field_distribution": stats.field_distribution,
        }
