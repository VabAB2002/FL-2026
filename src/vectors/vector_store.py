"""Qdrant vector database client for SEC filing chunks."""

from __future__ import annotations

from typing import Any

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, FieldCondition, Filter, MatchValue, PointStruct, VectorParams

from ..infrastructure.logger import get_logger

logger = get_logger("finloom.vectors.store")


class VectorStore:
    """Wrapper for Qdrant vector database operations."""

    def __init__(self, host: str = "localhost", port: int = 6333):
        """
        Initialize Qdrant client.

        Args:
            host: Qdrant server host
            port: Qdrant server port
        """
        # Disable version check to work with v1.7.4 server
        self.client = QdrantClient(host=host, port=port, prefer_grpc=False)
        self.collection_name = "sec_filings"
        logger.info(f"Qdrant client initialized: {host}:{port}")

    def create_collection(
        self, vector_size: int = 3072, distance: Distance = Distance.COSINE, force: bool = False
    ) -> None:
        """
        Create collection for SEC filing embeddings.

        Args:
            vector_size: Embedding dimension (3072 for text-embedding-3-large)
            distance: Distance metric (COSINE recommended)
            force: If True, recreate collection if exists
        """
        # Check if collection exists (handle API differences gracefully)
        try:
            collections = self.client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
        except Exception:
            exists = False

        if force and exists:
            logger.warning(f"Deleting existing collection: {self.collection_name}")
            self.client.delete_collection(self.collection_name)
            exists = False

        if not exists:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=vector_size, distance=distance),
            )
            logger.info(
                f"Created collection '{self.collection_name}': "
                f"{vector_size}D vectors, {distance} distance"
            )
        else:
            logger.info(f"Collection '{self.collection_name}' already exists")

    def upsert_points(self, points: list[PointStruct]) -> None:
        """
        Upload embedding points to Qdrant.

        Args:
            points: List of PointStruct objects with id, vector, and payload
        """
        if not points:
            return

        self.client.upsert(collection_name=self.collection_name, points=points)
        logger.debug(f"Upserted {len(points)} points to {self.collection_name}")

    def get_collection_info(self) -> dict[str, Any]:
        """Get collection statistics."""
        try:
            info = self.client.get_collection(self.collection_name)
            return {
                "exists": True,
                "points_count": info.points_count,
                "vectors_count": info.vectors_count,
                "status": info.status,
            }
        except Exception:
            return {"exists": False}

    def search(
        self,
        query_vector: list[float],
        limit: int = 5,
        score_threshold: float | None = None,
        payload_filter: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for similar chunks.

        Args:
            query_vector: Embedding vector to search for
            limit: Number of results to return
            score_threshold: Minimum similarity score
            payload_filter: Optional dict of payload field -> value to filter on
                            (e.g., {"ticker": "AAPL"})

        Returns:
            List of search results with payload and score
        """
        # Build Qdrant filter from payload_filter dict
        query_filter = None
        if payload_filter:
            conditions = [
                FieldCondition(key=key, match=MatchValue(value=value))
                for key, value in payload_filter.items()
            ]
            query_filter = Filter(must=conditions)

        # Use query_points (v1.16+ API)
        results = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            query_filter=query_filter,
            limit=limit,
            score_threshold=score_threshold,
        )

        return [
            {
                "id": result.id,
                "score": result.score,
                "payload": result.payload,
            }
            for result in results.points
        ]

    def close(self) -> None:
        """Close Qdrant client connection."""
        self.client.close()
        logger.info("Qdrant client closed")
