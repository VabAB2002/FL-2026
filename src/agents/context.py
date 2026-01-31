"""Shared resource context for agent tools."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from src.infrastructure.logger import get_logger

if TYPE_CHECKING:
    from src.retrieval.hoprag_retriever import HopRAGRetriever
    from src.retrieval.hybrid_search import HybridRetriever
    from src.storage.connection import Database

logger = get_logger("finloom.agents.context")


class AgentContext:
    """Manages shared resources for agent tools with lazy loading.

    Resources are initialized on first access and reused across tool
    calls. The passage graph (59 MB) is only loaded if multi_hop_search
    is actually called.
    """

    def __init__(
        self,
        db_path: str | None = None,
        passage_graph_path: str = "data/passage_graph.pkl",
    ):
        self._db_path = db_path
        self._passage_graph_path = Path(passage_graph_path)

        self._db: Database | None = None
        self._hybrid_retriever: HybridRetriever | None = None
        self._hoprag_retriever: HopRAGRetriever | None = None

        logger.info("AgentContext initialized (resources load on demand)")

    @property
    def db(self) -> Database:
        if self._db is None:
            from src.storage.connection import Database

            logger.info("Loading DuckDB connection...")
            self._db = Database(db_path=self._db_path, read_only=True)
        return self._db

    @property
    def hybrid_retriever(self) -> HybridRetriever:
        if self._hybrid_retriever is None:
            from src.retrieval.hybrid_search import HybridRetriever

            logger.info("Loading hybrid retriever (vector + keyword + graph)...")
            self._hybrid_retriever = HybridRetriever(
                use_reranking=True, use_graph=True
            )
        return self._hybrid_retriever

    @property
    def hoprag_retriever(self) -> HopRAGRetriever:
        if self._hoprag_retriever is None:
            from src.retrieval.hoprag_retriever import create_hoprag_retriever

            logger.info("Loading HopRAG retriever (passage graph)...")
            self._hoprag_retriever = create_hoprag_retriever(
                passage_graph_path=str(self._passage_graph_path),
                use_reranking=True,
            )
        return self._hoprag_retriever

    def close(self) -> None:
        """Release all resources."""
        if self._db is not None:
            self._db.close()
        if self._hybrid_retriever is not None:
            self._hybrid_retriever.close()
        elif self._hoprag_retriever is not None:
            # hoprag wraps hybrid, only close if hybrid wasn't created separately
            self._hoprag_retriever.close()
        logger.info("AgentContext resources closed")
