"""Graph search client using Neo4j."""

from typing import Any

from src.graph.graph_connector import Neo4jClient
from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


class GraphSearch:
    """
    Graph-based search using Neo4j relationships.

    Finds content related to detected entities and their connections.
    """

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "neo4j", password: str = "finloom123"):
        """
        Initialize graph search client.

        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
        """
        self.connector = Neo4jClient(uri=uri, user=user, password=password)
        logger.info(f"Graph search initialized: {uri}")

    def search_by_entity(self, entity_name: str, entity_type: str = "COMPANY", top_k: int = 10) -> list[dict]:
        """
        Search for sections related to an entity.

        For companies, searches by ticker or name.
        For persons, searches by executive name.

        Args:
            entity_name: Name or ticker of entity (e.g., "AAPL" or "Apple")
            entity_type: Type of entity (COMPANY, PERSON)
            top_k: Number of results to return

        Returns:
            List of sections related to the entity
        """
        if entity_type == "PERSON":
            query = """
            MATCH (c:Company)-[:HAS_EXECUTIVE]->(p:Person)
            WHERE p.name CONTAINS $entity_name
            MATCH (c)-[:FILED]->(f:Filing)-[:CONTAINS_SECTION]->(s:Section)
            RETURN s.content_summary as content,
                   s.section_type as section_type,
                   c.name as company_name,
                   c.ticker as ticker,
                   f.filing_date as filing_date
            LIMIT $top_k
            """
        else:
            # Default: search by company ticker or name
            query = """
            MATCH (c:Company)
            WHERE c.ticker = $entity_name OR c.name CONTAINS $entity_name
            MATCH (c)-[:FILED]->(f:Filing)-[:CONTAINS_SECTION]->(s:Section)
            RETURN s.content_summary as content,
                   s.section_type as section_type,
                   c.name as company_name,
                   c.ticker as ticker,
                   f.filing_date as filing_date
            ORDER BY f.filing_date DESC
            LIMIT $top_k
            """

        try:
            results = self.connector.execute_query(
                query,
                {"entity_name": entity_name, "top_k": top_k},
            )

            return [
                {
                    "content": record["content"] or "",
                    "score": 1.0,
                    "metadata": {
                        "section_type": record.get("section_type"),
                        "company_name": record.get("company_name", ""),
                        "ticker": record.get("ticker", ""),
                        "filing_date": record.get("filing_date", ""),
                        "source": "graph",
                    },
                }
                for record in results
                if record.get("content")
            ]
        except Exception as e:
            logger.warning(f"Graph search failed: {e}")
            return []

    def close(self):
        """Close Neo4j connection."""
        self.connector.close()
