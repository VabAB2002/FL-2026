"""Graph search client using Neo4j."""

from typing import Any

from src.graph.graph_connector import Neo4jClient
from src.infrastructure.logger import get_logger

logger = get_logger(__name__)


class GraphSearch:
    """
    Graph-based search using Neo4j relationships.

    Queries the actual graph structure:
    - Company -> Filing -> RiskFactor (via DISCLOSES_RISK)
    - Company -> Filing -> FinancialMetric (via REPORTS_METRIC)
    - Company -> Person (via HAS_EXECUTIVE)
    - Filing -> Entity nodes (via MENTIONS_* relationships)
    - Filing.community_summary for filing-level summaries
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
        Search for content related to an entity.

        For companies, returns risk factors, filing summaries, and executive info.
        For persons, returns filings associated with that executive.

        Args:
            entity_name: Name or ticker of entity (e.g., "AAPL" or "Apple")
            entity_type: Type of entity (COMPANY, PERSON)
            top_k: Number of results to return

        Returns:
            List of content items related to the entity
        """
        if entity_type == "PERSON":
            return self._search_person(entity_name, top_k)
        return self._search_company(entity_name, top_k)

    def _search_company(self, entity_name: str, top_k: int) -> list[dict]:
        """Search by company ticker or name.

        Gathers risk factors, filing summaries, and executive info
        from the actual graph relationships. Each category gets a
        guaranteed share of slots so one type doesn't crowd out others.
        """
        # Allocate slots: risks get most, summaries and executives get guaranteed slots
        exec_results = self._query_executives(entity_name, limit=5)
        summary_results = self._query_filing_summaries(entity_name, min(5, top_k))

        # Risk factors fill remaining slots
        reserved = len(exec_results) + len(summary_results)
        risk_budget = max(top_k - reserved, top_k // 2)
        risk_results = self._query_risk_factors(entity_name, risk_budget)

        # Interleave: risks first, then summaries, then executives
        results = risk_results + summary_results + exec_results
        return results[:top_k]

    def _query_risk_factors(self, entity_name: str, top_k: int) -> list[dict]:
        """Fetch risk factors for a company via DISCLOSES_RISK."""
        query = """
        MATCH (c:Company)-[:FILED]->(f:Filing)-[:DISCLOSES_RISK]->(r:RiskFactor)
        WHERE c.ticker = $entity_name OR c.name CONTAINS $entity_name
        RETURN r.description as content,
               r.category as category,
               r.severity as severity,
               c.name as company_name,
               c.ticker as ticker,
               f.filing_date as filing_date
        ORDER BY f.filing_date DESC, r.severity DESC
        LIMIT $top_k
        """
        try:
            records = self.connector.execute_query(
                query, {"entity_name": entity_name, "top_k": top_k}
            )
            return [
                {
                    "content": record["content"] or "",
                    "score": min(1.0, (record.get("severity") or 3) / 5.0),
                    "metadata": {
                        "section_type": f"risk_factor ({record.get('category', 'unknown')})",
                        "company_name": record.get("company_name", ""),
                        "ticker": record.get("ticker", ""),
                        "filing_date": str(record.get("filing_date", "")),
                        "source": "graph",
                    },
                }
                for record in records
                if record.get("content")
            ]
        except Exception as e:
            logger.warning(f"Risk factor query failed: {e}")
            return []

    def _query_filing_summaries(self, entity_name: str, top_k: int) -> list[dict]:
        """Fetch filing community summaries."""
        query = """
        MATCH (c:Company)-[:FILED]->(f:Filing)
        WHERE (c.ticker = $entity_name OR c.name CONTAINS $entity_name)
              AND f.community_summary IS NOT NULL
        RETURN f.community_summary as content,
               c.name as company_name,
               c.ticker as ticker,
               f.filing_date as filing_date,
               f.accession_number as accession
        ORDER BY f.filing_date DESC
        LIMIT $top_k
        """
        try:
            records = self.connector.execute_query(
                query, {"entity_name": entity_name, "top_k": top_k}
            )
            return [
                {
                    "content": record["content"] or "",
                    "score": 0.7,  # Summaries are useful but less specific
                    "metadata": {
                        "section_type": "filing_summary",
                        "company_name": record.get("company_name", ""),
                        "ticker": record.get("ticker", ""),
                        "filing_date": str(record.get("filing_date", "")),
                        "accession_number": record.get("accession", ""),
                        "source": "graph",
                    },
                }
                for record in records
                if record.get("content")
            ]
        except Exception as e:
            logger.warning(f"Filing summary query failed: {e}")
            return []

    def _query_executives(self, entity_name: str, limit: int = 5) -> list[dict]:
        """Fetch executive info for a company."""
        query = """
        MATCH (c:Company)-[r:HAS_EXECUTIVE]->(p:Person)
        WHERE c.ticker = $entity_name OR c.name CONTAINS $entity_name
        RETURN p.name as name,
               r.role as role,
               c.name as company_name,
               c.ticker as ticker
        LIMIT $limit
        """
        try:
            records = self.connector.execute_query(
                query, {"entity_name": entity_name, "limit": limit}
            )
            if not records:
                return []

            # Combine executives into a single result
            exec_lines = [f"- {r['name']} ({r.get('role', 'N/A')})" for r in records]
            company = records[0].get("company_name", entity_name)
            ticker = records[0].get("ticker", "")

            return [
                {
                    "content": f"Key executives at {company}:\n" + "\n".join(exec_lines),
                    "score": 0.5,
                    "metadata": {
                        "section_type": "executives",
                        "company_name": company,
                        "ticker": ticker,
                        "source": "graph",
                    },
                }
            ]
        except Exception as e:
            logger.warning(f"Executive query failed: {e}")
            return []

    def _search_person(self, person_name: str, top_k: int) -> list[dict]:
        """Search for filings and companies associated with a person."""
        query = """
        MATCH (c:Company)-[:HAS_EXECUTIVE]->(p:Person)
        WHERE p.name CONTAINS $person_name
        MATCH (c)-[:FILED]->(f:Filing)-[:DISCLOSES_RISK]->(r:RiskFactor)
        RETURN r.description as content,
               r.category as category,
               c.name as company_name,
               c.ticker as ticker,
               f.filing_date as filing_date,
               p.name as person_name
        ORDER BY f.filing_date DESC
        LIMIT $top_k
        """
        try:
            records = self.connector.execute_query(
                query, {"person_name": person_name, "top_k": top_k}
            )
            return [
                {
                    "content": record["content"] or "",
                    "score": 0.8,
                    "metadata": {
                        "section_type": f"risk_factor ({record.get('category', 'unknown')})",
                        "company_name": record.get("company_name", ""),
                        "ticker": record.get("ticker", ""),
                        "filing_date": str(record.get("filing_date", "")),
                        "person": record.get("person_name", ""),
                        "source": "graph",
                    },
                }
                for record in records
                if record.get("content")
            ]
        except Exception as e:
            logger.warning(f"Person search failed: {e}")
            return []

    def close(self):
        """Close Neo4j connection."""
        self.connector.close()
