"""
Leiden community detection for the SEC filings knowledge graph.

Uses Neo4j Graph Data Science (GDS) library to:
- Project the graph into an in-memory representation
- Run Leiden clustering with hierarchical levels
- Query community membership and statistics
"""

from __future__ import annotations

from graphdatascience import GraphDataScience

from src.graph.graph_connector import Neo4jClient
from src.infrastructure.config import get_config
from src.infrastructure.logger import get_logger

logger = get_logger("finloom.graph.find_communities")


class CommunityDetector:
    """Run Leiden clustering on the Neo4j knowledge graph."""

    # Node labels and relationship types to include in the projection.
    # Must match what actually exists in the graph (see db.labels / db.relationshipTypes).
    NODE_LABELS = [
        "Company", "Person", "Filing", "RiskFactor", "FinancialMetric",
        "Org", "Product", "Gpe", "Law", "Risk", "Metric",
    ]
    REL_TYPES = [
        "FILED",
        "HAS_EXECUTIVE",
        "DISCLOSES_RISK",
        "REPORTS_METRIC",
        "MENTIONS_ORG",
        "MENTIONS_PERSON",
        "MENTIONS_PRODUCT",
        "MENTIONS_RISK",
        "MENTIONS_LAW",
        "MENTIONS_GPE",
        "MENTIONS_METRIC",
    ]

    def __init__(self, neo4j_client: Neo4jClient):
        """
        Initialize community detector.

        Args:
            neo4j_client: Neo4j client instance (used for direct queries)
        """
        self.client = neo4j_client

        # Get connection config for GDS client
        config = get_config()
        neo4j_cfg = config.get_neo4j_config()

        self.gds = GraphDataScience(
            neo4j_cfg["uri"],
            auth=(neo4j_cfg["user"], neo4j_cfg["password"]),
            database=neo4j_cfg["database"],
        )
        logger.info("CommunityDetector initialized")

    def project_graph(self, graph_name: str = "sec-filings") -> object:
        """
        Project the knowledge graph into GDS in-memory format.

        Drops any existing projection with the same name first.

        Args:
            graph_name: Name for the projected graph

        Returns:
            GDS Graph object
        """
        # Drop existing projection if present
        existing = self.gds.graph.list()
        if any(g["graphName"] == graph_name for _, g in existing.iterrows()):
            logger.info(f"Dropping existing projection '{graph_name}'")
            self.gds.graph.drop(graph_name)

        # Leiden requires undirected edges, so project all relationships as UNDIRECTED
        rel_projection = {
            rel: {"orientation": "UNDIRECTED"} for rel in self.REL_TYPES
        }

        G, result = self.gds.graph.project(
            graph_name,
            self.NODE_LABELS,
            rel_projection,
        )

        logger.info(
            f"Projected '{graph_name}': "
            f"{G.node_count():,} nodes, {G.relationship_count():,} relationships"
        )
        return G

    def run_leiden(
        self,
        G: object,
        include_hierarchy: bool = True,
        seed: int = 42,
    ) -> dict:
        """
        Run Leiden community detection algorithm.

        Args:
            G: GDS Graph object (from project_graph)
            include_hierarchy: Include intermediate communities for hierarchy
            seed: Random seed for reproducibility

        Returns:
            Statistics dict with community_count, levels, modularity, etc.
        """
        logger.info("Running Leiden clustering...")

        result = self.gds.leiden.write(
            G,
            writeProperty="community",
            includeIntermediateCommunities=include_hierarchy,
            randomSeed=seed,
        )

        stats = {
            "node_count": result["nodePropertiesWritten"],
            "community_count": result["communityCount"],
            "levels": result.get("ranLevels", result.get("levels", 1)),
            "modularity": result.get("modularity"),
            "computation_ms": result["computeMillis"],
        }

        logger.info(
            f"Leiden complete: {stats['community_count']} communities "
            f"across {stats['levels']} levels "
            f"(modularity={stats['modularity']}, {stats['computation_ms']}ms)"
        )
        return stats

    def get_communities(self) -> list[dict]:
        """
        Get all communities with member counts and node type distribution.

        Returns:
            List of dicts with community_id, member_count, node_types
        """
        query = """
        MATCH (n)
        WHERE n.community IS NOT NULL
        WITH n.community AS community_id, labels(n)[0] AS node_type, count(*) AS cnt
        WITH community_id, collect({type: node_type, count: cnt}) AS type_counts,
             sum(cnt) AS member_count
        RETURN community_id, member_count, type_counts
        ORDER BY member_count DESC
        """
        return self.client.execute_query(query)

    def get_community_members(self, community_id: int, limit: int = 100) -> list[dict]:
        """
        Get members of a specific community.

        Args:
            community_id: Community ID to query
            limit: Max members to return

        Returns:
            List of node dicts with properties and labels
        """
        query = """
        MATCH (n)
        WHERE n.community = $community_id
        RETURN n, labels(n) AS types
        LIMIT $limit
        """
        return self.client.execute_query(
            query, {"community_id": community_id, "limit": limit}
        )

    def get_community_relationships(
        self, community_id: int, limit: int = 200
    ) -> list[dict]:
        """
        Get relationships within a community.

        Args:
            community_id: Community ID
            limit: Max relationships to return

        Returns:
            List of relationship dicts
        """
        query = """
        MATCH (a)-[r]->(b)
        WHERE a.community = $community_id AND b.community = $community_id
        RETURN type(r) AS rel_type, count(r) AS count
        ORDER BY count DESC
        LIMIT $limit
        """
        return self.client.execute_query(
            query, {"community_id": community_id, "limit": limit}
        )
