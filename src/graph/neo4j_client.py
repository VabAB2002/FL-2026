"""
Neo4j database client wrapper.

Provides:
- Connection management with pooling
- Query execution (read and write)
- Index and constraint creation
- Transaction support
- Health checks
"""

from typing import Any

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable

from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger("finloom.graph.neo4j_client")


class Neo4jClient:
    """
    Neo4j database client with connection pooling.
    
    Handles all database operations including:
    - Query execution
    - Index management
    - Constraint management
    - Connection health checks
    """

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
    ):
        """
        Initialize Neo4j client.
        
        Args:
            uri: Neo4j URI (default: from config)
            user: Username (default: from config)
            password: Password (default: from config)
            database: Database name (default: from config)
        """
        config = get_config()
        neo4j_config = config.get_neo4j_config()

        self.uri = uri or neo4j_config["uri"]
        self.user = user or neo4j_config["user"]
        self.password = password or neo4j_config["password"]
        self.database = database or neo4j_config["database"]

        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password),
                max_connection_pool_size=neo4j_config["max_connection_pool_size"],
                connection_timeout=neo4j_config["connection_timeout"],
                max_transaction_retry_time=neo4j_config["max_transaction_retry_time"],
            )
            logger.info(f"Connected to Neo4j at {self.uri}")
        except ServiceUnavailable as e:
            logger.error(f"Failed to connect to Neo4j at {self.uri}: {e}")
            raise

    def close(self) -> None:
        """Close database connection and release resources."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def execute_query(
        self, query: str, parameters: dict | None = None
    ) -> list[dict[str, Any]]:
        """
        Execute a read query and return results.
        
        Args:
            query: Cypher query string
            parameters: Query parameters
        
        Returns:
            List of result records as dictionaries
        
        Raises:
            Neo4jError: If query execution fails
        """
        try:
            with self.driver.session(database=self.database) as session:
                result = session.run(query, parameters or {})
                records = [record.data() for record in result]
                logger.debug(f"Query executed: {query[:100]}... returned {len(records)} records")
                return records
        except Neo4jError as e:
            logger.error(f"Query failed: {query[:100]}... Error: {e}")
            raise

    def execute_write(
        self, query: str, parameters: dict | None = None
    ) -> Any:
        """
        Execute a write query (CREATE, MERGE, DELETE, etc.).
        
        Args:
            query: Cypher query string
            parameters: Query parameters
        
        Returns:
            Query result summary
        
        Raises:
            Neo4jError: If query execution fails
        """
        try:
            with self.driver.session(database=self.database) as session:
                def _run_query(tx):
                    return tx.run(query, parameters or {})

                result = session.execute_write(_run_query)
                logger.debug(f"Write query executed: {query[:100]}...")
                return result
        except Neo4jError as e:
            logger.error(f"Write query failed: {query[:100]}... Error: {e}")
            raise

    def create_indexes(self) -> None:
        """
        Create indexes for better query performance.
        
        Creates indexes on commonly queried node properties.
        Note: Unique constraints (created separately) automatically create indexes,
        so we skip those here.
        """
        indexes = [
            "CREATE INDEX company_ticker IF NOT EXISTS FOR (c:Company) ON (c.ticker)",
            "CREATE INDEX person_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
            "CREATE INDEX metric_concept IF NOT EXISTS FOR (m:FinancialMetric) ON (m.concept_name)",
            "CREATE INDEX section_type IF NOT EXISTS FOR (s:Section) ON (s.section_type)",
            "CREATE INDEX risk_category IF NOT EXISTS FOR (r:RiskFactor) ON (r.category)",
            "CREATE INDEX event_type IF NOT EXISTS FOR (e:Event) ON (e.event_type)",
        ]

        created = 0
        for index_query in indexes:
            try:
                self.execute_write(index_query)
                created += 1
            except Neo4jError as e:
                # Index might already exist, log but don't fail
                logger.warning(f"Index creation failed (may already exist): {e}")

        logger.info(f"Created/verified {created} indexes")

    def create_constraints(self) -> None:
        """
        Create uniqueness constraints.
        
        Ensures data integrity by enforcing uniqueness:
        - Company.cik must be unique
        - Filing.accession_number must be unique
        """
        constraints = [
            "CREATE CONSTRAINT company_cik_unique IF NOT EXISTS "
            "FOR (c:Company) REQUIRE c.cik IS UNIQUE",
            "CREATE CONSTRAINT filing_accession_unique IF NOT EXISTS "
            "FOR (f:Filing) REQUIRE f.accession_number IS UNIQUE",
        ]

        created = 0
        for constraint_query in constraints:
            try:
                self.execute_write(constraint_query)
                created += 1
            except Neo4jError as e:
                # Constraint might already exist, log but don't fail
                logger.warning(f"Constraint creation failed (may already exist): {e}")

        logger.info(f"Created/verified {created} constraints")

    def verify_connection(self) -> bool:
        """
        Test database connection.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            result = self.execute_query("RETURN 1 as test")
            success = result[0]["test"] == 1
            if success:
                logger.info("Neo4j connection verified")
            return success
        except Exception as e:
            logger.error(f"Connection verification failed: {e}")
            return False

    def get_database_info(self) -> dict[str, Any]:
        """
        Get database metadata and statistics.
        
        Returns:
            Dictionary with database info (version, node counts, etc.)
        """
        try:
            # Get Neo4j version
            version_result = self.execute_query("CALL dbms.components() YIELD name, versions RETURN name, versions")

            # Get node counts by label
            label_counts = self.execute_query(
                """
                CALL db.labels() YIELD label
                CALL apoc.cypher.run(
                    'MATCH (n:`' + label + '`) RETURN count(n) as count', 
                    {}
                ) YIELD value
                RETURN label, value.count as count
                """
            )

            # Get relationship counts by type
            rel_counts = self.execute_query(
                """
                CALL db.relationshipTypes() YIELD relationshipType
                CALL apoc.cypher.run(
                    'MATCH ()-[r:`' + relationshipType + '`]->() RETURN count(r) as count', 
                    {}
                ) YIELD value
                RETURN relationshipType, value.count as count
                """
            )

            return {
                "version": version_result[0] if version_result else "unknown",
                "node_counts": {r["label"]: r["count"] for r in label_counts},
                "relationship_counts": {r["relationshipType"]: r["count"] for r in rel_counts},
            }
        except Exception as e:
            logger.warning(f"Could not retrieve database info: {e}")
            # Fallback to basic count
            try:
                total_nodes = self.execute_query("MATCH (n) RETURN count(n) as count")
                total_rels = self.execute_query("MATCH ()-[r]->() RETURN count(r) as count")
                return {
                    "total_nodes": total_nodes[0]["count"] if total_nodes else 0,
                    "total_relationships": total_rels[0]["count"] if total_rels else 0,
                }
            except Exception:
                return {"error": "Could not retrieve database info"}

    def clear_database(self, confirm: bool = False) -> None:
        """
        DANGER: Delete all nodes and relationships.
        
        Args:
            confirm: Must be True to execute (safety check)
        
        Raises:
            ValueError: If confirm is not True
        """
        if not confirm:
            raise ValueError(
                "Must confirm database clearing by passing confirm=True. "
                "This will delete ALL data!"
            )

        logger.warning("Clearing entire database...")

        # Delete in batches to avoid memory issues
        batch_size = 10000
        deleted = 0

        while True:
            result = self.execute_write(
                f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n RETURN count(n) as deleted"
            )
            batch_deleted = result.value()[0] if result else 0
            deleted += batch_deleted

            if batch_deleted == 0:
                break

        logger.warning(f"Database cleared: {deleted} nodes deleted")
