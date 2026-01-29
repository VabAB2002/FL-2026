#!/usr/bin/env python3
"""
Initialize Neo4j schema with indexes and constraints.

Run this once after starting Neo4j to set up the graph database schema.

Usage:
    python scripts/init_neo4j_schema.py
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.graph.neo4j_client import Neo4jClient
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("neo4j_init")


def main() -> int:
    """Initialize Neo4j schema."""
    logger.info("=" * 70)
    logger.info("Neo4j Schema Initialization")
    logger.info("=" * 70)

    try:
        # Connect to Neo4j
        logger.info("Connecting to Neo4j...")
        with Neo4jClient() as client:
            # Test connection
            if not client.verify_connection():
                logger.error("❌ Cannot connect to Neo4j")
                logger.error("Ensure Neo4j is running: docker-compose up -d neo4j")
                return 1

            logger.info("✓ Neo4j connection verified")

            # Create constraints first (they automatically create indexes for unique properties)
            logger.info("Creating constraints...")
            client.create_constraints()
            logger.info("✓ Constraints created/verified")

            # Create additional indexes for non-unique properties
            logger.info("Creating indexes...")
            client.create_indexes()
            logger.info("✓ Indexes created/verified")

            # Verify schema
            logger.info("Verifying schema...")
            try:
                # Neo4j 5.x uses SHOW INDEXES instead of CALL db.indexes()
                stats = client.execute_query("SHOW INDEXES")
                index_count = len(stats) if stats else 0

                constraint_stats = client.execute_query("SHOW CONSTRAINTS")
                constraint_count = len(constraint_stats) if constraint_stats else 0

                logger.info(f"✓ Schema verified: {index_count} indexes, {constraint_count} constraints")
            except Exception as e:
                logger.warning(f"Could not verify schema (non-critical): {e}")
                logger.info("✓ Schema creation completed (verification skipped)")

            # Get database info
            db_info = client.get_database_info()
            logger.info(f"Database info: {db_info}")

            logger.info("=" * 70)
            logger.info("✅ Neo4j schema initialization complete!")
            logger.info("=" * 70)
            logger.info("")
            logger.info("Next steps:")
            logger.info("  1. Access Neo4j Browser: http://localhost:7474")
            logger.info("  2. Login with: neo4j / finloom123 (or your NEO4J_PASSWORD)")
            logger.info("  3. Run test query: CALL db.indexes()")
            logger.info("")

            return 0

    except Exception as e:
        logger.error(f"❌ Schema initialization failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
