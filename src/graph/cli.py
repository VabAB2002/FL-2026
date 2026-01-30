#!/usr/bin/env python3
"""
Build knowledge graph from extracted entities and XBRL facts.

Usage:
    python -m src.graph                    # Full build (233 files)
    python -m src.graph --pilot            # Pilot mode (20 files)
    python -m src.graph --limit 50         # Custom limit
    python -m src.graph --workers 1        # Sequential mode
"""

from __future__ import annotations

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from src.graph.graph_builder import GraphBuilder
from src.graph.graph_connector import Neo4jClient
from src.graph.xbrl_importer import XBRLImporter
from src.storage.database import Database
from src.infrastructure.config import get_project_root
from src.infrastructure.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("finloom.graph.cli")


def process_file_batch(batch: list[Path], batch_id: int) -> dict:
    """
    Process a batch of files in a worker process.

    Args:
        batch: List of entity files to process
        batch_id: Worker ID for logging

    Returns:
        Statistics dictionary
    """
    # Each worker gets its own Neo4j client
    neo4j = Neo4jClient()
    builder = GraphBuilder(neo4j, batch_size=1000)

    logger.info(f"Worker {batch_id}: Processing {len(batch)} files")

    stats = builder.build_from_filings(batch)

    neo4j.close()

    return stats


def verify_graph(client: Neo4jClient) -> dict:
    """
    Verify graph statistics.

    Returns:
        Dictionary with graph stats
    """
    logger.info("Verifying graph statistics...")

    try:
        stats = {}

        total_nodes = client.execute_query("MATCH (n) RETURN count(n) as count")
        stats["total_nodes"] = total_nodes[0]["count"] if total_nodes else 0

        total_rels = client.execute_query("MATCH ()-[r]->() RETURN count(r) as count")
        stats["total_relationships"] = total_rels[0]["count"] if total_rels else 0

        labels_query = """
        MATCH (n)
        RETURN labels(n)[0] as label, count(n) as count
        ORDER BY count DESC
        """
        label_counts = client.execute_query(labels_query)
        stats["by_label"] = {r["label"]: r["count"] for r in label_counts}

        rel_query = """
        MATCH ()-[r]->()
        RETURN type(r) as rel_type, count(r) as count
        ORDER BY count DESC
        """
        rel_counts = client.execute_query(rel_query)
        stats["by_relationship"] = {r["rel_type"]: r["count"] for r in rel_counts}

        return stats

    except Exception as e:
        logger.warning(f"Could not retrieve detailed stats: {e}")
        return {"error": str(e)}


def main() -> int:
    """Main entry point."""
    root = get_project_root()

    parser = argparse.ArgumentParser(description="Build knowledge graph")
    parser.add_argument(
        "--pilot",
        action="store_true",
        help="Pilot mode: build from 20 files only",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of files to process",
    )
    parser.add_argument(
        "--no-xbrl",
        action="store_true",
        help="Skip XBRL import",
    )
    parser.add_argument(
        "--all-facts",
        action="store_true",
        help="Import all XBRL facts (not just key concepts)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("KNOWLEDGE GRAPH CONSTRUCTION")
    logger.info("=" * 70)

    # Initialize clients
    logger.info("Connecting to Neo4j...")
    neo4j = Neo4jClient()

    if not neo4j.verify_connection():
        logger.error("Cannot connect to Neo4j")
        logger.error("Ensure Neo4j is running: make neo4j-up")
        return 1

    logger.info("Neo4j connection verified")

    logger.info("Connecting to DuckDB...")
    db_path = root / "data" / "database" / "finloom.dev.duckdb"
    duckdb = Database(db_path=str(db_path), read_only=True)
    logger.info("DuckDB connection verified")

    # Get entity files
    entity_dir = root / "data" / "extracted_entities"
    all_files = sorted(entity_dir.glob("*.json"))

    # Determine file limit
    if args.pilot:
        file_limit = 20
        logger.info("PILOT MODE: Processing 20 files")
    elif args.limit:
        file_limit = args.limit
        logger.info(f"Custom limit: Processing {file_limit} files")
    else:
        file_limit = len(all_files)
        logger.info(f"FULL BUILD: Processing all {file_limit} files")

    entity_files = all_files[:file_limit]

    logger.info(f"Found {len(all_files)} total entity files")
    logger.info(f"Processing {len(entity_files)} files")
    logger.info("")

    # Build graph from entities
    if args.workers > 1:
        logger.info(f"Building graph with {args.workers} parallel workers...")

        batch_size = len(entity_files) // args.workers
        if batch_size == 0:
            batch_size = 1

        batches = []
        for i in range(0, len(entity_files), batch_size):
            batch = entity_files[i : i + batch_size]
            batches.append((batch, len(batches)))

        combined_stats = {
            "files_processed": 0,
            "nodes_created": 0,
            "relationships_created": 0,
            "duplicates_merged": 0,
        }

        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(process_file_batch, batch, batch_id)
                for batch, batch_id in batches
            ]

            for future in as_completed(futures):
                try:
                    stats = future.result()
                    for key in combined_stats:
                        combined_stats[key] += stats.get(key, 0)
                except Exception as e:
                    logger.error(f"Worker failed: {e}")

        stats = combined_stats
    else:
        logger.info("Building graph (sequential mode)...")
        builder = GraphBuilder(neo4j, batch_size=1000)
        stats = builder.build_from_filings(entity_files)

    logger.info("")
    logger.info("Entity graph construction complete:")
    logger.info(f"  Files processed: {stats['files_processed']:,}")
    logger.info(f"  Nodes created: {stats['nodes_created']:,}")
    logger.info(f"  Relationships: {stats['relationships_created']:,}")
    logger.info(f"  Duplicates merged: {stats['duplicates_merged']:,}")
    logger.info("")

    # Import XBRL facts
    if not args.no_xbrl:
        logger.info("Importing XBRL facts...")
        xbrl_importer = XBRLImporter(neo4j, duckdb)
        xbrl_stats = xbrl_importer.import_facts(key_concepts_only=not args.all_facts)

        logger.info(f"  Facts imported: {xbrl_stats['facts_imported']:,}")
        logger.info(
            f"  Relationships created: {xbrl_stats['relationships_created']:,}"
        )
        logger.info("")

    # Verify graph
    graph_stats = verify_graph(neo4j)

    if "error" not in graph_stats:
        logger.info("Graph Statistics:")
        logger.info(f"  Total nodes: {graph_stats['total_nodes']:,}")
        logger.info(f"  Total relationships: {graph_stats['total_relationships']:,}")
        logger.info("")
        logger.info("  Nodes by type:")
        for label, count in graph_stats.get("by_label", {}).items():
            logger.info(f"    {label}: {count:,}")
        logger.info("")
        logger.info("  Relationships by type:")
        for rel_type, count in graph_stats.get("by_relationship", {}).items():
            logger.info(f"    {rel_type}: {count:,}")

    # Close connections
    neo4j.close()
    duckdb.close()

    logger.info("")
    logger.info("=" * 70)
    logger.info("KNOWLEDGE GRAPH CONSTRUCTION COMPLETE")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Explore in Neo4j Browser: http://localhost:7474")
    logger.info("  2. Test query: MATCH (c:Company)-[:FILED]->(f:Filing) RETURN c, f LIMIT 10")
    logger.info("  3. Run full build: python -m src.graph")
    logger.info("")

    return 0


if __name__ == "__main__":
    sys.exit(main())
