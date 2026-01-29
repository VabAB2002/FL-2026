#!/usr/bin/env python3
"""
Run community detection and generate LLM summaries.

Usage:
    python scripts/pipelines/detect_and_summarize_communities.py                # Full run
    python scripts/pipelines/detect_and_summarize_communities.py --detect-only  # Skip summarization
    python scripts/pipelines/detect_and_summarize_communities.py --min-members 10
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.graph.community_detection import CommunityDetector
from src.graph.neo4j_client import Neo4jClient
from src.graph.summarization import CommunitySummarizer
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("community_detection")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Detect communities and generate summaries"
    )
    parser.add_argument(
        "--detect-only",
        action="store_true",
        help="Run Leiden clustering only, skip summarization",
    )
    parser.add_argument(
        "--min-members",
        type=int,
        default=5,
        help="Minimum community size to summarize (default: 5)",
    )
    parser.add_argument(
        "--max-summarize",
        type=int,
        default=500,
        help="Max communities to summarize (default: 500, for cost control)",
    )
    parser.add_argument(
        "--graph-name",
        default="sec-filings",
        help="Name for GDS graph projection (default: sec-filings)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for Leiden (default: 42)",
    )
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("COMMUNITY DETECTION & SUMMARIZATION")
    logger.info("=" * 70)

    # --- Connect ---
    logger.info("Connecting to Neo4j...")
    client = Neo4jClient()

    if not client.verify_connection():
        logger.error("Cannot connect to Neo4j. Ensure it is running: make neo4j-up")
        return 1

    logger.info("Neo4j connection verified")

    # --- Step 1: Leiden clustering ---
    detector = CommunityDetector(client)

    logger.info(f"Projecting graph as '{args.graph_name}'...")
    G = detector.project_graph(args.graph_name)

    logger.info("Running Leiden clustering...")
    stats = detector.run_leiden(
        args.graph_name,
        include_hierarchy=True,
        seed=args.seed,
    )

    logger.info(f"Detected {stats['community_count']} communities across {stats['levels']} levels")
    logger.info(f"  Modularity: {stats.get('modularity', 'N/A')}")
    logger.info(f"  Compute time: {stats['computation_ms']}ms")
    logger.info("")

    # --- Step 2: Retrieve communities ---
    communities = detector.get_communities()
    logger.info(f"Retrieved {len(communities)} communities total")

    # Filter by minimum member count
    large = [c for c in communities if c["member_count"] >= args.min_members]
    logger.info(f"Communities with {args.min_members}+ members: {len(large)}")

    if args.detect_only:
        _print_top_communities(large)
        client.close()
        logger.info("=" * 70)
        logger.info("COMMUNITY DETECTION COMPLETE (summarization skipped)")
        logger.info("=" * 70)
        return 0

    # --- Step 3: Summarize ---
    to_summarize = large[: args.max_summarize]
    logger.info(f"Summarizing {len(to_summarize)} communities...")

    try:
        summarizer = CommunitySummarizer()
    except ValueError as e:
        logger.error(f"Cannot initialize summarizer: {e}")
        logger.error("Set DEEPSEEK_API_KEY env var or pass --detect-only to skip")
        return 1

    summaries: list[dict] = []
    for i, community in enumerate(to_summarize, 1):
        community_id = community["community_id"]

        if i % 25 == 0:
            logger.info(f"Progress: {i}/{len(to_summarize)} communities summarized")

        # Get members
        members = detector.get_community_members(community_id, limit=100)
        relationships = detector.get_community_relationships(community_id)

        # Generate summary
        summary = summarizer.summarize_community(community_id, members, relationships)

        # Persist to Neo4j
        summarizer.save_summary(client, community_id, summary)

        summaries.append(summary)

    # --- Step 4: Save summaries to JSON ---
    output_dir = Path(__file__).parent.parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)

    summary_file = output_dir / "community_summaries.json"
    with open(summary_file, "w") as f:
        json.dump(summaries, f, indent=2)

    logger.info(f"Saved {len(summaries)} summaries to {summary_file}")

    # --- Display sample ---
    _print_sample_summaries(summaries, count=5)

    client.close()

    logger.info("")
    logger.info("=" * 70)
    logger.info("COMMUNITY DETECTION & SUMMARIZATION COMPLETE")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Explore communities in Neo4j Browser: http://localhost:7474")
    logger.info("  2. Query: MATCH (n) WHERE n.community IS NOT NULL RETURN n.community, count(*) ORDER BY count(*) DESC LIMIT 20")
    logger.info("  3. Review summaries: data/community_summaries.json")
    logger.info("")

    return 0


def _print_top_communities(communities: list[dict], count: int = 10) -> None:
    """Log the largest communities."""
    logger.info("")
    logger.info(f"Top {count} communities by size:")
    for i, c in enumerate(communities[:count], 1):
        types = c.get("type_counts", [])
        type_str = ", ".join(f"{t['type']}:{t['count']}" for t in types[:4])
        logger.info(f"  {i}. Community {c['community_id']}: {c['member_count']} members ({type_str})")


def _print_sample_summaries(summaries: list[dict], count: int = 5) -> None:
    """Log sample summaries."""
    logger.info("")
    logger.info(f"Sample summaries ({count}):")
    for i, s in enumerate(summaries[:count], 1):
        logger.info(f"  {i}. {s.get('title', 'Untitled')}")
        logger.info(f"     {s.get('description', '')[:120]}")
        logger.info(f"     Themes: {', '.join(s.get('themes', []))}")
        logger.info(f"     Members: {s.get('member_count', '?')}")


if __name__ == "__main__":
    sys.exit(main())
