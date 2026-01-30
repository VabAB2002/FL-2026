"""CLI entry point for retrieval module testing."""

import argparse
import os
import sys

from dotenv import load_dotenv

from src.infrastructure.logger import get_logger, setup_logging
from src.retrieval import HybridRetriever

# Load environment variables
load_dotenv()

# Ensure logging is configured so output appears in the terminal
setup_logging()

logger = get_logger(__name__)


def main():
    """Test hybrid retrieval with a query."""
    parser = argparse.ArgumentParser(description="Test hybrid search")
    parser.add_argument("query", help="Search query")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results (default: 5)")
    parser.add_argument(
        "--no-rerank", action="store_true", help="Disable Cohere reranking"
    )

    args = parser.parse_args()

    # Check API keys
    if not os.getenv("OPENAI_API_KEY"):
        logger.error("OPENAI_API_KEY not set")
        sys.exit(1)

    if not args.no_rerank and not os.getenv("COHERE_API_KEY"):
        logger.warning("COHERE_API_KEY not set, disabling reranking")
        args.no_rerank = True

    # Initialize retriever
    logger.info("Initializing hybrid retriever...")
    retriever = HybridRetriever(use_reranking=not args.no_rerank)

    # Search
    logger.info(f"\nQuery: {args.query}\n")
    results = retriever.retrieve(args.query, top_k=args.top_k)

    # Display results
    logger.info(f"\nFound {len(results)} results:\n")
    for i, result in enumerate(results, 1):
        meta = result["metadata"]
        logger.info(f"--- Result {i} (score: {result['score']:.3f}) ---")
        company = meta.get("company_name", "N/A")
        ticker = meta.get("ticker", "N/A")
        logger.info(f"Company: {company} ({ticker})")
        section_item = meta.get("section_item", "N/A")
        section_title = meta.get("section_title", "N/A")
        logger.info(f"Section: {section_item} - {section_title}")
        sources = meta.get("sources", [meta.get("source", "unknown")])
        logger.info(f"Sources: {', '.join(sources)}")
        logger.info(f"Content: {result['content'][:200]}...")
        logger.info("")

    # Cleanup
    retriever.close()


if __name__ == "__main__":
    main()
