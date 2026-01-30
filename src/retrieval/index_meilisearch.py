"""CLI script to index chunks in Meilisearch."""

import json
from pathlib import Path

from tqdm import tqdm

from src.infrastructure.logger import get_logger
from src.retrieval.keyword_search import KeywordSearch

logger = get_logger(__name__)


def load_chunks(chunks_dir: Path) -> list[dict]:
    """
    Load all chunks from JSON files.

    Args:
        chunks_dir: Directory containing chunk JSON files

    Returns:
        List of chunk documents
    """
    all_chunks = []
    chunk_files = sorted(chunks_dir.glob("*.json"))

    logger.info(f"Loading chunks from {len(chunk_files)} files...")

    for file_path in tqdm(chunk_files, desc="Loading files"):
        with open(file_path) as f:
            data = json.load(f)

        for chunk in data["chunks"]:
            # Prepare document for Meilisearch
            doc = {
                "chunk_id": chunk["chunk_id"],
                "content": chunk["text"],
                "ticker": data["ticker"],
                "company_name": data["company_name"],
                "section_item": chunk["section_item"],
                "section_title": chunk["section_title"],
                "filing_date": data["filing_date"],
            }
            all_chunks.append(doc)

    logger.info(f"Loaded {len(all_chunks)} chunks")
    return all_chunks


def main():
    """Index all chunks in Meilisearch."""
    chunks_dir = Path("data/chunks")

    if not chunks_dir.exists():
        logger.error(f"Chunks directory not found: {chunks_dir}")
        return

    # Initialize Meilisearch client
    logger.info("Initializing Meilisearch...")
    keyword_search = KeywordSearch()

    # Create index with configuration
    logger.info("Creating/configuring index...")
    keyword_search.create_index()

    # Load chunks
    chunks = load_chunks(chunks_dir)

    # Index documents in batches
    logger.info(f"Indexing {len(chunks)} documents...")
    keyword_search.index_documents(chunks, batch_size=1000)

    # Get stats
    stats = keyword_search.get_stats()
    logger.info(f"""
============================================================
Indexing complete!
  Documents: {stats['number_of_documents']:,}
  Indexing: {stats['is_indexing']}
============================================================
""")


if __name__ == "__main__":
    main()
