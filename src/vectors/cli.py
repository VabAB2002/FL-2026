#!/usr/bin/env python3
"""
Generate embeddings for SEC filing chunks and upload to Qdrant.

Reads chunk files from data/chunks/*.json, generates OpenAI embeddings,
and uploads to Qdrant vector database with metadata.

Usage:
    python -m src.vectors                # Process all chunks
    python -m src.vectors --limit 100    # Test with 100 chunks
    python -m src.vectors --resume       # Resume from checkpoint
    python -m src.vectors --force        # Recreate collection
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from qdrant_client.models import PointStruct
from tqdm import tqdm

load_dotenv()

from src.infrastructure.config import get_project_root
from src.infrastructure.logger import get_logger, setup_logging
from src.vectors import EmbeddingClient, VectorStore

setup_logging()
logger = get_logger("finloom.vectors.cli")

CHECKPOINT_FILE = get_project_root() / "data" / "progress" / "embeddings_checkpoint.json"


def load_chunks(chunks_dir: Path, limit: int | None = None) -> list[dict]:
    """
    Load chunks from JSON files.

    Args:
        chunks_dir: Directory containing chunk JSON files
        limit: Maximum number of chunks to load (for testing)

    Returns:
        List of chunk dictionaries
    """
    chunk_files = sorted(chunks_dir.glob("*.json"))
    logger.info(f"Found {len(chunk_files)} chunk files in {chunks_dir}")

    all_chunks = []
    for chunk_file in chunk_files:
        with open(chunk_file) as f:
            data = json.load(f)
            chunks = data.get("chunks", [])

            # Add filing metadata to each chunk
            for chunk in chunks:
                chunk["ticker"] = data.get("ticker")
                chunk["company_name"] = data.get("company_name")
                chunk["filing_date"] = data.get("filing_date")
                chunk["form_type"] = data.get("form_type", "10-K")

            all_chunks.extend(chunks)

        if limit and len(all_chunks) >= limit:
            all_chunks = all_chunks[:limit]
            break

    logger.info(f"Loaded {len(all_chunks)} chunks")
    return all_chunks


def load_checkpoint() -> set[str]:
    """Load set of already processed chunk IDs."""
    if not CHECKPOINT_FILE.exists():
        return set()

    with open(CHECKPOINT_FILE) as f:
        data = json.load(f)
        processed_ids = set(data.get("processed_chunk_ids", []))
        logger.info(f"Loaded checkpoint: {len(processed_ids)} chunks already processed")
        return processed_ids


def save_checkpoint(processed_ids: set[str]) -> None:
    """Save checkpoint of processed chunk IDs."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"processed_chunk_ids": sorted(processed_ids)}, f)


def process_chunks(
    chunks: list[dict],
    embedding_client: EmbeddingClient,
    vector_store: VectorStore,
    batch_size: int = 100,
    resume: bool = False,
) -> dict:
    """
    Generate embeddings and upload to Qdrant.

    Args:
        chunks: List of chunk dictionaries
        embedding_client: OpenAI embedding client
        vector_store: Qdrant vector store
        batch_size: Number of chunks per batch
        resume: If True, skip already processed chunks

    Returns:
        Processing statistics
    """
    processed_ids = load_checkpoint() if resume else set()

    # Filter out already processed chunks
    if resume and processed_ids:
        chunks = [c for c in chunks if c["chunk_id"] not in processed_ids]
        logger.info(f"Resuming: {len(chunks)} chunks remaining")

    if not chunks:
        logger.info("No chunks to process")
        return {"processed": 0, "skipped": len(processed_ids)}

    # Estimate cost
    total_tokens = sum(c.get("token_count", 0) for c in chunks)
    cost_info = embedding_client.estimate_cost(total_tokens)
    logger.info(
        f"Estimated cost: ${cost_info['estimated_cost_usd']:.2f} "
        f"for {total_tokens:,} tokens"
    )

    stats = {"processed": 0, "failed": 0, "total_tokens": total_tokens}

    # Process in batches
    for i in tqdm(range(0, len(chunks), batch_size), desc="Processing batches"):
        batch = chunks[i : i + batch_size]

        try:
            # Extract texts
            texts = [chunk["text"] for chunk in batch]

            # Generate embeddings
            embeddings = embedding_client.embed_texts(texts)

            if len(embeddings) != len(batch):
                logger.error(f"Embedding count mismatch: {len(embeddings)} != {len(batch)}")
                stats["failed"] += len(batch)
                continue

            # Create Qdrant points - use string IDs with hash to ensure uniqueness
            points = []
            for idx, (chunk, embedding) in enumerate(zip(batch, embeddings)):
                # Generate numeric ID from chunk_id hash
                point_id = abs(hash(chunk["chunk_id"])) % (10**10)
                
                point = PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "chunk_id": chunk["chunk_id"],  # Store original ID in payload
                        "content": chunk["text"],
                        "accession_number": chunk["accession_number"],
                        "ticker": chunk.get("ticker"),
                        "company_name": chunk.get("company_name"),
                        "filing_date": chunk.get("filing_date"),
                        "form_type": chunk.get("form_type"),
                        "section_item": chunk.get("section_item"),
                        "section_title": chunk.get("section_title"),
                        "chunk_index": chunk.get("chunk_index"),
                        "token_count": chunk.get("token_count"),
                        "contains_table": chunk.get("contains_table", False),
                        "context_prefix": chunk.get("context_prefix"),
                    },
                )
                points.append(point)
                processed_ids.add(chunk["chunk_id"])

            # Upload to Qdrant
            vector_store.upsert_points(points)
            stats["processed"] += len(points)

            # Save checkpoint every batch
            if resume:
                save_checkpoint(processed_ids)

        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            stats["failed"] += len(batch)

    return stats


def main() -> int:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Generate embeddings and upload to Qdrant")
    parser.add_argument(
        "--chunks-dir",
        type=Path,
        default=get_project_root() / "data" / "chunks",
        help="Directory containing chunk JSON files",
    )
    parser.add_argument("--limit", type=int, help="Limit number of chunks (for testing)")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Chunks per embedding batch"
    )
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--force", action="store_true", help="Recreate Qdrant collection")
    parser.add_argument("--qdrant-host", default="localhost", help="Qdrant host")
    parser.add_argument("--qdrant-port", type=int, default=6333, help="Qdrant port")

    args = parser.parse_args()

    # Check API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not found in environment")
        return 1

    try:
        # Initialize clients
        logger.info("Initializing clients...")
        embedding_client = EmbeddingClient(api_key=api_key)
        vector_store = VectorStore(host=args.qdrant_host, port=args.qdrant_port)

        # Create collection
        vector_store.create_collection(vector_size=3072, force=args.force)

        # Load chunks
        logger.info("Loading chunks...")
        chunks = load_chunks(args.chunks_dir, limit=args.limit)

        if not chunks:
            logger.error("No chunks found")
            return 1

        # Process chunks
        logger.info("Processing chunks...")
        stats = process_chunks(
            chunks, embedding_client, vector_store, args.batch_size, args.resume
        )

        # Report results
        logger.info("\n" + "=" * 60)
        logger.info("Processing complete!")
        logger.info(f"  Processed: {stats['processed']:,} chunks")
        logger.info(f"  Failed: {stats['failed']:,} chunks")
        logger.info(f"  Total tokens: {stats['total_tokens']:,}")

        # Collection info
        info = vector_store.get_collection_info()
        if info["exists"]:
            logger.info(f"\nQdrant collection '{vector_store.collection_name}':")
            logger.info(f"  Points: {info['points_count']:,}")
            logger.info(f"  Status: {info['status']}")

        logger.info("=" * 60)

        vector_store.close()
        return 0

    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
