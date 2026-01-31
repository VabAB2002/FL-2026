#!/usr/bin/env python3
"""
Build passage graph for multi-hop SEC filing retrieval.

Usage:
    python scripts/build_passage_graph.py                    # Full build
    python scripts/build_passage_graph.py --skip-pseudo      # Skip LLM step
    python scripts/build_passage_graph.py --resume           # Resume from checkpoint
    python scripts/build_passage_graph.py --limit 100        # Test with 100 chunks
    python scripts/build_passage_graph.py --stats            # Print stats only
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

from src.infrastructure.logger import get_logger, setup_logging
from src.retrieval.passage_graph import PassageGraph
from src.retrieval.pseudo_query_generator import PseudoQueryGenerator
from src.vectors.embedding_client import EmbeddingClient
from src.vectors.vector_store import VectorStore

setup_logging()
logger = get_logger("finloom.scripts.build_passage_graph")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CHECKPOINT_FILE = PROJECT_ROOT / "data" / "progress" / "pseudo_query_checkpoint.json"
GRAPH_OUTPUT = PROJECT_ROOT / "data" / "passage_graph.pkl"


def build_local_edges(graph: PassageGraph) -> dict[str, int]:
    """Build all non-LLM edges."""
    stats = {}

    logger.info("Building same-filing edges...")
    stats["same_filing"] = graph.build_same_filing_edges()

    logger.info("Building entity co-occurrence edges...")
    stats["entity_cooccurrence"] = graph.build_entity_cooccurrence_edges()

    logger.info("Building temporal edges...")
    stats["temporal"] = graph.build_temporal_edges()

    return stats


def load_checkpoint() -> dict[str, list[str]]:
    """Load pseudo-query checkpoint: chunk_id -> questions."""
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        logger.info(f"Loaded checkpoint: {len(data)} chunks already processed")
        return data
    return {}


def save_checkpoint(data: dict[str, list[str]]) -> None:
    """Save pseudo-query checkpoint."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f)


def _generate_for_chunk(
    generator: PseudoQueryGenerator,
    chunk_id: str,
    meta: dict,
) -> tuple[str, list[str]]:
    """Worker function: generate questions for a single chunk."""
    questions = generator.generate_questions(
        chunk_text=meta["text"],
        context_prefix=meta.get("text_preview", "")[:80],
    )
    return chunk_id, questions


def build_pseudo_query_edges(
    graph: PassageGraph,
    qdrant_host: str = "localhost",
    qdrant_port: int = 6333,
    resume: bool = False,
    limit: int | None = None,
    concurrency: int = 20,
) -> int:
    """
    Generate pseudo-queries via DeepSeek, then connect chunks via Qdrant search.

    Pipeline per chunk:
    1. Generate 3 follow-up questions (DeepSeek, concurrent)
    2. Embed each question (OpenAI)
    3. Search Qdrant for top-5 similar chunks per question
    4. Add edges (deduplicated, no self-loops)
    """
    generator = PseudoQueryGenerator()
    embedder = EmbeddingClient()
    vector_store = VectorStore(host=qdrant_host, port=qdrant_port)

    # Get all chunk IDs
    all_chunk_ids = list(graph._chunk_meta.keys())
    if limit:
        all_chunk_ids = all_chunk_ids[:limit]

    # Load or start checkpoint
    checkpoint = load_checkpoint() if resume else {}

    # Filter to remaining chunks
    remaining_ids = [cid for cid in all_chunk_ids if cid not in checkpoint]
    estimate = PseudoQueryGenerator.estimate_cost(len(remaining_ids))
    logger.info(
        f"Pseudo-query generation: {len(remaining_ids):,} chunks remaining, "
        f"estimated cost: ${estimate['estimated_cost_usd']}"
    )

    # Phase 1: Generate questions concurrently (checkpoint-resumable)
    save_interval = 500
    generated_count = 0
    checkpoint_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(
                _generate_for_chunk, generator, cid, graph._chunk_meta[cid]
            ): cid
            for cid in remaining_ids
        }

        pbar = tqdm(total=len(remaining_ids), desc="Generating questions")
        for future in as_completed(futures):
            try:
                chunk_id, questions = future.result()
                with checkpoint_lock:
                    checkpoint[chunk_id] = questions
                    generated_count += 1

                    if generated_count % save_interval == 0:
                        save_checkpoint(checkpoint)
                        logger.info(
                            f"Checkpoint saved: {len(checkpoint):,} chunks processed"
                        )
            except Exception as e:
                cid = futures[future]
                logger.warning(f"Failed to generate questions for {cid}: {e}")

            pbar.update(1)
        pbar.close()

    # Final checkpoint save
    save_checkpoint(checkpoint)
    logger.info(f"Question generation complete: {len(checkpoint):,} total")

    # Phase 2: Vector search and edge creation (concurrent)
    logger.info("Building pseudo-query edges via vector search...")
    total_edges = 0

    def _search_chunk(chunk_id: str, questions: list[str]) -> tuple[str, list[tuple[str, float]]]:
        """Embed questions and search Qdrant. Returns (chunk_id, [(target_id, score)])."""
        results_list = []
        for question in questions:
            try:
                query_vector = embedder.embed_single(question)
                results = vector_store.search(query_vector, limit=5)
                for r in results:
                    target_id = r["payload"].get("chunk_id", "")
                    score = r["score"]
                    if target_id and target_id != chunk_id and score >= 0.60:
                        results_list.append((target_id, score))
            except Exception as e:
                logger.warning(f"Vector search failed for chunk {chunk_id}: {e}")
        return chunk_id, results_list

    # Build work list
    work = [
        (cid, checkpoint.get(cid, []))
        for cid in all_chunk_ids
        if checkpoint.get(cid)
    ]

    # Process concurrently, collect results, add edges on main thread
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(_search_chunk, cid, questions): cid
            for cid, questions in work
        }

        pbar = tqdm(total=len(work), desc="Searching for edges")
        for future in as_completed(futures):
            try:
                chunk_id, search_results = future.result()
                if search_results:
                    target_ids = [r[0] for r in search_results]
                    scores = [r[1] for r in search_results]
                    added = graph.add_pseudo_query_edges(chunk_id, target_ids, scores)
                    total_edges += added
            except Exception as e:
                cid = futures[future]
                logger.warning(f"Search failed for {cid}: {e}")
            pbar.update(1)
        pbar.close()

    vector_store.close()
    logger.info(f"Pseudo-query edges: {total_edges:,}")
    return total_edges


def main() -> int:
    parser = argparse.ArgumentParser(description="Build passage graph")
    parser.add_argument(
        "--chunks-dir", type=Path,
        default=PROJECT_ROOT / "data" / "chunks",
    )
    parser.add_argument("--limit", type=int, help="Limit chunks for testing")
    parser.add_argument(
        "--skip-pseudo", action="store_true",
        help="Skip pseudo-query edges (no API calls)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume pseudo-query generation from checkpoint",
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Print stats for existing graph and exit",
    )
    parser.add_argument("--output", type=Path, default=GRAPH_OUTPUT)
    parser.add_argument("--qdrant-host", default="localhost")
    parser.add_argument("--qdrant-port", type=int, default=6333)
    parser.add_argument(
        "--concurrency", type=int, default=20,
        help="Number of concurrent DeepSeek API calls (default: 20)",
    )
    args = parser.parse_args()

    if args.stats:
        pg = PassageGraph.load(args.output)
        stats = pg.stats()
        print(json.dumps(stats, indent=2))
        return 0

    # Phase 1: Load chunks as nodes
    graph = PassageGraph()
    node_count = graph.load_chunks(args.chunks_dir, limit=args.limit)
    logger.info(f"Loaded {node_count:,} chunk nodes")

    # Phase 2: Build local edges (no API calls)
    local_stats = build_local_edges(graph)

    # Phase 3: Save intermediate graph
    graph.save(args.output)
    logger.info(f"Saved intermediate graph to {args.output}")

    # Phase 4: Pseudo-query edges (optional)
    if not args.skip_pseudo:
        pseudo_count = build_pseudo_query_edges(
            graph,
            qdrant_host=args.qdrant_host,
            qdrant_port=args.qdrant_port,
            resume=args.resume,
            limit=args.limit,
            concurrency=args.concurrency,
        )
        local_stats["pseudo_query"] = pseudo_count
        removed = graph.prune_pseudo_query_edges(max_per_node=10)
        local_stats["pseudo_query_pruned"] = removed
        graph.save(args.output)

    # Report
    stats = graph.stats()
    logger.info("=" * 60)
    logger.info("PASSAGE GRAPH BUILD COMPLETE")
    logger.info(f"  Nodes:  {stats['node_count']:,}")
    logger.info(f"  Edges:  {stats['edge_count']:,}")
    for etype, count in stats.get("edges_by_type", {}).items():
        logger.info(f"    {etype}: {count:,}")
    logger.info(f"  Avg degree:   {stats['avg_degree']:.1f}")
    logger.info(f"  Max degree:   {stats['max_degree']}")
    logger.info(f"  Isolated:     {stats['isolated_nodes']}")
    logger.info(f"  Components:   {stats['connected_components']}")
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
