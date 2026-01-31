"""
HopRAG multi-hop retrieval over the passage graph.

Algorithm: Retrieve (initial via HybridRetriever) → Expand (graph neighbors)
→ Prune (LLM filters noise) → Repeat (up to 3 hops) → Rerank (Cohere).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from qdrant_client.models import FieldCondition, Filter, MatchAny

from src.infrastructure.logger import get_logger
from src.retrieval.hybrid_search import HybridRetriever
from src.retrieval.llm_pruning import LLMPruner
from src.retrieval.passage_graph import PassageGraph
from src.retrieval.query_router import QueryRouter, QueryType, detect_companies
from src.retrieval.reranker import Reranker

logger = get_logger("finloom.retrieval.hoprag")

# Hop decay factor: results from later hops get lower base scores
HOP_DECAY = 0.85


@dataclass
class HopContext:
    """Tracks state across hops."""

    query: str
    query_type: QueryType = QueryType.COMPLEX_ANALYSIS
    hop_number: int = 0
    retrieved_chunks: list[dict] = field(default_factory=list)
    visited_chunk_ids: set[str] = field(default_factory=set)
    hop_trace: list[dict] = field(default_factory=list)


class HopRAGRetriever:
    """
    Multi-hop retriever using passage graph traversal with LLM pruning.

    Wraps HybridRetriever for initial search, then walks the PassageGraph
    to find logically connected passages across hops.
    """

    def __init__(
        self,
        hybrid_retriever: HybridRetriever,
        passage_graph: PassageGraph,
        llm_pruner: LLMPruner,
        query_router: QueryRouter,
        reranker: Reranker | None = None,
        default_max_hops: int = 2,
        initial_top_k: int = 10,
        neighbors_per_seed: int = 15,
        max_candidates_per_hop: int = 30,
        keep_per_hop: int = 5,
        min_edge_weight: float = 0.4,
    ):
        self.hybrid = hybrid_retriever
        self.graph = passage_graph
        self.pruner = llm_pruner
        self.router = query_router
        self.reranker = reranker
        self.default_max_hops = default_max_hops
        self.initial_top_k = initial_top_k
        self.neighbors_per_seed = neighbors_per_seed
        self.max_candidates_per_hop = max_candidates_per_hop
        self.keep_per_hop = keep_per_hop
        self.min_edge_weight = min_edge_weight

    def retrieve(
        self,
        query: str,
        top_k: int = 10,
        max_hops: int | None = None,
    ) -> list[dict]:
        """
        Main entry point. Routes query and performs multi-hop if needed.

        Returns results in standard format:
            {"content", "score", "metadata"} where metadata includes
            chunk_id, ticker, company_name, section_item, section_title,
            filing_date, sources, hop_number, edge_type.
        """
        # Route query
        decision = self.router.route(query)

        if decision.query_type == QueryType.SIMPLE_FACT:
            logger.info("Simple query — using hybrid search only")
            return self.hybrid.retrieve(query, top_k=top_k)

        effective_max_hops = max_hops or decision.max_hops or self.default_max_hops
        logger.info(
            f"Multi-hop retrieval: type={decision.query_type.value}, "
            f"max_hops={effective_max_hops}"
        )

        ctx = HopContext(query=query, query_type=decision.query_type)

        # Hop 0: Initial retrieval
        self._hop_zero(ctx)

        # Hops 1-N: Graph expansion + LLM pruning
        for hop in range(1, effective_max_hops + 1):
            ctx.hop_number = hop
            self._expand_hop(ctx)

            # Check if this hop found anything
            last_trace = ctx.hop_trace[-1] if ctx.hop_trace else {}
            if last_trace.get("kept_count", 0) == 0:
                logger.info(f"Hop {hop}: no new passages found, stopping")
                break

        # Fetch full text from Qdrant for multi-hop results (they only have
        # 200-char text_preview from the passage graph)
        self._enrich_with_full_text(ctx.retrieved_chunks)

        # Final rerank
        results = self._final_rerank(query, ctx.retrieved_chunks, top_k)

        logger.info(
            f"HopRAG complete: {len(results)} results from "
            f"{ctx.hop_number + 1} hops, "
            f"{len(ctx.visited_chunk_ids)} chunks visited"
        )
        return results

    def _hop_zero(self, ctx: HopContext) -> None:
        """Initial retrieval using HybridRetriever.

        For cross-filing queries, runs separate searches per company so
        seeds include passages from all mentioned companies.
        """
        if ctx.query_type == QueryType.CROSS_FILING:
            results = self._cross_filing_seed(ctx.query)
        else:
            results = self.hybrid.retrieve(ctx.query, top_k=self.initial_top_k)

        for doc in results:
            chunk_id = doc.get("metadata", {}).get("chunk_id", "")
            if chunk_id and chunk_id not in ctx.visited_chunk_ids:
                doc["metadata"]["hop_number"] = 0
                doc["metadata"]["edge_type"] = None
                # Ensure sources tracks hoprag
                sources = doc["metadata"].get("sources", [])
                if "hoprag_hop0" not in sources:
                    sources.append("hoprag_hop0")
                doc["metadata"]["sources"] = sources

                ctx.retrieved_chunks.append(doc)
                ctx.visited_chunk_ids.add(chunk_id)

        ctx.hop_trace.append({
            "hop": 0,
            "candidates_count": len(results),
            "kept_count": len(ctx.retrieved_chunks),
        })

        tickers = {doc["metadata"].get("ticker", "?") for doc in ctx.retrieved_chunks}
        logger.info(
            f"Hop 0: {len(ctx.retrieved_chunks)} seed passages "
            f"(companies: {', '.join(sorted(tickers))})"
        )

    def _cross_filing_seed(self, query: str) -> list[dict]:
        """Seed retrieval for cross-filing queries.

        Detects all companies in the query and runs a separate hybrid
        search per company, splitting the initial_top_k budget evenly.
        Falls back to an unfiltered search if no companies are detected.
        """
        tickers = detect_companies(query)

        if len(tickers) < 2:
            # Shouldn't happen (router classified as CROSS_FILING) but
            # fall back to unfiltered search to avoid single-company lock
            logger.info("Cross-filing: fewer than 2 companies detected, using unfiltered search")
            return self.hybrid.retrieve(query, top_k=self.initial_top_k, ticker=None)

        per_company_k = max(3, self.initial_top_k // len(tickers))
        all_results: list[dict] = []
        seen_chunk_ids: set[str] = set()

        for ticker in sorted(tickers):
            results = self.hybrid.retrieve(
                query, top_k=per_company_k, ticker=ticker
            )
            for doc in results:
                cid = doc.get("metadata", {}).get("chunk_id", "")
                if cid and cid not in seen_chunk_ids:
                    all_results.append(doc)
                    seen_chunk_ids.add(cid)

            logger.info(f"Cross-filing seed: {ticker} → {len(results)} results")

        return all_results

    def _expand_hop(self, ctx: HopContext) -> None:
        """One hop: get neighbors → pre-filter → LLM prune → accumulate."""
        # Seeds = chunks added in the previous hop
        prev_hop = ctx.hop_number - 1
        seed_ids = [
            doc["metadata"]["chunk_id"]
            for doc in ctx.retrieved_chunks
            if doc["metadata"].get("hop_number") == prev_hop
        ]

        if not seed_ids:
            ctx.hop_trace.append({
                "hop": ctx.hop_number,
                "candidates_count": 0,
                "kept_count": 0,
            })
            return

        # Get weighted neighbors from passage graph
        candidates = self._get_weighted_neighbors(
            seed_ids,
            ctx.visited_chunk_ids,
            cross_company=ctx.query_type == QueryType.CROSS_FILING,
        )

        if not candidates:
            ctx.hop_trace.append({
                "hop": ctx.hop_number,
                "candidates_count": 0,
                "kept_count": 0,
            })
            logger.info(f"Hop {ctx.hop_number}: no neighbor candidates")
            return

        logger.info(
            f"Hop {ctx.hop_number}: {len(candidates)} candidates from "
            f"{len(seed_ids)} seeds"
        )

        # LLM prune
        pruning_result = self.pruner.prune(
            query=ctx.query,
            current_context=ctx.retrieved_chunks,
            candidates=candidates,
            max_keep=self.keep_per_hop,
        )

        # Accumulate kept passages
        kept_set = set(pruning_result.kept_chunk_ids)
        kept_docs = []
        for cand in candidates:
            cid = cand["metadata"]["chunk_id"]
            if cid in kept_set:
                cand["metadata"]["hop_number"] = ctx.hop_number
                sources = cand["metadata"].get("sources", [])
                sources.append(f"hoprag_hop{ctx.hop_number}")
                cand["metadata"]["sources"] = sources
                # Apply hop decay to score
                cand["score"] = cand["score"] * (HOP_DECAY ** ctx.hop_number)
                kept_docs.append(cand)

        ctx.retrieved_chunks.extend(kept_docs)

        # Mark all candidates (kept + pruned) as visited
        for cid in pruning_result.kept_chunk_ids:
            ctx.visited_chunk_ids.add(cid)
        for cid in pruning_result.pruned_chunk_ids:
            ctx.visited_chunk_ids.add(cid)

        ctx.hop_trace.append({
            "hop": ctx.hop_number,
            "candidates_count": len(candidates),
            "kept_count": len(kept_docs),
        })

        logger.info(
            f"Hop {ctx.hop_number}: kept {len(kept_docs)}/{len(candidates)}"
        )

    def _get_weighted_neighbors(
        self,
        seed_chunk_ids: list[str],
        visited: set[str],
        cross_company: bool = False,
    ) -> list[dict]:
        """
        Get neighbor chunks from PassageGraph, ranked by edge weight.

        When cross_company is True, reserves half the per-seed slots for
        neighbors from a different company so same-filing edges don't
        crowd out cross-company connections.

        Returns candidate dicts in standard format with added edge_type
        and edge_weight metadata.
        """
        # Collect neighbors across all seeds, deduplicate by chunk_id
        neighbor_scores: dict[str, dict[str, Any]] = {}

        for seed_id in seed_chunk_ids:
            if seed_id not in self.graph.graph:
                continue

            seed_ticker = self.graph.graph.nodes[seed_id].get("ticker", "")

            edges = list(self.graph.graph.edges(seed_id, data=True))
            # Sort by weight descending
            edges.sort(key=lambda e: e[2].get("weight", 0), reverse=True)

            if cross_company and seed_ticker:
                # Partition edges into same-company and cross-company
                cross_slots = self.neighbors_per_seed // 2
                same_slots = self.neighbors_per_seed - cross_slots
                same_count = 0
                cross_count = 0

                for _, neighbor_id, edge_data in edges:
                    if same_count >= same_slots and cross_count >= cross_slots:
                        break
                    if neighbor_id in visited:
                        continue
                    weight = edge_data.get("weight", 0)
                    if weight < self.min_edge_weight:
                        continue

                    nbr_ticker = self.graph.graph.nodes.get(
                        neighbor_id, {}
                    ).get("ticker", "")
                    is_cross = nbr_ticker != seed_ticker

                    if is_cross and cross_count >= cross_slots:
                        continue
                    if not is_cross and same_count >= same_slots:
                        continue

                    self._add_neighbor(
                        neighbor_scores, neighbor_id, edge_data, weight
                    )
                    if is_cross:
                        cross_count += 1
                    else:
                        same_count += 1
            else:
                # Original behavior: top N by weight regardless of company
                count = 0
                for _, neighbor_id, edge_data in edges:
                    if count >= self.neighbors_per_seed:
                        break
                    if neighbor_id in visited:
                        continue
                    weight = edge_data.get("weight", 0)
                    if weight < self.min_edge_weight:
                        continue

                    self._add_neighbor(
                        neighbor_scores, neighbor_id, edge_data, weight
                    )
                    count += 1

        # Sort all candidates by edge weight, cap total
        candidates = sorted(
            neighbor_scores.values(),
            key=lambda c: c["edge_weight"],
            reverse=True,
        )[: self.max_candidates_per_hop]

        # Remove the temporary edge_weight field
        for c in candidates:
            c.pop("edge_weight", None)

        return candidates

    def _add_neighbor(
        self,
        neighbor_scores: dict[str, dict[str, Any]],
        neighbor_id: str,
        edge_data: dict,
        weight: float,
    ) -> None:
        """Add or update a neighbor in the candidate map."""
        # Keep the best edge if we've seen this neighbor from another seed
        if neighbor_id in neighbor_scores:
            if weight <= neighbor_scores[neighbor_id]["edge_weight"]:
                return

        node_attrs = self.graph.graph.nodes.get(neighbor_id, {})
        neighbor_scores[neighbor_id] = {
            "content": node_attrs.get("text_preview", ""),
            "score": weight,
            "metadata": {
                "chunk_id": neighbor_id,
                "ticker": node_attrs.get("ticker", ""),
                "company_name": node_attrs.get("company_name", ""),
                "section_item": node_attrs.get("section_item", ""),
                "section_title": node_attrs.get("section_title", ""),
                "filing_date": node_attrs.get("filing_date", ""),
                "sources": [],
                "edge_type": edge_data.get("type", "unknown"),
            },
            "edge_weight": weight,
        }

    def _enrich_with_full_text(self, results: list[dict]) -> None:
        """Fetch full text from Qdrant for multi-hop results.

        Hop-0 results already have full content from HybridRetriever.
        Hop 1+ results only have ~200-char text_preview from the passage
        graph. Without enrichment, the reranker unfairly downranks them.
        """
        # Collect chunk_ids that need full text (hop > 0)
        ids_needing_text = [
            doc["metadata"]["chunk_id"]
            for doc in results
            if doc["metadata"].get("hop_number", 0) > 0
        ]

        if not ids_needing_text:
            return

        try:
            qdrant_client = self.hybrid.vector_search.vector_store.client
            collection = self.hybrid.vector_search.vector_store.collection_name

            # Batch fetch from Qdrant using chunk_id filter
            points, _ = qdrant_client.scroll(
                collection_name=collection,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="chunk_id",
                            match=MatchAny(any=ids_needing_text),
                        )
                    ]
                ),
                limit=len(ids_needing_text),
                with_payload=True,
                with_vectors=False,
            )

            # Build lookup: chunk_id -> full content
            content_map: dict[str, str] = {}
            for point in points:
                cid = point.payload.get("chunk_id", "")
                content = point.payload.get("content", "")
                if cid and content:
                    content_map[cid] = content

            # Replace truncated previews with full text
            enriched = 0
            for doc in results:
                cid = doc["metadata"]["chunk_id"]
                if cid in content_map:
                    doc["content"] = content_map[cid]
                    enriched += 1

            logger.info(
                f"Enriched {enriched}/{len(ids_needing_text)} multi-hop "
                f"results with full text from Qdrant"
            )

        except Exception as e:
            logger.warning(
                f"Failed to enrich multi-hop results: {e}. "
                f"Reranking will use text previews."
            )

    def _final_rerank(
        self,
        query: str,
        results: list[dict],
        top_k: int,
    ) -> list[dict]:
        """Final reranking of all accumulated results."""
        if not results:
            return []

        if self.reranker:
            try:
                reranked = self.reranker.rerank(query, results, top_k=top_k)
                return reranked
            except Exception as e:
                logger.warning(f"Reranking failed: {e}, using score-based ordering")

        # Fallback: sort by score
        results.sort(key=lambda r: r.get("score", 0), reverse=True)
        return results[:top_k]

    def close(self) -> None:
        """Close underlying resources."""
        self.hybrid.close()


def create_hoprag_retriever(
    passage_graph_path: str = "data/passage_graph.pkl",
    use_reranking: bool = True,
    **kwargs,
) -> HopRAGRetriever:
    """Convenience factory that wires up all dependencies."""
    hybrid = HybridRetriever(use_reranking=use_reranking)
    pg = PassageGraph.load(Path(passage_graph_path))
    pruner = LLMPruner()
    router = QueryRouter()
    reranker = hybrid.reranker if hybrid.use_reranking else None

    return HopRAGRetriever(
        hybrid_retriever=hybrid,
        passage_graph=pg,
        llm_pruner=pruner,
        query_router=router,
        reranker=reranker,
        **kwargs,
    )
