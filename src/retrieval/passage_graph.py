"""
Passage graph for multi-hop reasoning over SEC filing chunks.

Nodes = chunks (42K), Edges = logical connections (200K+ target).
Edge types: same_filing, entity_cooccurrence, temporal, pseudo_query.
"""

from __future__ import annotations

import json
import pickle
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import networkx as nx

from src.infrastructure.logger import get_logger

logger = get_logger("finloom.retrieval.passage_graph")

# Known company tickers and names for entity co-occurrence detection
_COMPANY_ENTITIES: dict[str, list[str]] = {
    "AMD": ["AMD", "Advanced Micro Devices"],
    "AAPL": ["Apple"],
    "AMZN": ["Amazon"],
    "BAC": ["Bank of America"],
    "CSCO": ["Cisco"],
    "DIS": ["Disney", "Walt Disney"],
    "GOOG": ["Google", "Alphabet"],
    "GS": ["Goldman Sachs"],
    "HD": ["Home Depot"],
    "IBM": ["IBM"],
    "INTC": ["Intel"],
    "JPM": ["JPMorgan", "JP Morgan"],
    "META": ["Meta", "Facebook"],
    "MSFT": ["Microsoft"],
    "NVDA": ["NVIDIA"],
    "ORCL": ["Oracle"],
    "TSLA": ["Tesla"],
    "WFC": ["Wells Fargo"],
    "WMT": ["Walmart"],
    "BRKA": ["Berkshire Hathaway", "Berkshire"],
}


def _build_entity_patterns() -> dict[str, re.Pattern]:
    """Build compiled regex patterns for each ticker."""
    patterns = {}
    for ticker, names in _COMPANY_ENTITIES.items():
        parts = [re.escape(n) for n in names] + [re.escape(ticker)]
        patterns[ticker] = re.compile(r"\b(?:" + "|".join(parts) + r")\b", re.IGNORECASE)
    return patterns


def _extract_entities(text: str, patterns: dict[str, re.Pattern]) -> set[str]:
    """Return set of tickers mentioned in text."""
    found = set()
    for ticker, pattern in patterns.items():
        if pattern.search(text):
            found.add(ticker)
    return found


def _parse_fiscal_year(filing_date: str) -> int:
    """Extract year from filing_date like '2016-02-18 00:00:00'."""
    return int(filing_date[:4])


class PassageGraph:
    """
    Graph where chunks are nodes and edges represent logical connections.

    Supports four edge types with configurable weights.
    """

    def __init__(self):
        self.graph: nx.Graph = nx.Graph()
        self._chunks_by_accession: dict[str, list[str]] = defaultdict(list)
        self._chunks_by_ticker_section_year: dict[
            tuple[str, str, int], list[str]
        ] = defaultdict(list)
        self._chunk_meta: dict[str, dict] = {}
        self._entity_patterns = _build_entity_patterns()

    def load_chunks(self, chunks_dir: Path, limit: int | None = None) -> int:
        """
        Load all chunks as graph nodes.

        Returns number of nodes added.
        """
        chunk_files = sorted(chunks_dir.glob("*.json"))
        node_count = 0

        for file_path in chunk_files:
            with open(file_path) as f:
                data = json.load(f)

            ticker = data["ticker"]
            company_name = data["company_name"]
            filing_date = data["filing_date"]
            accession = data["accession_number"]
            fiscal_year = _parse_fiscal_year(filing_date)

            for chunk in data["chunks"]:
                chunk_id = chunk["chunk_id"]
                section_item = chunk["section_item"]

                meta = {
                    "ticker": ticker,
                    "company_name": company_name,
                    "filing_date": filing_date,
                    "fiscal_year": fiscal_year,
                    "accession_number": accession,
                    "section_item": section_item,
                    "section_title": chunk["section_title"],
                    "chunk_index": chunk["chunk_index"],
                    "text_preview": chunk["text"][:200],
                    "text": chunk["text"],
                }

                self.graph.add_node(chunk_id, **{k: v for k, v in meta.items() if k != "text"})
                self._chunk_meta[chunk_id] = meta
                self._chunks_by_accession[accession].append(chunk_id)
                self._chunks_by_ticker_section_year[
                    (ticker, section_item, fiscal_year)
                ].append(chunk_id)

                node_count += 1
                if limit and node_count >= limit:
                    return node_count

        logger.info(f"Loaded {node_count:,} chunk nodes from {len(chunk_files)} files")
        return node_count

    def build_same_filing_edges(self) -> int:
        """
        Connect chunks from the same filing.

        - Sequential: chunk[i] -> chunk[i+1] within same section (weight=0.8)
        - Cross-section: first chunk of each section linked (weight=0.5)
        """
        edge_count = 0

        for accession, chunk_ids in self._chunks_by_accession.items():
            # Group by section within this filing
            sections: dict[str, list[str]] = defaultdict(list)
            for cid in chunk_ids:
                section = self._chunk_meta[cid]["section_item"]
                sections[section].append(cid)

            # Sequential edges within each section
            for section, ids in sections.items():
                ids.sort(key=lambda c: self._chunk_meta[c]["chunk_index"])
                for i in range(len(ids) - 1):
                    self.graph.add_edge(
                        ids[i], ids[i + 1],
                        weight=0.8, type="same_filing", subtype="sequential",
                    )
                    edge_count += 1

            # Cross-section edges (first chunk of each section)
            section_heads = []
            for section, ids in sections.items():
                ids.sort(key=lambda c: self._chunk_meta[c]["chunk_index"])
                section_heads.append(ids[0])

            for i in range(len(section_heads)):
                for j in range(i + 1, len(section_heads)):
                    self.graph.add_edge(
                        section_heads[i], section_heads[j],
                        weight=0.5, type="same_filing", subtype="cross_section",
                    )
                    edge_count += 1

        logger.info(f"Same-filing edges: {edge_count:,}")
        return edge_count

    def build_entity_cooccurrence_edges(self, max_per_entity: int = 5) -> int:
        """
        Connect chunks that mention the same company across different filings.

        For each entity, find all chunks mentioning it, then connect chunks
        from different filings. Cap at max_per_entity edges per chunk per entity.
        """
        # Build entity -> chunk_ids index
        entity_index: dict[str, list[str]] = defaultdict(list)
        for chunk_id, meta in self._chunk_meta.items():
            # Skip matching the chunk's own company (too many matches)
            own_ticker = meta["ticker"]
            entities = _extract_entities(meta["text"], self._entity_patterns)
            entities.discard(own_ticker)
            for entity in entities:
                entity_index[entity].append(chunk_id)

        edge_count = 0
        # Track per-chunk edge counts to enforce cap
        chunk_entity_edges: dict[str, int] = defaultdict(int)

        for entity, chunk_ids in entity_index.items():
            if len(chunk_ids) < 2:
                continue

            # Group by accession to only connect cross-filing
            by_accession: dict[str, list[str]] = defaultdict(list)
            for cid in chunk_ids:
                acc = self._chunk_meta[cid]["accession_number"]
                by_accession[acc].append(cid)

            accessions = list(by_accession.keys())
            if len(accessions) < 2:
                continue

            # Connect chunks across different filings
            for i in range(len(accessions)):
                for j in range(i + 1, len(accessions)):
                    for cid_a in by_accession[accessions[i]][:max_per_entity]:
                        if chunk_entity_edges[cid_a] >= max_per_entity * 5:
                            continue
                        for cid_b in by_accession[accessions[j]][:max_per_entity]:
                            if chunk_entity_edges[cid_b] >= max_per_entity * 5:
                                continue
                            if not self.graph.has_edge(cid_a, cid_b):
                                self.graph.add_edge(
                                    cid_a, cid_b,
                                    weight=0.6, type="entity_cooccurrence",
                                    entity=entity,
                                )
                                edge_count += 1
                                chunk_entity_edges[cid_a] += 1
                                chunk_entity_edges[cid_b] += 1

        logger.info(f"Entity co-occurrence edges: {edge_count:,}")
        return edge_count

    def build_temporal_edges(self) -> int:
        """
        Connect same company + same section across consecutive fiscal years.

        Positional alignment: chunk at position i in year N connects to
        chunk at position i in year N+1.
        """
        # Group keys by (ticker, section) to find year sequences
        ticker_sections: dict[tuple[str, str], list[int]] = defaultdict(list)
        for (ticker, section, year) in self._chunks_by_ticker_section_year:
            ticker_sections[(ticker, section)].append(year)

        edge_count = 0

        for (ticker, section), years in ticker_sections.items():
            years = sorted(set(years))
            for y_idx in range(len(years) - 1):
                year_a, year_b = years[y_idx], years[y_idx + 1]
                # Only connect consecutive years (gap of 1 or 2)
                if year_b - year_a > 2:
                    continue

                ids_a = self._chunks_by_ticker_section_year[(ticker, section, year_a)]
                ids_b = self._chunks_by_ticker_section_year[(ticker, section, year_b)]

                ids_a.sort(key=lambda c: self._chunk_meta[c]["chunk_index"])
                ids_b.sort(key=lambda c: self._chunk_meta[c]["chunk_index"])

                # Positional alignment
                for i in range(min(len(ids_a), len(ids_b))):
                    self.graph.add_edge(
                        ids_a[i], ids_b[i],
                        weight=0.7, type="temporal",
                        year_from=year_a, year_to=year_b,
                    )
                    edge_count += 1

        logger.info(f"Temporal edges: {edge_count:,}")
        return edge_count

    def add_pseudo_query_edges(
        self,
        chunk_id: str,
        target_chunk_ids: list[str],
        scores: list[float],
        min_score: float = 0.60,
    ) -> int:
        """
        Add pseudo-query edges for a single source chunk.

        Called by the batch script after LLM + vector search.
        Skips self-loops, already-existing edges, and low-similarity results.
        """
        added = 0
        for target_id, score in zip(target_chunk_ids, scores):
            if score < min_score:
                continue
            if target_id == chunk_id:
                continue
            if target_id not in self._chunk_meta:
                continue
            if not self.graph.has_edge(chunk_id, target_id):
                self.graph.add_edge(
                    chunk_id, target_id,
                    weight=0.9 * score, type="pseudo_query",
                )
                added += 1
        return added

    def prune_pseudo_query_edges(self, max_per_node: int = 10) -> int:
        """
        Keep only the top-K strongest pseudo-query edges per node.

        Prevents hub nodes from dominating the graph. Removes weaker
        pseudo-query edges while preserving all local edge types.

        Returns number of edges removed.
        """
        # Collect pseudo-query edges per node, sorted by weight
        node_edges: dict[str, list[tuple[str, str, float]]] = defaultdict(list)
        for u, v, d in self.graph.edges(data=True):
            if d.get("type") == "pseudo_query":
                w = d.get("weight", 0)
                node_edges[u].append((u, v, w))
                node_edges[v].append((v, u, w))

        # Determine which edges each node wants to keep (top-K by weight)
        edges_to_keep: set[tuple[str, str]] = set()
        for node, edges in node_edges.items():
            edges.sort(key=lambda x: x[2], reverse=True)
            for src, tgt, _ in edges[:max_per_node]:
                edges_to_keep.add(tuple(sorted([src, tgt])))

        # Remove pseudo-query edges not in any node's top-K
        to_remove = []
        for u, v, d in self.graph.edges(data=True):
            if d.get("type") != "pseudo_query":
                continue
            edge_key = tuple(sorted([u, v]))
            if edge_key not in edges_to_keep:
                to_remove.append((u, v))

        self.graph.remove_edges_from(to_remove)
        logger.info(
            f"Pruned {len(to_remove):,} weak pseudo-query edges "
            f"(kept top-{max_per_node} per node)"
        )
        return len(to_remove)

    def save(self, path: Path | None = None) -> None:
        """Save graph as pickle."""
        if path is None:
            path = Path("data/passage_graph.pkl")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self.graph, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"Saved passage graph to {path} ({path.stat().st_size / 1024 / 1024:.1f} MB)")

    @classmethod
    def load(cls, path: Path | None = None) -> PassageGraph:
        """Load graph from pickle."""
        if path is None:
            path = Path("data/passage_graph.pkl")
        pg = cls()
        with open(path, "rb") as f:
            pg.graph = pickle.load(f)
        # Rebuild metadata from node attributes
        for node_id, attrs in pg.graph.nodes(data=True):
            pg._chunk_meta[node_id] = dict(attrs)
        logger.info(f"Loaded passage graph: {pg.graph.number_of_nodes():,} nodes, {pg.graph.number_of_edges():,} edges")
        return pg

    def stats(self) -> dict[str, Any]:
        """Return graph statistics."""
        g = self.graph
        edges_by_type: dict[str, int] = defaultdict(int)
        for _, _, data in g.edges(data=True):
            edges_by_type[data.get("type", "unknown")] += 1

        degrees = [d for _, d in g.degree()]
        return {
            "node_count": g.number_of_nodes(),
            "edge_count": g.number_of_edges(),
            "edges_by_type": dict(edges_by_type),
            "avg_degree": sum(degrees) / max(len(degrees), 1),
            "max_degree": max(degrees) if degrees else 0,
            "isolated_nodes": len(list(nx.isolates(g))),
            "connected_components": nx.number_connected_components(g),
        }
