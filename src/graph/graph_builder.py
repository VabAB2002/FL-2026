"""
Build Neo4j knowledge graph from extracted entities.

Handles entity deduplication, node creation, and relationship mapping
from SpaCy + LLM extraction results.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fuzzywuzzy import fuzz

from src.graph.graph_connector import Neo4jClient
from src.infrastructure.logger import get_logger

logger = get_logger("finloom.graph.graph_builder")


class GraphBuilder:
    """Build knowledge graph from extracted entities with deduplication."""

    def __init__(self, neo4j_client: Neo4jClient, batch_size: int = 500):
        """
        Initialize graph builder.

        Args:
            neo4j_client: Neo4j client instance
            batch_size: Number of operations to batch before flush
        """
        self.client = neo4j_client
        self.entity_cache: dict[str, str] = {}  # Cache for deduplication
        self.batch_size = batch_size
        self.relationship_batch: list[dict] = []  # Pending relationships
        self.stats = {
            "nodes_created": 0,
            "relationships_created": 0,
            "duplicates_merged": 0,
            "files_processed": 0,
        }
        logger.info(f"GraphBuilder initialized (batch_size={batch_size})")

    def build_from_filings(self, entity_files: list[Path]) -> dict[str, Any]:
        """
        Build graph from entity extraction JSON files.

        Args:
            entity_files: List of paths to entity JSON files

        Returns:
            Statistics dictionary
        """
        logger.info(f"Building graph from {len(entity_files)} filings...")

        for i, entity_file in enumerate(entity_files, 1):
            if i % 10 == 0:
                logger.info(
                    f"Progress: {i}/{len(entity_files)} files "
                    f"({self.stats['nodes_created']:,} nodes, "
                    f"{self.stats['relationships_created']:,} relationships)"
                )

            try:
                self._process_filing(entity_file)
                self._flush_relationships()  # Flush after each filing
                self.stats["files_processed"] += 1
            except Exception as e:
                logger.error(f"Failed to process {entity_file.name}: {e}")
                continue

        # Final flush for any remaining relationships
        self._flush_relationships(force=True)
        
        logger.info(f"Graph build complete: {self.stats}")
        return self.stats

    def _process_filing(self, entity_file: Path) -> None:
        """Process single filing JSON file."""
        with open(entity_file) as f:
            data = json.load(f)

        accession = data["accession_number"]
        ticker = data.get("ticker", "UNKNOWN")
        filing_date = data.get("filing_date")

        # Create Company node
        company_id = self._create_company_node(ticker, data.get("company_name"))

        # Create Filing node
        filing_id = self._create_filing_node(accession, ticker, filing_date)

        # Link Company -> Filing
        self._create_relationship(
            company_id, filing_id, "FILED", {"filing_date": filing_date}
        )

        # Process sections
        for section in data.get("sections", []):
            self._process_section(section, filing_id, company_id)

    def _process_section(
        self, section: dict, filing_id: str, company_id: str
    ) -> None:
        """Process section and extract entities."""

        # Process LLM-extracted structured entities first (higher quality)
        llm_data = section.get("llm_extraction", {})
        if llm_data.get("extraction_success"):
            # Process LLM People (with roles)
            for person_data in llm_data.get("people", []):
                person_id, is_new = self._create_person_node(person_data)
                if is_new:
                    self.stats["nodes_created"] += 1
                else:
                    self.stats["duplicates_merged"] += 1

                # Link Company -> Person
                self._create_relationship(
                    company_id,
                    person_id,
                    "HAS_EXECUTIVE",
                    {"role": person_data.get("role", "Unknown")},
                )
                self.stats["relationships_created"] += 1

            # Process LLM Risk Factors
            for risk_data in llm_data.get("risk_factors", []):
                risk_id = self._create_risk_factor_node(risk_data)
                self.stats["nodes_created"] += 1

                # Link Filing -> RiskFactor
                self._create_relationship(filing_id, risk_id, "DISCLOSES_RISK")
                self.stats["relationships_created"] += 1

        # Process SpaCy entities (bulk NER)
        entities_by_type = section.get("entities_by_type", {})
        for ent_type, entities in entities_by_type.items():
            # Skip PERSON if we have LLM people (higher quality)
            if ent_type == "PERSON" and llm_data.get("people"):
                continue

            # Skip RISK if we have LLM risks
            if ent_type == "RISK" and llm_data.get("risk_factors"):
                continue

            for entity in entities:
                entity_text = entity.get("text", "").strip()
                if not entity_text or len(entity_text) < 2:
                    continue

                entity_id, is_new = self._create_entity_node(ent_type, entity_text)
                if is_new:
                    self.stats["nodes_created"] += 1
                else:
                    self.stats["duplicates_merged"] += 1

                # Link Filing -> Entity
                self._create_relationship(filing_id, entity_id, f"MENTIONS_{ent_type}")
                self.stats["relationships_created"] += 1

    def _create_company_node(self, ticker: str, name: str | None = None) -> str:
        """Create or get Company node."""
        query = """
        MERGE (c:Company {ticker: $ticker})
        ON CREATE SET c.name = $name
        RETURN elementId(c) as id
        """
        result = self.client.execute_query(
            query, {"ticker": ticker, "name": name or ticker}
        )
        return result[0]["id"]

    def _create_filing_node(
        self, accession: str, ticker: str, filing_date: str | None
    ) -> str:
        """Create Filing node."""
        query = """
        MERGE (f:Filing {accession_number: $accession})
        ON CREATE SET 
            f.ticker = $ticker,
            f.filing_date = date($filing_date)
        RETURN elementId(f) as id
        """
        result = self.client.execute_query(
            query,
            {"accession": accession, "ticker": ticker, "filing_date": filing_date},
        )
        node_id = result[0]["id"]
        self.stats["nodes_created"] += 1
        return node_id

    def _create_entity_node(self, ent_type: str, text: str) -> tuple[str, bool]:
        """
        Create entity node with full deduplication.

        Returns:
            (node_id, is_new) tuple
        """
        # Check cache for exact match
        cache_key = f"{ent_type}:{text.lower()}"
        if cache_key in self.entity_cache:
            return self.entity_cache[cache_key], False

        # Fuzzy matching for PERSON and ORG only (expensive)
        if ent_type in ["PERSON", "ORG"]:
            for cached_key, cached_id in self.entity_cache.items():
                if cached_key.startswith(f"{ent_type}:"):
                    cached_text = cached_key.split(":", 1)[1]
                    similarity = fuzz.ratio(text.lower(), cached_text)
                    if similarity >= 90:
                        # Found fuzzy match
                        logger.debug(f"Fuzzy match: '{text}' ≈ '{cached_text}' ({similarity}%)")
                        self.entity_cache[cache_key] = cached_id
                        return cached_id, False

        # Create new node
        label = ent_type.capitalize().replace("_", "")
        query = f"""
        CREATE (e:{label} {{text: $text}})
        RETURN elementId(e) as id
        """
        result = self.client.execute_write(query, {"text": text})
        # Get node ID from result
        node_id = self.client.execute_query(
            f"MATCH (e:{label} {{text: $text}}) RETURN elementId(e) as id LIMIT 1",
            {"text": text},
        )[0]["id"]

        self.entity_cache[cache_key] = node_id
        return node_id, True

    def _create_person_node(self, person_data: dict) -> tuple[str, bool]:
        """Create Person node with role from LLM extraction."""
        name = person_data.get("name", "").strip()
        if not name:
            # Return dummy node if no name
            return "", False

        role = person_data.get("role", "Unknown")
        cache_key = f"PERSON:{name.lower()}"

        if cache_key in self.entity_cache:
            return self.entity_cache[cache_key], False

        # Fuzzy match on existing people
        for cached_key, cached_id in self.entity_cache.items():
            if cached_key.startswith("PERSON:"):
                cached_name = cached_key.split(":", 1)[1]
                similarity = fuzz.ratio(name.lower(), cached_name)
                if similarity >= 90:
                    logger.debug(f"Fuzzy match: '{name}' ≈ '{cached_name}' ({similarity}%)")
                    self.entity_cache[cache_key] = cached_id
                    return cached_id, False

        # Create new Person node
        query = """
        MERGE (p:Person {name: $name})
        ON CREATE SET p.role = $role
        RETURN elementId(p) as id
        """
        result = self.client.execute_query(query, {"name": name, "role": role})
        node_id = result[0]["id"]

        self.entity_cache[cache_key] = node_id
        return node_id, True

    def _create_risk_factor_node(self, risk_data: dict) -> str:
        """Create RiskFactor node from LLM extraction."""
        query = """
        CREATE (r:RiskFactor {
            category: $category,
            severity: $severity,
            description: $description
        })
        RETURN elementId(r) as id
        """
        result = self.client.execute_query(
            query,
            {
                "category": risk_data.get("category", "Unknown"),
                "severity": risk_data.get("severity", 3),
                "description": risk_data.get("description", "")[:1000],  # Truncate
            },
        )
        return result[0]["id"]

    def _create_relationship(
        self,
        from_id: str,
        to_id: str,
        rel_type: str,
        properties: dict | None = None,
    ) -> None:
        """Queue relationship for batched creation."""
        if not from_id or not to_id:
            return

        # Add to batch
        self.relationship_batch.append({
            "from_id": from_id,
            "to_id": to_id,
            "rel_type": rel_type,
            "properties": properties or {}
        })
        
        self.stats["relationships_created"] += 1
        
        # Auto-flush if batch full
        if len(self.relationship_batch) >= self.batch_size:
            self._flush_relationships()
    
    def _flush_relationships(self, force: bool = False) -> None:
        """
        Flush batched relationships to Neo4j.
        
        Args:
            force: Flush even if batch not full
        """
        if not self.relationship_batch:
            return
        
        if not force and len(self.relationship_batch) < self.batch_size:
            return
        
        # Group by relationship type for efficient batching
        by_type: dict[str, list] = {}
        for rel in self.relationship_batch:
            rel_type = rel["rel_type"]
            if rel_type not in by_type:
                by_type[rel_type] = []
            by_type[rel_type].append(rel)
        
        # Flush each type
        for rel_type, rels in by_type.items():
            try:
                self._flush_relationship_batch(rel_type, rels)
            except Exception as e:
                logger.error(f"Failed to flush {rel_type} batch: {e}")
        
        # Clear batch
        self.relationship_batch.clear()
    
    def _flush_relationship_batch(self, rel_type: str, rels: list[dict]) -> None:
        """Flush single relationship type batch."""
        # Check if any relationships have properties
        has_props = any(rel["properties"] for rel in rels)
        
        if has_props:
            # Use UNWIND with properties
            query = f"""
            UNWIND $rels as rel
            MATCH (a) WHERE elementId(a) = rel.from_id
            MATCH (b) WHERE elementId(b) = rel.to_id
            MERGE (a)-[r:{rel_type}]->(b)
            SET r += rel.properties
            """
        else:
            # Simpler query without properties
            query = f"""
            UNWIND $rels as rel
            MATCH (a) WHERE elementId(a) = rel.from_id
            MATCH (b) WHERE elementId(b) = rel.to_id
            MERGE (a)-[r:{rel_type}]->(b)
            """
        
        self.client.execute_write(query, {"rels": rels})
