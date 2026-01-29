"""
Import XBRL facts from DuckDB to Neo4j as Financial Metric nodes.

Supports importing key financial concepts or all facts.
"""

from __future__ import annotations

from src.graph.neo4j_client import Neo4jClient
from src.storage.database import Database
from src.utils.logger import get_logger

logger = get_logger("finloom.graph.xbrl_importer")


class XBRLImporter:
    """Import XBRL financial facts to Neo4j graph."""

    # Key financial concepts (most important metrics)
    KEY_CONCEPTS = [
        "us-gaap:Revenue",
        "us-gaap:Revenues",
        "us-gaap:NetIncomeLoss",
        "us-gaap:Assets",
        "us-gaap:Liabilities",
        "us-gaap:StockholdersEquity",
        "us-gaap:EarningsPerShareBasic",
        "us-gaap:EarningsPerShareDiluted",
        "us-gaap:OperatingIncomeLoss",
        "us-gaap:CashAndCashEquivalentsAtCarryingValue",
    ]

    def __init__(self, neo4j_client: Neo4jClient, duckdb: Database):
        """
        Initialize XBRL importer.

        Args:
            neo4j_client: Neo4j client instance
            duckdb: DuckDB database instance
        """
        self.neo4j = neo4j_client
        self.duckdb = duckdb
        logger.info("XBRLImporter initialized")

    def import_facts(self, key_concepts_only: bool = True) -> dict:
        """
        Import XBRL facts as FinancialMetric nodes.

        Args:
            key_concepts_only: If True, only import major financial metrics

        Returns:
            Statistics dictionary
        """
        logger.info(
            f"Importing XBRL facts (key_concepts_only={key_concepts_only})..."
        )

        # Build query filter
        if key_concepts_only:
            concept_list = ','.join([f"'{c}'" for c in self.KEY_CONCEPTS])
            concept_filter = f"AND concept_name IN ({concept_list})"
        else:
            concept_filter = ""

        # Query facts from DuckDB
        query = f"""
            SELECT 
                f.accession_number,
                f.concept_name,
                f.value,
                f.unit,
                f.period_start,
                f.period_end
            FROM facts f
            WHERE f.value IS NOT NULL
            {concept_filter}
            ORDER BY f.accession_number, f.concept_name
        """

        logger.info("Querying DuckDB for facts...")
        facts = self.duckdb.connection.execute(query).fetchall()
        logger.info(f"Found {len(facts):,} facts to import")

        if not facts:
            logger.warning("No facts found to import")
            return {"facts_imported": 0, "relationships_created": 0}

        # Import in batches
        batch_size = 1000
        total_imported = 0

        for i in range(0, len(facts), batch_size):
            batch = facts[i : i + batch_size]
            self._import_fact_batch(batch)
            total_imported += len(batch)

            if (i + batch_size) % 10000 == 0:
                logger.info(f"Imported {total_imported:,} / {len(facts):,} facts...")

        logger.info(f"✓ Imported {len(facts):,} XBRL facts")

        return {
            "facts_imported": len(facts),
            "relationships_created": len(facts),  # 1 relationship per fact
        }

    def _import_fact_batch(self, batch: list[tuple]) -> None:
        """
        Import batch of facts to Neo4j.

        Args:
            batch: List of fact tuples (accession, concept, value, unit, start, end)
        """
        # Prepare data for Cypher UNWIND
        facts_data = []
        for fact in batch:
            accession, concept, value, unit, period_start, period_end = fact

            # Skip if value is not numeric
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue

            facts_data.append(
                {
                    "accession_number": accession,
                    "concept_name": concept,
                    "value": value,
                    "unit": unit or "USD",
                    "period_start": str(period_start) if period_start else None,
                    "period_end": str(period_end) if period_end else None,
                }
            )

        if not facts_data:
            return

        # Batch create with UNWIND
        query = """
        UNWIND $facts as fact
        MATCH (f:Filing {accession_number: fact.accession_number})
        CREATE (m:FinancialMetric {
            concept_name: fact.concept_name,
            value: fact.value,
            unit: fact.unit,
            period_start: date(fact.period_start),
            period_end: date(fact.period_end)
        })
        CREATE (f)-[:REPORTS_METRIC]->(m)
        """

        try:
            self.neo4j.execute_write(query, {"facts": facts_data})
        except Exception as e:
            logger.error(f"Failed to import batch: {e}")
            # Try individual imports for this batch
            for fact_data in facts_data:
                try:
                    self._import_single_fact(fact_data)
                except Exception as e2:
                    logger.error(
                        f"Failed to import fact for {fact_data['accession_number']}: {e2}"
                    )

    def _import_single_fact(self, fact_data: dict) -> None:
        """Import single fact (fallback for batch failures)."""
        query = """
        MATCH (f:Filing {accession_number: $accession_number})
        CREATE (m:FinancialMetric {
            concept_name: $concept_name,
            value: $value,
            unit: $unit,
            period_start: date($period_start),
            period_end: date($period_end)
        })
        CREATE (f)-[:REPORTS_METRIC]->(m)
        """
        self.neo4j.execute_write(query, fact_data)
