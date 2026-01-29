#!/usr/bin/env python3
"""
Extract entities from all SEC filings using SpaCy NER.

Processes all 233 filings, extracts entities from each section (Items 1-9),
and saves results as JSON files for downstream graph construction.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tqdm import tqdm

from src.extraction.entity_extractor import FinancialEntityExtractor
from src.storage.database import Database
from src.utils.config import get_config
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("entity_extraction")


def get_sections_from_database(accession_number: str, db_connection) -> dict[str, str] | None:
    """
    Get pre-extracted sections from database.
    
    Args:
        accession_number: Filing accession number
        db_connection: DuckDB connection
    
    Returns:
        Dict mapping section identifiers to content, or None if not found
    """
    try:
        sections = db_connection.execute("""
            SELECT item, markdown
            FROM filing_sections
            WHERE accession_number = ?
            ORDER BY item
        """, [accession_number]).fetchall()
        
        if not sections:
            logger.debug(f"No sections found in database for {accession_number}")
            return None
        
        logger.debug(f"Loaded {len(sections)} sections from database for {accession_number}")
        return {f"item_{item.lower()}": markdown for item, markdown in sections}
        
    except Exception as e:
        logger.warning(f"Failed to load sections from database: {e}")
        return None


def split_into_sections_fallback(markdown: str) -> dict[str, str]:
    """
    Fallback regex parser for old data without database sections.
    
    Args:
        markdown: Full filing markdown content
    
    Returns:
        Dict mapping section identifiers to content
    """
    sections: dict[str, str] = {}

    # Find all Item headers with regex
    # Pattern: "Item X. | Title | Page |" or "Item XY. | Title | Page |"
    item_pattern = re.compile(r"^Item\s+(\d+[A-Z]?)\.\s*\|?\s*(.+?)(?:\s*\|)?$", re.MULTILINE)

    matches = list(item_pattern.finditer(markdown))

    if not matches:
        # Fallback: treat entire document as one section
        logger.warning("No Item headers found, treating as single section")
        return {"full_document": markdown}

    # Extract sections between matches
    for i, match in enumerate(matches):
        item_num = match.group(1).lower()
        item_title = match.group(2).strip()
        section_key = f"item_{item_num}"

        # Get content from this match to next match (or end)
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(markdown)

        content = markdown[start:end].strip()

        if content:
            sections[section_key] = content
            logger.debug(f"Extracted {section_key}: {item_title} ({len(content)} chars)")

    return sections


def main() -> int:
    """Run entity extraction pipeline."""
    logger.info("=" * 70)
    logger.info("SEC Filing Entity Extraction Pipeline")
    logger.info("=" * 70)

    # Load config and database
    config = get_config()
    
    # Use development database in data/database/ directory
    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.dev.duckdb"
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        return 1
    
    logger.info(f"Database: {db_path}")
    db = Database(db_path=str(db_path))

    # Initialize SpaCy extractor
    logger.info("Initializing SpaCy entity extractor...")
    extractor = FinancialEntityExtractor(model_name="en_core_web_trf")

    # Get all filings with markdown
    logger.info("Loading filings from database...")
    filings = db.connection.execute(
        """
        SELECT f.accession_number, f.full_markdown, c.ticker, c.company_name, f.filing_date
        FROM filings f
        JOIN companies c ON f.cik = c.cik
        WHERE f.full_markdown IS NOT NULL
        ORDER BY c.ticker, f.filing_date
    """
    ).fetchall()

    logger.info(f"Found {len(filings)} filings to process")

    # Create output directory
    output_dir = Path(__file__).parent.parent / "data" / "extracted_entities"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Extraction statistics
    total_entities = 0
    total_sections = 0
    entity_type_counts: dict[str, int] = {}

    # Process each filing
    for accession, markdown, ticker, company_name, filing_date in tqdm(
        filings, desc="Extracting entities"
    ):
        logger.info(f"Processing {ticker}: {accession}")

        # Try to get sections from database first
        sections = get_sections_from_database(accession, db.connection)
        
        # Fallback to markdown parsing if not in database
        if sections is None:
            logger.debug(f"  No database sections, using fallback regex parser")
            sections = split_into_sections_fallback(markdown)
        
        logger.debug(f"  Found {len(sections)} sections")

        # Extract entities from each section
        extraction_results = {
            "accession_number": accession,
            "ticker": ticker,
            "company_name": company_name,
            "filing_date": str(filing_date),
            "total_sections": len(sections),
            "sections": [],
        }

        for section_key, section_text in sections.items():
            # Extract entities
            result = extractor.extract_from_section(section_text, section_key)

            extraction_results["sections"].append(result)

            # Update statistics
            total_sections += 1
            total_entities += result["total_entities"]

            # Count by entity type
            for entity_type, entities in result["entities_by_type"].items():
                entity_type_counts[entity_type] = (
                    entity_type_counts.get(entity_type, 0) + len(entities)
                )

        # Save to JSON
        output_file = output_dir / f"{accession}.json"
        with open(output_file, "w") as f:
            json.dump(extraction_results, f, indent=2)

        logger.debug(f"  Saved: {output_file.name}")

    # Close database
    db.close()

    # Print summary
    logger.info("=" * 70)
    logger.info("EXTRACTION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Filings processed: {len(filings)}")
    logger.info(f"Total sections: {total_sections}")
    logger.info(f"Total entities extracted: {total_entities:,}")
    logger.info("")
    logger.info("Entity type breakdown:")
    for entity_type in sorted(entity_type_counts.keys()):
        count = entity_type_counts[entity_type]
        logger.info(f"  {entity_type:20s}: {count:>8,}")
    logger.info("")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"JSON files created: {len(list(output_dir.glob('*.json')))}")
    logger.info("=" * 70)

    return 0


if __name__ == "__main__":
    sys.exit(main())
