#!/usr/bin/env python3
"""
Add LLM extraction to existing entity JSON files (PARALLEL VERSION).

Augments 233 SpaCy-only entity files with structured Person + RiskFactor
extraction using DeepSeek V3.2 via the SectionRetriever pipeline.

Uses asyncio for concurrent processing (10x faster than sequential).
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.extraction.llm_extractor import LLMExtractor, LLMProvider
from src.extraction.section_retriever import SectionRetriever
from src.storage.database import Database
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("llm_augment_parallel")


async def augment_entity_file_async(
    entity_file: Path,
    extractor: LLMExtractor,
    retriever: SectionRetriever,
    semaphore: asyncio.Semaphore,
    skip_existing: bool = True
) -> dict:
    """
    Add LLM extraction to single entity file (async version).
    
    Args:
        entity_file: Path to entity JSON file
        extractor: LLM extractor instance
        retriever: Section retriever instance
        semaphore: Concurrency limiter
    
    Returns:
        Dict with stats (success, people_count, risk_count)
    """
    async with semaphore:
        # Load existing entity file
        with open(entity_file) as f:
            data = json.load(f)
        
        accession = data["accession_number"]
        ticker = data["ticker"]
        
        # Skip if already has LLM extraction (and it's not empty)
        if skip_existing:
            for section in data.get("sections", []):
                if "llm_extraction" in section:
                    llm = section["llm_extraction"]
                    people_count = len(llm.get("people", []))
                    risks_count = len(llm.get("risk_factors", []))
                    # Skip only if has actual data (not empty extraction)
                    if people_count > 0 or risks_count > 0:
                        logger.debug(f"Skipping {ticker} ({accession}) - already has LLM extraction")
                        return {"success": True, "skipped": True, "people": 0, "risks": 0}
                    else:
                        logger.debug(f"Reprocessing {ticker} ({accession}) - has empty LLM extraction")
                    break
        
        logger.info(f"Processing {ticker} ({accession})")
        
        stats = {"success": True, "skipped": False, "people": 0, "risks": 0}
        
        # Extract from Item 10/11 (people) and Item 1A (risks)
        try:
            # Run extraction in thread pool (blocking I/O)
            loop = asyncio.get_event_loop()
            
            # Get sections
            item10_text = await loop.run_in_executor(
                None, retriever.get_section, accession, "ITEM 10"
            )
            item1a_text = await loop.run_in_executor(
                None, retriever.get_section, accession, "ITEM 1A"
            )
            
            # Create section entry if not exists
            if not data.get("sections"):
                data["sections"] = []
            
            # Extract people from Item 10
            people = []
            if item10_text:
                # Check for "incorporated by reference"
                if "incorporated" in item10_text.lower() and len(item10_text) < 500:
                    # Try Item 1 as fallback
                    logger.debug(f"  Item 10 incorporated by reference, trying Item 1")
                    item1_text = await loop.run_in_executor(
                        None, retriever.get_section, accession, "ITEM 1"
                    )
                    if item1_text and len(item1_text) > 500:
                        result = await loop.run_in_executor(
                            None, extractor.extract_from_section, item1_text, "item_1"
                        )
                        people = result.people if result.extraction_success else []
                else:
                    result = await loop.run_in_executor(
                        None, extractor.extract_from_section, item10_text, "item_10"
                    )
                    people = result.people if result.extraction_success else []
            
            # Extract risks from Item 1A
            risks = []
            if item1a_text:
                result = await loop.run_in_executor(
                    None, extractor.extract_from_section, item1a_text, "item_1a"
                )
                risks = result.risk_factors if result.extraction_success else []
            
            # Find or create section to add LLM extraction
            if data["sections"]:
                section = data["sections"][0]
            else:
                section = {
                    "section_type": "combined",
                    "total_entities": 0,
                    "entities_by_type": {},
                    "raw_entities": []
                }
                data["sections"].append(section)
            
            # Add LLM extraction to section
            section["llm_extraction"] = {
                "extraction_success": True,
                "people": [
                    {
                        "name": p.name if hasattr(p, 'name') else p.get("name", ""),
                        "role": p.role if hasattr(p, 'role') else p.get("role", ""),
                        "start_date": p.start_date if hasattr(p, 'start_date') else p.get("start_date")
                    }
                    for p in people
                ],
                "risk_factors": [
                    {
                        "category": r.category if hasattr(r, 'category') else r.get("category", "unknown"),
                        "severity": r.severity if hasattr(r, 'severity') else r.get("severity", 3),
                        "description": r.description if hasattr(r, 'description') else r.get("description", "")
                    }
                    for r in risks
                ]
            }
            
            stats["people"] = len(people)
            stats["risks"] = len(risks)
            
            # Save updated file
            with open(entity_file, "w") as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"  ✓ {ticker}: {len(people)} people, {len(risks)} risks")
            
        except Exception as e:
            logger.error(f"  ✗ Failed to process {ticker}: {e}")
            stats["success"] = False
        
        return stats


async def main_async(
    limit: int | None = None,
    skip_existing: bool = True,
    max_concurrent: int = 10
) -> int:
    """
    Main execution: augment all entity files with LLM extraction (async).
    
    Args:
        limit: Max number of files to process (for testing)
        skip_existing: Skip files that already have LLM extraction
        max_concurrent: Maximum concurrent tasks
    
    Returns:
        Exit code
    """
    logger.info("=" * 70)
    logger.info(f"LLM EXTRACTION AUGMENTATION - DeepSeek V3.2 (PARALLEL {max_concurrent}x)")
    logger.info("=" * 70)
    
    # Initialize
    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.dev.duckdb"
    db = Database(db_path=str(db_path), read_only=True)  # Read-only for concurrent access
    
    extractor = LLMExtractor(provider=LLMProvider.DEEPSEEK)
    retriever = SectionRetriever(db)
    
    # Get entity files
    entity_dir = Path(__file__).parent.parent / "data" / "extracted_entities"
    entity_files = sorted(entity_dir.glob("*.json"))
    
    if limit:
        entity_files = entity_files[:limit]
    
    logger.info(f"Found {len(entity_files)} entity files to process")
    logger.info(f"Max concurrent tasks: {max_concurrent}")
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(max_concurrent)
    
    # Process all files concurrently
    start_time = time.time()
    
    tasks = [
        augment_entity_file_async(entity_file, extractor, retriever, semaphore, skip_existing)
        for entity_file in entity_files
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Aggregate stats
    total_stats = {
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "total_people": 0,
        "total_risks": 0
    }
    
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Task failed with exception: {result}")
            total_stats["failed"] += 1
            continue
        
        total_stats["processed"] += 1
        if result.get("skipped"):
            total_stats["skipped"] += 1
        if not result.get("success"):
            total_stats["failed"] += 1
        
        total_stats["total_people"] += result.get("people", 0)
        total_stats["total_risks"] += result.get("risks", 0)
    
    elapsed = time.time() - start_time
    
    # Print summary
    logger.info("")
    logger.info("=" * 70)
    logger.info("AUGMENTATION COMPLETE")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Files processed: {total_stats['processed']}")
    logger.info(f"  Augmented: {total_stats['processed'] - total_stats['skipped']}")
    logger.info(f"  Skipped (already done): {total_stats['skipped']}")
    logger.info(f"  Failed: {total_stats['failed']}")
    logger.info("")
    logger.info("Extractions:")
    logger.info(f"  People: {total_stats['total_people']}")
    logger.info(f"  Risk Factors: {total_stats['total_risks']}")
    logger.info("")
    # Cost estimation (DeepSeek: ~$0.0087 per file)
    actual_processed = total_stats['processed'] - total_stats['skipped']
    if actual_processed > 0:
        logger.info(f"Time: {elapsed/60:.1f} minutes ({elapsed/actual_processed:.1f}s per file)")
        estimated_cost = actual_processed * 0.0087
        logger.info(f"Estimated cost: ~${estimated_cost:.2f}")
    else:
        logger.info(f"Time: {elapsed:.1f} seconds (no files processed)")
    
    # Print section retriever stats
    logger.info("")
    retriever.print_stats()
    
    return 0


def main(
    limit: int | None = None,
    skip_existing: bool = True,
    max_concurrent: int = 10
) -> int:
    """Wrapper to run async main."""
    return asyncio.run(main_async(limit, skip_existing, max_concurrent))


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Add LLM extraction (parallel)")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    parser.add_argument("--no-skip", action="store_true", help="Re-process files that already have LLM extraction")
    parser.add_argument("--concurrent", type=int, default=10, help="Max concurrent tasks (default: 10)")
    
    args = parser.parse_args()
    
    sys.exit(main(
        limit=args.limit,
        skip_existing=not args.no_skip,
        max_concurrent=args.concurrent
    ))
