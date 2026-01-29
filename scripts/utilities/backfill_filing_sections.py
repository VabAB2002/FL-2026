#!/usr/bin/env python3
"""
Backfill filing_sections table from full_markdown.

For filings with incomplete sections, extracts sections using:
1. Regex patterns (Tier 2)
2. LLM section finder (Tier 3 for edge cases)

Then inserts the extracted sections into filing_sections table.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.extraction.section_extractor import SectionExtractor
from src.extraction.llm_section_finder import LLMSectionFinder
from src.storage.database import Database
from src.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("backfill")


# All expected items in a 10-K
ALL_ITEMS = [
    'ITEM 1', 'ITEM 1A', 'ITEM 1B', 'ITEM 1C', 'ITEM 2', 'ITEM 3', 'ITEM 4',
    'ITEM 5', 'ITEM 6', 'ITEM 7', 'ITEM 7A', 'ITEM 8', 'ITEM 9', 'ITEM 9A',
    'ITEM 9B', 'ITEM 9C', 'ITEM 10', 'ITEM 11', 'ITEM 12', 'ITEM 13',
    'ITEM 14', 'ITEM 15', 'ITEM 16'
]


def get_incomplete_filings(db: Database, min_sections: int = 20) -> list[dict]:
    """
    Get filings with incomplete section coverage.
    
    Args:
        db: Database connection
        min_sections: Minimum number of sections to be considered complete
    
    Returns:
        List of filing metadata for incomplete filings
    """
    query = """
    WITH section_counts AS (
        SELECT 
            accession_number,
            COUNT(*) as section_count
        FROM filing_sections
        GROUP BY accession_number
    )
    SELECT 
        f.accession_number,
        f.cik,
        c.ticker,
        c.company_name,
        f.filing_date,
        LENGTH(f.full_markdown) as markdown_size,
        COALESCE(sc.section_count, 0) as current_sections
    FROM filings f
    JOIN companies c ON f.cik = c.cik
    LEFT JOIN section_counts sc ON f.accession_number = sc.accession_number
    WHERE f.full_markdown IS NOT NULL
      AND COALESCE(sc.section_count, 0) < ?
      AND YEAR(f.filing_date) >= 2024
    ORDER BY c.ticker, f.filing_date DESC
    """
    
    filings = db.connection.execute(query, [min_sections]).fetchall()
    
    return [
        {
            'accession_number': f[0],
            'cik': f[1],
            'ticker': f[2],
            'company_name': f[3],
            'filing_date': str(f[4]),
            'markdown_size': f[5],
            'current_sections': f[6],
        }
        for f in filings
    ]


def get_existing_sections(db: Database, accession_number: str) -> set[str]:
    """Get set of items already in filing_sections for this filing."""
    result = db.connection.execute(
        "SELECT item FROM filing_sections WHERE accession_number = ?",
        [accession_number]
    ).fetchall()
    return {r[0] for r in result}


def extract_and_store_sections(
    db: Database,
    filing: dict,
    regex_extractor: SectionExtractor,
    llm_finder: LLMSectionFinder | None,
    dry_run: bool = False
) -> dict:
    """
    Extract missing sections and store in database.
    
    Args:
        db: Database connection
        filing: Filing metadata
        regex_extractor: Regex-based section extractor
        llm_finder: LLM section finder (optional)
        dry_run: If True, don't actually insert into database
    
    Returns:
        Dictionary with extraction statistics
    """
    acc = filing['accession_number']
    ticker = filing['ticker']
    
    logger.info(f"Processing {ticker} ({acc})")
    logger.info(f"  Current sections: {filing['current_sections']}/23")
    
    # Get full markdown
    result = db.connection.execute(
        "SELECT full_markdown FROM filings WHERE accession_number = ?",
        [acc]
    ).fetchone()
    
    if not result or not result[0]:
        logger.error(f"  No full_markdown found for {acc}")
        return {'error': 'no_markdown'}
    
    full_markdown = result[0]
    
    # Get existing sections
    existing = get_existing_sections(db, acc)
    logger.debug(f"  Existing: {existing}")
    
    # Extract missing sections
    stats = {
        'regex_success': 0,
        'llm_success': 0,
        'failed': 0,
        'skipped': 0,
    }
    
    sections_to_insert = []
    
    for item in ALL_ITEMS:
        if item in existing:
            stats['skipped'] += 1
            continue
        
        # Try regex first
        section_text = regex_extractor.extract_section(full_markdown, item)
        
        if section_text:
            stats['regex_success'] += 1
            sections_to_insert.append((item, section_text, 'regex'))
            logger.debug(f"  ✓ {item}: {len(section_text)} chars (regex)")
            continue
        
        # Try LLM if available
        if llm_finder:
            try:
                section_text = llm_finder.find_section(full_markdown, item)
                if section_text:
                    stats['llm_success'] += 1
                    sections_to_insert.append((item, section_text, 'llm'))
                    logger.info(f"  ✓ {item}: {len(section_text)} chars (LLM)")
                    continue
            except Exception as e:
                logger.warning(f"  LLM failed for {item}: {e}")
        
        stats['failed'] += 1
        logger.debug(f"  ✗ {item}: Not found")
    
    # Insert into database
    if not dry_run and sections_to_insert:
        logger.info(f"  Inserting {len(sections_to_insert)} sections into database...")
        
        for item, markdown, source in sections_to_insert:
            # Get next ID from sequence
            next_id = db.connection.execute(
                "SELECT nextval('filing_sections_id_seq')"
            ).fetchone()[0]
            
            word_count = len(markdown.split())
            
            db.connection.execute(
                """
                INSERT INTO filing_sections 
                (id, accession_number, item, item_title, markdown, word_count)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [next_id, acc, item, '', markdown, word_count]
            )
        
        logger.info(f"  ✓ Inserted {len(sections_to_insert)} sections")
    
    logger.info(f"  Results: {stats['regex_success']} regex, {stats['llm_success']} LLM, "
                f"{stats['failed']} failed, {stats['skipped']} skipped")
    
    return stats


def main(dry_run: bool = False, use_llm: bool = True, limit: int | None = None) -> int:
    """
    Backfill filing_sections table.
    
    Args:
        dry_run: If True, don't actually modify database
        use_llm: If True, use LLM for sections that regex can't find
        limit: Limit number of filings to process (for testing)
    
    Returns:
        Exit code
    """
    print("=" * 80)
    print("FILING SECTIONS BACKFILL")
    print("=" * 80)
    print(f"Dry run: {dry_run}")
    print(f"Use LLM: {use_llm}")
    if limit:
        print(f"Limit: {limit} filings")
    print()
    
    # Connect to database
    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.dev.duckdb"
    db = Database(db_path=str(db_path))
    
    # Initialize extractors
    regex_extractor = SectionExtractor()
    
    llm_finder = None
    if use_llm:
        try:
            llm_finder = LLMSectionFinder()
            logger.info("LLM section finder initialized")
        except Exception as e:
            logger.warning(f"LLM section finder not available: {e}")
    
    # Find incomplete filings
    logger.info("Finding filings with incomplete sections...")
    incomplete_filings = get_incomplete_filings(db, min_sections=20)
    
    if limit:
        incomplete_filings = incomplete_filings[:limit]
    
    logger.info(f"Found {len(incomplete_filings)} incomplete filings\n")
    
    if not incomplete_filings:
        logger.info("No incomplete filings found!")
        return 0
    
    # Process each filing
    total_stats = {
        'regex_success': 0,
        'llm_success': 0,
        'failed': 0,
        'skipped': 0,
    }
    
    for i, filing in enumerate(incomplete_filings, 1):
        print(f"\n[{i}/{len(incomplete_filings)}] {filing['ticker']}")
        print("-" * 80)
        
        stats = extract_and_store_sections(
            db, filing, regex_extractor, llm_finder, dry_run
        )
        
        for key in total_stats:
            if key in stats:
                total_stats[key] += stats[key]
    
    # Print summary
    print("\n" + "=" * 80)
    print("BACKFILL SUMMARY")
    print("=" * 80)
    print(f"\nFilings processed: {len(incomplete_filings)}")
    print(f"Sections added via regex: {total_stats['regex_success']}")
    print(f"Sections added via LLM: {total_stats['llm_success']}")
    print(f"Sections failed: {total_stats['failed']}")
    print(f"Sections skipped (already exist): {total_stats['skipped']}")
    print(f"\nTotal new sections: {total_stats['regex_success'] + total_stats['llm_success']}")
    
    if dry_run:
        print("\n⚠ DRY RUN - No changes were made to the database")
    else:
        print("\n✓ Database updated successfully")
    
    db.close()
    return 0


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Backfill filing_sections from full_markdown")
    parser.add_argument("--dry-run", action="store_true", help="Don't modify database, just show what would be done")
    parser.add_argument("--no-llm", action="store_true", help="Don't use LLM fallback (regex only)")
    parser.add_argument("--limit", type=int, help="Limit number of filings to process")
    
    args = parser.parse_args()
    
    sys.exit(main(
        dry_run=args.dry_run,
        use_llm=not args.no_llm,
        limit=args.limit
    ))
