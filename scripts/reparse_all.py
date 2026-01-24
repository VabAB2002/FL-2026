#!/usr/bin/env python3
"""
Re-parse all downloaded XBRL files with the enhanced parser.

This script extracts ALL XBRL concepts (not just the core 46) and includes
hierarchy and label information for RAG-ready data.

Usage:
    python scripts/reparse_all.py                # Re-parse all filings
    python scripts/reparse_all.py --ticker AAPL  # Re-parse only Apple
    python scripts/reparse_all.py --clear        # Clear facts first, then re-parse
"""

import argparse
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parsers.xbrl_parser import XBRLParser
from src.storage.database import Database
from src.utils.logger import get_logger

logger = get_logger("finloom.reparse")


def reparse_all_filings(
    db: Database,
    ticker: str = None,
    clear_first: bool = False,
) -> None:
    """Re-parse all downloaded XBRL files."""
    
    # Get filings to process
    if ticker:
        company = db.connection.execute(
            "SELECT cik FROM companies WHERE UPPER(ticker) = UPPER(?)",
            [ticker]
        ).fetchone()
        
        if not company:
            print(f"Error: Company with ticker '{ticker}' not found.")
            return
        
        filings = db.connection.execute("""
            SELECT accession_number, local_path, cik
            FROM filings
            WHERE cik = ? AND download_status = 'completed'
            ORDER BY filing_date DESC
        """, [company[0]]).fetchall()
    else:
        filings = db.connection.execute("""
            SELECT accession_number, local_path, cik
            FROM filings
            WHERE download_status = 'completed'
            ORDER BY filing_date DESC
        """).fetchall()
    
    if not filings:
        print("No completed filings found.")
        return
    
    print(f"Found {len(filings)} filings to re-parse")
    
    # Clear existing facts if requested
    if clear_first:
        print("\nClearing existing facts...")
        if ticker:
            db.connection.execute("""
                DELETE FROM facts 
                WHERE accession_number IN (
                    SELECT accession_number FROM filings WHERE cik = ?
                )
            """, [company[0]])
        else:
            db.connection.execute("DELETE FROM facts")
        
        # Reset sequence
        try:
            db.connection.execute("DROP SEQUENCE IF EXISTS facts_id_seq")
            db.connection.execute("CREATE SEQUENCE facts_id_seq START 1")
        except:
            pass
        
        print("Facts cleared.")
    
    # Initialize parser with extract_all_facts=True
    parser = XBRLParser(extract_all_facts=True)
    
    # Process each filing
    total_facts = 0
    success_count = 0
    error_count = 0
    
    start_time = time.time()
    
    for i, (accession_number, local_path, cik) in enumerate(filings, 1):
        if not local_path:
            print(f"  [{i}/{len(filings)}] {accession_number}: No local path, skipping")
            continue
        
        filing_path = Path(local_path)
        if not filing_path.exists():
            print(f"  [{i}/{len(filings)}] {accession_number}: Path not found, skipping")
            continue
        
        print(f"  [{i}/{len(filings)}] Parsing {accession_number}...", end=" ", flush=True)
        
        try:
            # Parse the filing
            result = parser.parse_filing(filing_path, accession_number)
            
            if result.success and result.facts:
                # Delete existing facts for this filing (if not cleared globally)
                if not clear_first:
                    db.connection.execute(
                        "DELETE FROM facts WHERE accession_number = ?",
                        [accession_number]
                    )
                
                # Insert new facts
                fact_count = 0
                for fact in result.facts:
                    try:
                        db.insert_fact(
                            accession_number=accession_number,
                            **fact.to_dict()
                        )
                        fact_count += 1
                    except Exception as e:
                        logger.debug(f"Failed to insert fact: {e}")
                
                # Update concept_categories table
                for fact in result.facts:
                    if fact.section or fact.label:
                        try:
                            db.upsert_concept_category(
                                concept_name=fact.concept_name,
                                section=fact.section,
                                parent_concept=fact.parent_concept,
                                depth=fact.depth,
                                label=fact.label,
                                data_type="monetary" if fact.unit == "USD" else 
                                          "shares" if fact.unit == "shares" else
                                          "pure" if fact.value is not None else "string"
                            )
                        except Exception as e:
                            logger.debug(f"Failed to upsert concept category: {e}")
                
                # Update filing status
                db.update_filing_status(
                    accession_number=accession_number,
                    xbrl_processed=True
                )
                
                print(f"{fact_count} facts")
                total_facts += fact_count
                success_count += 1
            else:
                print(f"FAILED: {result.error_message or 'No facts'}")
                error_count += 1
                
        except Exception as e:
            print(f"ERROR: {e}")
            error_count += 1
            logger.error(f"Failed to parse {accession_number}: {e}")
    
    elapsed = time.time() - start_time
    
    print()
    print("=" * 60)
    print(f"Re-parsing complete!")
    print(f"  Filings processed: {success_count}")
    print(f"  Filings failed: {error_count}")
    print(f"  Total facts extracted: {total_facts:,}")
    print(f"  Time elapsed: {elapsed:.1f}s")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Re-parse all downloaded XBRL files with enhanced parser"
    )
    
    parser.add_argument(
        "--ticker", "-t",
        help="Only re-parse filings for a specific company ticker"
    )
    
    parser.add_argument(
        "--clear", "-c",
        action="store_true",
        help="Clear existing facts before re-parsing"
    )
    
    args = parser.parse_args()
    
    # Initialize database
    print("Initializing database...")
    db = Database()
    db.initialize_schema()
    
    print(f"Database: {db.db_path}")
    print()
    
    try:
        reparse_all_filings(
            db=db,
            ticker=args.ticker,
            clear_first=args.clear,
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
