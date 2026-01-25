#!/usr/bin/env python3
"""
Re-process existing extractions to add HTML content and extract tables.

This script re-runs the unstructured extraction on filings that were already
processed but have no content_html (and therefore no tables extracted).
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.processing.unstructured_pipeline import UnstructuredDataPipeline


def get_filings_to_reprocess(db_path: str) -> list[tuple[str, str]]:
    """Get filings that need reprocessing (have sections but no HTML)."""
    conn = duckdb.connect(db_path, read_only=True)
    
    results = conn.execute("""
        SELECT DISTINCT 
            f.accession_number,
            f.local_path
        FROM filings f
        JOIN sections s ON f.accession_number = s.accession_number
        WHERE f.download_status = 'completed'
        AND (s.content_html IS NULL OR s.content_html = '')
        ORDER BY f.accession_number
    """).fetchall()
    
    conn.close()
    return results


def reprocess_filing(
    accession: str,
    filing_path: str,
    db_path: str
) -> tuple[str, bool, str]:
    """Reprocess a single filing."""
    try:
        pipeline = UnstructuredDataPipeline(
            db_path=db_path,
            use_xbrl_parser=True,
            priority_sections_only=False  # Extract all sections
        )
        
        result = pipeline.process_filing(accession, Path(filing_path))
        
        return (
            accession,
            result.success,
            f"{result.sections_count}s, {result.tables_count}t" if result.success else result.error_message
        )
        
    except Exception as e:
        return (accession, False, str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Re-process filings to extract HTML and tables"
    )
    parser.add_argument(
        "--db",
        default="data/database/finloom.duckdb",
        help="Path to database",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=10,
        help="Number of parallel workers",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of filings to process (for testing)",
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("RE-PROCESSING FILINGS FOR TABLE EXTRACTION")
    print("=" * 70)
    print()
    
    # Get filings to reprocess
    print("Finding filings to reprocess...")
    filings = get_filings_to_reprocess(args.db)
    
    if args.limit:
        filings = filings[:args.limit]
    
    print(f"Found {len(filings)} filings to reprocess")
    print()
    
    if not filings:
        print("✅ No filings need reprocessing!")
        return
    
    # Process in parallel
    successful = []
    failed = []
    
    with ThreadPoolExecutor(max_workers=args.parallel) as executor:
        futures = {
            executor.submit(
                reprocess_filing,
                accession,
                filing_path,
                args.db
            ): accession
            for accession, filing_path in filings
        }
        
        with tqdm(total=len(filings), desc="Reprocessing") as pbar:
            for future in as_completed(futures):
                accession, success, message = future.result()
                
                if success:
                    successful.append(accession)
                else:
                    failed.append((accession, message))
                
                pbar.update(1)
                pbar.set_postfix({
                    "ok": len(successful),
                    "fail": len(failed)
                })
    
    # Summary
    print()
    print("=" * 70)
    print("REPROCESSING COMPLETE")
    print("=" * 70)
    print(f"✅ Successful: {len(successful)}")
    print(f"❌ Failed: {len(failed)}")
    
    if failed:
        print()
        print("Failed filings:")
        for accession, error in failed[:10]:
            print(f"  {accession}: {error}")
        if len(failed) > 10:
            print(f"  ... and {len(failed) - 10} more")


if __name__ == "__main__":
    main()
