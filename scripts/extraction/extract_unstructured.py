#!/usr/bin/env python3
"""
Batch extraction tool for unstructured data from SEC filings.

Usage:
    python scripts/extract_unstructured.py --all
    python scripts/extract_unstructured.py --ticker AAPL
    python scripts/extract_unstructured.py --year 2024
    python scripts/extract_unstructured.py --accession 0001193125-24-123456
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import duckdb
from tqdm import tqdm

from src.processing.unstructured_pipeline import UnstructuredDataPipeline
from src.utils.logger import get_logger

logger = get_logger("finloom.scripts.extract_unstructured")


def process_filing_with_own_pipeline(
    filing_tuple: tuple[str, Path],
    db_path: str,
    use_xbrl: bool,
) -> 'ProcessingResult':
    """
    Process one filing with its own pipeline instance (thread-safe).

    Each worker creates its own pipeline to avoid shared state issues.
    """
    accession, path = filing_tuple

    try:
        pipeline = UnstructuredDataPipeline(
            db_path=db_path,
            use_xbrl_parser=use_xbrl,
            priority_sections_only=False,
        )

        return pipeline.process_filing(accession, path)
    except Exception as e:
        logger.error(f"Failed to process {accession}: {e}", exc_info=True)
        from src.processing.unstructured_pipeline import ProcessingResult
        return ProcessingResult(
            success=False,
            accession_number=accession,
            error_message=str(e)
        )


def get_filings_to_process(
    db_path: str,
    ticker: Optional[str] = None,
    year: Optional[int] = None,
    accession: Optional[str] = None,
    all_filings: bool = False,
    missing: bool = False,
) -> list[tuple[str, Path]]:
    """Get list of filings to process."""
    conn = duckdb.connect(db_path, read_only=True)

    query = """
        SELECT f.accession_number, f.local_path
        FROM filings f
        WHERE f.download_status = 'completed'
          AND f.local_path IS NOT NULL
    """

    params = []

    if ticker:
        query += " AND f.cik IN (SELECT cik FROM companies WHERE ticker = ?)"
        params.append(ticker)

    if year:
        query += " AND EXTRACT(YEAR FROM f.filing_date) = ?"
        params.append(year)

    if accession:
        query += " AND f.accession_number = ?"
        params.append(accession)

    if missing:
        # Find filings marked processed but with NO sections
        query += """ AND NOT EXISTS (
            SELECT 1 FROM sections s
            WHERE s.accession_number = f.accession_number
        )"""
    elif not all_filings and not ticker and not year and not accession:
        # Default: process only unprocessed filings
        query += " AND f.sections_processed = FALSE"

    query += " ORDER BY f.filing_date DESC"

    results = conn.execute(query, params).fetchall()
    conn.close()
    
    filings = []
    for acc, local_path in results:
        path = Path(local_path)
        if path.exists():
            filings.append((acc, path))
        else:
            logger.warning(f"Filing path not found: {local_path}")
    
    return filings


def main():
    parser = argparse.ArgumentParser(
        description="Extract unstructured data from SEC filings"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all filings"
    )
    
    parser.add_argument(
        "--ticker",
        type=str,
        help="Process filings for specific ticker"
    )
    
    parser.add_argument(
        "--year",
        type=int,
        help="Process filings from specific year"
    )
    
    parser.add_argument(
        "--accession",
        type=str,
        help="Process specific filing by accession number"
    )

    parser.add_argument(
        "--missing",
        action="store_true",
        help="Process filings with no extracted sections (regardless of status flag)"
    )

    parser.add_argument(
        "--parallel",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4, recommended: 2-4 for DuckDB)"
    )
    
    parser.add_argument(
        "--db",
        type=str,
        default="data/database/finloom.duckdb",
        help="Path to database"
    )
    
    parser.add_argument(
        "--use-xbrl",
        action="store_true",
        default=True,
        help="Use XBRL-aware section parser"
    )
    
    args = parser.parse_args()
    
    db_path = project_root / args.db
    
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(1)
    
    # Get filings to process
    logger.info("Finding filings to process...")
    filings = get_filings_to_process(
        str(db_path),
        ticker=args.ticker,
        year=args.year,
        accession=args.accession,
        all_filings=args.all,
        missing=args.missing,
    )
    
    if not filings:
        logger.info("No filings to process")
        return
    
    logger.info(f"Found {len(filings)} filings to process")

    # Process filings with per-worker pipelines (thread-safe)
    if args.parallel > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        logger.info(f"Processing {len(filings)} filings with {args.parallel} workers (per-worker pipelines)")

        results = []
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(
                    process_filing_with_own_pipeline,
                    filing,
                    str(db_path),
                    args.use_xbrl
                ): filing[0]  # accession number for tracking
                for filing in filings
            }

            for future in tqdm(as_completed(futures), total=len(filings), desc="Processing"):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    accession = futures[future]
                    logger.error(f"Worker failed for {accession}: {e}")
                    from src.processing.unstructured_pipeline import ProcessingResult
                    results.append(ProcessingResult(
                        success=False,
                        accession_number=accession,
                        error_message=str(e)
                    ))
    else:
        logger.info(f"Processing {len(filings)} filings sequentially")
        results = []
        for filing in tqdm(filings, desc="Processing"):
            result = process_filing_with_own_pipeline(
                filing,
                str(db_path),
                args.use_xbrl
            )
            results.append(result)
    
    # Summary
    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful
    
    total_sections = sum(r.sections_count for r in results)
    total_tables = sum(r.tables_count for r in results)
    total_footnotes = sum(r.footnotes_count for r in results)
    total_chunks = sum(r.chunks_count for r in results)
    
    avg_quality = sum(r.quality_score for r in results if r.success) / max(successful, 1)
    
    logger.info("=" * 60)
    logger.info("PROCESSING COMPLETE")
    logger.info(f"Total filings: {len(results)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Total sections: {total_sections}")
    logger.info(f"Total tables: {total_tables}")
    logger.info(f"Total footnotes: {total_footnotes}")
    logger.info(f"Total chunks: {total_chunks}")
    logger.info(f"Average quality score: {avg_quality:.2f}/100")
    logger.info("=" * 60)
    
    # Print failures
    if failed > 0:
        logger.warning(f"\nFailed filings ({failed}):")
        for result in results:
            if not result.success:
                logger.warning(f"  {result.accession_number}: {result.error_message}")


if __name__ == "__main__":
    main()
