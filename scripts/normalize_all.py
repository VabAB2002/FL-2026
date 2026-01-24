#!/usr/bin/env python3
"""
Normalize all filings to create Bloomberg-style comparable data.

This is the ETL (Extract, Transform, Load) pipeline that:
1. Extracts facts from raw XBRL filings
2. Transforms them using concept mappings
3. Loads normalized metrics into normalized_financials table

Usage:
    python scripts/normalize_all.py                # Normalize all companies
    python scripts/normalize_all.py --ticker AAPL  # Normalize only Apple
    python scripts/normalize_all.py --clear        # Clear existing data first
"""

import argparse
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.business.concept_mapper import ConceptMapper
from src.storage.database import Database
from src.utils.logger import get_logger

logger = get_logger("finloom.normalize")


def normalize_all_filings(
    db: Database,
    ticker: str = None,
    clear_first: bool = False,
) -> dict:
    """
    Normalize all filings and populate normalized_financials table.
    
    Args:
        db: Database instance
        ticker: Optional ticker to process only one company
        clear_first: If True, clear existing normalized data
    
    Returns:
        Dictionary with processing statistics
    """
    stats = {
        "filings_processed": 0,
        "filings_failed": 0,
        "metrics_created": 0,
        "companies": set(),
    }
    
    # Clear existing normalized data if requested
    if clear_first:
        logger.info("Clearing existing normalized data...")
        if ticker:
            db.connection.execute(
                "DELETE FROM normalized_financials WHERE company_ticker = ?",
                [ticker]
            )
        else:
            db.connection.execute("DELETE FROM normalized_financials")
        logger.info("Cleared.")
    
    # Get list of filings to process (deduplicated)
    if ticker:
        # Get company CIK first
        company = db.connection.execute(
            "SELECT cik, ticker FROM companies WHERE UPPER(ticker) = UPPER(?)",
            [ticker]
        ).fetchone()
        
        if not company:
            logger.error(f"Company with ticker '{ticker}' not found")
            return stats
        
        cik, ticker = company
        filings = db.get_latest_filing_per_period(ticker=ticker)
    else:
        filings = db.get_latest_filing_per_period()
    
    if not filings:
        logger.warning("No processed filings found")
        return stats
    
    logger.info(f"Found {len(filings)} unique filings to normalize (deduplicated)")
    
    # Initialize mapper
    mapper = ConceptMapper(db)
    
    # Process each filing
    start_time = time.time()
    
    for i, filing_row in enumerate(filings, 1):
        accession_number = filing_row[0]
        cik = filing_row[1]
        company_ticker = filing_row[2]
        form_type = filing_row[3]
        filing_date = filing_row[4]
        period_of_report = filing_row[5]
        
        stats["companies"].add(company_ticker)
        
        logger.info(f"  [{i}/{len(filings)}] Normalizing {company_ticker} - {accession_number} ({form_type})")
        
        try:
            # Normalize the filing
            normalized_metrics = mapper.normalize_filing(accession_number)
            
            if not normalized_metrics:
                logger.warning(f"    No metrics extracted")
                stats["filings_failed"] += 1
                continue
            
            # Insert into database
            for metric in normalized_metrics:
                db.insert_normalized_metric(
                    company_ticker=company_ticker,
                    fiscal_year=metric.fiscal_year,
                    fiscal_quarter=metric.fiscal_quarter,
                    metric_id=metric.metric_id,
                    metric_value=float(metric.metric_value),
                    source_concept=metric.source_concept,
                    source_accession=accession_number,
                    confidence_score=metric.confidence_score,
                )
                stats["metrics_created"] += 1
            
            logger.info(f"    Extracted {len(normalized_metrics)} metrics")
            stats["filings_processed"] += 1
            
        except Exception as e:
            logger.error(f"    Failed to normalize {accession_number}: {e}")
            stats["filings_failed"] += 1
    
    elapsed = time.time() - start_time
    
    # Print summary
    print()
    print("=" * 70)
    print("Normalization Complete!")
    print("=" * 70)
    print(f"  Filings processed:  {stats['filings_processed']:,}")
    print(f"  Filings failed:     {stats['filings_failed']:,}")
    print(f"  Metrics created:    {stats['metrics_created']:,}")
    print(f"  Companies:          {len(stats['companies'])}")
    print(f"  Time elapsed:       {elapsed:.1f}s")
    print("=" * 70)
    
    return stats


def verify_normalization(db: Database, ticker: str = None) -> None:
    """
    Verify normalization results.
    
    Args:
        db: Database instance
        ticker: Optional ticker to check specific company
    """
    print()
    print("=" * 70)
    print("Normalization Verification")
    print("=" * 70)
    
    # Check total normalized metrics
    total = db.connection.execute(
        "SELECT COUNT(*) FROM normalized_financials"
    ).fetchone()[0]
    print(f"\nTotal normalized metrics: {total:,}")
    
    # Check by company
    print("\nMetrics by Company:")
    results = db.connection.execute("""
        SELECT 
            company_ticker,
            COUNT(DISTINCT fiscal_year) as years,
            COUNT(DISTINCT metric_id) as metrics,
            COUNT(*) as total_metrics
        FROM normalized_financials
        GROUP BY company_ticker
        ORDER BY company_ticker
    """).fetchall()
    
    for ticker_val, years, metrics, total_metrics in results:
        print(f"  {ticker_val:6} {years:2} years, {metrics:2} metrics, {total_metrics:5} total")
    
    # Check sample data for a specific company
    if ticker or results:
        sample_ticker = ticker or results[0][0]
        print(f"\nSample data for {sample_ticker}:")
        
        sample = db.connection.execute("""
            SELECT 
                fiscal_year,
                metric_id,
                metric_value,
                source_concept,
                confidence_score
            FROM normalized_financials
            WHERE company_ticker = ?
            ORDER BY fiscal_year DESC, metric_id
            LIMIT 10
        """, [sample_ticker]).fetchall()
        
        for year, metric_id, value, concept, conf in sample:
            value_float = float(value) if value else 0
            value_str = f"${value_float/1e9:.1f}B" if abs(value_float) > 1e9 else f"${value_float/1e6:.1f}M"
            print(f"  {year} | {metric_id:20} {value_str:>12} (conf: {conf:.2f})")
    
    print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Normalize SEC filings to Bloomberg-style comparable data"
    )
    parser.add_argument(
        "--ticker", "-t",
        help="Only normalize filings for a specific company ticker"
    )
    parser.add_argument(
        "--clear", "-c",
        action="store_true",
        help="Clear existing normalized data before processing"
    )
    parser.add_argument(
        "--verify", "-v",
        action="store_true",
        help="Verify normalization results after processing"
    )
    
    args = parser.parse_args()
    
    # Initialize database
    logger.info("Initializing database...")
    db = Database()
    db.initialize_schema()
    
    logger.info(f"Database: {db.db_path}")
    print()
    
    try:
        # Run normalization
        stats = normalize_all_filings(
            db=db,
            ticker=args.ticker,
            clear_first=args.clear,
        )
        
        # Verify if requested
        if args.verify:
            verify_normalization(db, ticker=args.ticker)
        
        logger.info("Success!")
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to normalize: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
