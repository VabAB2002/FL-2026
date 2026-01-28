#!/usr/bin/env python3
"""
Historical Data Backfill Script

Downloads and processes historical 10-K filings for all configured companies.
Implements checkpointing for resume capability.

Usage:
    python scripts/01_backfill_historical.py [--resume] [--company CIK]
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.downloader import SECDownloader
from src.ingestion.sec_api import SECApi
from src.parsers.xbrl_parser import SimpleXBRLParser, XBRLParser
from src.processing.unstructured_pipeline import UnstructuredDataPipeline
from src.storage.database import Database, initialize_database
from src.utils.config import get_settings, load_config
from src.utils.logger import get_logger, setup_logging
from src.validation.data_quality import DataQualityChecker

logger = get_logger("finloom.backfill")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill historical SEC 10-K filings"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint",
    )
    parser.add_argument(
        "--company",
        type=str,
        help="Process only this CIK",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Only download, skip parsing",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Only parse already downloaded filings",
    )
    parser.add_argument(
        "--use-simple-parser",
        action="store_true",
        help="Use simple XBRL parser (no Arelle dependency)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be processed without doing it",
    )
    return parser.parse_args()


def setup_companies(db: Database, settings) -> None:
    """Initialize company records in database."""
    logger.info(f"Setting up {len(settings.companies)} companies")
    
    for company in settings.companies:
        db.upsert_company(
            cik=company.cik,
            company_name=company.name,
            ticker=company.ticker,
        )
    
    logger.info("Companies initialized")


def download_filings(
    downloader: SECDownloader,
    db: Database,
    settings,
    company_cik: str = None,
    resume: bool = True,
) -> dict:
    """
    Download filings for all or specific company.
    
    Returns:
        Stats dict with counts.
    """
    stats = {
        "companies_processed": 0,
        "filings_downloaded": 0,
        "filings_failed": 0,
        "total_bytes": 0,
    }
    
    companies = settings.companies
    if company_cik:
        companies = [c for c in companies if c.cik == company_cik]
    
    for company in companies:
        logger.info(f"Processing {company.name} ({company.ticker})")
        
        try:
            # Download all 10-K filings for company
            results = downloader.download_company_filings(
                cik=company.cik,
                form_type="10-K",
                start_year=settings.extraction.start_year,
                end_year=settings.extraction.end_year,
                resume=resume,
            )
            
            # Record results in database
            for result in results:
                if result.success:
                    stats["filings_downloaded"] += 1
                    stats["total_bytes"] += result.total_bytes
                    
                    # Update filing record with actual filing date from SEC
                    db.upsert_filing(
                        accession_number=result.accession_number,
                        cik=result.cik,
                        form_type=result.form_type or "10-K",
                        filing_date=result.filing_date,  # ✅ FIXED: Use actual SEC filing date
                        acceptance_datetime=result.acceptance_datetime,
                        local_path=result.local_path,
                        download_status="completed",
                    )
                else:
                    stats["filings_failed"] += 1
                    # Even for failed downloads, use actual filing date if available
                    db.upsert_filing(
                        accession_number=result.accession_number,
                        cik=result.cik,
                        form_type=result.form_type or "10-K",
                        filing_date=result.filing_date or datetime.now().date(),
                        download_status="failed",
                    )
                    db.log_processing(
                        pipeline_stage="download",
                        status="failed",
                        accession_number=result.accession_number,
                        cik=result.cik,
                        error_message=result.error_message,
                    )
            
            stats["companies_processed"] += 1
            
        except Exception as e:
            logger.error(f"Failed to process {company.name}: {e}")
            db.log_processing(
                pipeline_stage="download",
                status="failed",
                cik=company.cik,
                error_message=str(e),
            )
    
    return stats


def parse_filings(
    db: Database,
    settings,
    use_simple_parser: bool = False,
) -> dict:
    """
    Parse all downloaded but unprocessed filings.
    
    Returns:
        Stats dict with counts.
    """
    stats = {
        "filings_processed": 0,
        "xbrl_success": 0,
        "xbrl_failed": 0,
        "sections_success": 0,
        "sections_failed": 0,
        "facts_extracted": 0,
        "sections_extracted": 0,
    }
    
    # Initialize parsers
    if use_simple_parser:
        xbrl_parser = SimpleXBRLParser()
    else:
        try:
            # Get extract_all_facts from config
            extract_all = settings.extraction.extract_all_xbrl_facts
            logger.info(f"Initializing XBRL parser (extract_all_facts={extract_all})")
            xbrl_parser = XBRLParser(extract_all_facts=extract_all)
        except ImportError:
            logger.warning("Arelle not available, using simple parser")
            xbrl_parser = SimpleXBRLParser()
    
    # Initialize unstructured pipeline for markdown extraction
    unstructured_pipeline = UnstructuredDataPipeline(str(db.db_path))
    quality_checker = DataQualityChecker()
    
    # Get unprocessed filings
    unprocessed = db.get_unprocessed_filings("xbrl")
    logger.info(f"Found {len(unprocessed)} filings to parse")
    
    for filing in unprocessed:
        accession = filing["accession_number"]
        local_path = filing.get("local_path")
        
        if not local_path:
            logger.warning(f"No local path for {accession}")
            continue
        
        filing_path = Path(local_path)
        if not filing_path.exists():
            logger.warning(f"Filing path does not exist: {filing_path}")
            continue
        
        logger.info(f"Parsing {accession}")
        start_time = time.time()
        
        try:
            # Parse XBRL
            xbrl_result = xbrl_parser.parse_filing(filing_path, accession)
            
            if xbrl_result.success:
                # Validate fact completeness
                extract_all = settings.extraction.extract_all_xbrl_facts
                completeness_issues = quality_checker.validate_fact_completeness(
                    facts=xbrl_result.facts,
                    accession_number=accession,
                    extract_all_mode=extract_all
                )
                
                # Log quality issues (insert_quality_issue method not yet implemented)
                for issue in completeness_issues:
                    # db.insert_quality_issue(**issue)
                    logger.warning(f"Quality issue: {issue['message']}")
                
                # Insert facts into database
                for fact in xbrl_result.facts:
                    db.insert_fact(
                        accession_number=accession,
                        **fact.to_dict(),
                    )
                stats["facts_extracted"] += len(xbrl_result.facts)
                stats["xbrl_success"] += 1
                
                # Log extraction metrics
                logger.info(
                    f"Extracted {len(xbrl_result.facts)} facts "
                    f"(mode: {'all' if extract_all else 'core'})"
                )
                
                # Update filing with period info
                if xbrl_result.period_end:
                    db.update_filing_status(
                        accession_number=accession,
                        xbrl_processed=True,
                    )
                
                # Log detailed processing metrics
                db.log_processing(
                    pipeline_stage="xbrl_parse",
                    status="completed",
                    accession_number=accession,
                    cik=filing["cik"],
                    processing_time_ms=xbrl_result.parse_time_ms,
                    records_processed=len(xbrl_result.facts),
                    context={
                        "extraction_mode": "all_facts" if extract_all else "core_only",
                        "fact_count": len(xbrl_result.facts),
                        "core_fact_count": len(xbrl_result.core_facts),
                        "has_hierarchy": any(f.section for f in xbrl_result.facts),
                        "has_labels": any(f.label for f in xbrl_result.facts),
                        "sections": list(set(f.section for f in xbrl_result.facts if f.section)),
                    }
                )
            else:
                stats["xbrl_failed"] += 1
                logger.warning(f"XBRL parsing failed: {xbrl_result.error_message}")
            
            # Extract markdown using unstructured pipeline
            markdown_result = unstructured_pipeline.process_filing(accession, filing_path)
            
            if markdown_result.success:
                stats["sections_success"] += 1
                logger.info(f"Extracted markdown: {markdown_result.markdown_word_count:,} words")
            else:
                stats["sections_failed"] += 1
                logger.warning(f"Markdown extraction failed: {markdown_result.error_message}")
            
            # Note: Validation removed - markdown-only architecture doesn't have sections list
            
            stats["filings_processed"] += 1
            elapsed = time.time() - start_time
            logger.info(f"Parsed {accession} in {elapsed:.1f}s")
            
            # Log processing
            records_count = len(xbrl_result.facts) if xbrl_result.success else 0
            if markdown_result.success:
                records_count += 1  # Count markdown as 1 record
            
            db.log_processing(
                pipeline_stage="parse",
                status="completed",
                accession_number=accession,
                processing_time_ms=int(elapsed * 1000),
                records_processed=records_count,
            )
            
        except Exception as e:
            logger.error(f"Failed to parse {accession}: {e}")
            db.log_processing(
                pipeline_stage="parse",
                status="failed",
                accession_number=accession,
                error_message=str(e),
            )
    
    return stats


def main():
    """Main entry point."""
    args = parse_args()
    
    # Setup
    setup_logging()
    load_config()
    settings = get_settings()
    
    logger.info("=" * 60)
    logger.info("SEC 10-K Historical Backfill")
    logger.info("=" * 60)
    logger.info(f"Companies: {len(settings.companies)}")
    logger.info(f"Date range: {settings.extraction.start_year}-{settings.extraction.end_year}")
    logger.info(f"Resume mode: {args.resume}")
    
    if args.dry_run:
        logger.info("DRY RUN - No changes will be made")
        for company in settings.companies:
            logger.info(f"  Would process: {company.name} ({company.ticker})")
        return 0
    
    # Initialize database
    logger.info("Initializing database...")
    initialize_database()
    
    total_stats = {
        "start_time": datetime.now(),
        "download": {},
        "parse": {},
    }
    
    with Database() as db:
        # Setup companies
        setup_companies(db, settings)
        
        # Download phase
        if not args.parse_only:
            logger.info("")
            logger.info("-" * 40)
            logger.info("PHASE 1: Downloading Filings")
            logger.info("-" * 40)
            
            with SECDownloader() as downloader:
                total_stats["download"] = download_filings(
                    downloader=downloader,
                    db=db,
                    settings=settings,
                    company_cik=args.company,
                    resume=args.resume,
                )
            
            logger.info(f"Download complete: {total_stats['download']}")
        
        # Parse phase
        if not args.download_only:
            logger.info("")
            logger.info("-" * 40)
            logger.info("PHASE 2: Parsing Filings")
            logger.info("-" * 40)
            
            total_stats["parse"] = parse_filings(
                db=db,
                settings=settings,
                use_simple_parser=args.use_simple_parser,
            )
            
            logger.info(f"Parse complete: {total_stats['parse']}")
    
    # Summary
    total_stats["end_time"] = datetime.now()
    total_stats["duration"] = total_stats["end_time"] - total_stats["start_time"]
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Duration: {total_stats['duration']}")
    
    if total_stats.get("download"):
        d = total_stats["download"]
        logger.info(f"Downloaded: {d.get('filings_downloaded', 0)} filings, "
                   f"{d.get('filings_failed', 0)} failed")
        logger.info(f"Total data: {d.get('total_bytes', 0) / 1024 / 1024:.1f} MB")
    
    if total_stats.get("parse"):
        p = total_stats["parse"]
        logger.info(f"Parsed: {p.get('filings_processed', 0)} filings")
        logger.info(f"XBRL: {p.get('xbrl_success', 0)} success, {p.get('xbrl_failed', 0)} failed")
        logger.info(f"Sections: {p.get('sections_success', 0)} success, {p.get('sections_failed', 0)} failed")
        logger.info(f"Facts extracted: {p.get('facts_extracted', 0)}")
        logger.info(f"Sections extracted: {p.get('sections_extracted', 0)}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
