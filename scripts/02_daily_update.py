#!/usr/bin/env python3
"""
Daily Update Script

Checks for new SEC filings from tracked companies and processes them.
Designed to be run via cron for automated updates.

Usage:
    python scripts/02_daily_update.py
    
Cron example (run daily at 9 AM):
    0 9 * * * cd /path/to/FinLoom-2026 && /path/to/venv/bin/python scripts/02_daily_update.py
"""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ingestion.downloader import SECDownloader
from src.ingestion.sec_api import SECApi
from src.parsers.section_parser import SectionParser
from src.parsers.xbrl_parser import SimpleXBRLParser
from src.storage.database import Database
from src.utils.config import get_settings, load_config
from src.utils.logger import get_logger, setup_logging
from src.validation.data_quality import DataQualityChecker

logger = get_logger("finloom.daily_update")


def check_new_filings(sec_api: SECApi, db: Database, settings) -> list:
    """
    Check for new filings from tracked companies.
    
    Returns:
        List of new filing info objects.
    """
    new_filings = []
    today = datetime.now().date()
    lookback_days = 7  # Check last 7 days
    
    start_date = today - timedelta(days=lookback_days)
    
    for company in settings.companies:
        logger.info(f"Checking {company.name} for new filings...")
        
        try:
            filings = sec_api.get_company_filings(
                cik=company.cik,
                form_type="10-K",
                start_date=start_date,
                end_date=today,
            )
            
            for filing in filings:
                # Check if we already have this filing
                existing = db.get_filing(filing.accession_number)
                
                if not existing:
                    logger.info(f"Found new filing: {filing.accession_number}")
                    new_filings.append(filing)
                elif existing.get("download_status") == "failed":
                    # Retry failed filings
                    logger.info(f"Retrying failed filing: {filing.accession_number}")
                    new_filings.append(filing)
                    
        except Exception as e:
            logger.error(f"Failed to check {company.name}: {e}")
    
    return new_filings


def process_new_filings(
    new_filings: list,
    downloader: SECDownloader,
    db: Database,
) -> dict:
    """
    Download and process new filings.
    
    Returns:
        Stats dict.
    """
    stats = {
        "downloaded": 0,
        "parsed": 0,
        "failed": 0,
    }
    
    xbrl_parser = SimpleXBRLParser()
    section_parser = SectionParser(priority_only=True)
    quality_checker = DataQualityChecker()
    
    for filing in new_filings:
        logger.info(f"Processing {filing.accession_number}")
        
        try:
            # Download
            result = downloader.download_filing(filing)
            
            if not result.success:
                stats["failed"] += 1
                db.upsert_filing(
                    accession_number=filing.accession_number,
                    cik=filing.cik,
                    form_type=filing.form_type,
                    filing_date=filing.filing_date,
                    download_status="failed",
                )
                continue
            
            stats["downloaded"] += 1
            
            # Record in database
            db.upsert_filing(
                accession_number=filing.accession_number,
                cik=filing.cik,
                form_type=filing.form_type,
                filing_date=filing.filing_date,
                period_of_report=None,  # Will be extracted from XBRL
                primary_document=filing.primary_document,
                is_xbrl=filing.is_xbrl,
                is_inline_xbrl=filing.is_inline_xbrl,
                local_path=result.local_path,
                download_status="completed",
            )
            
            # Parse XBRL
            filing_path = Path(result.local_path)
            xbrl_result = xbrl_parser.parse_filing(filing_path, filing.accession_number)
            
            if xbrl_result.success:
                for fact in xbrl_result.facts:
                    db.insert_fact(
                        accession_number=filing.accession_number,
                        **fact.to_dict(),
                    )
                db.update_filing_status(
                    accession_number=filing.accession_number,
                    xbrl_processed=True,
                )
            
            # Parse sections
            section_result = section_parser.parse_filing(filing_path, filing.accession_number)
            
            if section_result.success:
                for section in section_result.sections:
                    db.insert_section(
                        accession_number=filing.accession_number,
                        **section.to_dict(),
                    )
                db.update_filing_status(
                    accession_number=filing.accession_number,
                    sections_processed=True,
                )
            
            stats["parsed"] += 1
            
            # Log processing
            db.log_processing(
                pipeline_stage="daily_update",
                status="completed",
                accession_number=filing.accession_number,
                cik=filing.cik,
                records_processed=len(xbrl_result.facts) + len(section_result.sections),
            )
            
        except Exception as e:
            stats["failed"] += 1
            logger.error(f"Failed to process {filing.accession_number}: {e}")
            db.log_processing(
                pipeline_stage="daily_update",
                status="failed",
                accession_number=filing.accession_number,
                error_message=str(e),
            )
    
    return stats


def main():
    """Main entry point."""
    # Setup
    setup_logging()
    load_config()
    settings = get_settings()
    
    logger.info("=" * 60)
    logger.info("SEC 10-K Daily Update")
    logger.info(f"Started at: {datetime.now()}")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    with Database() as db:
        with SECApi() as sec_api:
            # Check for new filings
            logger.info("Checking for new filings...")
            new_filings = check_new_filings(sec_api, db, settings)
            logger.info(f"Found {len(new_filings)} new filings")
            
            if not new_filings:
                logger.info("No new filings to process")
                return 0
            
            # Process new filings
            with SECDownloader() as downloader:
                stats = process_new_filings(new_filings, downloader, db)
    
    elapsed = time.time() - start_time
    
    # Summary
    logger.info("=" * 60)
    logger.info("Daily Update Complete")
    logger.info(f"Duration: {elapsed:.1f}s")
    logger.info(f"Downloaded: {stats['downloaded']}")
    logger.info(f"Parsed: {stats['parsed']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info("=" * 60)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
