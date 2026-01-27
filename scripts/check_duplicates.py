#!/usr/bin/env python3
"""
Check for duplicate data in FinLoom structured database.

This script analyzes all major tables for duplicate records based on their
unique constraints and business logic.
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.utils.logger import get_logger

logger = get_logger("finloom.scripts.check_duplicates")


def check_company_duplicates(db: Database) -> dict:
    """Check for duplicate companies."""
    logger.info("\n" + "="*80)
    logger.info("Checking COMPANIES table for duplicates...")
    logger.info("="*80)
    
    # Check for duplicate CIKs (should be caught by PRIMARY KEY)
    cik_dupes = db.connection.execute("""
        SELECT cik, COUNT(*) as count
        FROM companies
        GROUP BY cik
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for duplicate tickers (multiple CIKs with same ticker)
    ticker_dupes = db.connection.execute("""
        SELECT ticker, COUNT(*) as count, STRING_AGG(cik, ', ') as ciks
        FROM companies
        WHERE ticker IS NOT NULL
        GROUP BY ticker
        HAVING COUNT(*) > 1
    """).fetchall()
    
    stats = {
        "table": "companies",
        "duplicate_ciks": len(cik_dupes),
        "duplicate_tickers": len(ticker_dupes),
        "details": {
            "ciks": cik_dupes,
            "tickers": ticker_dupes
        }
    }
    
    if cik_dupes:
        logger.warning(f"⚠️  Found {len(cik_dupes)} duplicate CIKs (should not happen!)")
        for cik, count in cik_dupes:
            logger.warning(f"   CIK {cik}: {count} entries")
    else:
        logger.info("✓ No duplicate CIKs found")
    
    if ticker_dupes:
        logger.warning(f"⚠️  Found {len(ticker_dupes)} tickers with multiple companies")
        for ticker, count, ciks in ticker_dupes:
            logger.warning(f"   Ticker {ticker}: {count} companies (CIKs: {ciks})")
    else:
        logger.info("✓ No duplicate tickers found")
    
    return stats


def check_filing_duplicates(db: Database) -> dict:
    """Check for duplicate filings."""
    logger.info("\n" + "="*80)
    logger.info("Checking FILINGS table for duplicates...")
    logger.info("="*80)
    
    # Check for duplicate accession numbers (should be caught by PRIMARY KEY)
    accession_dupes = db.connection.execute("""
        SELECT accession_number, COUNT(*) as count
        FROM filings
        GROUP BY accession_number
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for potential business duplicates (same CIK, form_type, and filing_date)
    business_dupes = db.connection.execute("""
        SELECT 
            cik, 
            form_type, 
            filing_date,
            COUNT(*) as count,
            STRING_AGG(accession_number, ', ') as accession_numbers
        FROM filings
        GROUP BY cik, form_type, filing_date
        HAVING COUNT(*) > 1
        ORDER BY count DESC, cik, filing_date DESC
    """).fetchall()
    
    stats = {
        "table": "filings",
        "duplicate_accessions": len(accession_dupes),
        "business_duplicates": len(business_dupes),
        "details": {
            "accessions": accession_dupes,
            "business": business_dupes
        }
    }
    
    if accession_dupes:
        logger.warning(f"⚠️  Found {len(accession_dupes)} duplicate accession numbers (should not happen!)")
        for acc, count in accession_dupes:
            logger.warning(f"   Accession {acc}: {count} entries")
    else:
        logger.info("✓ No duplicate accession numbers found")
    
    if business_dupes:
        logger.warning(f"⚠️  Found {len(business_dupes)} potential business duplicate filings")
        logger.warning("   (same CIK, form type, and filing date - may be legitimate amendments)")
        for cik, form, date, count, accessions in business_dupes[:10]:  # Show first 10
            logger.warning(f"   CIK {cik}, {form}, {date}: {count} filings")
            logger.warning(f"      Accessions: {accessions}")
    else:
        logger.info("✓ No business duplicate filings found")
    
    return stats


def check_facts_duplicates(db: Database) -> dict:
    """Check for duplicate facts."""
    logger.info("\n" + "="*80)
    logger.info("Checking FACTS table for duplicates...")
    logger.info("="*80)
    
    # Check for duplicate fact IDs (should be caught by PRIMARY KEY)
    id_dupes = db.connection.execute("""
        SELECT id, COUNT(*) as count
        FROM facts
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates (same accession, concept, period, dimensions)
    # Note: dimensions is JSON, so we need to handle NULL carefully
    business_dupes = db.connection.execute("""
        SELECT 
            accession_number,
            concept_name,
            period_end,
            COALESCE(dimensions::VARCHAR, 'NULL') as dims,
            COUNT(*) as count,
            STRING_AGG(id::VARCHAR, ', ') as ids
        FROM facts
        GROUP BY accession_number, concept_name, period_end, COALESCE(dimensions::VARCHAR, 'NULL')
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 100
    """).fetchall()
    
    # Get total duplicate facts count
    total_dupes = db.connection.execute("""
        SELECT COUNT(*) as total
        FROM (
            SELECT 
                accession_number,
                concept_name,
                period_end,
                COALESCE(dimensions::VARCHAR, 'NULL') as dims,
                COUNT(*) as count
            FROM facts
            GROUP BY accession_number, concept_name, period_end, COALESCE(dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
        ) duplicates
    """).fetchone()[0]
    
    stats = {
        "table": "facts",
        "duplicate_ids": len(id_dupes),
        "business_duplicates": total_dupes,
        "details": {
            "ids": id_dupes,
            "business": business_dupes
        }
    }
    
    if id_dupes:
        logger.warning(f"⚠️  Found {len(id_dupes)} duplicate fact IDs (should not happen!)")
    else:
        logger.info("✓ No duplicate fact IDs found")
    
    if total_dupes > 0:
        logger.warning(f"⚠️  Found {total_dupes} business duplicate facts")
        logger.warning("   (same accession, concept, period - may indicate data quality issues)")
        if business_dupes:
            logger.warning(f"   Showing first {min(10, len(business_dupes))} examples:")
            for acc, concept, period, dims, count, ids in business_dupes[:10]:
                logger.warning(f"   {acc[:20]}... | {concept[:40]}... | {period} | dims:{dims[:20]}... : {count}x")
    else:
        logger.info("✓ No business duplicate facts found")
    
    return stats


def check_sections_duplicates(db: Database) -> dict:
    """Check for duplicate sections."""
    logger.info("\n" + "="*80)
    logger.info("Checking SECTIONS table for duplicates...")
    logger.info("="*80)
    
    # Check for duplicate section IDs (should be caught by PRIMARY KEY)
    id_dupes = db.connection.execute("""
        SELECT id, COUNT(*) as count
        FROM sections
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates (same accession, section_type)
    business_dupes = db.connection.execute("""
        SELECT 
            accession_number,
            section_type,
            COUNT(*) as count,
            STRING_AGG(id::VARCHAR, ', ') as ids,
            STRING_AGG(word_count::VARCHAR, ', ') as word_counts
        FROM sections
        GROUP BY accession_number, section_type
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    """).fetchall()
    
    stats = {
        "table": "sections",
        "duplicate_ids": len(id_dupes),
        "business_duplicates": len(business_dupes),
        "details": {
            "ids": id_dupes,
            "business": business_dupes
        }
    }
    
    if id_dupes:
        logger.warning(f"⚠️  Found {len(id_dupes)} duplicate section IDs (should not happen!)")
    else:
        logger.info("✓ No duplicate section IDs found")
    
    if business_dupes:
        logger.warning(f"⚠️  Found {len(business_dupes)} business duplicate sections")
        logger.warning("   (same accession and section type - may indicate reprocessing)")
        for acc, section_type, count, ids, word_counts in business_dupes[:10]:
            logger.warning(f"   {acc} | {section_type}: {count}x (IDs: {ids})")
            logger.warning(f"      Word counts: {word_counts}")
    else:
        logger.info("✓ No business duplicate sections found")
    
    return stats


def check_normalized_financials_duplicates(db: Database) -> dict:
    """Check for duplicate normalized financials using built-in method."""
    logger.info("\n" + "="*80)
    logger.info("Checking NORMALIZED_FINANCIALS table for duplicates...")
    logger.info("="*80)
    
    duplicates = db.detect_duplicates("normalized_financials")
    
    stats = {
        "table": "normalized_financials",
        "duplicate_groups": len(duplicates),
        "total_duplicate_records": sum(d["count"] - 1 for d in duplicates),
        "details": duplicates
    }
    
    if duplicates:
        logger.warning(f"⚠️  Found {len(duplicates)} duplicate groups in normalized_financials")
        logger.warning(f"   Total duplicate records: {stats['total_duplicate_records']}")
        logger.warning("\n   Top 10 duplicates by count:")
        
        for i, dup in enumerate(duplicates[:10], 1):
            logger.warning(f"   {i}. {dup['ticker']} FY{dup['year']} Q{dup['quarter'] or 'N/A'} {dup['metric']}: {dup['count']}x")
            for record in dup['records']:
                keeper_mark = " [KEEP]" if record['keep'] else " [DELETE]"
                logger.warning(f"      ID {record['id']}: confidence={record['confidence']:.2f}, "
                             f"value={record['value']}, created={record['created_at']}{keeper_mark}")
    else:
        logger.info("✓ No duplicate normalized financials found")
    
    return stats


def check_chunks_duplicates(db: Database) -> dict:
    """Check for duplicate chunks."""
    logger.info("\n" + "="*80)
    logger.info("Checking CHUNKS table for duplicates...")
    logger.info("="*80)
    
    # Check if table exists and has data
    try:
        count = db.connection.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        if count == 0:
            logger.info("ℹ️  CHUNKS table is empty (no data to check)")
            return {"table": "chunks", "no_data": True}
    except Exception as e:
        logger.info(f"ℹ️  CHUNKS table not accessible: {e}")
        return {"table": "chunks", "error": str(e)}
    
    # Check for duplicate chunk IDs
    id_dupes = db.connection.execute("""
        SELECT chunk_id, COUNT(*) as count
        FROM chunks
        GROUP BY chunk_id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates
    business_dupes = db.connection.execute("""
        SELECT 
            accession_number,
            section_id,
            chunk_level,
            chunk_index,
            COUNT(*) as count
        FROM chunks
        GROUP BY accession_number, section_id, chunk_level, chunk_index
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    """).fetchall()
    
    stats = {
        "table": "chunks",
        "total_chunks": count,
        "duplicate_ids": len(id_dupes),
        "business_duplicates": len(business_dupes)
    }
    
    if id_dupes:
        logger.warning(f"⚠️  Found {len(id_dupes)} duplicate chunk IDs")
    else:
        logger.info("✓ No duplicate chunk IDs found")
    
    if business_dupes:
        logger.warning(f"⚠️  Found {len(business_dupes)} business duplicate chunks")
    else:
        logger.info("✓ No business duplicate chunks found")
    
    return stats


def main():
    """Main function to check all duplicates."""
    logger.info("="*80)
    logger.info("FinLoom Duplicate Data Check")
    logger.info("="*80)
    
    # Initialize database
    db = Database()
    
    # Get database statistics
    logger.info("\nDatabase Statistics:")
    tables = ['companies', 'filings', 'facts', 'sections', 'chunks', 'normalized_financials']
    for table in tables:
        try:
            count = db.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table}: {count:,} records")
        except Exception as e:
            logger.info(f"  {table}: Error - {e}")
    
    # Run all duplicate checks
    all_stats = {}
    
    all_stats['companies'] = check_company_duplicates(db)
    all_stats['filings'] = check_filing_duplicates(db)
    all_stats['facts'] = check_facts_duplicates(db)
    all_stats['sections'] = check_sections_duplicates(db)
    all_stats['chunks'] = check_chunks_duplicates(db)
    all_stats['normalized_financials'] = check_normalized_financials_duplicates(db)
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("SUMMARY")
    logger.info("="*80)
    
    has_issues = False
    
    for table, stats in all_stats.items():
        if stats.get('no_data'):
            logger.info(f"✓ {table.upper()}: No data")
            continue
        if stats.get('error'):
            logger.info(f"⚠️  {table.upper()}: {stats['error']}")
            continue
            
        issues = []
        
        if stats.get('duplicate_ciks', 0) > 0:
            issues.append(f"{stats['duplicate_ciks']} duplicate CIKs")
            has_issues = True
        if stats.get('duplicate_tickers', 0) > 0:
            issues.append(f"{stats['duplicate_tickers']} duplicate tickers")
            has_issues = True
        if stats.get('duplicate_accessions', 0) > 0:
            issues.append(f"{stats['duplicate_accessions']} duplicate accessions")
            has_issues = True
        if stats.get('business_duplicates', 0) > 0:
            issues.append(f"{stats['business_duplicates']} business duplicates")
            has_issues = True
        if stats.get('duplicate_ids', 0) > 0:
            issues.append(f"{stats['duplicate_ids']} duplicate IDs")
            has_issues = True
        if stats.get('duplicate_groups', 0) > 0:
            issues.append(f"{stats['duplicate_groups']} duplicate groups")
            has_issues = True
        
        if issues:
            logger.warning(f"⚠️  {table.upper()}: {', '.join(issues)}")
        else:
            logger.info(f"✓ {table.upper()}: No duplicates")
    
    if not has_issues:
        logger.info("\n✅ All tables are clean - no duplicates found!")
    else:
        logger.warning("\n⚠️  Found duplicates in one or more tables")
        logger.warning("   Run appropriate cleanup scripts to resolve issues")
        logger.warning("   For normalized_financials, use: db.remove_duplicates(dry_run=False)")
    
    db.close()
    
    return 0 if not has_issues else 1


if __name__ == "__main__":
    sys.exit(main())
