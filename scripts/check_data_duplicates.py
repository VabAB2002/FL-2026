#!/usr/bin/env python3
"""
Unified duplicate detection and analysis tool for FinLoom database.

This script provides three modes:
- quick: Fast duplicate check using direct DuckDB connection
- full: Comprehensive check using Database class with all validation
- report: Detailed analysis with cleanup recommendations and SQL scripts

Usage:
    python scripts/check_data_duplicates.py --mode quick
    python scripts/check_data_duplicates.py --mode full
    python scripts/check_data_duplicates.py --mode report
"""

import argparse
import sys
from pathlib import Path

import duckdb

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


# ============================================================================
# QUICK MODE - Fast duplicate check bypassing config validation
# ============================================================================

def check_table_exists(conn, table_name):
    """Check if a table exists."""
    try:
        conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return True
    except:
        return False


def get_table_count(conn, table_name):
    """Get row count for a table."""
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except:
        return 0


def quick_check_companies(conn):
    """Quick check of companies table."""
    print("\n" + "=" * 80)
    print("COMPANIES TABLE")
    print("=" * 80)

    if not check_table_exists(conn, "companies"):
        print("⚠️  Table does not exist")
        return

    count = get_table_count(conn, "companies")
    print(f"Total records: {count:,}")

    cik_dupes = conn.execute("""
        SELECT cik, COUNT(*) as count
        FROM companies
        GROUP BY cik
        HAVING COUNT(*) > 1
    """).fetchall()

    ticker_dupes = conn.execute("""
        SELECT ticker, COUNT(*) as count, STRING_AGG(cik, ', ') as ciks
        FROM companies
        WHERE ticker IS NOT NULL
        GROUP BY ticker
        HAVING COUNT(*) > 1
    """).fetchall()

    if cik_dupes:
        print(f"⚠️  {len(cik_dupes)} duplicate CIKs found!")
        for cik, cnt in cik_dupes[:5]:
            print(f"   CIK {cik}: {cnt} entries")
    else:
        print("✓ No duplicate CIKs")

    if ticker_dupes:
        print(f"⚠️  {len(ticker_dupes)} tickers with multiple companies")
        for ticker, cnt, ciks in ticker_dupes[:5]:
            print(f"   Ticker {ticker}: {cnt} companies (CIKs: {ciks})")
    else:
        print("✓ No duplicate tickers")


def quick_check_filings(conn):
    """Quick check of filings table."""
    print("\n" + "=" * 80)
    print("FILINGS TABLE")
    print("=" * 80)

    if not check_table_exists(conn, "filings"):
        print("⚠️  Table does not exist")
        return

    count = get_table_count(conn, "filings")
    print(f"Total records: {count:,}")

    acc_dupes = conn.execute("""
        SELECT accession_number, COUNT(*) as count
        FROM filings
        GROUP BY accession_number
        HAVING COUNT(*) > 1
    """).fetchall()

    biz_dupes = conn.execute("""
        SELECT cik, form_type, filing_date, COUNT(*) as count
        FROM filings
        GROUP BY cik, form_type, filing_date
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    if acc_dupes:
        print(f"⚠️  {len(acc_dupes)} duplicate accession numbers!")
        for acc, cnt in acc_dupes[:5]:
            print(f"   {acc}: {cnt} entries")
    else:
        print("✓ No duplicate accession numbers")

    if biz_dupes:
        print(f"⚠️  {len(biz_dupes)} potential business duplicate filings")
        for cik, form, date, cnt in biz_dupes[:5]:
            print(f"   CIK {cik}, {form}, {date}: {cnt} filings")
    else:
        print("✓ No business duplicate filings")


def quick_check_facts(conn):
    """Quick check of facts table."""
    print("\n" + "=" * 80)
    print("FACTS TABLE")
    print("=" * 80)

    if not check_table_exists(conn, "facts"):
        print("⚠️  Table does not exist")
        return

    count = get_table_count(conn, "facts")
    print(f"Total records: {count:,}")

    id_dupes = conn.execute("""
        SELECT id, COUNT(*) as count
        FROM facts
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()

    total_dupes = conn.execute("""
        SELECT COUNT(*) as total
        FROM (
            SELECT accession_number, concept_name, period_end,
                   COALESCE(dimensions::VARCHAR, 'NULL') as dims,
                   COUNT(*) as count
            FROM facts
            GROUP BY accession_number, concept_name, period_end, 
                     COALESCE(dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
        ) duplicates
    """).fetchone()[0]

    if id_dupes:
        print(f"⚠️  {len(id_dupes)} duplicate fact IDs!")
    else:
        print("✓ No duplicate fact IDs")

    if total_dupes > 0:
        print(f"⚠️  {total_dupes} business duplicate facts")

        examples = conn.execute("""
            SELECT accession_number, concept_name, COUNT(*) as count
            FROM facts
            GROUP BY accession_number, concept_name, period_end, 
                     COALESCE(dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
            ORDER BY count DESC
            LIMIT 5
        """).fetchall()

        for acc, concept, cnt in examples:
            print(f"   {acc[:20]}... | {concept[:40]}... : {cnt}x")
    else:
        print("✓ No business duplicate facts")


def quick_check_sections(conn):
    """Quick check of sections table."""
    print("\n" + "=" * 80)
    print("SECTIONS TABLE")
    print("=" * 80)

    if not check_table_exists(conn, "sections"):
        print("⚠️  Table does not exist")
        return

    count = get_table_count(conn, "sections")
    print(f"Total records: {count:,}")

    id_dupes = conn.execute("""
        SELECT id, COUNT(*) as count
        FROM sections
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()

    biz_dupes = conn.execute("""
        SELECT accession_number, section_type, COUNT(*) as count
        FROM sections
        GROUP BY accession_number, section_type
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()

    if id_dupes:
        print(f"⚠️  {len(id_dupes)} duplicate section IDs!")
    else:
        print("✓ No duplicate section IDs")

    if biz_dupes:
        print(f"⚠️  {len(biz_dupes)} business duplicate sections")
        for acc, section_type, cnt in biz_dupes[:5]:
            print(f"   {acc} | {section_type}: {cnt}x")
    else:
        print("✓ No business duplicate sections")


def quick_check_normalized_financials(conn):
    """Quick check of normalized_financials table."""
    print("\n" + "=" * 80)
    print("NORMALIZED_FINANCIALS TABLE")
    print("=" * 80)

    if not check_table_exists(conn, "normalized_financials"):
        print("⚠️  Table does not exist")
        return

    count = get_table_count(conn, "normalized_financials")
    print(f"Total records: {count:,}")

    id_dupes = conn.execute("""
        SELECT id, COUNT(*) as count
        FROM normalized_financials
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()

    biz_dupes = conn.execute("""
        SELECT company_ticker, fiscal_year, fiscal_quarter, metric_id, COUNT(*) as count
        FROM normalized_financials
        GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
        HAVING COUNT(*) > 1
        ORDER BY count DESC
    """).fetchall()

    if id_dupes:
        print(f"⚠️  {len(id_dupes)} duplicate IDs!")
    else:
        print("✓ No duplicate IDs")

    if biz_dupes:
        print(f"⚠️  {len(biz_dupes)} duplicate groups")
        total_dupes = sum(cnt - 1 for _, _, _, _, cnt in biz_dupes)
        print(f"   Total duplicate records: {total_dupes}")

        print("\n   Top 10 duplicates:")
        for ticker, year, quarter, metric, cnt in biz_dupes[:10]:
            q_str = f"Q{quarter}" if quarter else "Annual"
            print(f"   {ticker} FY{year} {q_str} {metric}: {cnt}x")
    else:
        print("✓ No duplicate groups")


def quick_check_chunks(conn):
    """Quick check of chunks table."""
    print("\n" + "=" * 80)
    print("CHUNKS TABLE")
    print("=" * 80)

    if not check_table_exists(conn, "chunks"):
        print("⚠️  Table does not exist")
        return

    count = get_table_count(conn, "chunks")

    if count == 0:
        print("ℹ️  Table is empty")
        return

    print(f"Total records: {count:,}")

    id_dupes = conn.execute("""
        SELECT chunk_id, COUNT(*) as count
        FROM chunks
        GROUP BY chunk_id
        HAVING COUNT(*) > 1
    """).fetchall()

    if id_dupes:
        print(f"⚠️  {len(id_dupes)} duplicate chunk IDs!")
    else:
        print("✓ No duplicate chunk IDs")


def run_quick_mode():
    """Run quick duplicate check mode."""
    print("=" * 80)
    print("FINLOOM DATABASE DUPLICATE CHECK (QUICK MODE)")
    print("=" * 80)

    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.duckdb"

    if not db_path.exists():
        print(f"\n❌ Database not found: {db_path}")
        return 1

    print(f"\nDatabase: {db_path}")
    print(f"Size: {db_path.stat().st_size / (1024 * 1024):.2f} MB")

    conn = duckdb.connect(str(db_path), read_only=True)

    quick_check_companies(conn)
    quick_check_filings(conn)
    quick_check_facts(conn)
    quick_check_sections(conn)
    quick_check_chunks(conn)
    quick_check_normalized_financials(conn)

    print("\n" + "=" * 80)
    print("QUICK CHECK COMPLETE")
    print("=" * 80)
    print()

    conn.close()
    return 0


# ============================================================================
# FULL MODE - Comprehensive check using Database class
# ============================================================================

def full_check_companies(db):
    """Comprehensive check of companies table."""
    from src.utils.logger import get_logger
    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("\n" + "=" * 80)
    logger.info("Checking COMPANIES table for duplicates...")
    logger.info("=" * 80)

    cik_dupes = db.connection.execute("""
        SELECT cik, COUNT(*) as count
        FROM companies
        GROUP BY cik
        HAVING COUNT(*) > 1
    """).fetchall()

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
        "details": {"ciks": cik_dupes, "tickers": ticker_dupes},
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


def full_check_filings(db):
    """Comprehensive check of filings table."""
    from src.utils.logger import get_logger
    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("\n" + "=" * 80)
    logger.info("Checking FILINGS table for duplicates...")
    logger.info("=" * 80)

    accession_dupes = db.connection.execute("""
        SELECT accession_number, COUNT(*) as count
        FROM filings
        GROUP BY accession_number
        HAVING COUNT(*) > 1
    """).fetchall()

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
        "details": {"accessions": accession_dupes, "business": business_dupes},
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
        for cik, form, date, count, accessions in business_dupes[:10]:
            logger.warning(f"   CIK {cik}, {form}, {date}: {count} filings")
            logger.warning(f"      Accessions: {accessions}")
    else:
        logger.info("✓ No business duplicate filings found")

    return stats


def full_check_facts(db):
    """Comprehensive check of facts table."""
    from src.utils.logger import get_logger
    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("\n" + "=" * 80)
    logger.info("Checking FACTS table for duplicates...")
    logger.info("=" * 80)

    id_dupes = db.connection.execute("""
        SELECT id, COUNT(*) as count
        FROM facts
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()

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
        "details": {"ids": id_dupes, "business": business_dupes},
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


def full_check_sections(db):
    """Comprehensive check of sections table."""
    from src.utils.logger import get_logger
    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("\n" + "=" * 80)
    logger.info("Checking SECTIONS table for duplicates...")
    logger.info("=" * 80)

    id_dupes = db.connection.execute("""
        SELECT id, COUNT(*) as count
        FROM sections
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()

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
        "details": {"ids": id_dupes, "business": business_dupes},
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


def full_check_normalized_financials(db):
    """Comprehensive check of normalized_financials table using built-in method."""
    from src.utils.logger import get_logger
    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("\n" + "=" * 80)
    logger.info("Checking NORMALIZED_FINANCIALS table for duplicates...")
    logger.info("=" * 80)

    duplicates = db.detect_duplicates("normalized_financials")

    stats = {
        "table": "normalized_financials",
        "duplicate_groups": len(duplicates),
        "total_duplicate_records": sum(d["count"] - 1 for d in duplicates),
        "details": duplicates,
    }

    if duplicates:
        logger.warning(f"⚠️  Found {len(duplicates)} duplicate groups in normalized_financials")
        logger.warning(f"   Total duplicate records: {stats['total_duplicate_records']}")
        logger.warning("\n   Top 10 duplicates by count:")

        for i, dup in enumerate(duplicates[:10], 1):
            logger.warning(f"   {i}. {dup['ticker']} FY{dup['year']} Q{dup['quarter'] or 'N/A'} {dup['metric']}: {dup['count']}x")
            for record in dup["records"]:
                keeper_mark = " [KEEP]" if record["keep"] else " [DELETE]"
                logger.warning(
                    f"      ID {record['id']}: confidence={record['confidence']:.2f}, "
                    f"value={record['value']}, created={record['created_at']}{keeper_mark}"
                )
    else:
        logger.info("✓ No duplicate normalized financials found")

    return stats


def full_check_chunks(db):
    """Comprehensive check of chunks table."""
    from src.utils.logger import get_logger
    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("\n" + "=" * 80)
    logger.info("Checking CHUNKS table for duplicates...")
    logger.info("=" * 80)

    try:
        count = db.connection.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        if count == 0:
            logger.info("ℹ️  CHUNKS table is empty (no data to check)")
            return {"table": "chunks", "no_data": True}
    except Exception as e:
        logger.info(f"ℹ️  CHUNKS table not accessible: {e}")
        return {"table": "chunks", "error": str(e)}

    id_dupes = db.connection.execute("""
        SELECT chunk_id, COUNT(*) as count
        FROM chunks
        GROUP BY chunk_id
        HAVING COUNT(*) > 1
    """).fetchall()

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
        "business_duplicates": len(business_dupes),
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


def run_full_mode():
    """Run comprehensive duplicate check mode."""
    from src.storage.database import Database
    from src.utils.logger import get_logger

    logger = get_logger("finloom.scripts.check_duplicates")

    logger.info("=" * 80)
    logger.info("FinLoom Duplicate Data Check (FULL MODE)")
    logger.info("=" * 80)

    db = Database()

    logger.info("\nDatabase Statistics:")
    tables = ["companies", "filings", "facts", "sections", "chunks", "normalized_financials"]
    for table in tables:
        try:
            count = db.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"  {table}: {count:,} records")
        except Exception as e:
            logger.info(f"  {table}: Error - {e}")

    all_stats = {}
    all_stats["companies"] = full_check_companies(db)
    all_stats["filings"] = full_check_filings(db)
    all_stats["facts"] = full_check_facts(db)
    all_stats["sections"] = full_check_sections(db)
    all_stats["chunks"] = full_check_chunks(db)
    all_stats["normalized_financials"] = full_check_normalized_financials(db)

    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)

    has_issues = False

    for table, stats in all_stats.items():
        if stats.get("no_data"):
            logger.info(f"✓ {table.upper()}: No data")
            continue
        if stats.get("error"):
            logger.info(f"⚠️  {table.upper()}: {stats['error']}")
            continue

        issues = []

        if stats.get("duplicate_ciks", 0) > 0:
            issues.append(f"{stats['duplicate_ciks']} duplicate CIKs")
            has_issues = True
        if stats.get("duplicate_tickers", 0) > 0:
            issues.append(f"{stats['duplicate_tickers']} duplicate tickers")
            has_issues = True
        if stats.get("duplicate_accessions", 0) > 0:
            issues.append(f"{stats['duplicate_accessions']} duplicate accessions")
            has_issues = True
        if stats.get("business_duplicates", 0) > 0:
            issues.append(f"{stats['business_duplicates']} business duplicates")
            has_issues = True
        if stats.get("duplicate_ids", 0) > 0:
            issues.append(f"{stats['duplicate_ids']} duplicate IDs")
            has_issues = True
        if stats.get("duplicate_groups", 0) > 0:
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


# ============================================================================
# REPORT MODE - Detailed analysis with cleanup recommendations
# ============================================================================

def analyze_filing_duplicates(conn):
    """Analyze filing duplicates in detail."""
    print("\n" + "=" * 80)
    print("DETAILED ANALYSIS: FILING DUPLICATES")
    print("=" * 80)

    dupes = conn.execute("""
        WITH filing_groups AS (
            SELECT 
                cik, 
                form_type, 
                filing_date,
                COUNT(*) as count,
                STRING_AGG(accession_number, ', ' ORDER BY accession_number) as accession_numbers,
                STRING_AGG(download_status, ', ' ORDER BY accession_number) as statuses
            FROM filings
            GROUP BY cik, form_type, filing_date
            HAVING COUNT(*) > 1
        )
        SELECT 
            fg.*,
            c.ticker,
            c.company_name
        FROM filing_groups fg
        JOIN companies c ON fg.cik = c.cik
        ORDER BY fg.count DESC, c.ticker
    """).fetchall()

    if not dupes:
        print("✓ No filing duplicates found")
        return

    print(f"\nFound {len(dupes)} groups of duplicate filings:\n")

    for cik, form, date, count, accessions, statuses, ticker, name in dupes:
        print(f"{ticker} ({name})")
        print(f"  Form: {form}, Date: {date}, Duplicates: {count}")
        print(f"  Accessions: {accessions}")
        print(f"  Statuses: {statuses}")
        print()

    print("\nRECOMMENDATION:")
    print("These appear to be duplicate imports. Each filing should only appear once.")
    print("Review the accession numbers - keep the most recent or complete one.")
    print("This likely happened during data migration or reprocessing.")


def analyze_fact_duplicates(conn):
    """Analyze fact duplicates in detail."""
    print("\n" + "=" * 80)
    print("DETAILED ANALYSIS: FACT DUPLICATES")
    print("=" * 80)

    total_groups = conn.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT 
                accession_number, 
                concept_name, 
                period_end,
                COALESCE(dimensions::VARCHAR, 'NULL') as dims
            FROM facts
            GROUP BY accession_number, concept_name, period_end, 
                     COALESCE(dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
        ) dupes
    """).fetchone()[0]

    total_dupes = conn.execute("""
        SELECT SUM(count - 1) as total_dupes
        FROM (
            SELECT COUNT(*) as count
            FROM facts
            GROUP BY accession_number, concept_name, period_end, 
                     COALESCE(dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
        ) dupes
    """).fetchone()[0]

    print(f"\nTotal duplicate groups: {total_groups:,}")
    print(f"Total duplicate records (to remove): {total_dupes or 0:,}")

    if total_dupes and total_dupes > 0:
        print("\nTop 10 most duplicated facts:")
        samples = conn.execute("""
            SELECT 
                f.accession_number,
                fil.form_type,
                c.ticker,
                f.concept_name,
                f.period_end,
                COUNT(*) as count,
                STRING_AGG(DISTINCT f.value::VARCHAR, ', ') as values
            FROM facts f
            JOIN filings fil ON f.accession_number = fil.accession_number
            JOIN companies c ON fil.cik = c.cik
            GROUP BY f.accession_number, fil.form_type, c.ticker, 
                     f.concept_name, f.period_end, 
                     COALESCE(f.dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 10
        """).fetchall()

        for acc, form, ticker, concept, period, cnt, values in samples:
            print(f"\n  {ticker} {form} - {acc[:20]}...")
            print(f"    Concept: {concept}")
            print(f"    Period: {period}")
            print(f"    Count: {cnt}x")
            print(f"    Values: {values}")

    print("\n\nRECOMMENDATION:")
    print("1. These duplicates likely occurred during XBRL reprocessing")
    print("2. Create a cleanup script that keeps the FIRST inserted record")
    print("3. Use the fact ID (lowest) as the keeper for each duplicate group")
    if total_dupes and total_dupes > 0:
        print(f"4. This will free up ~{total_dupes * 0.001:.1f}MB of space")


def generate_cleanup_script(conn):
    """Generate SQL cleanup scripts."""
    print("\n" + "=" * 80)
    print("CLEANUP SCRIPTS")
    print("=" * 80)

    print("\n1. FILING DUPLICATES CLEANUP:")
    print("-" * 60)
    print("""
-- Step 1: Identify which filings to keep (keep the first accession number)
CREATE TEMP TABLE filings_to_keep AS
SELECT MIN(accession_number) as keeper_accession
FROM filings
GROUP BY cik, form_type, filing_date;

-- Step 2: Delete orphaned facts for filings we'll remove
DELETE FROM facts
WHERE accession_number NOT IN (SELECT keeper_accession FROM filings_to_keep);

-- Step 3: Delete orphaned sections
DELETE FROM sections
WHERE accession_number NOT IN (SELECT keeper_accession FROM filings_to_keep);

-- Step 4: Delete duplicate filings
DELETE FROM filings
WHERE accession_number NOT IN (SELECT keeper_accession FROM filings_to_keep);

-- Step 5: Cleanup
DROP TABLE filings_to_keep;
    """)

    print("\n2. FACT DUPLICATES CLEANUP:")
    print("-" * 60)
    print("""
-- Create a CTE to identify keepers (lowest ID per group)
WITH duplicate_groups AS (
    SELECT 
        accession_number,
        concept_name,
        period_end,
        COALESCE(dimensions::VARCHAR, 'NULL') as dims,
        MIN(id) as keeper_id
    FROM facts
    GROUP BY 
        accession_number, 
        concept_name, 
        period_end, 
        COALESCE(dimensions::VARCHAR, 'NULL')
    HAVING COUNT(*) > 1
),
facts_to_delete AS (
    SELECT f.id
    FROM facts f
    JOIN duplicate_groups dg 
        ON f.accession_number = dg.accession_number
        AND f.concept_name = dg.concept_name
        AND f.period_end = dg.period_end
        AND COALESCE(f.dimensions::VARCHAR, 'NULL') = dg.dims
    WHERE f.id != dg.keeper_id
)
DELETE FROM facts
WHERE id IN (SELECT id FROM facts_to_delete);
    """)

    print("\n⚠️  IMPORTANT:")
    print("  - Back up the database before running cleanup scripts")
    print("  - Test on a copy first")
    print("  - Run in a transaction and verify results before committing")


def run_report_mode():
    """Run detailed analysis and report mode."""
    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.duckdb"

    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return 1

    print("=" * 80)
    print("FINLOOM DUPLICATE ANALYSIS REPORT")
    print("=" * 80)
    print(f"\nDatabase: {db_path}")
    print(f"Size: {db_path.stat().st_size / (1024 * 1024):.2f} MB")

    conn = duckdb.connect(str(db_path), read_only=True)

    analyze_filing_duplicates(conn)
    analyze_fact_duplicates(conn)
    generate_cleanup_script(conn)

    print("\n" + "=" * 80)
    print("REPORT COMPLETE")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Review the duplicate groups above")
    print("2. Create a backup: cp data/database/finloom.duckdb data/database/finloom_pre_cleanup.duckdb")
    print("3. Run the cleanup scripts in a DuckDB session")
    print("4. Verify the results")
    print("5. Add UNIQUE constraints to prevent future duplicates")
    print()

    conn.close()
    return 0


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Unified duplicate detection and analysis tool for FinLoom database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode quick    # Fast duplicate check
  %(prog)s --mode full     # Comprehensive check with validation
  %(prog)s --mode report   # Detailed analysis with cleanup SQL
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["quick", "full", "report"],
        required=True,
        help="Check mode: quick (fast), full (comprehensive), report (detailed analysis)",
    )

    args = parser.parse_args()

    if args.mode == "quick":
        return run_quick_mode()
    elif args.mode == "full":
        return run_full_mode()
    elif args.mode == "report":
        return run_report_mode()
    else:
        print(f"Unknown mode: {args.mode}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
