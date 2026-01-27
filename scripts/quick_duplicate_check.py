#!/usr/bin/env python3
"""
Quick duplicate check for FinLoom database - bypasses config validation.
"""

import sys
from pathlib import Path

import duckdb


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


def check_companies(conn):
    """Check companies table for duplicates."""
    print("\n" + "="*80)
    print("COMPANIES TABLE")
    print("="*80)
    
    if not check_table_exists(conn, "companies"):
        print("⚠️  Table does not exist")
        return
    
    count = get_table_count(conn, "companies")
    print(f"Total records: {count:,}")
    
    # Check for duplicate CIKs
    cik_dupes = conn.execute("""
        SELECT cik, COUNT(*) as count
        FROM companies
        GROUP BY cik
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for duplicate tickers
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


def check_filings(conn):
    """Check filings table for duplicates."""
    print("\n" + "="*80)
    print("FILINGS TABLE")
    print("="*80)
    
    if not check_table_exists(conn, "filings"):
        print("⚠️  Table does not exist")
        return
    
    count = get_table_count(conn, "filings")
    print(f"Total records: {count:,}")
    
    # Check for duplicate accession numbers
    acc_dupes = conn.execute("""
        SELECT accession_number, COUNT(*) as count
        FROM filings
        GROUP BY accession_number
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates
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


def check_facts(conn):
    """Check facts table for duplicates."""
    print("\n" + "="*80)
    print("FACTS TABLE")
    print("="*80)
    
    if not check_table_exists(conn, "facts"):
        print("⚠️  Table does not exist")
        return
    
    count = get_table_count(conn, "facts")
    print(f"Total records: {count:,}")
    
    # Check for duplicate IDs
    id_dupes = conn.execute("""
        SELECT id, COUNT(*) as count
        FROM facts
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates
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
        
        # Get examples
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


def check_sections(conn):
    """Check sections table for duplicates."""
    print("\n" + "="*80)
    print("SECTIONS TABLE")
    print("="*80)
    
    if not check_table_exists(conn, "sections"):
        print("⚠️  Table does not exist")
        return
    
    count = get_table_count(conn, "sections")
    print(f"Total records: {count:,}")
    
    # Check for duplicate IDs
    id_dupes = conn.execute("""
        SELECT id, COUNT(*) as count
        FROM sections
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates
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


def check_normalized_financials(conn):
    """Check normalized_financials table for duplicates."""
    print("\n" + "="*80)
    print("NORMALIZED_FINANCIALS TABLE")
    print("="*80)
    
    if not check_table_exists(conn, "normalized_financials"):
        print("⚠️  Table does not exist")
        return
    
    count = get_table_count(conn, "normalized_financials")
    print(f"Total records: {count:,}")
    
    # Check for duplicate IDs
    id_dupes = conn.execute("""
        SELECT id, COUNT(*) as count
        FROM normalized_financials
        GROUP BY id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    # Check for business duplicates
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
            
            # Get details for this group
            details = conn.execute("""
                SELECT id, metric_value, confidence_score, created_at
                FROM normalized_financials
                WHERE company_ticker = ?
                  AND fiscal_year = ?
                  AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                  AND metric_id = ?
                ORDER BY confidence_score DESC, created_at DESC
            """, [ticker, year, quarter, metric]).fetchall()
            
            for i, (id, value, conf, created) in enumerate(details):
                marker = " [KEEP]" if i == 0 else " [DELETE]"
                print(f"      ID {id}: value={value}, conf={conf:.2f}, created={created}{marker}")
    else:
        print("✓ No duplicate groups")


def check_chunks(conn):
    """Check chunks table for duplicates."""
    print("\n" + "="*80)
    print("CHUNKS TABLE")
    print("="*80)
    
    if not check_table_exists(conn, "chunks"):
        print("⚠️  Table does not exist")
        return
    
    count = get_table_count(conn, "chunks")
    
    if count == 0:
        print("ℹ️  Table is empty")
        return
    
    print(f"Total records: {count:,}")
    
    # Check for duplicate chunk IDs
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


def main():
    """Main function."""
    print("="*80)
    print("FINLOOM DATABASE DUPLICATE CHECK")
    print("="*80)
    
    # Connect to database
    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.duckdb"
    
    if not db_path.exists():
        print(f"\n❌ Database not found: {db_path}")
        return 1
    
    print(f"\nDatabase: {db_path}")
    print(f"Size: {db_path.stat().st_size / (1024*1024):.2f} MB")
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # Run checks
    check_companies(conn)
    check_filings(conn)
    check_facts(conn)
    check_sections(conn)
    check_chunks(conn)
    check_normalized_financials(conn)
    
    # Summary
    print("\n" + "="*80)
    print("DUPLICATE CHECK COMPLETE")
    print("="*80)
    print()
    
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
