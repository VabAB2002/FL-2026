#!/usr/bin/env python3
"""
Comprehensive inspection of the fresh database content.
"""
import duckdb
from datetime import datetime

DB_PATH = '/Users/V-Personal/FinLoom-2026/data/finloom.dev.duckdb'

def print_section(title):
    """Print a formatted section header."""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def main():
    conn = duckdb.connect(DB_PATH)
    
    print_section("DATABASE OVERVIEW")
    
    # Database file size
    import os
    db_size = os.path.getsize(DB_PATH)
    print(f"Database file size: {db_size:,} bytes ({db_size/1024/1024:.2f} MB)")
    print()
    
    # Table counts
    tables = {
        'companies': 'Companies tracked',
        'filings': 'SEC filings downloaded',
        'facts': 'XBRL facts extracted',
        'sections': 'Document sections parsed',
        'chunks': 'Text chunks for RAG',
        'tables': 'Extracted tables',
        'footnotes': 'Extracted footnotes',
        'normalized_financials': 'Normalized metrics',
        'processing_logs': 'Processing log entries',
        'data_quality_issues': 'Data quality issues'
    }
    
    print("TABLE RECORD COUNTS:")
    for table, description in tables.items():
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:.<30} {count:>10,}  ({description})")
        except Exception as e:
            print(f"  {table:.<30} {'ERROR':>10}  ({e})")
    
    print_section("COMPANIES")
    companies = conn.execute("""
        SELECT ticker, company_name, cik 
        FROM companies 
        ORDER BY ticker
    """).fetchall()
    
    for ticker, name, cik in companies:
        print(f"  {ticker:6} {name[:50]:50} CIK:{cik}")
    
    print_section("FILINGS SUMMARY")
    
    # Filings by company
    filings_summary = conn.execute("""
        SELECT 
            c.ticker,
            COUNT(*) as total_filings,
            MIN(f.filing_date) as earliest,
            MAX(f.filing_date) as latest,
            SUM(CASE WHEN f.xbrl_processed THEN 1 ELSE 0 END) as xbrl_processed,
            SUM(CASE WHEN f.sections_processed THEN 1 ELSE 0 END) as sections_processed
        FROM filings f
        JOIN companies c ON f.cik = c.cik
        GROUP BY c.ticker
        ORDER BY c.ticker
    """).fetchall()
    
    print(f"{'Ticker':<10} {'Filings':<10} {'Date Range':<30} {'XBRL':<8} {'Sections':<8}")
    print("-" * 80)
    for row in filings_summary:
        ticker, total, earliest, latest, xbrl, sections = row
        date_range = f"{earliest} to {latest}" if earliest and latest else "N/A"
        print(f"{ticker:<10} {total:<10} {date_range:<30} {xbrl:<8} {sections:<8}")
    
    print_section("DETAILED FILINGS")
    
    # List all filings
    filings = conn.execute("""
        SELECT 
            c.ticker,
            f.accession_number,
            f.form_type,
            f.filing_date,
            f.period_of_report,
            f.xbrl_processed,
            f.sections_processed,
            f.download_status
        FROM filings f
        JOIN companies c ON f.cik = c.cik
        ORDER BY c.ticker, f.filing_date DESC
    """).fetchall()
    
    current_ticker = None
    for ticker, accession, form, filing_date, period, xbrl, sections, status in filings:
        if ticker != current_ticker:
            print(f"\n{ticker}:")
            current_ticker = ticker
        
        xbrl_icon = "✓" if xbrl else "✗"
        sections_icon = "✓" if sections else "✗"
        print(f"  {accession:20} {form:6} {filing_date}  XBRL:{xbrl_icon} Sections:{sections_icon}  ({status})")
    
    print_section("FACTS STATISTICS")
    
    # Facts by filing
    facts_by_filing = conn.execute("""
        SELECT 
            c.ticker,
            f.accession_number,
            f.filing_date,
            COUNT(*) as fact_count,
            COUNT(DISTINCT fa.concept_name) as unique_concepts
        FROM facts fa
        JOIN filings f ON fa.accession_number = f.accession_number
        JOIN companies c ON f.cik = c.cik
        GROUP BY c.ticker, f.accession_number, f.filing_date
        ORDER BY c.ticker, f.filing_date DESC
    """).fetchall()
    
    print(f"{'Ticker':<8} {'Filing Date':<12} {'Facts':<10} {'Unique Concepts':<18} {'Accession':<25}")
    print("-" * 80)
    for ticker, accession, filing_date, fact_count, unique_concepts in facts_by_filing:
        print(f"{ticker:<8} {str(filing_date):<12} {fact_count:<10,} {unique_concepts:<18,} {accession:<25}")
    
    print_section("TOP FINANCIAL CONCEPTS")
    
    # Most common concepts
    top_concepts = conn.execute("""
        SELECT 
            concept_name,
            COUNT(*) as occurrences,
            COUNT(DISTINCT accession_number) as filings
        FROM facts
        GROUP BY concept_name
        ORDER BY occurrences DESC
        LIMIT 20
    """).fetchall()
    
    print(f"{'Concept Name':<60} {'Count':<10} {'Filings':<10}")
    print("-" * 80)
    for concept, count, filings in top_concepts:
        print(f"{concept:<60} {count:<10,} {filings:<10}")
    
    print_section("SAMPLE FACTS - APPLE (AAPL) LATEST FILING")
    
    # Get latest Apple filing
    latest_aapl = conn.execute("""
        SELECT f.accession_number, f.filing_date
        FROM filings f
        JOIN companies c ON f.cik = c.cik
        WHERE c.ticker = 'AAPL'
        ORDER BY f.filing_date DESC
        LIMIT 1
    """).fetchone()
    
    if latest_aapl:
        accession, filing_date = latest_aapl
        print(f"Filing: {accession} (Date: {filing_date})")
        print()
        
        # Get key financial facts
        key_facts = conn.execute("""
            SELECT 
                concept_name,
                value,
                unit,
                period_type,
                period_end
            FROM facts
            WHERE accession_number = ?
              AND concept_name IN (
                  'us-gaap:Assets',
                  'us-gaap:Liabilities',
                  'us-gaap:StockholdersEquity',
                  'us-gaap:Revenues',
                  'us-gaap:NetIncomeLoss',
                  'us-gaap:EarningsPerShareBasic',
                  'us-gaap:EarningsPerShareDiluted'
              )
              AND dimensions IS NULL
            ORDER BY concept_name, period_end DESC
        """, [accession]).fetchall()
        
        print(f"{'Concept':<50} {'Value':<20} {'Unit':<10} {'Period End':<15}")
        print("-" * 100)
        for concept, value, unit, period_type, period_end in key_facts:
            concept_short = concept.replace('us-gaap:', '')
            value_str = f"{value:,.2f}" if value else "N/A"
            print(f"{concept_short:<50} {value_str:<20} {unit or 'N/A':<10} {str(period_end):<15}")
    
    print_section("SECTIONS EXTRACTED")
    
    sections_summary = conn.execute("""
        SELECT 
            c.ticker,
            f.filing_date,
            s.section_type,
            s.word_count
        FROM sections s
        JOIN filings f ON s.accession_number = f.accession_number
        JOIN companies c ON f.cik = c.cik
        ORDER BY c.ticker, f.filing_date DESC, s.section_type
    """).fetchall()
    
    if sections_summary:
        print(f"{'Ticker':<8} {'Filing Date':<12} {'Section':<15} {'Words':<10}")
        print("-" * 50)
        for ticker, filing_date, section_type, word_count in sections_summary:
            print(f"{ticker:<8} {str(filing_date):<12} {section_type:<15} {word_count or 0:<10,}")
    else:
        print("  No sections extracted yet")
    
    print_section("DATA QUALITY")
    
    # Check for duplicate facts (should be 0!)
    duplicates = conn.execute("""
        SELECT COUNT(*) 
        FROM (
            SELECT accession_number, concept_name, period_end, 
                   COALESCE(dimensions::VARCHAR, 'NULL') as dims,
                   COUNT(*) as cnt
            FROM facts
            GROUP BY accession_number, concept_name, period_end, 
                     COALESCE(dimensions::VARCHAR, 'NULL')
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    print(f"Duplicate fact groups: {duplicates}")
    if duplicates == 0:
        print("  ✅ No duplicates found - duplicate prevention is working!")
    else:
        print(f"  ⚠️  Found {duplicates} duplicate groups - this shouldn't happen!")
    
    print()
    
    # Check filing dates are correct (not all the same)
    filing_dates_check = conn.execute("""
        SELECT 
            COUNT(DISTINCT filing_date) as unique_dates,
            MIN(filing_date) as earliest,
            MAX(filing_date) as latest
        FROM filings
    """).fetchone()
    
    unique_dates, earliest, latest = filing_dates_check
    print(f"Filing date range: {earliest} to {latest}")
    print(f"Unique filing dates: {unique_dates}")
    if unique_dates > 1:
        print("  ✅ Filing dates are diverse - actual SEC dates are being used!")
    else:
        print("  ⚠️  All filing dates are the same - this might be a problem!")
    
    print_section("SUMMARY")
    
    total_filings = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    total_facts = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    total_companies = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    
    print(f"Total companies tracked: {total_companies}")
    print(f"Total filings downloaded: {total_filings}")
    print(f"Total XBRL facts extracted: {total_facts:,}")
    print(f"Average facts per filing: {total_facts/total_filings if total_filings > 0 else 0:,.0f}")
    print()
    print("Database is fresh and ready for use!")
    
    conn.close()

if __name__ == "__main__":
    main()
