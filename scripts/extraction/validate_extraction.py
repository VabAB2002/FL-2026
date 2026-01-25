#!/usr/bin/env python3
"""
Validate extraction completeness - check if we're getting all sections and tables.

This script analyzes a filing before and after extraction to verify completeness.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import duckdb
from bs4 import BeautifulSoup
import re


def analyze_raw_filing(filing_path: Path) -> dict:
    """Analyze what's available in the raw filing."""
    
    # Find the main HTML file
    html_files = list(filing_path.glob("*.htm")) + list(filing_path.glob("*.html"))
    
    if not html_files:
        return {"error": "No HTML files found"}
    
    # Use the largest file (usually the main 10-K)
    main_file = max(html_files, key=lambda f: f.stat().st_size)
    
    with open(main_file, 'r', encoding='utf-8', errors='ignore') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Count available data
    all_tables = soup.find_all('table')
    
    # Filter out likely layout tables
    data_tables = []
    for table in all_tables:
        rows = table.find_all('tr')
        if len(rows) >= 2:  # At least header + 1 data row
            # Check if it has actual data (not just navigation)
            text = table.get_text().lower()
            if any(keyword in text for keyword in ['$', 'million', 'thousand', 'assets', 'revenue', 'income', 'cash']):
                data_tables.append(table)
    
    # Find sections
    text_content = soup.get_text()
    
    sections_found = {}
    section_patterns = {
        "Item 1": r"(?i)item\s+1\.?\s*[-—–]?\s*business",
        "Item 1A": r"(?i)item\s+1a\.?\s*[-—–]?\s*risk\s+factors",
        "Item 7": r"(?i)item\s+7\.?\s*[-—–]?\s*management",
        "Item 8": r"(?i)item\s+8\.?\s*[-—–]?\s*financial\s+statements",
        "Item 9A": r"(?i)item\s+9a\.?\s*[-—–]?\s*controls",
    }
    
    for section_name, pattern in section_patterns.items():
        if re.search(pattern, text_content):
            sections_found[section_name] = True
    
    # Look for financial statements
    financial_keywords = [
        'balance sheet', 'consolidated balance', 
        'income statement', 'statement of income', 'statement of operations',
        'cash flow', 'statement of cash'
    ]
    
    financials_found = []
    for keyword in financial_keywords:
        if keyword in text_content.lower():
            financials_found.append(keyword)
    
    return {
        "file_size_mb": main_file.stat().st_size / 1024 / 1024,
        "total_tables": len(all_tables),
        "data_tables": len(data_tables),
        "sections_available": sections_found,
        "financial_statements": financials_found,
        "has_xbrl": any('.xml' in f.name for f in filing_path.glob("*")),
    }


def analyze_extracted_data(accession: str, db_path: str) -> dict:
    """Analyze what was extracted and stored in database."""
    
    conn = duckdb.connect(db_path, read_only=True)
    
    # Get sections
    sections = conn.execute("""
        SELECT 
            section_type,
            word_count,
            LENGTH(content_text) as text_length,
            LENGTH(content_html) as html_length,
            contains_tables
        FROM sections
        WHERE accession_number = ?
        ORDER BY section_type
    """, [accession]).fetchall()
    
    # Get tables
    tables = conn.execute("""
        SELECT 
            table_type,
            table_category,
            row_count,
            column_count,
            is_financial_statement
        FROM tables
        WHERE accession_number = ?
    """, [accession]).fetchall()
    
    # Get chunks
    chunks = conn.execute("""
        SELECT 
            chunk_level,
            COUNT(*) as count
        FROM chunks
        WHERE accession_number = ?
        GROUP BY chunk_level
        ORDER BY chunk_level
    """, [accession]).fetchall()
    
    # Get footnotes
    footnotes = conn.execute("""
        SELECT COUNT(*) as count
        FROM footnotes
        WHERE accession_number = ?
    """, [accession]).fetchone()
    
    conn.close()
    
    return {
        "sections": [
            {
                "type": s[0],
                "words": s[1],
                "has_text": s[2] is not None and s[2] > 0,
                "has_html": s[3] is not None and s[3] > 0,
                "tables_in_section": s[4] or 0
            }
            for s in sections
        ],
        "tables": [
            {
                "type": t[0],
                "category": t[1],
                "rows": t[2],
                "cols": t[3],
                "is_financial": t[4]
            }
            for t in tables
        ],
        "chunks": {f"level_{c[0]}": c[1] for c in chunks},
        "footnotes": footnotes[0] if footnotes else 0,
    }


def compare_and_report(raw: dict, extracted: dict, accession: str):
    """Compare raw vs extracted and report findings."""
    
    print("=" * 80)
    print(f"EXTRACTION VALIDATION REPORT: {accession}")
    print("=" * 80)
    print()
    
    # Raw filing analysis
    print("📄 RAW FILING ANALYSIS:")
    print(f"   File size: {raw.get('file_size_mb', 0):.1f} MB")
    print(f"   Total HTML tables: {raw.get('total_tables', 0)}")
    print(f"   Data tables (filtered): {raw.get('data_tables', 0)}")
    print(f"   Has XBRL: {'Yes' if raw.get('has_xbrl') else 'No'}")
    print()
    
    print("   Sections found in raw HTML:")
    for section, found in raw.get('sections_available', {}).items():
        print(f"      ✓ {section}")
    print()
    
    print("   Financial statements mentioned:")
    for statement in raw.get('financial_statements', [])[:5]:
        print(f"      • {statement}")
    print()
    
    # Extracted data analysis
    print("🗄️  EXTRACTED DATA:")
    print(f"   Sections extracted: {len(extracted.get('sections', []))}")
    
    sections_with_html = sum(1 for s in extracted.get('sections', []) if s['has_html'])
    print(f"   Sections with HTML: {sections_with_html}")
    
    print(f"   Tables extracted: {len(extracted.get('tables', []))}")
    
    financial_tables = sum(1 for t in extracted.get('tables', []) if t.get('is_financial'))
    print(f"   Financial statement tables: {financial_tables}")
    
    print(f"   Chunks created: {sum(extracted.get('chunks', {}).values())}")
    print(f"   Footnotes: {extracted.get('footnotes', 0)}")
    print()
    
    # Detailed section breakdown
    print("   Section details:")
    for section in extracted.get('sections', []):
        html_status = "✅" if section['has_html'] else "❌"
        print(f"      {section['type']}: {section['words']:,} words, "
              f"HTML: {html_status}, Tables: {section['tables_in_section']}")
    print()
    
    # Table samples
    if extracted.get('tables'):
        print("   Sample tables:")
        for i, table in enumerate(extracted.get('tables', [])[:5], 1):
            fin_mark = "💰" if table.get('is_financial') else "📊"
            print(f"      {fin_mark} {table['type']}: {table['rows']}×{table['cols']} "
                  f"({table['category'] or 'unknown'})")
        if len(extracted.get('tables', [])) > 5:
            print(f"      ... and {len(extracted['tables']) - 5} more tables")
    print()
    
    # Validation checks
    print("✓ VALIDATION CHECKS:")
    
    checks_passed = []
    checks_failed = []
    
    # Check 1: Sections extracted
    if len(extracted.get('sections', [])) >= 3:
        checks_passed.append("At least 3 sections extracted")
    else:
        checks_failed.append(f"Only {len(extracted.get('sections', []))} sections extracted")
    
    # Check 2: HTML content
    if sections_with_html > 0:
        checks_passed.append(f"HTML content preserved ({sections_with_html} sections)")
    else:
        checks_failed.append("No HTML content - tables cannot be extracted!")
    
    # Check 3: Tables extracted
    expected_tables = max(raw.get('data_tables', 0) * 0.7, 5)  # Expect at least 70% or 5 min
    if len(extracted.get('tables', [])) >= expected_tables:
        checks_passed.append(f"Table extraction working ({len(extracted['tables'])} tables)")
    else:
        checks_failed.append(f"Low table count: {len(extracted['tables'])} vs {raw.get('data_tables', 0)} available")
    
    # Check 4: Financial statements
    if financial_tables > 0:
        checks_passed.append(f"Financial statements detected ({financial_tables} tables)")
    else:
        if raw.get('financial_statements'):
            checks_failed.append("Financial statements present but not detected")
    
    # Check 5: Chunks
    if sum(extracted.get('chunks', {}).values()) > 0:
        checks_passed.append(f"Chunking working ({sum(extracted['chunks'].values())} chunks)")
    else:
        checks_failed.append("No chunks created")
    
    for check in checks_passed:
        print(f"   ✅ {check}")
    
    for check in checks_failed:
        print(f"   ❌ {check}")
    
    print()
    
    # Overall assessment
    if not checks_failed:
        print("🎉 OVERALL: EXCELLENT - All validation checks passed!")
    elif len(checks_failed) <= 1:
        print("✅ OVERALL: GOOD - Minor issues detected")
    else:
        print("⚠️  OVERALL: NEEDS ATTENTION - Multiple issues found")
    
    print()
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Validate extraction completeness for a filing"
    )
    parser.add_argument(
        "--accession",
        required=True,
        help="Accession number to validate (e.g., 0001065280-24-000046)",
    )
    parser.add_argument(
        "--db",
        default="data/database/finloom.duckdb",
        help="Path to database",
    )
    
    args = parser.parse_args()
    
    # Get filing path from database
    conn = duckdb.connect(args.db, read_only=True)
    result = conn.execute("""
        SELECT local_path
        FROM filings
        WHERE accession_number = ?
    """, [args.accession]).fetchone()
    conn.close()
    
    if not result:
        print(f"❌ Filing {args.accession} not found in database")
        return
    
    filing_path = Path(result[0])
    
    if not filing_path.exists():
        print(f"❌ Filing path not found: {filing_path}")
        return
    
    # Analyze raw filing
    print("Analyzing raw filing...")
    raw_analysis = analyze_raw_filing(filing_path)
    
    if "error" in raw_analysis:
        print(f"❌ Error: {raw_analysis['error']}")
        return
    
    # Analyze extracted data
    print("Analyzing extracted data...")
    extracted_analysis = analyze_extracted_data(args.accession, args.db)
    
    # Compare and report
    compare_and_report(raw_analysis, extracted_analysis, args.accession)


if __name__ == "__main__":
    main()
