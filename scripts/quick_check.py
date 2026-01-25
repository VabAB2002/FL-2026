#!/usr/bin/env python3
"""
Quick check: Are we getting tables and HTML content?
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

db_path = "data/database/finloom.duckdb"
conn = duckdb.connect(db_path, read_only=True)

print("=" * 70)
print("QUICK EXTRACTION CHECK")
print("=" * 70)
print()

# Overall stats
overall = conn.execute("""
    SELECT 
        COUNT(DISTINCT f.accession_number) as total_filings,
        COUNT(DISTINCT s.accession_number) as filings_with_sections,
        COUNT(s.id) as total_sections,
        COUNT(CASE WHEN s.content_html IS NOT NULL AND LENGTH(s.content_html) > 0 
              THEN 1 END) as sections_with_html,
        COUNT(DISTINCT t.accession_number) as filings_with_tables,
        COUNT(t.id) as total_tables,
        COUNT(CASE WHEN t.is_financial_statement = TRUE THEN 1 END) as financial_tables
    FROM filings f
    LEFT JOIN sections s ON f.accession_number = s.accession_number
    LEFT JOIN tables t ON f.accession_number = t.accession_number
    WHERE f.download_status = 'completed'
""").fetchone()

print(f"📊 OVERALL STATUS:")
print(f"   Total filings: {overall[0]}")
print(f"   Filings with sections: {overall[1]}")
print(f"   Total sections: {overall[2]:,}")
print(f"   Sections with HTML: {overall[3]:,} ({overall[3]*100//overall[2] if overall[2] > 0 else 0}%)")
print(f"   Filings with tables: {overall[4]}")
print(f"   Total tables: {overall[5]:,}")
print(f"   Financial statement tables: {overall[6]:,}")
print()

# Check if HTML is being captured
if overall[3] == 0:
    print("❌ WARNING: NO HTML CONTENT CAPTURED!")
    print("   This means tables CANNOT be extracted.")
    print("   The bug is still present or re-processing hasn't run yet.")
    print()
elif overall[3] < overall[2] * 0.5:
    print(f"⚠️  PARTIAL: Only {overall[3]*100//overall[2]}% of sections have HTML")
    print("   Some filings processed without the fix.")
    print()
else:
    print(f"✅ GOOD: {overall[3]*100//overall[2]}% of sections have HTML")
    print()

# Check table extraction
if overall[5] == 0:
    print("❌ WARNING: NO TABLES EXTRACTED!")
    if overall[3] == 0:
        print("   Cause: No HTML content available")
    else:
        print("   Cause: Unknown - HTML exists but no tables extracted")
    print()
elif overall[5] < overall[1] * 10:
    print(f"⚠️  LOW: Only {overall[5]} tables from {overall[1]} filings")
    print(f"   Expected: ~{overall[1] * 50} tables")
    print()
else:
    print(f"✅ GOOD: {overall[5]:,} tables extracted")
    print()

# Sample of recent extractions
recent = conn.execute("""
    SELECT 
        c.ticker,
        COUNT(s.id) as sections,
        COUNT(CASE WHEN s.content_html IS NOT NULL THEN 1 END) as with_html,
        COUNT(t.id) as tables
    FROM filings f
    JOIN companies c ON f.cik = c.cik
    LEFT JOIN sections s ON f.accession_number = s.accession_number
    LEFT JOIN tables t ON f.accession_number = t.accession_number
    WHERE f.download_status = 'completed'
    GROUP BY c.ticker
    ORDER BY MAX(s.created_at) DESC
    LIMIT 10
""").fetchall()

print("📋 RECENT FILINGS (Last 10):")
print(f"   {'Ticker':<10} {'Sections':<10} {'Has HTML':<10} {'Tables':<10}")
print("   " + "-" * 50)
for row in recent:
    ticker, sections, with_html, tables = row
    html_status = "✅" if with_html > 0 else "❌"
    table_status = "✅" if tables > 0 else "❌"
    print(f"   {ticker:<10} {sections:<10} {html_status} {with_html:<9} {table_status} {tables:<9}")

print()
print("=" * 70)

# Recommendations
if overall[3] == 0:
    print("🔧 RECOMMENDATION: Run the fix!")
    print("   python scripts/reprocess_for_tables.py --parallel 10")
elif overall[3] < overall[2]:
    print("🔧 RECOMMENDATION: Complete re-processing")
    print("   python scripts/reprocess_for_tables.py --parallel 10")
else:
    print("✅ System is working correctly!")
    if overall[4] < overall[0]:
        print("   Run remaining filings:")
        print("   python scripts/extract_unstructured.py --all --parallel 10")

conn.close()
