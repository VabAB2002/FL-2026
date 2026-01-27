#!/usr/bin/env python3
"""
Detailed duplicate analysis and cleanup recommendations.
"""

import sys
from pathlib import Path
import duckdb


def analyze_filing_duplicates(conn):
    """Analyze filing duplicates in detail."""
    print("\n" + "="*80)
    print("DETAILED ANALYSIS: FILING DUPLICATES")
    print("="*80)
    
    # Get all filing duplicates with details
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
    print("\n" + "="*80)
    print("DETAILED ANALYSIS: FACT DUPLICATES")
    print("="*80)
    
    # Get count of duplicate groups
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
    
    # Get total duplicate records
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
    print(f"Total duplicate records (to remove): {total_dupes:,}")
    
    # Sample some duplicates
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
    
    # Check if values are the same or different
    print("\n\nChecking value consistency:")
    diff_values = conn.execute("""
        SELECT COUNT(DISTINCT value) > 1 as has_diff
        FROM (
            SELECT 
                accession_number,
                concept_name,
                period_end,
                COALESCE(dimensions::VARCHAR, 'NULL') as dims,
                value,
                COUNT(*) as count
            FROM facts
            GROUP BY accession_number, concept_name, period_end, 
                     COALESCE(dimensions::VARCHAR, 'NULL'), value
            HAVING COUNT(*) > 1
            LIMIT 100
        ) sample
    """).fetchone()[0]
    
    if diff_values:
        print("⚠️  WARNING: Some duplicate facts have DIFFERENT values!")
        print("   This indicates a data quality issue - need careful review.")
    else:
        print("✓ Duplicate facts appear to have the same values")
        print("  Safe to remove duplicates keeping just one record per group")
    
    print("\n\nRECOMMENDATION:")
    print("1. These duplicates likely occurred during XBRL reprocessing")
    print("2. Create a cleanup script that keeps the FIRST inserted record")
    print("3. Use the fact ID (lowest) as the keeper for each duplicate group")
    print(f"4. This will free up ~{total_dupes * 0.001:.1f}MB of space")


def generate_cleanup_script(conn):
    """Generate SQL cleanup scripts."""
    print("\n" + "="*80)
    print("CLEANUP SCRIPTS")
    print("="*80)
    
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


def main():
    """Main function."""
    db_path = Path(__file__).parent.parent / "data" / "database" / "finloom.duckdb"
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return 1
    
    print("="*80)
    print("FINLOOM DUPLICATE ANALYSIS REPORT")
    print("="*80)
    print(f"\nDatabase: {db_path}")
    print(f"Size: {db_path.stat().st_size / (1024*1024):.2f} MB")
    
    conn = duckdb.connect(str(db_path), read_only=True)
    
    # Run detailed analyses
    analyze_filing_duplicates(conn)
    analyze_fact_duplicates(conn)
    generate_cleanup_script(conn)
    
    print("\n" + "="*80)
    print("REPORT COMPLETE")
    print("="*80)
    print("\nNext steps:")
    print("1. Review the duplicate groups above")
    print("2. Create a backup: cp data/database/finloom.duckdb data/database/finloom_pre_cleanup.duckdb")
    print("3. Run the cleanup scripts in a DuckDB session")
    print("4. Verify the results")
    print("5. Add UNIQUE constraints to prevent future duplicates")
    print()
    
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
