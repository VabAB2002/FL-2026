#!/usr/bin/env python3
"""
Validate normalized financial data quality.
Checks for duplicates, orphaned records, and business rule violations.

Usage:
    python scripts/validate_normalized.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database


def validate_data(db: Database) -> dict:
    """Run all validation checks."""
    
    results = {
        "total_records": 0,
        "duplicates": 0,
        "orphaned_tickers": 0,
        "missing_source": 0,
        "low_confidence": 0,
        "validation_errors": []
    }
    
    print("\n" + "=" * 80)
    print("  DATA QUALITY VALIDATION REPORT")
    print("=" * 80 + "\n")
    
    # Check 1: Total records
    total = db.connection.execute("SELECT COUNT(*) FROM normalized_financials").fetchone()[0]
    results["total_records"] = total
    print(f"✓ Total Records: {total:,}")
    
    # Check 2: Duplicates
    dups = db.connection.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT company_ticker, fiscal_year, fiscal_quarter, metric_id, COUNT(*) as cnt
            FROM normalized_financials
            GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    results["duplicates"] = dups
    
    if dups > 0:
        print(f"✗ Duplicates Found: {dups} groups")
        results["validation_errors"].append(f"Found {dups} duplicate metric groups")
    else:
        print(f"✓ No Duplicates: Data is clean")
    
    # Check 3: Orphaned tickers (no company record)
    orphaned = db.connection.execute("""
        SELECT COUNT(DISTINCT n.company_ticker)
        FROM normalized_financials n
        LEFT JOIN companies c ON n.company_ticker = c.ticker
        WHERE c.ticker IS NULL
    """).fetchone()[0]
    results["orphaned_tickers"] = orphaned
    
    if orphaned > 0:
        print(f"✗ Orphaned Tickers: {orphaned}")
        results["validation_errors"].append(f"Found {orphaned} orphaned tickers")
    else:
        print(f"✓ All Tickers Valid: Every ticker has company record")
    
    # Check 4: Missing source accession
    missing_source = db.connection.execute("""
        SELECT COUNT(*) FROM normalized_financials 
        WHERE source_accession IS NULL
    """).fetchone()[0]
    results["missing_source"] = missing_source
    
    print(f"  Records without source: {missing_source:,} ({missing_source/total*100:.1f}%)")
    
    # Check 5: Low confidence scores
    low_conf = db.connection.execute("""
        SELECT COUNT(*) FROM normalized_financials 
        WHERE confidence_score < 0.9
    """).fetchone()[0]
    results["low_confidence"] = low_conf
    
    print(f"  Low confidence (<0.9): {low_conf:,} ({low_conf/total*100:.1f}%)")
    
    # Check 6: Data completeness per company
    print(f"\n  Metrics per Company:")
    completeness = db.connection.execute("""
        SELECT 
            company_ticker,
            COUNT(DISTINCT fiscal_year) as years,
            COUNT(DISTINCT metric_id) as metrics,
            COUNT(*) as total
        FROM normalized_financials
        GROUP BY company_ticker
        ORDER BY company_ticker
    """).fetchall()
    
    for ticker, years, metrics, total_metrics in completeness:
        expected = years * 41  # 41 standard metrics
        coverage = (metrics / 41) * 100
        print(f"    {ticker:6} {years:2} years, {metrics:2}/41 metrics ({coverage:.0f}% coverage), {total_metrics:4} records")
    
    # Check 7: Expected vs actual record counts
    print(f"\n  Expected Records per Company (assuming ~38 metrics/year):")
    for ticker, years, metrics, total_metrics in completeness:
        expected_records = years * 38  # Average metrics per year
        ratio = total_metrics / expected_records if expected_records > 0 else 0
        status = "✓" if 0.9 <= ratio <= 1.1 else "⚠"
        print(f"    {status} {ticker:6} Expected: ~{expected_records:3}, Actual: {total_metrics:4} ({ratio:.2f}x)")
    
    print("\n" + "=" * 80)
    
    if len(results["validation_errors"]) == 0:
        print("✓ DATA QUALITY: EXCELLENT - No issues found")
    else:
        print(f"✗ DATA QUALITY: NEEDS ATTENTION - {len(results['validation_errors'])} issues")
        for error in results["validation_errors"]:
            print(f"  - {error}")
    
    print("=" * 80 + "\n")
    
    return results


def main():
    db = Database()
    
    try:
        results = validate_data(db)
        
        # Return non-zero exit code if validation errors
        if results["validation_errors"]:
            sys.exit(1)
    except Exception as e:
        print(f"\nError during validation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
