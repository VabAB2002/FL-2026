#!/usr/bin/env python3
"""
Test idempotency of normalization pipeline.
Running normalization multiple times should produce identical results.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database


def test_idempotency():
    """Test that running normalization twice produces same results."""
    
    db = Database()
    
    # Get count before
    count_before = db.connection.execute(
        "SELECT COUNT(*) FROM normalized_financials"
    ).fetchone()[0]
    
    print(f"Records before: {count_before}")
    
    # Run normalization again (should update, not insert)
    from scripts.normalize_all import normalize_all_filings
    stats = normalize_all_filings(db)
    
    # Get count after
    count_after = db.connection.execute(
        "SELECT COUNT(*) FROM normalized_financials"
    ).fetchone()[0]
    
    print(f"Records after: {count_after}")
    print(f"Metrics processed: {stats['metrics_created']}")
    
    # Check for duplicates
    dupes = db.connection.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT company_ticker, fiscal_year, fiscal_quarter, metric_id
            FROM normalized_financials
            GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    print(f"Duplicate groups: {dupes}")
    
    # Assert
    assert count_before == count_after, f"Count changed: {count_before} -> {count_after}"
    assert dupes == 0, f"Found {dupes} duplicate groups"
    
    print("\n✓ Idempotency test PASSED!")
    db.close()


if __name__ == "__main__":
    test_idempotency()
