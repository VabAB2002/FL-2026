#!/usr/bin/env python3
"""
Clean duplicate normalized metrics from database.
Keeps only the record with highest confidence score per (ticker, year, quarter, metric).

Usage:
    python scripts/clean_duplicates.py --dry-run    # Preview what would be deleted
    python scripts/clean_duplicates.py --execute    # Actually delete duplicates
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.database import Database
from src.utils.logger import get_logger

logger = get_logger("finloom.cleanup")


def clean_duplicates(db: Database, dry_run: bool = False) -> dict:
    """Remove duplicate normalized metrics."""
    
    logger.info("Analyzing duplicates...")
    
    # Find all duplicates
    duplicates = db.connection.execute("""
        SELECT 
            company_ticker, 
            fiscal_year, 
            fiscal_quarter, 
            metric_id,
            COUNT(*) as count
        FROM normalized_financials
        GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
        HAVING COUNT(*) > 1
    """).fetchall()
    
    logger.info(f"Found {len(duplicates)} duplicate groups")
    
    if len(duplicates) == 0:
        logger.info("No duplicates found!")
        return {"duplicates_removed": 0, "duplicate_groups": 0}
    
    total_removed = 0
    
    for ticker, year, quarter, metric, count in duplicates:
        # Keep the record with highest confidence and latest created_at
        keeper = db.connection.execute("""
            SELECT id
            FROM normalized_financials
            WHERE company_ticker = ?
              AND fiscal_year = ?
              AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
              AND metric_id = ?
            ORDER BY confidence_score DESC, created_at DESC
            LIMIT 1
        """, [ticker, year, quarter, metric]).fetchone()
        
        keeper_id = keeper[0]
        
        # Delete all others
        if not dry_run:
            db.connection.execute("""
                DELETE FROM normalized_financials
                WHERE company_ticker = ?
                  AND fiscal_year = ?
                  AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                  AND metric_id = ?
                  AND id != ?
            """, [ticker, year, quarter, metric, keeper_id])
            
            total_removed += (count - 1)
            
            logger.info(f"  Cleaned: {ticker} {year} {metric} (removed {count-1}, kept {keeper_id})")
        else:
            total_removed += (count - 1)
            logger.info(f"  Would clean: {ticker} {year} {metric} (would remove {count-1}, keep best)")
    
    logger.info(f"Total records {'would be removed' if dry_run else 'removed'}: {total_removed}")
    
    return {"duplicates_removed": total_removed, "duplicate_groups": len(duplicates)}


def main():
    parser = argparse.ArgumentParser(
        description="Clean duplicate normalized metrics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python clean_duplicates.py --dry-run    # Preview what would be deleted
  python clean_duplicates.py --execute    # Actually delete duplicates
        """
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    parser.add_argument("--execute", action="store_true", help="Actually delete duplicates (required)")
    
    args = parser.parse_args()
    
    if not args.execute and not args.dry_run:
        print("\nERROR: Must specify either --dry-run or --execute")
        print("\nUse --dry-run to see what would be deleted")
        print("Use --execute to actually delete duplicates")
        print("\nExamples:")
        print("  python scripts/clean_duplicates.py --dry-run")
        print("  python scripts/clean_duplicates.py --execute")
        sys.exit(1)
    
    db = Database()
    db.initialize_schema()
    
    try:
        print("\n" + "=" * 80)
        print("  DUPLICATE CLEANUP TOOL")
        print("=" * 80 + "\n")
        
        results = clean_duplicates(db, dry_run=args.dry_run)
        
        print("\n" + "=" * 80)
        if args.dry_run:
            print("  DRY RUN SUMMARY")
            print("=" * 80)
            print(f"\n  Duplicate groups found: {results['duplicate_groups']}")
            print(f"  Records that would be removed: {results['duplicates_removed']}")
            print("\n  No data was actually deleted")
            print("  Run with --execute to actually clean duplicates\n")
        else:
            print("  CLEANUP COMPLETE")
            print("=" * 80)
            print(f"\n  Duplicate groups cleaned: {results['duplicate_groups']}")
            print(f"  Records removed: {results['duplicates_removed']}")
            print("\n  Database has been cleaned!\n")
        print("=" * 80 + "\n")
        
    except Exception as e:
        logger.error(f"Failed to clean duplicates: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
