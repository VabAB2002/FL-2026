#!/usr/bin/env python3
"""
Data Health Check CLI.

Runs comprehensive health checks on the FinLoom database and reports issues.

Usage:
    python scripts/check_data_health.py              # Full health check
    python scripts/check_data_health.py --json       # Output as JSON
    python scripts/check_data_health.py --fix        # Auto-fix duplicates
    python scripts/check_data_health.py --watch      # Continuous monitoring
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
import json
import time
from datetime import datetime

import duckdb

from src.monitoring.health_checker import DatabaseHealthChecker
from src.utils.logger import get_logger

logger = get_logger("finloom.health_check")


def print_report(report, verbose: bool = False):
    """Print health report in human-readable format."""
    print()
    print("=" * 70)
    print(f"DATABASE HEALTH CHECK - {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Status with color indicator
    status_symbols = {
        "healthy": "✅ HEALTHY",
        "warning": "⚠️  WARNING",
        "critical": "❌ CRITICAL",
    }
    print(f"\nStatus: {status_symbols.get(report.status, report.status)}")

    # Completeness section
    print("\n" + "-" * 40)
    print("DATA COMPLETENESS")
    print("-" * 40)

    c = report.completeness
    print(f"Total filings: {c.total_filings}")
    print()

    # Coverage table
    print(f"{'Data Type':<12} {'Coverage':<12} {'Filings':<12} {'Total Records':<15}")
    print(f"{'-'*12} {'-'*12} {'-'*12} {'-'*15}")

    coverage_data = [
        ("Sections", c.sections_coverage, c.filings_with_sections, c.total_sections),
        ("Tables", c.tables_coverage, c.filings_with_tables, c.total_tables),
        ("Footnotes", c.footnotes_coverage, c.filings_with_footnotes, c.total_footnotes),
        ("Chunks", c.chunks_coverage, c.filings_with_chunks, c.total_chunks),
    ]

    for name, coverage, filings, total in coverage_data:
        status = "✅" if coverage == 100 else "⚠️ " if coverage >= 50 else "❌"
        print(f"{name:<12} {coverage:>6.1f}% {status}  {filings:>5}/{c.total_filings:<5}  {total:>12,}")

    print(f"\nDatabase size: {c.database_size_mb:.1f} MB")

    # Integrity section
    print("\n" + "-" * 40)
    print("REFERENTIAL INTEGRITY")
    print("-" * 40)

    i = report.integrity
    if i.has_issues:
        print(f"❌ Found {i.total_orphans} orphaned records:")
        if i.orphan_sections > 0:
            print(f"   - Orphan sections: {i.orphan_sections}")
        if i.orphan_tables > 0:
            print(f"   - Orphan tables: {i.orphan_tables}")
        if i.orphan_footnotes > 0:
            print(f"   - Orphan footnotes: {i.orphan_footnotes}")
        if i.orphan_chunks > 0:
            print(f"   - Orphan chunks: {i.orphan_chunks}")
    else:
        print("✅ No orphaned records found")

    # Duplicates section
    print("\n" + "-" * 40)
    print("DUPLICATE DETECTION")
    print("-" * 40)

    has_any_duplicates = False
    for table_name, dup_report in report.duplicates.items():
        if dup_report.has_duplicates:
            has_any_duplicates = True
            print(f"❌ {table_name}: {dup_report.duplicate_count} duplicate combinations")
            print(f"   Unique columns: {', '.join(dup_report.unique_columns)}")

            if verbose and dup_report.sample_duplicates:
                print("   Sample duplicates:")
                for sample in dup_report.sample_duplicates[:3]:
                    print(f"     {sample}")

    if not has_any_duplicates:
        print("✅ No duplicates found in any table")

    # Warnings and errors
    if report.warnings:
        print("\n" + "-" * 40)
        print("WARNINGS")
        print("-" * 40)
        for w in report.warnings:
            print(f"⚠️  {w}")

    if report.errors:
        print("\n" + "-" * 40)
        print("ERRORS")
        print("-" * 40)
        for e in report.errors:
            print(f"❌ {e}")

    print("\n" + "=" * 70)


def fix_duplicates(db_path: str, dry_run: bool = True) -> dict:
    """
    Fix duplicate records by keeping only the latest/best version.

    Args:
        db_path: Path to database
        dry_run: If True, only report what would be deleted

    Returns:
        Dict with counts of fixed duplicates per table
    """
    print("\n" + "=" * 70)
    print(f"{'DRY RUN - ' if dry_run else ''}FIXING DUPLICATES")
    print("=" * 70)

    conn = duckdb.connect(db_path)
    fixed = {}

    # Define deduplication strategies
    tables_config = {
        "sections": {
            "unique_cols": ["accession_number", "section_type"],
            "order_by": "word_count DESC, id DESC",  # Keep largest content
        },
        "tables": {
            "unique_cols": ["accession_number", "table_index"],
            "order_by": "id DESC",  # Keep latest
        },
        "footnotes": {
            "unique_cols": ["accession_number", "footnote_id"],
            "order_by": "id DESC",
        },
        "chunks": {
            "unique_cols": ["accession_number", "chunk_index"],
            "order_by": "id DESC",
        },
    }

    for table, config in tables_config.items():
        unique_cols = ", ".join(config["unique_cols"])
        order_by = config["order_by"]

        # Count duplicates
        dup_count = conn.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT {unique_cols}
                FROM {table}
                GROUP BY {unique_cols}
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        if dup_count == 0:
            print(f"✅ {table}: No duplicates")
            fixed[table] = 0
            continue

        # Get IDs to delete (keep best, delete rest)
        ids_to_delete = conn.execute(f"""
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY {unique_cols}
                           ORDER BY {order_by}
                       ) as rn
                FROM {table}
            )
            SELECT id FROM ranked WHERE rn > 1
        """).fetchall()

        delete_count = len(ids_to_delete)

        if dry_run:
            print(f"⚠️  {table}: Would delete {delete_count} duplicate rows")
        else:
            # Actually delete
            if delete_count > 0:
                id_list = [r[0] for r in ids_to_delete]
                placeholders = ",".join(["?" for _ in id_list])
                conn.execute(f"DELETE FROM {table} WHERE id IN ({placeholders})", id_list)
                print(f"✅ {table}: Deleted {delete_count} duplicate rows")

        fixed[table] = delete_count

    conn.close()

    total_fixed = sum(fixed.values())
    print(f"\n{'Would fix' if dry_run else 'Fixed'}: {total_fixed} total duplicate rows")

    if dry_run and total_fixed > 0:
        print("\nRun with --fix --no-dry-run to apply changes")

    return fixed


def watch_health(db_path: str, interval: int = 60):
    """
    Continuously monitor database health.

    Args:
        db_path: Path to database
        interval: Seconds between checks
    """
    print(f"Watching database health (interval: {interval}s)")
    print("Press Ctrl+C to stop\n")

    checker = DatabaseHealthChecker(db_path)
    last_status = None

    try:
        while True:
            report = checker.full_health_check()

            # Only print full report on status change or first run
            if report.status != last_status:
                print_report(report)
                last_status = report.status
            else:
                # Print one-line status
                timestamp = datetime.now().strftime("%H:%M:%S")
                c = report.completeness
                status_symbol = {"healthy": "✅", "warning": "⚠️", "critical": "❌"}.get(report.status, "?")
                print(
                    f"[{timestamp}] {status_symbol} "
                    f"Sections: {c.sections_coverage:.0f}% | "
                    f"Tables: {c.tables_coverage:.0f}% | "
                    f"DB: {c.database_size_mb:.0f}MB"
                )

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\nStopped watching.")


def main():
    parser = argparse.ArgumentParser(
        description="Check FinLoom database health",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/check_data_health.py                    # Full health check
    python scripts/check_data_health.py --json             # JSON output
    python scripts/check_data_health.py --fix --dry-run    # Preview fixes
    python scripts/check_data_health.py --fix --no-dry-run # Apply fixes
    python scripts/check_data_health.py --watch            # Continuous monitoring
        """
    )
    parser.add_argument("--db", default="data/database/finloom.duckdb",
                       help="Path to database")
    parser.add_argument("--json", action="store_true",
                       help="Output as JSON")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Verbose output with samples")
    parser.add_argument("--fix", action="store_true",
                       help="Auto-fix duplicates")
    parser.add_argument("--dry-run", action="store_true", default=True,
                       help="Preview changes without applying (default)")
    parser.add_argument("--no-dry-run", action="store_true",
                       help="Actually apply fixes")
    parser.add_argument("--watch", action="store_true",
                       help="Continuous monitoring mode")
    parser.add_argument("--interval", type=int, default=60,
                       help="Watch interval in seconds (default: 60)")
    parser.add_argument("--filing", type=str,
                       help="Check specific filing by accession number")

    args = parser.parse_args()

    db_path = str(project_root / args.db)

    # Check database exists
    if not Path(db_path).exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    checker = DatabaseHealthChecker(db_path)

    # Handle specific filing check
    if args.filing:
        result = checker.get_filing_health(args.filing)
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"\nFiling: {args.filing}")
            print(f"  Sections: {result['sections']}")
            print(f"  Tables: {result['tables']}")
            print(f"  Footnotes: {result['footnotes']}")
            print(f"  Chunks: {result['chunks']}")
            print(f"  Has duplicates: {result['has_duplicates']}")
            print(f"  Is complete: {result['is_complete']}")
        return

    # Handle watch mode
    if args.watch:
        watch_health(db_path, args.interval)
        return

    # Handle fix mode
    if args.fix:
        dry_run = not args.no_dry_run
        fix_duplicates(db_path, dry_run=dry_run)
        return

    # Default: full health check
    report = checker.full_health_check()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print_report(report, verbose=args.verbose)

    # Exit code based on status
    exit_codes = {"healthy": 0, "warning": 1, "critical": 2}
    sys.exit(exit_codes.get(report.status, 1))


if __name__ == "__main__":
    main()
