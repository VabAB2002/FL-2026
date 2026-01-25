#!/usr/bin/env python3
"""
Simple Table Extraction Script.

Extracts tables from the WHOLE document ONCE (not per-section).
This avoids the duplication issue where each section had the entire HTML.

Flow:
1. Find the main HTML file
2. Extract all tables from it (once)
3. Store in staging tables
4. Merge to production (idempotent)
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import duckdb

from src.parsers.table_parser import TableParser
from src.storage.staging_manager import StagingManager
from src.storage.merge_coordinator import MergeCoordinator
from src.utils.logger import get_logger

warnings.filterwarnings("ignore")
logger = get_logger("finloom.extract_tables")


def find_main_html(filing_path: Path) -> Optional[Path]:
    """Find the main HTML document in a filing directory."""
    if filing_path.is_file():
        return filing_path

    # Look for common patterns
    patterns = ["*10-k*.htm", "*10k*.htm", "*annual*.htm", "*.htm"]

    for pattern in patterns:
        files = list(filing_path.glob(pattern))
        # Filter out exhibits
        files = [f for f in files if "ex" not in f.name.lower()[:3]]
        if files:
            # Return largest (usually main document)
            return max(files, key=lambda x: x.stat().st_size)

    return None


def extract_tables_from_filing(
    accession_number: str,
    filing_path: Path,
    table_parser: TableParser
) -> tuple[bool, int, str]:
    """
    Extract tables from a single filing.

    Returns: (success, table_count, message)
    """
    try:
        # Find main HTML
        html_file = find_main_html(filing_path)
        if not html_file:
            return False, 0, "No HTML file found"

        file_size_mb = html_file.stat().st_size / (1024 * 1024)

        # Extract tables (pass file path - method reads it)
        tables = table_parser.extract_tables(html_file)

        return True, len(tables), f"{file_size_mb:.1f}MB, {len(tables)} tables"

    except Exception as e:
        return False, 0, str(e)


def extract_and_store(
    accession_number: str,
    filing_path: Path,
    db_path: str,
    run_id: str,
    table_parser: TableParser
) -> tuple[bool, int, str]:
    """Extract tables and store in staging."""
    try:
        # Find main HTML
        html_file = find_main_html(Path(filing_path))
        if not html_file:
            return False, 0, "No HTML file found"

        # Extract tables (pass file path - method reads it)
        tables = table_parser.extract_tables(html_file)

        if not tables:
            return True, 0, "No tables found"

        # Store in staging
        conn = duckdb.connect(db_path)
        staging_table = f"tables_staging_{run_id}"

        # Get next ID
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tables").fetchone()[0]
        next_id = max_id + 1

        for table in tables:
            data = table.to_dict()
            conn.execute(f"""
                INSERT INTO {staging_table} (
                    id, accession_number, table_index, table_name, table_type,
                    headers, row_count, column_count, table_data, table_markdown,
                    table_caption, is_financial_statement, table_category,
                    footnote_refs, cell_metadata, extraction_quality
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                next_id,
                accession_number,
                data.get('table_index', 0),
                data.get('table_name'),
                data.get('table_type', 'other'),
                data.get('headers'),
                data.get('row_count', 0),
                data.get('column_count', 0),
                data.get('table_data'),
                data.get('table_markdown'),
                data.get('table_caption'),
                data.get('is_financial_statement', False),
                data.get('table_category'),
                data.get('footnote_refs'),
                data.get('cell_metadata'),
                data.get('extraction_quality', 1.0),
            ])
            next_id += 1

        conn.close()
        return True, len(tables), f"{len(tables)} tables"

    except Exception as e:
        return False, 0, str(e)


def get_filings_without_tables(db_path: str) -> list[tuple[str, str]]:
    """Get filings that have sections but no tables."""
    conn = duckdb.connect(db_path, read_only=True)

    filings = conn.execute("""
        SELECT f.accession_number, f.local_path
        FROM filings f
        INNER JOIN sections s ON f.accession_number = s.accession_number
        LEFT JOIN tables t ON f.accession_number = t.accession_number
        WHERE f.download_status = 'completed'
        AND f.local_path IS NOT NULL
        GROUP BY f.accession_number, f.local_path
        HAVING COUNT(DISTINCT s.id) > 0 AND COUNT(t.id) = 0
    """).fetchall()

    conn.close()
    return filings


def merge_tables_to_production(db_path: str, run_id: str) -> int:
    """Merge staging tables to production (idempotent)."""
    conn = duckdb.connect(db_path)
    staging_table = f"tables_staging_{run_id}"

    # Get accession numbers in staging
    accessions = conn.execute(f"""
        SELECT DISTINCT accession_number FROM {staging_table}
    """).fetchall()

    merged = 0
    for (acc_num,) in accessions:
        conn.execute("BEGIN TRANSACTION")
        try:
            # Delete existing tables for this filing (idempotent)
            conn.execute("DELETE FROM tables WHERE accession_number = ?", [acc_num])

            # Insert from staging
            conn.execute(f"""
                INSERT INTO tables
                SELECT * FROM {staging_table} WHERE accession_number = ?
            """, [acc_num])

            conn.execute("COMMIT")
            merged += 1
        except Exception as e:
            conn.execute("ROLLBACK")
            print(f"  ❌ Failed to merge {acc_num}: {e}")

    conn.close()
    return merged


def main():
    parser = argparse.ArgumentParser(description="Extract tables from filings")
    parser.add_argument("--db", default="data/database/finloom.duckdb")
    parser.add_argument("--parallel", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = str(project_root / args.db)

    # Get filings to process
    filings = get_filings_without_tables(db_path)
    if args.limit > 0:
        filings = filings[:args.limit]

    print("=" * 60)
    print("TABLE EXTRACTION (Whole Document Method)")
    print("=" * 60)
    print(f"Filings to process: {len(filings)}")
    print(f"Parallel workers: {args.parallel}")
    print(f"Dry run: {args.dry_run}")
    print()

    if not filings:
        print("No filings need table extraction!")
        return

    # Initialize
    staging_manager = StagingManager(db_path)
    run_id = staging_manager.generate_run_id()
    table_parser = TableParser()

    if not args.dry_run:
        # Create staging table for tables only
        conn = duckdb.connect(db_path)
        conn.execute(f"CREATE TABLE IF NOT EXISTS tables_staging_{run_id} AS SELECT * FROM tables WHERE 1=0")
        conn.close()
        print(f"Created staging table: tables_staging_{run_id}")

    # Process filings
    print(f"\nExtracting tables...")
    start_time = time.time()

    results = []

    if args.parallel > 1:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {}
            for acc_num, local_path in filings:
                if args.dry_run:
                    future = executor.submit(
                        extract_tables_from_filing,
                        acc_num, Path(local_path), table_parser
                    )
                else:
                    future = executor.submit(
                        extract_and_store,
                        acc_num, Path(local_path), db_path, run_id, TableParser()
                    )
                futures[future] = acc_num

            for i, future in enumerate(as_completed(futures), 1):
                acc_num = futures[future]
                try:
                    success, count, msg = future.result()
                    results.append((success, count))
                    status = "✅" if success else "❌"
                    print(f"[{i}/{len(filings)}] {status} {acc_num}: {msg}")
                except Exception as e:
                    results.append((False, 0))
                    print(f"[{i}/{len(filings)}] ❌ {acc_num}: {e}")
    else:
        # Sequential
        for i, (acc_num, local_path) in enumerate(filings, 1):
            if args.dry_run:
                success, count, msg = extract_tables_from_filing(
                    acc_num, Path(local_path), table_parser
                )
            else:
                success, count, msg = extract_and_store(
                    acc_num, Path(local_path), db_path, run_id, TableParser()
                )
            results.append((success, count))
            status = "✅" if success else "❌"
            print(f"[{i}/{len(filings)}] {status} {acc_num}: {msg}")

    elapsed = time.time() - start_time

    # Summary
    success_count = sum(1 for r in results if r[0])
    total_tables = sum(r[1] for r in results)

    print()
    print("=" * 60)
    print("EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Successful: {success_count}/{len(filings)}")
    print(f"Total tables: {total_tables:,}")
    print(f"Time: {elapsed:.1f} seconds ({elapsed/len(filings):.1f}s per filing)")

    if args.dry_run:
        print("\n⚠️  DRY RUN - No data stored")
    else:
        # Merge to production
        print(f"\nMerging to production...")
        merged = merge_tables_to_production(db_path, run_id)
        print(f"Merged: {merged} filings")

        # Cleanup staging
        conn = duckdb.connect(db_path)
        conn.execute(f"DROP TABLE IF EXISTS tables_staging_{run_id}")
        conn.close()
        print("Cleaned up staging table")

    print("\n✅ Done!")


if __name__ == "__main__":
    main()
