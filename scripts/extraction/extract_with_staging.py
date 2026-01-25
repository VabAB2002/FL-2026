#!/usr/bin/env python3
"""
Industry-Grade Extraction Pipeline with Staging Tables.

This script extracts unstructured data (sections, tables, footnotes, chunks)
using a staging table architecture that prevents:
- Duplicate records
- Lock contention during parallel processing
- Partial/corrupted data on failures

Architecture:
    Workers → Staging Tables (isolated) → Coordinator → Production Tables

Usage:
    # Extract missing data (default)
    python scripts/extract_with_staging.py

    # Re-extract specific filings (idempotent)
    python scripts/extract_with_staging.py --accession 0000320193-24-000123

    # Parallel extraction
    python scripts/extract_with_staging.py --parallel 4

    # Dry run (no database changes)
    python scripts/extract_with_staging.py --dry-run
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional
import warnings

import duckdb

from src.storage.staging_manager import StagingManager
from src.storage.merge_coordinator import MergeCoordinator
from src.parsers.section_parser import SectionParser
from src.parsers.table_parser import TableParser
from src.parsers.footnote_parser import FootnoteParser
from src.processing.chunker import SemanticChunker
from src.utils.logger import get_logger

# Suppress XML parsing warnings
warnings.filterwarnings("ignore")

logger = get_logger("finloom.extract")


@dataclass
class ExtractionResult:
    """Result of extracting a single filing."""
    success: bool
    accession_number: str
    sections_count: int = 0
    tables_count: int = 0
    footnotes_count: int = 0
    chunks_count: int = 0
    error_message: Optional[str] = None


class StagingPipeline:
    """
    Extraction pipeline that writes to staging tables.

    Each instance is thread-safe and writes to isolated staging tables
    identified by run_id.
    """

    def __init__(self, db_path: str, run_id: str):
        """
        Initialize staging pipeline.

        Args:
            db_path: Path to DuckDB database
            run_id: Unique run identifier for staging tables
        """
        self.db_path = db_path
        self.run_id = run_id
        self.staging_manager = StagingManager(db_path)

        # Initialize parsers (each pipeline instance gets its own)
        self.section_parser = SectionParser(
            priority_only=False,  # Extract all sections
            preserve_html=True,   # Needed for table extraction
        )
        self.table_parser = TableParser()
        self.footnote_parser = FootnoteParser()
        self.chunker = SemanticChunker()

    def extract_filing(
        self,
        accession_number: str,
        filing_path: Path
    ) -> ExtractionResult:
        """
        Extract all unstructured data from a filing to staging tables.

        Args:
            accession_number: Filing accession number
            filing_path: Path to filing directory

        Returns:
            ExtractionResult with counts and status
        """
        try:
            # Step 1: Parse sections
            section_result = self.section_parser.parse_filing(
                filing_path, accession_number
            )

            if not section_result.success:
                return ExtractionResult(
                    success=False,
                    accession_number=accession_number,
                    error_message=section_result.error_message or "Section parsing failed"
                )

            sections = section_result.sections

            # Step 2: Extract tables from sections with HTML
            all_tables = []
            table_start_index = 0
            for section in sections:
                if section.content_html:
                    tables = self.table_parser.extract_from_section_html(
                        section_html=section.content_html,
                        section_type=section.section_type,
                        start_index=table_start_index
                    )
                    all_tables.extend(tables)
                    table_start_index += len(tables)

            # Step 3: Extract footnotes (takes sections and tables)
            all_footnotes = self.footnote_parser.extract_footnotes(
                sections=sections,
                tables=all_tables,
                accession_number=accession_number
            )

            # Step 4: Generate chunks (SKIP FOR NOW - will implement later)
            all_chunks = []  # Skip chunk generation for speed

            # Step 5: Write to staging tables
            self._write_to_staging(
                accession_number,
                sections,
                all_tables,
                all_footnotes,
                all_chunks
            )

            return ExtractionResult(
                success=True,
                accession_number=accession_number,
                sections_count=len(sections),
                tables_count=len(all_tables),
                footnotes_count=len(all_footnotes),
                chunks_count=len(all_chunks)
            )

        except Exception as e:
            logger.error(f"Extraction failed for {accession_number}: {e}")
            return ExtractionResult(
                success=False,
                accession_number=accession_number,
                error_message=str(e)
            )

    def _write_to_staging(
        self,
        accession_number: str,
        sections: list,
        tables: list,
        footnotes: list,
        chunks: list
    ) -> None:
        """Write extracted data to staging tables."""
        conn = duckdb.connect(self.db_path)

        try:
            # Get staging table names
            sections_staging = self.staging_manager.get_staging_table_name("sections", self.run_id)
            tables_staging = self.staging_manager.get_staging_table_name("tables", self.run_id)
            footnotes_staging = self.staging_manager.get_staging_table_name("footnotes", self.run_id)
            chunks_staging = self.staging_manager.get_staging_table_name("chunks", self.run_id)

            # Get next IDs
            max_section_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM sections").fetchone()[0]
            max_table_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tables").fetchone()[0]

            # Write sections
            section_id = max_section_id + 1
            for section in sections:
                data = section.to_dict()
                conn.execute(f"""
                    INSERT INTO {sections_staging} (
                        id, accession_number, section_type, section_title, section_number,
                        content_text, content_html, word_count, character_count,
                        paragraph_count, extraction_confidence, extraction_method,
                        section_part, contains_tables, contains_lists, contains_footnotes,
                        cross_references, heading_hierarchy, extraction_quality, extraction_issues
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    section_id,
                    accession_number,
                    data['section_type'],
                    data['section_title'],
                    data['section_number'],
                    data['content_text'],
                    data['content_html'],
                    data['word_count'],
                    data['character_count'],
                    data['paragraph_count'],
                    data['extraction_confidence'],
                    data['extraction_method'],
                    data['section_part'],
                    data['contains_tables'],
                    data['contains_lists'],
                    data['contains_footnotes'],
                    data['cross_references'],
                    data['heading_hierarchy'],
                    data['extraction_quality'],
                    data['extraction_issues'],
                ])
                section_id += 1

            # Write tables
            table_id = max_table_id + 1
            for i, table in enumerate(tables):
                data = table.to_dict()
                conn.execute(f"""
                    INSERT INTO {tables_staging} (
                        id, accession_number, table_index, table_name, table_type,
                        headers, row_count, column_count, table_data, table_markdown,
                        table_caption, is_financial_statement, table_category,
                        footnote_refs, cell_metadata, extraction_quality
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    table_id,
                    accession_number,
                    i,
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
                table_id += 1

            # Write footnotes
            for footnote in footnotes:
                if hasattr(footnote, 'to_dict'):
                    data = footnote.to_dict()
                    data['accession_number'] = accession_number
                    conn.execute(f"""
                        INSERT INTO {footnotes_staging} (
                            footnote_id, accession_number, section_id, table_id,
                            marker, footnote_text, footnote_type, ref_links
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        data.get('footnote_id'),
                        accession_number,
                        data.get('section_id'),
                        data.get('table_id'),
                        data.get('marker', ''),
                        data.get('footnote_text', ''),
                        data.get('footnote_type', 'inline'),
                        data.get('ref_links'),
                    ])

            # Write chunks
            for i, chunk in enumerate(chunks):
                if hasattr(chunk, 'to_dict'):
                    data = chunk.to_dict()
                    conn.execute(f"""
                        INSERT INTO {chunks_staging} (
                            chunk_id, accession_number, section_id, parent_chunk_id,
                            chunk_level, chunk_index, chunk_text, chunk_markdown,
                            token_count, char_start, char_end, heading, section_type,
                            contains_tables, contains_lists, contains_numbers
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        data.get('chunk_id'),
                        data.get('accession_number', accession_number),
                        data.get('section_id'),
                        data.get('parent_chunk_id'),
                        data.get('chunk_level', 2),
                        data.get('chunk_index', i),
                        data.get('chunk_text', ''),
                        data.get('chunk_markdown'),
                        data.get('token_count', 0),
                        data.get('char_start', 0),
                        data.get('char_end', 0),
                        data.get('heading'),
                        data.get('section_type'),
                        data.get('contains_tables', False),
                        data.get('contains_lists', False),
                        data.get('contains_numbers', False),
                    ])

        finally:
            conn.close()


def get_filings_to_process(db_path: str, mode: str = "missing") -> list[tuple[str, str]]:
    """
    Get list of filings to process based on mode.

    Args:
        db_path: Path to database
        mode: "missing" (no sections), "no_tables", or "all"

    Returns:
        List of (accession_number, local_path) tuples
    """
    conn = duckdb.connect(db_path, read_only=True)

    try:
        if mode == "missing":
            # Filings that have no sections at all
            query = """
                SELECT f.accession_number, f.local_path
                FROM filings f
                LEFT JOIN sections s ON f.accession_number = s.accession_number
                WHERE f.download_status = 'completed'
                AND f.local_path IS NOT NULL
                GROUP BY f.accession_number, f.local_path
                HAVING COUNT(s.id) = 0
            """
        elif mode == "no_tables":
            # Filings that have sections but no tables
            query = """
                SELECT f.accession_number, f.local_path
                FROM filings f
                INNER JOIN sections s ON f.accession_number = s.accession_number
                LEFT JOIN tables t ON f.accession_number = t.accession_number
                WHERE f.download_status = 'completed'
                AND f.local_path IS NOT NULL
                GROUP BY f.accession_number, f.local_path
                HAVING COUNT(DISTINCT s.id) > 0 AND COUNT(t.id) = 0
            """
        else:  # all
            query = """
                SELECT accession_number, local_path
                FROM filings
                WHERE download_status = 'completed'
                AND local_path IS NOT NULL
            """

        return conn.execute(query).fetchall()

    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Extract unstructured data using staging tables (idempotent)"
    )
    parser.add_argument(
        "--db",
        default="data/database/finloom.duckdb",
        help="Path to database"
    )
    parser.add_argument(
        "--mode",
        choices=["missing", "no_tables", "all"],
        default="no_tables",
        help="Which filings to process"
    )
    parser.add_argument(
        "--accession",
        help="Process specific accession number(s), comma-separated"
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel workers (default: 1 for safety)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract but don't merge to production"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of filings to process"
    )

    args = parser.parse_args()

    db_path = str(project_root / args.db)

    # Initialize staging
    staging_manager = StagingManager(db_path)
    run_id = staging_manager.generate_run_id()

    print(f"{'='*60}")
    print(f"EXTRACTION WITH STAGING - Run ID: {run_id}")
    print(f"{'='*60}")

    # Get filings to process
    if args.accession:
        # Specific accessions
        conn = duckdb.connect(db_path, read_only=True)
        filings = []
        for acc in args.accession.split(","):
            result = conn.execute(
                "SELECT accession_number, local_path FROM filings WHERE accession_number = ?",
                [acc.strip()]
            ).fetchone()
            if result:
                filings.append(result)
        conn.close()
    else:
        filings = get_filings_to_process(db_path, args.mode)

    if args.limit > 0:
        filings = filings[:args.limit]

    print(f"Filings to process: {len(filings)}")
    print(f"Mode: {args.mode}")
    print(f"Parallel workers: {args.parallel}")
    print(f"Dry run: {args.dry_run}")
    print()

    if not filings:
        print("No filings to process!")
        return

    # Create staging tables
    print("Creating staging tables...")
    staging_manager.create_staging_tables(run_id)

    # Extract to staging
    print(f"\nExtracting {len(filings)} filings to staging...")

    results = []
    if args.parallel > 1:
        # Parallel extraction
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {}
            for acc_num, local_path in filings:
                pipeline = StagingPipeline(db_path, run_id)
                future = executor.submit(
                    pipeline.extract_filing,
                    acc_num,
                    Path(local_path)
                )
                futures[future] = acc_num

            for i, future in enumerate(as_completed(futures), 1):
                acc_num = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    status = "✅" if result.success else "❌"
                    print(f"[{i}/{len(filings)}] {status} {acc_num}: "
                          f"{result.sections_count}s, {result.tables_count}t, {result.chunks_count}c")
                except Exception as e:
                    print(f"[{i}/{len(filings)}] ❌ {acc_num}: {e}")
    else:
        # Sequential extraction
        pipeline = StagingPipeline(db_path, run_id)
        for i, (acc_num, local_path) in enumerate(filings, 1):
            result = pipeline.extract_filing(acc_num, Path(local_path))
            results.append(result)
            status = "✅" if result.success else "❌"
            print(f"[{i}/{len(filings)}] {status} {acc_num}: "
                  f"{result.sections_count}s, {result.tables_count}t, {result.chunks_count}c")

    # Summary
    success_count = sum(1 for r in results if r.success)
    total_sections = sum(r.sections_count for r in results)
    total_tables = sum(r.tables_count for r in results)
    total_chunks = sum(r.chunks_count for r in results)

    print(f"\n{'='*60}")
    print(f"EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"Successful: {success_count}/{len(filings)}")
    print(f"Sections: {total_sections}")
    print(f"Tables: {total_tables}")
    print(f"Chunks: {total_chunks}")

    # Merge to production (unless dry run)
    if args.dry_run:
        print(f"\n⚠️  DRY RUN - Data remains in staging tables")
        print(f"   Staging tables: sections_staging_{run_id}, etc.")
        print(f"   To merge: Use MergeCoordinator or re-run without --dry-run")
    else:
        print(f"\nMerging to production...")
        coordinator = MergeCoordinator(db_path)
        merge_results = coordinator.merge_all_from_run(run_id)

        merge_success = sum(1 for r in merge_results if r.success)
        print(f"Merged: {merge_success}/{len(merge_results)} filings")

        # Cleanup staging
        print("Cleaning up staging tables...")
        staging_manager.drop_staging_tables(run_id)

    print(f"\n✅ Done!")


if __name__ == "__main__":
    main()
