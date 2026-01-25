#!/usr/bin/env python3
"""
Reprocess filings that have 0 sections due to TOC matching bug.
Uses the fixed section parser that picks best match instead of first match.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import duckdb
from src.parsers.section_parser import SectionParser
from src.utils.logger import get_logger

logger = get_logger("finloom.reprocess")


def get_filings_without_sections(db_path: str) -> list[tuple[str, str]]:
    """Get filings that are marked processed but have no sections."""
    conn = duckdb.connect(db_path, read_only=True)
    filings = conn.execute("""
        SELECT f.accession_number, f.local_path
        FROM filings f
        LEFT JOIN sections s ON f.accession_number = s.accession_number
        WHERE f.sections_processed = TRUE
        GROUP BY f.accession_number, f.local_path
        HAVING COUNT(s.id) = 0
    """).fetchall()
    conn.close()
    return filings


def insert_sections(db_path: str, accession_number: str, sections: list) -> int:
    """Insert extracted sections into database."""
    if not sections:
        return 0

    conn = duckdb.connect(db_path)
    inserted = 0

    # Get current max ID
    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM sections").fetchone()[0]
    next_id = max_id + 1

    for section in sections:
        data = section.to_dict()
        conn.execute("""
            INSERT INTO sections (
                id, accession_number, section_type, section_title, section_number,
                content_text, content_html, word_count, character_count,
                paragraph_count, extraction_confidence, extraction_method,
                section_part, contains_tables, contains_lists, contains_footnotes,
                cross_references, heading_hierarchy, extraction_quality, extraction_issues
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            next_id,
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
        next_id += 1
        inserted += 1

    conn.close()
    return inserted


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Reprocess filings with missing sections")
    parser.add_argument("--db", default="data/database/finloom.duckdb", help="Database path")
    parser.add_argument("--dry-run", action="store_true", help="Don't insert, just test")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of filings to process")
    args = parser.parse_args()

    db_path = str(project_root / args.db)

    # Get filings to process
    filings = get_filings_without_sections(db_path)
    print(f"Found {len(filings)} filings with 0 sections")

    if args.limit > 0:
        filings = filings[:args.limit]
        print(f"Processing first {args.limit} filings")

    # Initialize parser
    section_parser = SectionParser(priority_only=True, preserve_html=False)

    # Process each filing
    success_count = 0
    total_sections = 0

    for i, (acc_num, local_path) in enumerate(filings, 1):
        if not local_path:
            print(f"[{i}/{len(filings)}] {acc_num}: No local path, skipping")
            continue

        path = Path(local_path)
        if not path.exists():
            print(f"[{i}/{len(filings)}] {acc_num}: Path not found, skipping")
            continue

        # Parse sections
        result = section_parser.parse_filing(path, acc_num)

        if result.success and result.sections:
            if args.dry_run:
                print(f"[{i}/{len(filings)}] {acc_num}: Would insert {len(result.sections)} sections")
            else:
                inserted = insert_sections(db_path, acc_num, result.sections)
                print(f"[{i}/{len(filings)}] {acc_num}: Inserted {inserted} sections")

            success_count += 1
            total_sections += len(result.sections)
        else:
            error = result.error_message or "No sections found"
            print(f"[{i}/{len(filings)}] {acc_num}: Failed - {error}")

    print(f"\n{'='*50}")
    print(f"Completed: {success_count}/{len(filings)} filings")
    print(f"Total sections: {total_sections}")
    if args.dry_run:
        print("(Dry run - no data was inserted)")


if __name__ == "__main__":
    main()
