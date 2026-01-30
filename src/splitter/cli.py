#!/usr/bin/env python3
"""
Chunk SEC 10-K filings into semantic chunks for RAG.

Reads pre-extracted sections from DuckDB and produces one JSON
file per filing in data/chunks/.

Usage:
    python -m src.splitter              # All filings
    python -m src.splitter --pilot      # 20 filings
    python -m src.splitter --limit 50   # Custom limit
    python -m src.splitter --ticker AAPL # Single company
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from src.splitter import ChunkConfig, FilingChunks, SemanticChunker
from src.storage.database import Database
from src.infrastructure.config import get_project_root
from src.infrastructure.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("finloom.splitter.cli")

_MIN_SECTION_WORDS = 10


def get_filings_with_sections(db: Database, ticker: str | None = None) -> list[dict]:
    """Query distinct filings that have sections in the database."""
    ticker_filter = f"AND c.ticker = '{ticker}'" if ticker else ""
    sql = f"""
        SELECT DISTINCT
            fs.accession_number,
            c.ticker,
            c.company_name,
            f.filing_date,
            f.form_type
        FROM filing_sections fs
        JOIN filings f ON fs.accession_number = f.accession_number
        JOIN companies c ON f.cik = c.cik
        WHERE f.form_type IN ('10-K', '10-K/A')
        {ticker_filter}
        ORDER BY c.ticker, f.filing_date
    """
    df = db.execute_query(sql)
    return df.to_dict("records")


def get_sections(db: Database, accession_number: str) -> list[dict]:
    """Get all sections for a filing, ordered by item."""
    sql = """
        SELECT item, item_title, markdown, word_count
        FROM filing_sections
        WHERE accession_number = ?
        ORDER BY id
    """
    df = db.execute_query(sql, [accession_number])
    return df.to_dict("records")


def main() -> int:
    """Main entry point."""
    root = get_project_root()

    parser = argparse.ArgumentParser(description="Chunk SEC filings for RAG")
    parser.add_argument("--pilot", action="store_true", help="Process 20 filings only")
    parser.add_argument("--limit", type=int, help="Custom filing limit")
    parser.add_argument("--ticker", type=str, help="Process only one company ticker")
    parser.add_argument("--min-tokens", type=int, default=100)
    parser.add_argument("--max-tokens", type=int, default=512)
    parser.add_argument("--overlap-tokens", type=int, default=50)
    parser.add_argument("--output-dir", type=str, default=str(root / "data" / "chunks"))
    parser.add_argument("--force", action="store_true", help="Overwrite existing chunk files")
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("SEMANTIC CHUNKING PIPELINE")
    logger.info("=" * 70)

    config = ChunkConfig(
        min_tokens=args.min_tokens,
        max_tokens=args.max_tokens,
        overlap_tokens=args.overlap_tokens,
    )
    chunker = SemanticChunker(config)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Config: min={config.min_tokens}, max={config.max_tokens}, overlap={config.overlap_tokens}")
    logger.info(f"Output: {output_dir}")
    logger.info("")

    db_path = root / "data" / "database" / "finloom.dev.duckdb"
    db = Database(db_path=str(db_path), read_only=True)

    filings = get_filings_with_sections(db, ticker=args.ticker)

    if args.pilot:
        filings = filings[:20]
        logger.info("PILOT MODE: Processing 20 filings")
    elif args.limit:
        filings = filings[: args.limit]
        logger.info(f"Processing {len(filings)} filings (limit={args.limit})")
    else:
        logger.info(f"Processing all {len(filings)} filings")

    if not filings:
        logger.error("No filings found with sections in the database")
        db.close()
        return 1

    total_chunks = 0
    total_tokens = 0
    total_tables = 0
    skipped = 0

    for i, filing in enumerate(filings, 1):
        accession = filing["accession_number"]
        ticker = filing["ticker"]
        filing_date = str(filing["filing_date"])

        out_file = output_dir / f"{accession}.json"
        if out_file.exists() and not args.force:
            skipped += 1
            continue

        sections = get_sections(db, accession)

        all_chunks = []
        for section in sections:
            markdown = section["markdown"]
            word_count = section["word_count"] or 0

            if word_count < _MIN_SECTION_WORDS or not markdown or not markdown.strip():
                continue

            item = section["item"]
            title = section["item_title"]
            prefix = f"Company: {ticker} | Filing: 10-K {filing_date} | Section: {item}"

            chunks = chunker.chunk_section(
                markdown=markdown,
                accession_number=accession,
                section_item=item,
                section_title=title,
                context_prefix=prefix,
            )
            all_chunks.extend(chunks)

        if not all_chunks:
            continue

        filing_tokens = sum(c.token_count for c in all_chunks)
        table_chunks = sum(1 for c in all_chunks if c.contains_table)

        result = FilingChunks(
            accession_number=accession,
            ticker=ticker,
            company_name=filing["company_name"],
            filing_date=filing_date,
            form_type=filing["form_type"],
            total_chunks=len(all_chunks),
            total_tokens=filing_tokens,
            chunks=all_chunks,
        )

        with open(out_file, "w") as f:
            json.dump(result.model_dump(), f, indent=2)

        total_chunks += len(all_chunks)
        total_tokens += filing_tokens
        total_tables += table_chunks

        if i % 25 == 0:
            logger.info(f"Progress: {i}/{len(filings)} filings ({total_chunks:,} chunks so far)")

    db.close()

    logger.info("")
    logger.info("=" * 70)
    logger.info("CHUNKING COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Filings processed: {len(filings) - skipped}")
    logger.info(f"  Filings skipped (already exist): {skipped}")
    logger.info(f"  Total chunks: {total_chunks:,}")
    logger.info(f"  Total tokens: {total_tokens:,}")
    logger.info(f"  Chunks with tables: {total_tables:,}")
    if total_chunks > 0:
        logger.info(f"  Avg tokens/chunk: {total_tokens // total_chunks}")
    logger.info(f"  Output: {output_dir}")
    logger.info("")

    return 0


if __name__ == "__main__":
    sys.exit(main())
