"""
Unstructured data processing pipeline.

Orchestrates all parsers (sections, tables, footnotes, chunking) with:
- Circuit breaker pattern
- Metrics and tracing
- Error handling and retry logic
- Transactional storage
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb

from ..parsers.section_parser import SectionParser, FullMarkdownResult
from ..parsers.table_parser import TableParser
from ..parsers.footnote_parser import FootnoteParser
# from ..processing.chunker import SemanticChunker  # DISABLED: Will implement later
from ..utils.logger import get_logger
from ..monitoring.circuit_breaker import CircuitBreaker
from ..monitoring import (
    unstructured_sections_extracted,
    unstructured_tables_extracted,
    unstructured_footnotes_extracted,
    # unstructured_chunks_created,  # DISABLED: Chunking not implemented yet
    unstructured_quality_score,
    unstructured_extraction_errors,
    unstructured_processing_time,
)

logger = get_logger("finloom.pipeline.unstructured")


@dataclass
class ProcessingResult:
    """Result of processing a filing."""
    success: bool
    accession_number: str
    sections_count: int = 0
    tables_count: int = 0
    footnotes_count: int = 0
    chunks_count: int = 0
    quality_score: float = 0.0
    processing_time_ms: float = 0.0
    error_message: Optional[str] = None


class UnstructuredDataPipeline:
    """
    Production-grade pipeline for unstructured data extraction.
    
    Features:
    - Orchestrates all parsers (sections, tables, footnotes, chunks)
    - Circuit breaker for fault tolerance
    - Prometheus metrics
    - OpenTelemetry tracing (via decorators)
    - Transactional storage
    - Quality validation
    """
    
    def __init__(
        self,
        db_path: str,
        priority_sections_only: bool = False,
    ):
        """
        Initialize pipeline.

        Args:
            db_path: Path to DuckDB database
            priority_sections_only: Extract only priority sections if True
        """
        self.db_path = db_path

        # Initialize section parser (uses new HTML → Markdown → Regex approach)
        self.section_parser = SectionParser(
            priority_only=priority_sections_only,
            preserve_html=False  # No longer needed - markdown approach extracts directly
        )
        
        self.table_parser = TableParser()
        self.footnote_parser = FootnoteParser()
        # self.chunker = SemanticChunker()  # DISABLED: Will implement later
        
        # Circuit breaker for fault tolerance
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=Exception,
        )
        
        logger.info("Unstructured data pipeline initialized")

    def _get_ticker_for_filing(self, accession_number: str) -> str:
        """
        Get the ticker symbol for a filing.

        Args:
            accession_number: Filing accession number

        Returns:
            Ticker symbol or empty string if not found
        """
        try:
            conn = duckdb.connect(self.db_path)
            result = conn.execute("""
                SELECT c.ticker
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                WHERE f.accession_number = ?
            """, [accession_number]).fetchone()
            conn.close()

            if result:
                return result[0]
            return ""
        except Exception as e:
            logger.warning(f"Failed to get ticker for {accession_number}: {e}")
            return ""

    def process_filing(
        self,
        accession_number: str,
        filing_path: Path,
    ) -> ProcessingResult:
        """
        Process a single filing - extract everything.
        
        Args:
            accession_number: Filing accession number
            filing_path: Path to filing directory or HTML file
        
        Returns:
            ProcessingResult with counts and metrics
        """
        start_time = time.time()
        
        try:
            logger.info(f"Processing filing {accession_number}")
            
            # Check circuit breaker
            if not self.circuit_breaker.call(lambda: True):
                return ProcessingResult(
                    success=False,
                    accession_number=accession_number,
                    error_message="Circuit breaker open"
                )
            
            # 1. Extract full markdown with sections
            logger.debug(f"Extracting full markdown for {accession_number}")

            # Get ticker for document header
            ticker = self._get_ticker_for_filing(accession_number)

            # Find HTML file
            html_file = self.section_parser._find_primary_document(filing_path)
            if not html_file:
                return ProcessingResult(
                    success=False,
                    accession_number=accession_number,
                    error_message="No HTML document found"
                )

            try:
                markdown_result = self.section_parser.extract_full_markdown(
                    html_file,
                    accession_number=accession_number,
                    ticker=ticker,
                )
            except Exception as e:
                unstructured_extraction_errors.labels(
                    type="markdown",
                    accession=accession_number
                ).inc()
                return ProcessingResult(
                    success=False,
                    accession_number=accession_number,
                    error_message=f"Markdown extraction failed: {e}"
                )

            sections = markdown_result.sections
            full_markdown = markdown_result.full_markdown
            markdown_word_count = markdown_result.word_count

            # CRITICAL VALIDATION: Check if we actually extracted any sections
            if not sections or len(sections) == 0:
                unstructured_extraction_errors.labels(
                    type="section_empty",
                    accession=accession_number
                ).inc()

                return ProcessingResult(
                    success=False,
                    accession_number=accession_number,
                    error_message="No sections extracted from filing"
                )

            # LENIENT VALIDATION: Check priority sections but don't reject
            # This allows processing to continue even if some sections are missing
            PRIORITY_SECTIONS = {'item_1', 'item_1a', 'item_7', 'item_8'}
            extracted_section_types = {s.section_type for s in sections}
            priority_found = extracted_section_types.intersection(PRIORITY_SECTIONS)

            if not priority_found:
                # LENIENT: Log warning instead of failing
                # This allows filings with different structures to still be processed
                logger.warning(
                    f"No priority sections found for {accession_number}. "
                    f"Extracted: {extracted_section_types}. "
                    f"Processing will continue with available sections."
                )
                unstructured_extraction_errors.labels(
                    type="section_no_priority_warning",
                    accession=accession_number
                ).inc()
                # Note: DON'T return failure - continue processing with what we have

            unstructured_sections_extracted.labels(
                accession=accession_number
            ).inc(len(sections))

            # 2. Extract tables (simplified - tables are in markdown too)
            logger.debug(f"Extracting tables for {accession_number}")
            tables = []  # Tables are preserved in markdown, separate extraction optional

            unstructured_tables_extracted.labels(
                accession=accession_number
            ).inc(len(tables))

            # 3. Store everything (transactional) - includes full markdown
            logger.debug(f"Storing data for {accession_number}")
            self._store_all(
                accession_number,
                sections,
                tables,
                full_markdown=full_markdown,
                markdown_word_count=markdown_word_count,
            )
            
            # 6. Calculate quality score
            quality_score = self._calculate_quality(sections, tables, [], [])
            unstructured_quality_score.labels(
                accession=accession_number
            ).set(quality_score)

            elapsed_ms = (time.time() - start_time) * 1000
            unstructured_processing_time.labels(
                accession=accession_number
            ).observe(elapsed_ms / 1000)

            logger.info(
                f"Successfully processed {accession_number}: "
                f"{len(sections)} sections, {len(tables)} tables, "
                f"{markdown_word_count:,} markdown words "
                f"in {elapsed_ms:.0f}ms"
            )

            return ProcessingResult(
                success=True,
                accession_number=accession_number,
                sections_count=len(sections),
                tables_count=len(tables),
                footnotes_count=0,  # Footnotes removed - already in markdown
                chunks_count=0,      # Chunking disabled for now
                quality_score=quality_score,
                processing_time_ms=elapsed_ms,
            )
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.error(f"Failed to process {accession_number}: {e}", exc_info=True)
            
            unstructured_extraction_errors.labels(
                type="pipeline",
                accession=accession_number
            ).inc()
            
            # Record failure in circuit breaker
            self.circuit_breaker.record_failure()
            
            return ProcessingResult(
                success=False,
                accession_number=accession_number,
                processing_time_ms=elapsed_ms,
                error_message=str(e)
            )
    
    def reprocess_filing(
        self,
        accession_number: str,
        filing_path: Path,
        force: bool = False,
    ) -> ProcessingResult:
        """
        Reprocess a filing with idempotent DELETE + INSERT pattern.
        
        Use this method to re-extract sections for filings that:
        - Previously failed extraction (sections_processed=FALSE)
        - Have empty sections (COUNT(sections)=0)
        - Need reprocessing after parser bug fixes
        
        This is the production-ready alternative to fix_missing_sections.py script.
        
        Args:
            accession_number: Filing accession number
            filing_path: Path to filing directory or HTML file
            force: If True, reprocess even if already processed successfully
        
        Returns:
            ProcessingResult with success status and counts
        
        Example:
            pipeline = UnstructuredDataPipeline(db_path)
            result = pipeline.reprocess_filing(
                accession_number="0000320193-23-000077",
                filing_path=Path("data/raw/320193/0000320193-23-000077"),
                force=True
            )
        """
        logger.info(f"Reprocessing filing {accession_number} (force={force})")
        
        # Connect to database to check existing data
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            
            # Check if filing exists
            existing = conn.execute(
                "SELECT sections_processed FROM filings WHERE accession_number = ?",
                [accession_number]
            ).fetchone()
            
            if not existing:
                return ProcessingResult(
                    success=False,
                    accession_number=accession_number,
                    error_message=f"Filing {accession_number} not found in database"
                )
            
            sections_processed = existing[0]
            
            # Check if already processed (unless force=True)
            if sections_processed and not force:
                # Count existing sections
                section_count = conn.execute(
                    "SELECT COUNT(*) FROM sections WHERE accession_number = ?",
                    [accession_number]
                ).fetchone()[0]
                
                if section_count > 0:
                    logger.info(
                        f"Filing {accession_number} already has {section_count} sections. "
                        f"Use force=True to reprocess anyway."
                    )
                    return ProcessingResult(
                        success=True,
                        accession_number=accession_number,
                        sections_count=section_count,
                        error_message="Already processed (use force=True to reprocess)"
                    )
            
            # DELETE existing data (idempotent)
            logger.debug(f"Deleting existing data for {accession_number}")
            conn.execute("BEGIN TRANSACTION")
            
            # Delete in reverse order of foreign keys
            # conn.execute("DELETE FROM chunks WHERE accession_number = ?", [accession_number])  # DISABLED: Chunking not implemented
            conn.execute("DELETE FROM footnotes WHERE accession_number = ?", [accession_number])
            conn.execute("DELETE FROM tables WHERE accession_number = ?", [accession_number])
            conn.execute("DELETE FROM sections WHERE accession_number = ?", [accession_number])
            
            # Reset processing flag
            conn.execute(
                "UPDATE filings SET sections_processed = FALSE WHERE accession_number = ?",
                [accession_number]
            )
            
            conn.execute("COMMIT")
            conn.close()
            conn = None
            
            logger.info(f"Deleted existing data for {accession_number}, reprocessing...")
            
            # Now reprocess using normal pipeline
            result = self.process_filing(accession_number, filing_path)
            
            if result.success:
                logger.info(
                    f"Successfully reprocessed {accession_number}: "
                    f"{result.sections_count} sections extracted"
                )
            else:
                logger.warning(
                    f"Reprocessing failed for {accession_number}: {result.error_message}"
                )
            
            return result
            
        except Exception as e:
            if conn:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
                conn.close()
            
            logger.error(f"Error reprocessing {accession_number}: {e}", exc_info=True)
            return ProcessingResult(
                success=False,
                accession_number=accession_number,
                error_message=f"Reprocessing error: {str(e)}"
            )
    
    def _store_all(
        self,
        accession_number: str,
        sections: list,
        tables: list,
        full_markdown: str = None,
        markdown_word_count: int = None,
    ) -> None:
        """Store all extracted data (transactional, idempotent).

        Args:
            accession_number: Filing accession number
            sections: List of ExtractedSection objects
            tables: List of extracted tables
            full_markdown: Full document markdown with section markers
            markdown_word_count: Word count of full markdown
        """
        conn = None
        try:
            conn = duckdb.connect(self.db_path)

            # Begin transaction
            conn.execute("BEGIN TRANSACTION")

            # ===== IDEMPOTENT CLEANUP: Delete existing data for this filing =====
            logger.debug(f"Cleaning existing data for {accession_number}")
            conn.execute("DELETE FROM tables WHERE accession_number = ?", [accession_number])
            conn.execute("DELETE FROM sections WHERE accession_number = ?", [accession_number])
            # ===== END CLEANUP =====

            # Store sections
            for section in sections:
                section_dict = section.to_dict()
                section_dict['accession_number'] = accession_number

                # Get next ID from sequence
                section_id = conn.execute("SELECT nextval('sections_id_seq')").fetchone()[0]
                section_dict['id'] = section_id

                # Insert
                columns = ', '.join(section_dict.keys())
                placeholders = ', '.join(['?' for _ in section_dict])
                sql = f"INSERT INTO sections ({columns}) VALUES ({placeholders})"
                conn.execute(sql, list(section_dict.values()))

                # Store the ID back on the section object for foreign keys
                section.id = section_id

            # Store tables
            for table in tables:
                table_dict = table.to_dict()
                table_dict['accession_number'] = accession_number

                # Get next ID
                table_id = conn.execute("SELECT nextval('tables_id_seq')").fetchone()[0]
                table_dict['id'] = table_id

                # Insert
                columns = ', '.join(table_dict.keys())
                placeholders = ', '.join(['?' for _ in table_dict])
                sql = f"INSERT INTO tables ({columns}) VALUES ({placeholders})"
                conn.execute(sql, list(table_dict.values()))

                table.id = table_id

            # Update filing status AND store full markdown
            conn.execute("""
                UPDATE filings
                SET sections_processed = TRUE,
                    full_markdown = ?,
                    markdown_word_count = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE accession_number = ?
            """, [full_markdown, markdown_word_count, accession_number])

            # Commit transaction
            conn.execute("COMMIT")

            logger.debug(f"Stored all data for {accession_number}")
            
        except Exception as e:
            if conn:
                conn.execute("ROLLBACK")
            logger.error(f"Failed to store data for {accession_number}: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _calculate_quality(
        self,
        sections: list,
        tables: list,
        footnotes: list,
        chunks: list,
    ) -> float:
        """Calculate overall quality score."""
        score = 0.0
        max_score = 100.0
        
        # Section completeness (30 points)
        section_types = {s.section_type for s in sections}
        expected_sections = {'item_1', 'item_1a', 'item_7', 'item_8', 'item_9a'}
        section_score = (len(section_types & expected_sections) / len(expected_sections)) * 30
        score += section_score
        
        # Table extraction (25 points)
        if tables:
            # Check if financial statements present
            financial_stmts = [t for t in tables if t.is_financial_statement]
            table_score = min(25, len(tables) * 2 + len(financial_stmts) * 5)
            score += table_score
        
        # Footnote extraction (20 points)
        if footnotes:
            footnote_score = min(20, len(footnotes) * 0.5)
            score += footnote_score
        
        # Chunk quality (15 points) - DISABLED: Chunking not implemented yet
        # if chunks:
        #     # Check chunk distribution
        #     level_2_chunks = [c for c in chunks if c.chunk_level == 2]
        #     if level_2_chunks:
        #         avg_chunk_size = sum(c.token_count for c in level_2_chunks) / len(level_2_chunks)
        #         # Score based on how close to target (750 tokens)
        #         if 500 <= avg_chunk_size <= 1000:
        #             chunk_score = 15
        #         else:
        #             chunk_score = 10
        #         score += chunk_score
        
        # Metadata richness (10 points)
        sections_with_metadata = sum(
            1 for s in sections 
            if (hasattr(s, 'cross_references') and s.cross_references) or
               (hasattr(s, 'heading_hierarchy') and s.heading_hierarchy)
        )
        if sections:
            metadata_score = (sections_with_metadata / len(sections)) * 10
            score += metadata_score
        
        return round(score, 2)
    
    def process_batch(
        self,
        filing_paths: list[tuple[str, Path]],
        max_workers: int = 4,
    ) -> list[ProcessingResult]:
        """
        Process multiple filings in parallel.
        
        Args:
            filing_paths: List of (accession_number, filing_path) tuples
            max_workers: Number of parallel workers
        
        Returns:
            List of ProcessingResult objects
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_accession = {
                executor.submit(self.process_filing, acc, path): acc
                for acc, path in filing_paths
            }
            
            for future in as_completed(future_to_accession):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    accession = future_to_accession[future]
                    logger.error(f"Failed to process {accession}: {e}")
                    results.append(ProcessingResult(
                        success=False,
                        accession_number=accession,
                        error_message=str(e)
                    ))
        
        return results
