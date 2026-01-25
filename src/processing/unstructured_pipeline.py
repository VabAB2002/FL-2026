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

from ..parsers.section_parser import SectionParser, InlineXBRLSectionParser
from ..parsers.table_parser import TableParser
from ..parsers.footnote_parser import FootnoteParser
from ..processing.chunker import SemanticChunker
from ..utils.logger import get_logger
from ..monitoring.circuit_breaker import CircuitBreaker
from ..monitoring import (
    unstructured_sections_extracted,
    unstructured_tables_extracted,
    unstructured_footnotes_extracted,
    unstructured_chunks_created,
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
        use_xbrl_parser: bool = True,
        priority_sections_only: bool = False,
    ):
        """
        Initialize pipeline.
        
        Args:
            db_path: Path to DuckDB database
            use_xbrl_parser: Use XBRL-aware section parser if True
            priority_sections_only: Extract only priority sections if True
        """
        self.db_path = db_path
        
        # Initialize parsers
        if use_xbrl_parser:
            self.section_parser = InlineXBRLSectionParser(
                priority_only=priority_sections_only,
                preserve_html=True  # CRITICAL: needed for table extraction
            )
        else:
            self.section_parser = SectionParser(
                priority_only=priority_sections_only,
                preserve_html=True  # CRITICAL: needed for table extraction
            )
        
        self.table_parser = TableParser()
        self.footnote_parser = FootnoteParser()
        self.chunker = SemanticChunker()
        
        # Circuit breaker for fault tolerance
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=Exception,
        )
        
        logger.info("Unstructured data pipeline initialized")
    
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
            
            # 1. Extract sections
            logger.debug(f"Extracting sections for {accession_number}")
            section_result = self.section_parser.parse_filing(filing_path, accession_number)
            
            if not section_result.success:
                unstructured_extraction_errors.labels(
                    type="section",
                    accession=accession_number
                ).inc()
                
                return ProcessingResult(
                    success=False,
                    accession_number=accession_number,
                    error_message=section_result.error_message
                )
            
            sections = section_result.sections
            unstructured_sections_extracted.labels(
                accession=accession_number
            ).inc(len(sections))
            
            # 2. Extract tables from sections
            logger.debug(f"Extracting tables for {accession_number}")
            tables = []
            for section in sections:
                if hasattr(section, 'content_html') and section.content_html:
                    section_tables = self.table_parser.extract_from_section_html(
                        section.content_html,
                        section.section_type,
                        start_index=len(tables)
                    )
                    tables.extend(section_tables)
            
            unstructured_tables_extracted.labels(
                accession=accession_number
            ).inc(len(tables))
            
            # 3. Extract footnotes
            logger.debug(f"Extracting footnotes for {accession_number}")
            footnotes = self.footnote_parser.extract_footnotes(
                sections, tables, accession_number
            )
            
            unstructured_footnotes_extracted.labels(
                accession=accession_number
            ).inc(len(footnotes))
            
            # 4. Create semantic chunks
            logger.debug(f"Creating chunks for {accession_number}")
            chunks = self.chunker.create_chunks(sections, accession_number)
            
            unstructured_chunks_created.labels(
                accession=accession_number
            ).inc(len(chunks))
            
            # 5. Store everything (transactional)
            logger.debug(f"Storing data for {accession_number}")
            self._store_all(accession_number, sections, tables, footnotes, chunks)
            
            # 6. Calculate quality score
            quality_score = self._calculate_quality(sections, tables, footnotes, chunks)
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
                f"{len(footnotes)} footnotes, {len(chunks)} chunks "
                f"in {elapsed_ms:.0f}ms"
            )
            
            return ProcessingResult(
                success=True,
                accession_number=accession_number,
                sections_count=len(sections),
                tables_count=len(tables),
                footnotes_count=len(footnotes),
                chunks_count=len(chunks),
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
    
    def _store_all(
        self,
        accession_number: str,
        sections: list,
        tables: list,
        footnotes: list,
        chunks: list,
    ) -> None:
        """Store all extracted data (transactional, idempotent)."""
        conn = None
        try:
            conn = duckdb.connect(self.db_path)

            # Begin transaction
            conn.execute("BEGIN TRANSACTION")

            # ===== IDEMPOTENT CLEANUP: Delete existing data for this filing =====
            logger.debug(f"Cleaning existing data for {accession_number}")
            conn.execute("DELETE FROM chunks WHERE accession_number = ?", [accession_number])
            conn.execute("DELETE FROM footnotes WHERE accession_number = ?", [accession_number])
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
            
            # Store footnotes
            for footnote in footnotes:
                footnote_dict = footnote.to_dict()
                
                # Insert
                columns = ', '.join(footnote_dict.keys())
                placeholders = ', '.join(['?' for _ in footnote_dict])
                sql = f"INSERT INTO footnotes ({columns}) VALUES ({placeholders})"
                conn.execute(sql, list(footnote_dict.values()))
            
            # Store chunks
            for chunk in chunks:
                chunk_dict = chunk.to_dict()
                
                # Insert
                columns = ', '.join(chunk_dict.keys())
                placeholders = ', '.join(['?' for _ in chunk_dict])
                sql = f"INSERT INTO chunks ({columns}) VALUES ({placeholders})"
                conn.execute(sql, list(chunk_dict.values()))
            
            # Update filing status
            conn.execute("""
                UPDATE filings 
                SET sections_processed = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE accession_number = ?
            """, [accession_number])
            
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
        
        # Chunk quality (15 points)
        if chunks:
            # Check chunk distribution
            level_2_chunks = [c for c in chunks if c.chunk_level == 2]
            if level_2_chunks:
                avg_chunk_size = sum(c.token_count for c in level_2_chunks) / len(level_2_chunks)
                # Score based on how close to target (750 tokens)
                if 500 <= avg_chunk_size <= 1000:
                    chunk_score = 15
                else:
                    chunk_score = 10
                score += chunk_score
        
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
