"""
DuckDB database operations for FinLoom SEC Data Pipeline.

Provides a database abstraction layer for storing and querying SEC filing data.
"""

import json
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Generator, List, Optional

import duckdb
import pandas as pd

from ..utils.config import get_absolute_path, get_settings
from ..utils.logger import get_logger

logger = get_logger("finloom.storage.database")


class Database:
    """
    DuckDB database wrapper for SEC filing data.
    
    Provides methods for CRUD operations on filings, facts, sections, etc.
    Thread-safe for read operations; write operations should be serialized.
    """
    
    def __init__(self, db_path: Optional[str] = None) -> None:
        """
        Initialize database connection.
        
        Args:
            db_path: Path to DuckDB database file. If None, uses config.
        """
        settings = get_settings()
        self.db_path = get_absolute_path(
            db_path or settings.storage.database_path
        )
        
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        
        logger.info(f"Database initialized: {self.db_path}")
    
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = duckdb.connect(str(self.db_path))
        return self._connection
    
    def close(self) -> None:
        """Close database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
    
    def __enter__(self) -> "Database":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
    
    @contextmanager
    def cursor(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Get a cursor for executing queries."""
        yield self.connection
    
    def initialize_schema(self) -> None:
        """
        Initialize database schema from SQL file.
        
        Creates all tables, indexes, and views if they don't exist.
        Enables WAL mode for crash recovery and better concurrency.
        """
        # Enable Write-Ahead Logging (WAL) for crash recovery
        try:
            self.connection.execute("PRAGMA wal_autocheckpoint=1000")
            logger.info("Enabled WAL auto-checkpoint at 1000 pages")
        except Exception as e:
            logger.warning(f"Could not set WAL auto-checkpoint: {e}")
        
        try:
            # Enable WAL mode for better crash recovery
            result = self.connection.execute("PRAGMA journal_mode=WAL").fetchone()
            logger.info(f"Set journal mode to WAL: {result}")
        except Exception as e:
            logger.warning(f"Could not enable WAL mode: {e}")
        
        # Find schema file
        schema_path = Path(__file__).parent / "schema.sql"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        # Read and execute schema
        with open(schema_path) as f:
            schema_sql = f.read()
        
        # Split into individual statements
        statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
        
        # Categorize statements
        create_tables = []
        create_indexes = []
        create_sequences = []
        create_views = []
        other = []
        
        for statement in statements:
            if not statement:
                continue
            # Remove leading comments to classify the statement
            lines = statement.split('\n')
            sql_lines = [l for l in lines if l.strip() and not l.strip().startswith('--')]
            if not sql_lines:
                continue
            upper = ' '.join(sql_lines).upper()
            if "CREATE TABLE" in upper:
                create_tables.append(statement)
            elif "CREATE INDEX" in upper:
                create_indexes.append(statement)
            elif "CREATE SEQUENCE" in upper:
                create_sequences.append(statement)
            elif "CREATE" in upper and "VIEW" in upper:
                create_views.append(statement)
            else:
                other.append(statement)
        
        # Execute in order: sequences, tables, indexes, views, other
        for statement in create_sequences + create_tables + create_indexes + create_views + other:
            try:
                self.connection.execute(statement)
            except Exception as e:
                error_str = str(e).lower()
                # Ignore "already exists" errors
                if "already exists" not in error_str and "duplicate" not in error_str:
                    logger.debug(f"Schema statement note: {e}")
        
        logger.info("Database schema initialized")
    
    # ==================== Company Operations ====================
    
    def upsert_company(
        self,
        cik: str,
        company_name: str,
        ticker: Optional[str] = None,
        sic_code: Optional[str] = None,
        sic_description: Optional[str] = None,
        state_of_incorporation: Optional[str] = None,
        fiscal_year_end: Optional[str] = None,
        category: Optional[str] = None,
        ein: Optional[str] = None,
    ) -> None:
        """Insert or update a company record."""
        sql = """
            INSERT INTO companies (
                cik, company_name, ticker, sic_code, sic_description,
                state_of_incorporation, fiscal_year_end, category, ein,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())
            ON CONFLICT (cik) DO UPDATE SET
                company_name = EXCLUDED.company_name,
                ticker = COALESCE(EXCLUDED.ticker, companies.ticker),
                sic_code = COALESCE(EXCLUDED.sic_code, companies.sic_code),
                sic_description = COALESCE(EXCLUDED.sic_description, companies.sic_description),
                state_of_incorporation = COALESCE(EXCLUDED.state_of_incorporation, companies.state_of_incorporation),
                fiscal_year_end = COALESCE(EXCLUDED.fiscal_year_end, companies.fiscal_year_end),
                category = COALESCE(EXCLUDED.category, companies.category),
                ein = COALESCE(EXCLUDED.ein, companies.ein),
                updated_at = now()
        """
        self.connection.execute(sql, [
            cik, company_name, ticker, sic_code, sic_description,
            state_of_incorporation, fiscal_year_end, category, ein
        ])
        logger.debug(f"Upserted company: {cik} ({company_name})")
    
    def get_company(self, cik: str) -> Optional[dict]:
        """Get company by CIK."""
        sql = "SELECT * FROM companies WHERE cik = ?"
        result = self.connection.execute(sql, [cik]).fetchone()
        
        if result:
            columns = [desc[0] for desc in self.connection.description]
            return dict(zip(columns, result))
        return None
    
    def get_all_companies(self) -> list[dict]:
        """Get all companies."""
        sql = "SELECT * FROM companies ORDER BY ticker"
        results = self.connection.execute(sql).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    # ==================== Filing Operations ====================
    
    def upsert_filing(
        self,
        accession_number: str,
        cik: str,
        form_type: str,
        filing_date: date,
        period_of_report: Optional[date] = None,
        acceptance_datetime: Optional[datetime] = None,
        primary_document: Optional[str] = None,
        primary_doc_description: Optional[str] = None,
        is_xbrl: bool = False,
        is_inline_xbrl: bool = False,
        edgar_url: Optional[str] = None,
        local_path: Optional[str] = None,
        download_status: str = "pending",
    ) -> None:
        """Insert or update a filing record."""
        sql = """
            INSERT INTO filings (
                accession_number, cik, form_type, filing_date, period_of_report,
                acceptance_datetime, primary_document, primary_doc_description,
                is_xbrl, is_inline_xbrl, edgar_url, local_path, download_status,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (accession_number) DO UPDATE SET
                download_status = EXCLUDED.download_status,
                local_path = COALESCE(EXCLUDED.local_path, filings.local_path),
                updated_at = now()
        """
        self.connection.execute(sql, [
            accession_number, cik, form_type, filing_date, period_of_report,
            acceptance_datetime, primary_document, primary_doc_description,
            is_xbrl, is_inline_xbrl, edgar_url, local_path, download_status
        ])
        logger.debug(f"Upserted filing: {accession_number}")
    
    def get_filing(self, accession_number: str) -> Optional[dict]:
        """Get filing by accession number."""
        sql = "SELECT * FROM filings WHERE accession_number = ?"
        result = self.connection.execute(sql, [accession_number]).fetchone()
        
        if result:
            columns = [desc[0] for desc in self.connection.description]
            return dict(zip(columns, result))
        return None
    
    def get_company_filings(
        self,
        cik: str,
        form_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> list[dict]:
        """Get filings for a company with optional filters."""
        sql = "SELECT * FROM filings WHERE cik = ?"
        params = [cik]
        
        if form_type:
            sql += " AND form_type = ?"
            params.append(form_type)
        if start_date:
            sql += " AND filing_date >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND filing_date <= ?"
            params.append(end_date)
        
        sql += " ORDER BY filing_date DESC"
        
        results = self.connection.execute(sql, params).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def update_filing_status(
        self,
        accession_number: str,
        download_status: Optional[str] = None,
        xbrl_processed: Optional[bool] = None,
        sections_processed: Optional[bool] = None,
        local_path: Optional[str] = None,
        processing_errors: Optional[dict] = None,
    ) -> None:
        """Update filing processing status."""
        updates = []
        params = []
        
        if download_status is not None:
            updates.append("download_status = ?")
            params.append(download_status)
        if xbrl_processed is not None:
            updates.append("xbrl_processed = ?")
            params.append(xbrl_processed)
        if sections_processed is not None:
            updates.append("sections_processed = ?")
            params.append(sections_processed)
        if local_path is not None:
            updates.append("local_path = ?")
            params.append(local_path)
        if processing_errors is not None:
            updates.append("processing_errors = ?")
            params.append(json.dumps(processing_errors))
        
        if not updates:
            return
        
        updates.append("updated_at = now()")
        params.append(accession_number)
        
        sql = f"UPDATE filings SET {', '.join(updates)} WHERE accession_number = ?"
        self.connection.execute(sql, params)
    
    def get_unprocessed_filings(self, processing_type: str = "xbrl") -> list[dict]:
        """Get filings that haven't been processed."""
        if processing_type == "xbrl":
            sql = "SELECT * FROM filings WHERE xbrl_processed = FALSE AND download_status = 'completed'"
        elif processing_type == "sections":
            sql = "SELECT * FROM filings WHERE sections_processed = FALSE AND download_status = 'completed'"
        else:
            sql = "SELECT * FROM filings WHERE download_status = 'completed'"
        
        results = self.connection.execute(sql).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    # ==================== Facts Operations ====================
    
    def insert_fact(
        self,
        accession_number: str,
        concept_name: str,
        value: Optional[Decimal] = None,
        value_text: Optional[str] = None,
        unit: Optional[str] = None,
        decimals: Optional[int] = None,
        period_type: Optional[str] = None,
        period_start: Optional[date] = None,
        period_end: Optional[date] = None,
        dimensions: Optional[dict] = None,
        concept_namespace: Optional[str] = None,
        concept_local_name: Optional[str] = None,
        is_custom: bool = False,
        is_negated: bool = False,
        section: Optional[str] = None,
        parent_concept: Optional[str] = None,
        label: Optional[str] = None,
        depth: Optional[int] = None,
    ) -> int:
        """Insert a fact record and return its ID. Skips if duplicate already exists."""
        # Check if fact already exists (duplicate prevention)
        dimensions_json = json.dumps(dimensions) if dimensions else None
        check_sql = """
            SELECT id FROM facts 
            WHERE accession_number = ? 
              AND concept_name = ? 
              AND period_end IS NOT DISTINCT FROM ?
              AND dimensions IS NOT DISTINCT FROM ?
        """
        existing = self.connection.execute(check_sql, [
            accession_number, 
            concept_name, 
            period_end,
            dimensions_json
        ]).fetchone()
        
        # If fact already exists, return existing ID without inserting
        if existing:
            logger.debug(f"Fact already exists: {concept_name} for {accession_number}, skipping duplicate")
            return existing[0]
        
        # Get next ID from sequence
        id_result = self.connection.execute("SELECT nextval('facts_id_seq')").fetchone()
        fact_id = id_result[0]
        
        sql = """
            INSERT INTO facts (
                id, accession_number, concept_name, concept_namespace, concept_local_name,
                value, value_text, unit, decimals, period_type, period_start, period_end,
                dimensions, is_custom, is_negated, section, parent_concept, label, depth
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.connection.execute(sql, [
            fact_id, accession_number, concept_name, concept_namespace, concept_local_name,
            float(value) if value is not None else None, value_text, unit, decimals,
            period_type, period_start, period_end,
            dimensions_json, is_custom, is_negated,
            section, parent_concept, label, depth
        ])
        return fact_id
    
    def insert_facts_batch(self, facts: list[dict]) -> int:
        """Insert multiple facts in a batch."""
        if not facts:
            return 0
        
        count = 0
        for fact in facts:
            self.insert_fact(**fact)
            count += 1
        
        return count
    
    def get_facts(
        self,
        accession_number: str,
        concept_name: Optional[str] = None,
    ) -> list[dict]:
        """Get facts for a filing."""
        sql = "SELECT * FROM facts WHERE accession_number = ?"
        params = [accession_number]
        
        if concept_name:
            sql += " AND concept_name = ?"
            params.append(concept_name)
        
        results = self.connection.execute(sql, params).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def upsert_concept_category(
        self,
        concept_name: str,
        section: Optional[str] = None,
        subsection: Optional[str] = None,
        parent_concept: Optional[str] = None,
        depth: Optional[int] = None,
        label: Optional[str] = None,
        data_type: Optional[str] = None,
    ) -> None:
        """Insert or update a concept category record."""
        sql = """
            INSERT INTO concept_categories (
                concept_name, section, subsection, parent_concept, 
                depth, label, data_type, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now(), now())
            ON CONFLICT (concept_name) DO UPDATE SET
                section = COALESCE(EXCLUDED.section, concept_categories.section),
                subsection = COALESCE(EXCLUDED.subsection, concept_categories.subsection),
                parent_concept = COALESCE(EXCLUDED.parent_concept, concept_categories.parent_concept),
                depth = COALESCE(EXCLUDED.depth, concept_categories.depth),
                label = COALESCE(EXCLUDED.label, concept_categories.label),
                data_type = COALESCE(EXCLUDED.data_type, concept_categories.data_type),
                updated_at = now()
        """
        self.connection.execute(sql, [
            concept_name, section, subsection, parent_concept, depth, label, data_type
        ])
    
    def get_concept_category(self, concept_name: str) -> Optional[dict]:
        """Get concept category by name."""
        sql = "SELECT * FROM concept_categories WHERE concept_name = ?"
        result = self.connection.execute(sql, [concept_name]).fetchone()
        
        if result:
            columns = [desc[0] for desc in self.connection.description]
            return dict(zip(columns, result))
        return None
    
    def get_concepts_by_section(self, section: str) -> list[dict]:
        """Get all concepts in a section."""
        sql = "SELECT * FROM concept_categories WHERE section = ? ORDER BY depth, concept_name"
        results = self.connection.execute(sql, [section]).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def get_all_sections(self) -> list[str]:
        """Get all unique sections."""
        sql = "SELECT DISTINCT section FROM concept_categories WHERE section IS NOT NULL ORDER BY section"
        results = self.connection.execute(sql).fetchall()
        return [row[0] for row in results]
    
    def get_fact_history(
        self,
        cik: str,
        concept_name: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """Get historical values for a concept across filings."""
        sql = """
            SELECT 
                f.accession_number,
                f.period_of_report,
                f.filing_date,
                fa.value,
                fa.unit,
                fa.period_start,
                fa.period_end
            FROM facts fa
            JOIN filings f ON fa.accession_number = f.accession_number
            WHERE f.cik = ? AND fa.concept_name = ?
            AND fa.dimensions IS NULL
        """
        params = [cik, concept_name]
        
        if start_date:
            sql += " AND f.period_of_report >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND f.period_of_report <= ?"
            params.append(end_date)
        
        sql += " ORDER BY f.period_of_report DESC"
        
        return self.connection.execute(sql, params).df()
    
    # ==================== Sections Operations ====================
    
    def insert_section(
        self,
        accession_number: str,
        section_type: str,
        content_text: str,
        section_title: Optional[str] = None,
        section_number: Optional[str] = None,
        content_html: Optional[str] = None,
        word_count: Optional[int] = None,
        character_count: Optional[int] = None,
        paragraph_count: Optional[int] = None,
        extraction_confidence: Optional[float] = None,
        extraction_method: Optional[str] = None,
    ) -> int:
        """Insert a section record and return its ID."""
        # Get next ID
        id_result = self.connection.execute("SELECT nextval('sections_id_seq')").fetchone()
        section_id = id_result[0]
        
        # Calculate counts if not provided
        if word_count is None and content_text:
            word_count = len(content_text.split())
        if character_count is None and content_text:
            character_count = len(content_text)
        if paragraph_count is None and content_text:
            paragraph_count = len([p for p in content_text.split("\n\n") if p.strip()])
        
        sql = """
            INSERT INTO sections (
                id, accession_number, section_type, section_title, section_number,
                content_text, content_html, word_count, character_count, paragraph_count,
                extraction_confidence, extraction_method
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.connection.execute(sql, [
            section_id, accession_number, section_type, section_title, section_number,
            content_text, content_html, word_count, character_count, paragraph_count,
            extraction_confidence, extraction_method
        ])
        return section_id
    
    def get_sections(
        self,
        accession_number: str,
        section_type: Optional[str] = None,
    ) -> list[dict]:
        """Get sections for a filing."""
        sql = "SELECT * FROM sections WHERE accession_number = ?"
        params = [accession_number]
        
        if section_type:
            sql += " AND section_type = ?"
            params.append(section_type)
        
        results = self.connection.execute(sql, params).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    # ==================== Processing Logs ====================
    
    def log_processing(
        self,
        pipeline_stage: str,
        status: str,
        accession_number: Optional[str] = None,
        cik: Optional[str] = None,
        operation: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        processing_time_ms: Optional[int] = None,
        records_processed: Optional[int] = None,
        records_failed: Optional[int] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        context: Optional[dict] = None,
    ) -> int:
        """Log a processing operation."""
        id_result = self.connection.execute("SELECT nextval('processing_logs_id_seq')").fetchone()
        log_id = id_result[0]
        
        sql = """
            INSERT INTO processing_logs (
                id, accession_number, cik, pipeline_stage, operation, status,
                started_at, completed_at, processing_time_ms,
                records_processed, records_failed, error_message, error_traceback, context
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.connection.execute(sql, [
            log_id, accession_number, cik, pipeline_stage, operation, status,
            started_at, completed_at, processing_time_ms,
            records_processed, records_failed, error_message, error_traceback,
            json.dumps(context) if context else None
        ])
        return log_id
    
    # ==================== Analytics ====================
    
    def get_processing_summary(self) -> pd.DataFrame:
        """Get processing status summary."""
        return self.connection.execute("SELECT * FROM processing_summary").df()
    
    def get_key_financials(self, cik: Optional[str] = None) -> pd.DataFrame:
        """Get key financial metrics."""
        sql = "SELECT * FROM key_financials"
        params = []
        
        if cik:
            sql = sql.replace("FROM key_financials", "FROM key_financials WHERE cik = ?")
            # Actually need to query the underlying tables
            sql = """
                SELECT 
                    c.ticker,
                    c.company_name,
                    f.accession_number,
                    f.period_of_report,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:Assets' THEN fa.value END) as total_assets,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:Liabilities' THEN fa.value END) as total_liabilities,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:StockholdersEquity' THEN fa.value END) as equity,
                    MAX(CASE WHEN fa.concept_name LIKE '%Revenue%' THEN fa.value END) as revenue,
                    MAX(CASE WHEN fa.concept_name = 'us-gaap:NetIncomeLoss' THEN fa.value END) as net_income
                FROM filings f
                JOIN companies c ON f.cik = c.cik
                LEFT JOIN facts fa ON f.accession_number = fa.accession_number
                WHERE c.cik = ?
                GROUP BY c.ticker, c.company_name, f.accession_number, f.period_of_report
                ORDER BY f.period_of_report DESC
            """
            params = [cik]
        
        return self.connection.execute(sql, params).df()
    
    def execute_query(self, sql: str, params: Optional[list] = None) -> pd.DataFrame:
        """Execute arbitrary SQL and return DataFrame."""
        return self.connection.execute(sql, params or []).df()
    
    # ==================== Normalization Layer ====================
    
    def upsert_standardized_metric(
        self,
        metric_id: str,
        metric_name: str,
        display_label: str,
        category: str,
        data_type: Optional[str] = None,
        description: Optional[str] = None,
        calculation_rule: Optional[str] = None,
    ) -> None:
        """Insert or update a standardized metric definition."""
        sql = """
            INSERT INTO standardized_metrics (
                metric_id, metric_name, display_label, category,
                data_type, description, calculation_rule, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (metric_id) DO UPDATE SET
                metric_name = EXCLUDED.metric_name,
                display_label = EXCLUDED.display_label,
                category = EXCLUDED.category,
                data_type = COALESCE(EXCLUDED.data_type, standardized_metrics.data_type),
                description = COALESCE(EXCLUDED.description, standardized_metrics.description),
                calculation_rule = COALESCE(EXCLUDED.calculation_rule, standardized_metrics.calculation_rule)
        """
        self.connection.execute(sql, [
            metric_id, metric_name, display_label, category,
            data_type, description, calculation_rule
        ])
    
    def insert_concept_mapping(
        self,
        metric_id: str,
        concept_name: str,
        priority: int,
        confidence_score: float = 1.0,
        applies_to_industry: Optional[str] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Insert a concept mapping."""
        id_result = self.connection.execute("SELECT nextval('concept_mappings_id_seq')").fetchone()
        mapping_id = id_result[0]
        
        sql = """
            INSERT INTO concept_mappings (
                mapping_id, metric_id, concept_name, priority,
                confidence_score, applies_to_industry, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (metric_id, concept_name) DO UPDATE SET
                priority = EXCLUDED.priority,
                confidence_score = EXCLUDED.confidence_score,
                applies_to_industry = COALESCE(EXCLUDED.applies_to_industry, concept_mappings.applies_to_industry),
                notes = COALESCE(EXCLUDED.notes, concept_mappings.notes)
        """
        self.connection.execute(sql, [
            mapping_id, metric_id, concept_name, priority,
            confidence_score, applies_to_industry, notes
        ])
        return mapping_id
    
    def get_latest_filing_per_period(
        self,
        ticker: Optional[str] = None,
        form_types: List[str] = None
    ) -> list:
        """
        Get the latest filing per fiscal period with priority logic.
        
        Uses the fiscal year from the facts data (MAX period_end year) to determine
        the fiscal period, since filing_date may be bulk-imported and period_of_report may be NULL.
        
        Priority:
        1. Amendments (10-K/A) over originals (10-K)
        2. Latest filing_date if multiple of same type
        
        Returns only ONE filing per (cik, fiscal_year)
        
        Args:
            ticker: Optional ticker to filter by company
            form_types: List of form types (defaults to ['10-K', '10-K/A'])
        
        Returns:
            List of tuples: (accession_number, cik, ticker, form_type, filing_date, fiscal_year)
        """
        if form_types is None:
            form_types = ['10-K', '10-K/A']
        
        sql = """
            WITH filing_years AS (
                SELECT 
                    f.accession_number,
                    f.cik,
                    f.form_type,
                    f.filing_date,
                    MAX(EXTRACT(YEAR FROM fa.period_end)) as fiscal_year
                FROM filings f
                LEFT JOIN facts fa ON f.accession_number = fa.accession_number
                WHERE f.xbrl_processed = TRUE
                  AND f.form_type IN ("""
        
        # Add placeholders for form_types
        placeholders = ', '.join(['?' for _ in form_types])
        sql += placeholders + ")"
        
        params = form_types.copy()
        
        sql += """
                GROUP BY f.accession_number, f.cik, f.form_type, f.filing_date
            ),
            ranked_filings AS (
                SELECT 
                    fy.accession_number,
                    fy.cik,
                    c.ticker,
                    fy.form_type,
                    fy.filing_date,
                    fy.fiscal_year,
                    ROW_NUMBER() OVER (
                        PARTITION BY fy.cik, fy.fiscal_year
                        ORDER BY 
                            CASE 
                                WHEN fy.form_type LIKE '%/A' THEN 1
                                ELSE 2
                            END ASC,
                            fy.filing_date DESC
                    ) as priority_rank
                FROM filing_years fy
                JOIN companies c ON fy.cik = c.cik
                WHERE fy.fiscal_year IS NOT NULL
        """
        
        if ticker:
            sql += " AND c.ticker = ?"
            params.append(ticker)
        
        sql += """
            )
            SELECT accession_number, cik, ticker, form_type, filing_date, fiscal_year
            FROM ranked_filings
            WHERE priority_rank = 1
            ORDER BY fiscal_year DESC
        """
        
        return self.connection.execute(sql, params).fetchall()
    
    def get_concept_mappings(self, metric_id: Optional[str] = None) -> list[dict]:
        """Get concept mappings, optionally filtered by metric."""
        if metric_id:
            sql = "SELECT * FROM concept_mappings WHERE metric_id = ? ORDER BY priority"
            results = self.connection.execute(sql, [metric_id]).fetchall()
        else:
            sql = "SELECT * FROM concept_mappings ORDER BY metric_id, priority"
            results = self.connection.execute(sql).fetchall()
        
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in results]
    
    def insert_normalized_metric(
        self,
        company_ticker: str,
        fiscal_year: int,
        metric_id: str,
        metric_value: float,
        source_concept: Optional[str] = None,
        source_accession: Optional[str] = None,
        confidence_score: float = 1.0,
        fiscal_quarter: Optional[int] = None,
    ) -> int:
        """
        Insert or update a normalized metric value.
        
        Uses check-then-upsert pattern to avoid duplicate records when
        confidence scores are equal.
        """
        
        # Check if record already exists
        existing = self.connection.execute("""
            SELECT id, confidence_score
            FROM normalized_financials
            WHERE company_ticker = ?
              AND fiscal_year = ?
              AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
              AND metric_id = ?
        """, [company_ticker, fiscal_year, fiscal_quarter, metric_id]).fetchone()
        
        if existing:
            existing_id, existing_confidence = existing
            
            # Only update if new confidence is higher or equal
            if confidence_score >= existing_confidence:
                self.connection.execute("""
                    UPDATE normalized_financials
                    SET metric_value = ?,
                        source_concept = ?,
                        source_accession = ?,
                        confidence_score = ?,
                        created_at = now()
                    WHERE id = ?
                """, [metric_value, source_concept, source_accession, 
                      confidence_score, existing_id])
                
                logger.debug(f"Updated metric {metric_id} for {company_ticker} "
                            f"FY{fiscal_year} (confidence: {existing_confidence:.2f} -> {confidence_score:.2f})")
            else:
                logger.debug(f"Skipped update for {metric_id} {company_ticker} "
                            f"FY{fiscal_year} (confidence {confidence_score:.2f} < {existing_confidence:.2f})")
            
            return existing_id
        else:
            # Insert new record
            norm_id = self.connection.execute(
                "SELECT nextval('normalized_financials_id_seq')"
            ).fetchone()[0]
            
            self.connection.execute("""
                INSERT INTO normalized_financials (
                    id, company_ticker, fiscal_year, fiscal_quarter, metric_id,
                    metric_value, source_concept, source_accession,
                    confidence_score, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            """, [norm_id, company_ticker, fiscal_year, fiscal_quarter, metric_id,
                  metric_value, source_concept, source_accession, confidence_score])
            
            logger.debug(f"Inserted new metric {metric_id} for {company_ticker} FY{fiscal_year}")
            
            return norm_id
    
    def get_normalized_metrics(
        self,
        ticker: Optional[str] = None,
        fiscal_year: Optional[int] = None,
        metric_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get normalized metrics with optional filters."""
        sql = "SELECT * FROM normalized_metrics_view WHERE 1=1"
        params = []
        
        if ticker:
            sql += " AND company_ticker = ?"
            params.append(ticker)
        if fiscal_year:
            sql += " AND fiscal_year = ?"
            params.append(fiscal_year)
        if metric_id:
            sql += " AND metric_id = ?"
            params.append(metric_id)
        
        sql += " ORDER BY company_ticker, fiscal_year DESC, metric_id"
        
        return self.connection.execute(sql, params).df()
    
    # ==================== Duplicate Management ====================
    
    def detect_duplicates(
        self,
        table: str = "normalized_financials"
    ) -> list[dict]:
        """
        Detect duplicate records in specified table.
        
        For normalized_financials, duplicates are defined as multiple records
        with the same (company_ticker, fiscal_year, fiscal_quarter, metric_id).
        
        Args:
            table: Table name to check for duplicates
        
        Returns:
            List of duplicate group dictionaries with metadata
        
        Example:
            duplicates = db.detect_duplicates("normalized_financials")
            for dup in duplicates:
                print(f"{dup['ticker']} {dup['year']} {dup['metric']}: {dup['count']} entries")
        """
        if table == "normalized_financials":
            # Find duplicate groups
            results = self.connection.execute("""
                SELECT 
                    company_ticker, 
                    fiscal_year, 
                    fiscal_quarter, 
                    metric_id,
                    COUNT(*) as count
                FROM normalized_financials
                GROUP BY company_ticker, fiscal_year, fiscal_quarter, metric_id
                HAVING COUNT(*) > 1
                ORDER BY count DESC, company_ticker, fiscal_year DESC
            """).fetchall()
            
            duplicates = []
            for ticker, year, quarter, metric, count in results:
                # Get details of all duplicate records in this group
                records = self.connection.execute("""
                    SELECT id, confidence_score, created_at, value
                    FROM normalized_financials
                    WHERE company_ticker = ?
                      AND fiscal_year = ?
                      AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                      AND metric_id = ?
                    ORDER BY confidence_score DESC, created_at DESC
                """, [ticker, year, quarter, metric]).fetchall()
                
                duplicates.append({
                    "table": table,
                    "ticker": ticker,
                    "year": year,
                    "quarter": quarter,
                    "metric": metric,
                    "count": count,
                    "records": [
                        {
                            "id": r[0],
                            "confidence": r[1],
                            "created_at": r[2],
                            "value": r[3],
                            "keep": i == 0  # First (best) record should be kept
                        }
                        for i, r in enumerate(records)
                    ]
                })
            
            return duplicates
        else:
            raise ValueError(f"Duplicate detection not implemented for table: {table}")
    
    def remove_duplicates(
        self,
        table: str = "normalized_financials",
        dry_run: bool = True
    ) -> dict:
        """
        Remove duplicate records, keeping the best one per group.
        
        For normalized_financials:
        - Keeps record with highest confidence_score
        - If tied, keeps most recent (created_at DESC)
        - Deletes all others
        
        Args:
            table: Table name to clean
            dry_run: If True, only reports what would be deleted (no actual deletion)
        
        Returns:
            Dict with statistics: duplicate_groups, records_removed, records_kept
        
        Example:
            # Preview what would be deleted
            stats = db.remove_duplicates("normalized_financials", dry_run=True)
            print(f"Would remove {stats['records_removed']} duplicate records")
            
            # Actually delete
            stats = db.remove_duplicates("normalized_financials", dry_run=False)
            print(f"Removed {stats['records_removed']} duplicates")
        """
        if table != "normalized_financials":
            raise ValueError(f"Duplicate removal not implemented for table: {table}")
        
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Detecting duplicates in {table}...")
        
        # Detect all duplicates
        duplicates = self.detect_duplicates(table)
        
        if not duplicates:
            logger.info("No duplicates found!")
            return {
                "duplicate_groups": 0,
                "records_removed": 0,
                "records_kept": 0
            }
        
        logger.info(f"Found {len(duplicates)} duplicate groups")
        
        total_removed = 0
        total_kept = len(duplicates)  # One kept per group
        
        if not dry_run:
            # Start transaction for safety
            self.connection.execute("BEGIN TRANSACTION")
        
        try:
            for dup in duplicates:
                ticker = dup["ticker"]
                year = dup["year"]
                quarter = dup["quarter"]
                metric = dup["metric"]
                count = dup["count"]
                
                # Get the best record to keep
                keeper = self.connection.execute("""
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
                    self.connection.execute("""
                        DELETE FROM normalized_financials
                        WHERE company_ticker = ?
                          AND fiscal_year = ?
                          AND COALESCE(fiscal_quarter, -1) = COALESCE(?, -1)
                          AND metric_id = ?
                          AND id != ?
                    """, [ticker, year, quarter, metric, keeper_id])
                
                removed = count - 1
                total_removed += removed
                
                log_msg = f"{'Would remove' if dry_run else 'Removed'} {removed} duplicate(s) for {ticker} {year} Q{quarter or 'N/A'} {metric} (kept id={keeper_id})"
                logger.info(f"  {log_msg}")
            
            if not dry_run:
                # Commit transaction
                self.connection.execute("COMMIT")
                logger.info(f"Successfully removed {total_removed} duplicate records")
            else:
                logger.info(f"Would remove {total_removed} duplicate records (dry run)")
            
            return {
                "duplicate_groups": len(duplicates),
                "records_removed": total_removed,
                "records_kept": total_kept
            }
            
        except Exception as e:
            if not dry_run:
                self.connection.execute("ROLLBACK")
                logger.error(f"Failed to remove duplicates, transaction rolled back: {e}")
            raise


def get_database() -> Database:
    """Get a database instance."""
    return Database()


def initialize_database() -> None:
    """Initialize the database with schema."""
    with Database() as db:
        db.initialize_schema()
    logger.info("Database initialized successfully")
