"""
Database connection management and schema initialization.

Provides the base Database class with connection lifecycle, transactions,
and schema setup for DuckDB. Composes repository classes for domain operations.
"""

import json
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Generator, List, Optional

import duckdb
import pandas as pd

from ..infrastructure.config import get_absolute_path, get_settings
from ..infrastructure.logger import get_logger

logger = get_logger("finloom.storage.connection")


class Database:
    """
    DuckDB database wrapper for SEC filing data.
    
    Provides connection management, schema initialization, and transaction support.
    Composes repository classes for domain-specific operations.
    Thread-safe for read operations; write operations should be serialized.
    """
    
    def __init__(self, db_path: Optional[str] = None, read_only: bool = False) -> None:
        """
        Initialize database connection.
        
        Args:
            db_path: Path to DuckDB database file. If None, uses config.
            read_only: If True, open in read-only mode (allows concurrent access).
        """
        settings = get_settings()
        self.db_path = get_absolute_path(
            db_path or settings.storage.database_path
        )
        
        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        self.read_only = read_only
        
        # Lazy-loaded repositories (avoid circular imports)
        self._company_repo = None
        self._filing_repo = None
        self._fact_repo = None
        self._section_repo = None
        self._analytics_repo = None
        self._normalization_repo = None
        
        logger.info(f"Database initialized: {self.db_path} (read_only={read_only})")
    
    @property
    def companies(self):
        """Get company repository."""
        if self._company_repo is None:
            from .company_repository import CompanyRepository
            self._company_repo = CompanyRepository(self)
        return self._company_repo
    
    @property
    def filings(self):
        """Get filing repository."""
        if self._filing_repo is None:
            from .filing_repository import FilingRepository
            self._filing_repo = FilingRepository(self)
        return self._filing_repo
    
    @property
    def facts(self):
        """Get fact repository."""
        if self._fact_repo is None:
            from .fact_repository import FactRepository
            self._fact_repo = FactRepository(self)
        return self._fact_repo
    
    @property
    def sections(self):
        """Get section repository."""
        if self._section_repo is None:
            from .section_repository import SectionRepository
            self._section_repo = SectionRepository(self)
        return self._section_repo
    
    @property
    def analytics(self):
        """Get analytics repository."""
        if self._analytics_repo is None:
            from .analytics import AnalyticsRepository
            self._analytics_repo = AnalyticsRepository(self)
        return self._analytics_repo
    
    @property
    def normalization(self):
        """Get normalization repository."""
        if self._normalization_repo is None:
            from .normalization import NormalizationRepository
            self._normalization_repo = NormalizationRepository(self)
        return self._normalization_repo
    
    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = duckdb.connect(str(self.db_path), read_only=self.read_only)
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
    
    # ==================== Backward Compatibility Delegation Methods ====================
    # Company operations
    def upsert_company(self, *args, **kwargs) -> None:
        return self.companies.upsert_company(*args, **kwargs)
    
    def get_company(self, *args, **kwargs) -> Optional[dict]:
        return self.companies.get_company(*args, **kwargs)
    
    def get_all_companies(self, *args, **kwargs) -> list[dict]:
        return self.companies.get_all_companies(*args, **kwargs)
    
    # Filing operations
    def upsert_filing(self, *args, **kwargs) -> None:
        return self.filings.upsert_filing(*args, **kwargs)
    
    def get_filing(self, *args, **kwargs) -> Optional[dict]:
        return self.filings.get_filing(*args, **kwargs)
    
    def get_company_filings(self, *args, **kwargs) -> list[dict]:
        return self.filings.get_company_filings(*args, **kwargs)
    
    def update_filing_status(self, *args, **kwargs) -> None:
        return self.filings.update_filing_status(*args, **kwargs)
    
    def get_unprocessed_filings(self, *args, **kwargs) -> list[dict]:
        return self.filings.get_unprocessed_filings(*args, **kwargs)
    
    # Fact operations
    def insert_fact(self, *args, **kwargs) -> int:
        return self.facts.insert_fact(*args, **kwargs)
    
    def insert_facts_batch(self, *args, **kwargs) -> int:
        return self.facts.insert_facts_batch(*args, **kwargs)
    
    def get_facts(self, *args, **kwargs) -> list[dict]:
        return self.facts.get_facts(*args, **kwargs)
    
    def upsert_concept_category(self, *args, **kwargs) -> None:
        return self.facts.upsert_concept_category(*args, **kwargs)
    
    def get_concept_category(self, *args, **kwargs) -> Optional[dict]:
        return self.facts.get_concept_category(*args, **kwargs)
    
    def get_concepts_by_section(self, *args, **kwargs) -> list[dict]:
        return self.facts.get_concepts_by_section(*args, **kwargs)
    
    def get_all_sections(self, *args, **kwargs) -> list[str]:
        return self.facts.get_all_sections(*args, **kwargs)
    
    def get_fact_history(self, *args, **kwargs) -> pd.DataFrame:
        return self.facts.get_fact_history(*args, **kwargs)
    
    # Analytics operations
    def log_processing(self, *args, **kwargs) -> int:
        return self.analytics.log_processing(*args, **kwargs)
    
    def get_processing_summary(self, *args, **kwargs) -> pd.DataFrame:
        return self.analytics.get_processing_summary(*args, **kwargs)
    
    def get_key_financials(self, *args, **kwargs) -> pd.DataFrame:
        return self.analytics.get_key_financials(*args, **kwargs)
    
    def execute_query(self, *args, **kwargs) -> pd.DataFrame:
        return self.analytics.execute_query(*args, **kwargs)
    
    # Normalization operations
    def upsert_standardized_metric(self, *args, **kwargs) -> None:
        return self.normalization.upsert_standardized_metric(*args, **kwargs)
    
    def insert_concept_mapping(self, *args, **kwargs) -> int:
        return self.normalization.insert_concept_mapping(*args, **kwargs)
    
    def get_latest_filing_per_period(self, *args, **kwargs) -> list:
        return self.normalization.get_latest_filing_per_period(*args, **kwargs)
    
    def get_concept_mappings(self, *args, **kwargs) -> list[dict]:
        return self.normalization.get_concept_mappings(*args, **kwargs)
    
    def insert_normalized_metric(self, *args, **kwargs) -> int:
        return self.normalization.insert_normalized_metric(*args, **kwargs)
    
    def get_normalized_metrics(self, *args, **kwargs) -> pd.DataFrame:
        return self.normalization.get_normalized_metrics(*args, **kwargs)
    
    def detect_duplicates(self, *args, **kwargs) -> list[dict]:
        return self.normalization.detect_duplicates(*args, **kwargs)
    
    def remove_duplicates(self, *args, **kwargs) -> dict:
        return self.normalization.remove_duplicates(*args, **kwargs)


def get_database() -> Database:
    """Get a database instance."""
    return Database()


def initialize_database() -> None:
    """Initialize the database with schema."""
    with Database() as db:
        db.initialize_schema()
    logger.info("Database initialized successfully")
