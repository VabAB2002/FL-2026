"""
Repository implementations for data access abstraction.

Provides concrete implementations of the repository protocols defined in src.core.
These repositories wrap the Database class and provide a cleaner interface for
common operations.

Usage:
    from src.storage.repositories import get_filing_repository, get_fact_repository

    filing_repo = get_filing_repository()
    filing = filing_repo.get_filing("0000320193-24-000123")

    fact_repo = get_fact_repository()
    facts = fact_repo.get_facts_for_filing("0000320193-24-000123")
"""

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from ..core.repository import (
    CompanyRepository,
    FactRepository,
    FilingRepository,
    NormalizedMetricsRepository,
    SectionRepository,
)
from ..utils.logger import get_logger
from .database import Database, get_database

logger = get_logger("finloom.storage.repositories")


# =============================================================================
# Filing Repository
# =============================================================================


class DuckDBFilingRepository(FilingRepository):
    """DuckDB implementation of FilingRepository."""

    def __init__(self, db: Optional[Database] = None):
        self._db = db

    @property
    def db(self) -> Database:
        """Lazy database initialization."""
        if self._db is None:
            self._db = get_database()
        return self._db

    def get_filing(self, accession_number: str) -> Optional[dict]:
        """Get filing by accession number."""
        return self.db.get_filing(accession_number)

    def get_filing_by_id(self, filing_id: int) -> Optional[dict]:
        """Get filing by internal ID."""
        result = self.db.connection.execute(
            "SELECT * FROM filings WHERE id = ?", [filing_id]
        ).fetchone()
        if result:
            columns = [desc[0] for desc in self.db.connection.description]
            return dict(zip(columns, result))
        return None

    def save_filing(self, filing: dict) -> int:
        """Save a filing and return its ID."""
        self.db.upsert_filing(**filing)
        # Get the ID
        result = self.db.connection.execute(
            "SELECT id FROM filings WHERE accession_number = ?",
            [filing["accession_number"]],
        ).fetchone()
        return result[0] if result else 0

    def list_filings(self, cik: str, form_type: Optional[str] = None) -> list[dict]:
        """List filings for a company."""
        return self.db.get_company_filings(cik, form_type=form_type)

    def get_processed_filings(self, cik: str) -> list[dict]:
        """Get processed filings for a company."""
        results = self.db.connection.execute(
            """
            SELECT * FROM filings
            WHERE cik = ? AND xbrl_processed = TRUE
            ORDER BY filing_date DESC
            """,
            [cik],
        ).fetchall()
        columns = [desc[0] for desc in self.db.connection.description]
        return [dict(zip(columns, row)) for row in results]


# =============================================================================
# Fact Repository
# =============================================================================


class DuckDBFactRepository(FactRepository):
    """DuckDB implementation of FactRepository."""

    def __init__(self, db: Optional[Database] = None):
        self._db = db

    @property
    def db(self) -> Database:
        """Lazy database initialization."""
        if self._db is None:
            self._db = get_database()
        return self._db

    def get_facts(self, filing_id: int) -> list[dict]:
        """Get all facts for a filing by ID."""
        # First get accession number from filing_id
        result = self.db.connection.execute(
            "SELECT accession_number FROM filings WHERE id = ?", [filing_id]
        ).fetchone()
        if not result:
            return []
        return self.get_facts_for_filing(result[0])

    def get_facts_for_filing(self, accession_number: str) -> list[dict]:
        """Get all facts for a filing by accession number."""
        return self.db.get_facts(accession_number)

    def get_facts_by_concept(self, filing_id: int, concept_name: str) -> list[dict]:
        """Get facts by concept name."""
        result = self.db.connection.execute(
            "SELECT accession_number FROM filings WHERE id = ?", [filing_id]
        ).fetchone()
        if not result:
            return []
        return self.db.get_facts(result[0], concept_name=concept_name)

    def save_facts(self, facts: list[dict]) -> int:
        """Save facts and return count saved."""
        return self.db.insert_facts_batch(facts)

    def delete_facts(self, filing_id: int) -> int:
        """Delete facts for a filing."""
        result = self.db.connection.execute(
            "SELECT accession_number FROM filings WHERE id = ?", [filing_id]
        ).fetchone()
        if not result:
            return 0
        accession = result[0]
        count = self.db.connection.execute(
            "SELECT COUNT(*) FROM facts WHERE accession_number = ?", [accession]
        ).fetchone()[0]
        self.db.connection.execute(
            "DELETE FROM facts WHERE accession_number = ?", [accession]
        )
        return count


# =============================================================================
# Company Repository
# =============================================================================


class DuckDBCompanyRepository(CompanyRepository):
    """DuckDB implementation of CompanyRepository."""

    def __init__(self, db: Optional[Database] = None):
        self._db = db

    @property
    def db(self) -> Database:
        """Lazy database initialization."""
        if self._db is None:
            self._db = get_database()
        return self._db

    def get_company(self, cik: str) -> Optional[dict]:
        """Get company by CIK."""
        return self.db.get_company(cik)

    def get_company_by_ticker(self, ticker: str) -> Optional[dict]:
        """Get company by ticker symbol."""
        result = self.db.connection.execute(
            "SELECT * FROM companies WHERE ticker = ?", [ticker.upper()]
        ).fetchone()
        if result:
            columns = [desc[0] for desc in self.db.connection.description]
            return dict(zip(columns, result))
        return None

    def save_company(self, company: dict) -> None:
        """Save or update a company."""
        self.db.upsert_company(**company)

    def list_companies(self) -> list[dict]:
        """List all companies."""
        return self.db.get_all_companies()


# =============================================================================
# Section Repository (REMOVED)
# =============================================================================
# Note: Section repository removed in markdown-only architecture.
# All unstructured data is stored in filings.full_markdown column.


# =============================================================================
# Normalized Metrics Repository
# =============================================================================


class DuckDBNormalizedMetricsRepository(NormalizedMetricsRepository):
    """DuckDB implementation of NormalizedMetricsRepository."""

    def __init__(self, db: Optional[Database] = None):
        self._db = db

    @property
    def db(self) -> Database:
        """Lazy database initialization."""
        if self._db is None:
            self._db = get_database()
        return self._db

    def get_metrics(
        self,
        cik: str,
        period_end: Optional[date] = None,
        metric_name: Optional[str] = None,
    ) -> list[dict]:
        """Get normalized metrics for a company."""
        # Get ticker from CIK
        company = self.db.get_company(cik)
        if not company:
            return []
        ticker = company.get("ticker")
        if not ticker:
            return []

        df = self.db.get_normalized_metrics(
            ticker=ticker,
            fiscal_year=period_end.year if period_end else None,
            metric_id=metric_name,
        )
        return df.to_dict("records") if not df.empty else []

    def save_metrics(self, metrics: list[dict]) -> int:
        """Save normalized metrics."""
        count = 0
        for metric in metrics:
            self.db.insert_normalized_metric(**metric)
            count += 1
        return count

    def get_latest_metrics(self, cik: str) -> list[dict]:
        """Get the latest metrics for a company."""
        company = self.db.get_company(cik)
        if not company:
            return []
        ticker = company.get("ticker")
        if not ticker:
            return []

        df = self.db.get_normalized_metrics(ticker=ticker)
        if df.empty:
            return []

        # Get latest fiscal year
        latest_year = df["fiscal_year"].max()
        return df[df["fiscal_year"] == latest_year].to_dict("records")


# =============================================================================
# Concept Mapping Repository (for ConceptMapper)
# =============================================================================


class DuckDBConceptMappingRepository:
    """Repository for concept mappings used by ConceptMapper."""

    def __init__(self, db: Optional[Database] = None):
        self._db = db

    @property
    def db(self) -> Database:
        """Lazy database initialization."""
        if self._db is None:
            self._db = get_database()
        return self._db

    def get_all_mappings(self) -> list[dict]:
        """Get all concept mappings."""
        return self.db.get_concept_mappings()

    def get_mappings_for_metric(self, metric_id: str) -> list[dict]:
        """Get mappings for a specific metric."""
        return self.db.get_concept_mappings(metric_id=metric_id)


# =============================================================================
# Global Repository Instances (Singletons)
# =============================================================================

_filing_repo: Optional[DuckDBFilingRepository] = None
_fact_repo: Optional[DuckDBFactRepository] = None
_company_repo: Optional[DuckDBCompanyRepository] = None
# _section_repo removed - markdown-only architecture
_metrics_repo: Optional[DuckDBNormalizedMetricsRepository] = None
_mapping_repo: Optional[DuckDBConceptMappingRepository] = None


def get_filing_repository(db: Optional[Database] = None) -> DuckDBFilingRepository:
    """Get or create filing repository singleton."""
    global _filing_repo
    if _filing_repo is None or db is not None:
        _filing_repo = DuckDBFilingRepository(db)
    return _filing_repo


def get_fact_repository(db: Optional[Database] = None) -> DuckDBFactRepository:
    """Get or create fact repository singleton."""
    global _fact_repo
    if _fact_repo is None or db is not None:
        _fact_repo = DuckDBFactRepository(db)
    return _fact_repo


def get_company_repository(db: Optional[Database] = None) -> DuckDBCompanyRepository:
    """Get or create company repository singleton."""
    global _company_repo
    if _company_repo is None or db is not None:
        _company_repo = DuckDBCompanyRepository(db)
    return _company_repo


# get_section_repository removed - markdown-only architecture


def get_metrics_repository(
    db: Optional[Database] = None,
) -> DuckDBNormalizedMetricsRepository:
    """Get or create normalized metrics repository singleton."""
    global _metrics_repo
    if _metrics_repo is None or db is not None:
        _metrics_repo = DuckDBNormalizedMetricsRepository(db)
    return _metrics_repo


def get_mapping_repository(
    db: Optional[Database] = None,
) -> DuckDBConceptMappingRepository:
    """Get or create concept mapping repository singleton."""
    global _mapping_repo
    if _mapping_repo is None or db is not None:
        _mapping_repo = DuckDBConceptMappingRepository(db)
    return _mapping_repo
