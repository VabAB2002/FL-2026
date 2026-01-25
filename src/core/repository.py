"""
Repository protocols (interfaces) for data access abstraction.

These protocols define the contract that concrete implementations must follow.
This allows modules to depend on abstractions rather than concrete implementations.
"""

from datetime import date
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class FilingRepository(Protocol):
    """Repository for filing data access."""

    def get_filing(self, accession_number: str) -> Optional[dict]:
        """Get filing by accession number."""
        ...

    def get_filing_by_id(self, filing_id: int) -> Optional[dict]:
        """Get filing by internal ID."""
        ...

    def save_filing(self, filing: dict) -> int:
        """Save a filing and return its ID."""
        ...

    def list_filings(self, cik: str, form_type: Optional[str] = None) -> list[dict]:
        """List filings for a company."""
        ...

    def get_processed_filings(self, cik: str) -> list[dict]:
        """Get processed filings for a company."""
        ...


@runtime_checkable
class FactRepository(Protocol):
    """Repository for XBRL fact data access."""

    def get_facts(self, filing_id: int) -> list[dict]:
        """Get all facts for a filing."""
        ...

    def get_facts_by_concept(self, filing_id: int, concept_name: str) -> list[dict]:
        """Get facts by concept name."""
        ...

    def save_facts(self, facts: list[dict]) -> int:
        """Save facts and return count saved."""
        ...

    def delete_facts(self, filing_id: int) -> int:
        """Delete facts for a filing."""
        ...


@runtime_checkable
class CompanyRepository(Protocol):
    """Repository for company data access."""

    def get_company(self, cik: str) -> Optional[dict]:
        """Get company by CIK."""
        ...

    def get_company_by_ticker(self, ticker: str) -> Optional[dict]:
        """Get company by ticker symbol."""
        ...

    def save_company(self, company: dict) -> None:
        """Save or update a company."""
        ...

    def list_companies(self) -> list[dict]:
        """List all companies."""
        ...


@runtime_checkable
class SectionRepository(Protocol):
    """Repository for document section data access."""

    def get_sections(self, filing_id: int) -> list[dict]:
        """Get all sections for a filing."""
        ...

    def get_section(self, filing_id: int, section_type: str) -> Optional[dict]:
        """Get a specific section by type."""
        ...

    def save_sections(self, sections: list[dict]) -> int:
        """Save sections and return count saved."""
        ...

    def delete_sections(self, filing_id: int) -> int:
        """Delete sections for a filing."""
        ...


@runtime_checkable
class NormalizedMetricsRepository(Protocol):
    """Repository for normalized financial metrics."""

    def get_metrics(
        self,
        cik: str,
        period_end: Optional[date] = None,
        metric_name: Optional[str] = None,
    ) -> list[dict]:
        """Get normalized metrics for a company."""
        ...

    def save_metrics(self, metrics: list[dict]) -> int:
        """Save normalized metrics."""
        ...

    def get_latest_metrics(self, cik: str) -> list[dict]:
        """Get the latest metrics for a company."""
        ...
