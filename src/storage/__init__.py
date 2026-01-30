"""Data storage module with repository pattern."""

from .analytics import AnalyticsRepository
from .company_repository import CompanyRepository
from .connection import Database, get_database, initialize_database
from .fact_repository import FactRepository
from .filing_repository import FilingRepository
from .normalization import NormalizationRepository
from .section_repository import SectionRepository

__all__ = [
    # Core database connection (backward compatible)
    "Database",
    "get_database",
    "initialize_database",
    # Repository classes (new access pattern)
    "CompanyRepository",
    "FilingRepository",
    "FactRepository",
    "SectionRepository",
    "AnalyticsRepository",
    "NormalizationRepository",
]
