"""Data storage module."""

from .database import Database, get_database, initialize_database
from .merge_coordinator import MergeCoordinator, MergeResult
from .repositories import (
    DuckDBCompanyRepository,
    DuckDBFactRepository,
    DuckDBFilingRepository,
    DuckDBNormalizedMetricsRepository,
    DuckDBSectionRepository,
    get_company_repository,
    get_fact_repository,
    get_filing_repository,
    get_mapping_repository,
    get_metrics_repository,
    get_section_repository,
)
from .s3_backup import S3Backup
from .staging_manager import StagingManager

__all__ = [
    # Database
    "Database",
    "get_database",
    "initialize_database",
    # Repositories
    "get_filing_repository",
    "get_fact_repository",
    "get_company_repository",
    "get_section_repository",
    "get_metrics_repository",
    "get_mapping_repository",
    "DuckDBFilingRepository",
    "DuckDBFactRepository",
    "DuckDBCompanyRepository",
    "DuckDBSectionRepository",
    "DuckDBNormalizedMetricsRepository",
    # Other
    "S3Backup",
    "StagingManager",
    "MergeCoordinator",
    "MergeResult",
]
