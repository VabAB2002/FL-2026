"""
Core domain layer for FinLoom.

This module provides:
- Exception hierarchy for consistent error handling
- Shared type definitions

Usage:
    from src.core import FinLoomError, ParsingError
    from src.core.base_types import FormType, CIK
"""

from .exceptions import (
    CacheConnectionError,
    CacheError,
    ConfigurationError,
    ConnectionError,
    DatabaseError,
    DataQualityError,
    DownloadError,
    FinLoomError,
    IngestionError,
    MissingConfigError,
    MonitoringError,
    ParsingError,
    PipelineError,
    ProcessingError,
    RateLimitError,
    SchemaValidationError,
    SECApiError,
    SectionParsingError,
    StorageError,
    TableParsingError,
    ValidationError,
    XBRLParsingError,
)
from .base_types import (
    AccessionNumber,
    CIK,
    DateLike,
    Dimensions,
    FactValue,
    FormType,
    PeriodType,
    ProcessingStatus,
    SectionType,
    Severity,
)

__all__ = [
    # Exceptions
    "FinLoomError",
    "IngestionError",
    "SECApiError",
    "RateLimitError",
    "DownloadError",
    "ParsingError",
    "XBRLParsingError",
    "SectionParsingError",
    "TableParsingError",
    "StorageError",
    "DatabaseError",
    "ConnectionError",
    "ValidationError",
    "SchemaValidationError",
    "DataQualityError",
    "ConfigurationError",
    "MissingConfigError",
    "ProcessingError",
    "PipelineError",
    "CacheError",
    "CacheConnectionError",
    "MonitoringError",
    # Types
    "FormType",
    "ProcessingStatus",
    "PeriodType",
    "Severity",
    "SectionType",
    "CIK",
    "AccessionNumber",
    "FactValue",
    "Dimensions",
    "DateLike",
]
