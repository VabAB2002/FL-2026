"""
Core exception hierarchy for FinLoom.

All custom exceptions inherit from FinLoomError for consistent error handling.
"""

from typing import Optional


class FinLoomError(Exception):
    """Base exception for all FinLoom errors."""

    def __init__(self, message: str, context: Optional[dict] = None):
        self.message = message
        self.context = context or {}
        super().__init__(message)

    def __str__(self) -> str:
        if self.context:
            return f"{self.message} | Context: {self.context}"
        return self.message


# Ingestion Errors
class IngestionError(FinLoomError):
    """Error during data ingestion from SEC."""
    pass


class SECApiError(IngestionError):
    """Error communicating with SEC API."""
    pass


class RateLimitError(SECApiError):
    """Rate limited by SEC API."""

    def __init__(self, retry_after: Optional[float] = None):
        self.retry_after = retry_after
        message = f"Rate limited by SEC. Retry after: {retry_after}s" if retry_after else "Rate limited by SEC"
        super().__init__(message)


class DownloadError(IngestionError):
    """Error downloading a filing."""
    pass


# Parsing Errors
class ParsingError(FinLoomError):
    """Error during document parsing."""
    pass


class XBRLParsingError(ParsingError):
    """Error parsing XBRL data."""
    pass


class SectionParsingError(ParsingError):
    """Error parsing document sections."""
    pass


class TableParsingError(ParsingError):
    """Error parsing HTML tables."""
    pass


# Storage Errors
class StorageError(FinLoomError):
    """Error during data storage operations."""
    pass


class DatabaseError(StorageError):
    """Database operation error."""
    pass


class ConnectionError(StorageError):
    """Database connection error."""
    pass


# Validation Errors
class ValidationError(FinLoomError):
    """Data validation error."""
    pass


class SchemaValidationError(ValidationError):
    """Schema validation failed."""
    pass


class DataQualityError(ValidationError):
    """Data quality check failed."""
    pass


# Configuration Errors
class ConfigurationError(FinLoomError):
    """Configuration error."""
    pass


class MissingConfigError(ConfigurationError):
    """Required configuration is missing."""
    pass


# Processing Errors
class ProcessingError(FinLoomError):
    """Error during data processing."""
    pass


class PipelineError(ProcessingError):
    """Pipeline execution error."""
    pass


# Caching Errors
class CacheError(FinLoomError):
    """Error during cache operations."""
    pass


class CacheConnectionError(CacheError):
    """Cache connection error."""
    pass


# Monitoring Errors
class MonitoringError(FinLoomError):
    """Error during monitoring operations."""
    pass
