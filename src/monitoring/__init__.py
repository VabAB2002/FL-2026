"""
Prometheus metrics exporter for FinLoom.

Provides business and system metrics for monitoring and alerting.

IMPORTANT: This module is imported by src.processing.unstructured_pipeline.
DO NOT import anything from src.processing here to avoid circular dependencies.
The correct pattern is: processing modules USE metrics defined here.
"""

import time
from functools import wraps
from typing import Any, Callable, Optional

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Info,
    start_http_server,
)

from ..utils.logger import get_logger

logger = get_logger("finloom.monitoring.metrics")


# =============================================================================
# Business Metrics
# =============================================================================

# Filing metrics
filings_downloaded = Counter(
    'finloom_filings_downloaded_total',
    'Total filings downloaded',
    ['company_ticker', 'form_type', 'status']
)

filings_processed = Counter(
    'finloom_filings_processed_total',
    'Total filings processed',
    ['company_ticker', 'form_type', 'stage', 'status']
)

# XBRL metrics
facts_extracted = Counter(
    'finloom_facts_extracted_total',
    'Total XBRL facts extracted',
    ['company_ticker', 'concept_namespace']
)

normalized_metrics_created = Counter(
    'finloom_normalized_metrics_total',
    'Total normalized metrics created',
    ['company_ticker', 'metric_id']
)

# =============================================================================
# Performance Metrics
# =============================================================================

download_duration = Histogram(
    'finloom_download_duration_seconds',
    'Time to download filing',
    ['company_ticker', 'form_type'],
    buckets=(1, 5, 10, 30, 60, 120, 300)
)

parse_duration = Histogram(
    'finloom_parse_duration_seconds',
    'Time to parse filing',
    ['parser_type'],
    buckets=(0.1, 0.5, 1, 5, 10, 30, 60)
)

normalization_duration = Histogram(
    'finloom_normalization_duration_seconds',
    'Time to normalize filing',
    ['company_ticker'],
    buckets=(0.1, 0.5, 1, 5, 10)
)

# =============================================================================
# System Health Metrics
# =============================================================================

database_size_bytes = Gauge(
    'finloom_database_size_bytes',
    'Total database size in bytes'
)

database_query_duration = Histogram(
    'finloom_database_query_duration_seconds',
    'Database query execution time',
    ['query_type'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0)
)

# =============================================================================
# Error Metrics
# =============================================================================

pipeline_errors = Counter(
    'finloom_pipeline_errors_total',
    'Total pipeline errors',
    ['stage', 'error_type']
)

validation_failures = Counter(
    'finloom_validation_failures_total',
    'Total validation failures',
    ['validation_type', 'severity']
)

# =============================================================================
# API Metrics
# =============================================================================

sec_api_requests = Counter(
    'finloom_sec_api_requests_total',
    'Total SEC API requests',
    ['endpoint', 'status']
)

sec_api_latency = Histogram(
    'finloom_sec_api_latency_seconds',
    'SEC API request latency',
    ['endpoint'],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30)
)

rate_limit_hits = Counter(
    'finloom_rate_limit_hits_total',
    'Total rate limit hits',
    ['api']
)

# =============================================================================
# Circuit Breaker Metrics
# =============================================================================

circuit_breaker_state = Gauge(
    'finloom_circuit_breaker_state',
    'Circuit breaker state (0=closed, 1=open, 2=half-open)',
    ['name']
)

circuit_breaker_failures = Counter(
    'finloom_circuit_breaker_failures_total',
    'Circuit breaker failures',
    ['name']
)

# =============================================================================
# Data Quality Metrics
# =============================================================================

data_quality_score = Gauge(
    'finloom_data_quality_score',
    'Data quality score (0-100)',
    ['company_ticker', 'accession_number']
)

duplicate_records = Gauge(
    'finloom_duplicate_records',
    'Number of duplicate records detected',
    ['table_name']
)

# =============================================================================
# Unstructured Data Extraction Metrics
# =============================================================================

unstructured_sections_extracted = Counter(
    'finloom_unstructured_sections_extracted_total',
    'Total sections extracted from filings',
    ['accession']
)

unstructured_tables_extracted = Counter(
    'finloom_unstructured_tables_extracted_total',
    'Total tables extracted from filings',
    ['accession']
)

unstructured_footnotes_extracted = Counter(
    'finloom_unstructured_footnotes_extracted_total',
    'Total footnotes extracted from filings',
    ['accession']
)

# unstructured_chunks_created = Counter(
#     'finloom_unstructured_chunks_created_total',
#     'Total semantic chunks created for RAG',
#     ['accession']
# )  # DISABLED: Chunking not implemented yet

unstructured_quality_score = Gauge(
    'finloom_unstructured_quality_score',
    'Unstructured data extraction quality score (0-100)',
    ['accession']
)

unstructured_extraction_errors = Counter(
    'finloom_unstructured_extraction_errors_total',
    'Errors during unstructured extraction',
    ['type', 'accession']
)

unstructured_processing_time = Histogram(
    'finloom_unstructured_processing_time_seconds',
    'Time to process unstructured data for filing',
    ['accession'],
    buckets=(1, 5, 10, 30, 60, 120, 300)
)

# =============================================================================
# System Info
# =============================================================================

system_info = Info('finloom_system', 'System information')


# =============================================================================
# Decorators for Automatic Instrumentation
# =============================================================================

def track_operation(metric_name: str, labels: Optional[dict] = None):
    """
    Decorator to track operation metrics.
    
    Args:
        metric_name: Name of metric to track (download, parse, normalize).
        labels: Dict of label names/values.
    
    Usage:
        @track_operation('download', {'company_ticker': 'AAPL'})
        def download_filing(filing):
            ...
    """
    labels = labels or {}
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            error_occurred = None
            
            try:
                result = func(*args, **kwargs)
                
                # Record success
                duration = time.time() - start_time
                
                if metric_name == 'download':
                    download_duration.labels(**labels).observe(duration)
                    filings_downloaded.labels(**labels, status='success').inc()
                elif metric_name == 'parse':
                    parse_duration.labels(parser_type=labels.get('parser_type', 'unknown')).observe(duration)
                elif metric_name == 'normalize':
                    normalization_duration.labels(**labels).observe(duration)
                
                return result
                
            except Exception as e:
                error_occurred = e
                # Record failure
                error_type = type(e).__name__
                pipeline_errors.labels(
                    stage=metric_name,
                    error_type=error_type
                ).inc()
                
                if metric_name == 'download':
                    filings_downloaded.labels(**labels, status='failure').inc()
                
                raise
            finally:
                if error_occurred is None:
                    logger.debug(f"{func.__name__} completed in {time.time() - start_time:.3f}s")
        
        return wrapper
    return decorator


def track_api_call(endpoint: str):
    """
    Decorator to track API calls.
    
    Usage:
        @track_api_call('sec_submissions')
        def get_submissions(cik):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                
                # Record success
                duration = time.time() - start_time
                sec_api_requests.labels(endpoint=endpoint, status='success').inc()
                sec_api_latency.labels(endpoint=endpoint).observe(duration)
                
                return result
                
            except Exception as e:
                # Record failure
                sec_api_requests.labels(endpoint=endpoint, status='failure').inc()
                raise
        
        return wrapper
    return decorator


# =============================================================================
# Metrics Server
# =============================================================================

_metrics_server_started = False


def start_metrics_server(port: int = 9090, addr: str = '0.0.0.0') -> None:
    """
    Start Prometheus metrics HTTP server.
    
    Args:
        port: Port to listen on.
        addr: Address to bind to.
    """
    global _metrics_server_started
    
    if _metrics_server_started:
        logger.warning("Metrics server already started")
        return
    
    try:
        start_http_server(port, addr=addr)
        _metrics_server_started = True
        logger.info(f"Metrics server started on http://{addr}:{port}/metrics")
        
        # Set system info
        system_info.info({
            'version': '1.0.0',
            'python_version': '3.13',
            'database': 'DuckDB'
        })
        
    except Exception as e:
        logger.error(f"Failed to start metrics server: {e}")
        raise


def update_database_size(size_bytes: int) -> None:
    """Update database size metric."""
    database_size_bytes.set(size_bytes)


def record_circuit_breaker_state(name: str, state: str) -> None:
    """
    Record circuit breaker state.
    
    Args:
        name: Circuit breaker name.
        state: State (closed, open, half_open).
    """
    state_value = {
        'closed': 0,
        'open': 1,
        'half_open': 2
    }.get(state, -1)
    
    circuit_breaker_state.labels(name=name).set(state_value)


# =============================================================================
# Health Checker Exports
# =============================================================================

from .health_checker import (
    DatabaseHealthChecker,
    DuplicateReport,
    IntegrityReport,
    CompletenessReport,
    HealthReport,
)

__all__ = [
    # Metrics
    'filings_downloaded',
    'filings_processed',
    'facts_extracted',
    'database_size_bytes',
    'pipeline_errors',
    'duplicate_records',
    # Functions
    'track_operation',
    'track_api_call',
    'start_metrics_server',
    'update_database_size',
    'record_circuit_breaker_state',
    # Health checker
    'DatabaseHealthChecker',
    'DuplicateReport',
    'IntegrityReport',
    'CompletenessReport',
    'HealthReport',
]
