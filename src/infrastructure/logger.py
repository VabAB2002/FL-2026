"""
Logging setup for FinLoom SEC Data Pipeline.

Provides structured JSON logging with correlation IDs and standard logging configuration.
"""

import json
import logging
import logging.config
import logging.handlers
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml

from .config import get_project_root, get_settings

# Thread-safe correlation ID storage
correlation_id: ContextVar[str] = ContextVar('correlation_id', default=None)
request_id: ContextVar[str] = ContextVar('request_id', default=None)


class CorrelationIdFilter(logging.Filter):
    """
    Filter that adds correlation ID to log records.
    
    Correlation IDs help trace requests across the system.
    """
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation ID to record."""
        record.correlation_id = correlation_id.get() or 'none'
        record.request_id = request_id.get() or 'none'
        return True


def set_correlation_id(cid: Optional[str] = None) -> str:
    """
    Set correlation ID for current context.
    
    Args:
        cid: Correlation ID. If None, generates a new UUID.
    
    Returns:
        The correlation ID that was set.
    """
    new_id = cid or str(uuid.uuid4())
    correlation_id.set(new_id)
    return new_id


def get_correlation_id() -> Optional[str]:
    """Get current correlation ID."""
    return correlation_id.get()


def set_request_id(rid: Optional[str] = None) -> str:
    """
    Set request ID for current context.
    
    Args:
        rid: Request ID. If None, generates a new UUID.
    
    Returns:
        The request ID that was set.
    """
    new_id = rid or str(uuid.uuid4())
    request_id.set(new_id)
    return new_id


def get_request_id() -> Optional[str]:
    """Get current request ID."""
    return request_id.get()


def clear_context() -> None:
    """Clear correlation and request IDs."""
    correlation_id.set(None)
    request_id.set(None)


class JsonFormatter(logging.Formatter):
    """
    Enhanced JSON log formatter for structured logging.
    
    Outputs log records as JSON objects with correlation IDs for easy parsing and tracing.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "correlation_id": getattr(record, 'correlation_id', 'none'),
            "request_id": getattr(record, 'request_id', 'none'),
        }
        
        # Add OpenTelemetry trace context if available
        try:
            from opentelemetry import trace
            span = trace.get_current_span()
            if span and span.get_span_context().is_valid:
                ctx = span.get_span_context()
                log_data["trace_id"] = format(ctx.trace_id, '032x')
                log_data["span_id"] = format(ctx.span_id, '016x')
        except ImportError:
            pass  # OpenTelemetry not installed
        except Exception:
            pass  # Ignore tracing errors
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
            log_data["exception_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
        
        # Add extra fields
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        return json.dumps(log_data)


class ContextAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds context to log messages.
    
    Allows adding extra fields to log records for structured logging.
    """
    
    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        """Process log message and add extra context."""
        extra = kwargs.get("extra", {})
        extra.update(self.extra)
        
        # Store extra fields for JsonFormatter
        if "extra_fields" not in extra:
            extra["extra_fields"] = {}
        extra["extra_fields"].update(self.extra)
        
        kwargs["extra"] = extra
        return msg, kwargs


def setup_logging(
    config_path: Optional[str] = None,
    log_level: Optional[str] = None,
) -> None:
    """
    Set up logging configuration.
    
    Args:
        config_path: Path to logging config YAML file.
        log_level: Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    project_root = get_project_root()
    
    # Ensure logs directory exists
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Try to load YAML config
    if config_path is None:
        config_path = project_root / "config" / "logging.yaml"
    else:
        config_path = Path(config_path)
        if not config_path.is_absolute():
            config_path = project_root / config_path
    
    if config_path.exists():
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        
        # Update log file paths to be absolute
        for handler_name, handler_config in config.get("handlers", {}).items():
            if "filename" in handler_config:
                filename = handler_config["filename"]
                if not Path(filename).is_absolute():
                    handler_config["filename"] = str(project_root / filename)
        
        # Add correlation ID filter to all handlers
        for handler_name in config.get("handlers", {}).keys():
            if "filters" not in config["handlers"][handler_name]:
                config["handlers"][handler_name]["filters"] = []
            if "correlation_id" not in config["handlers"][handler_name]["filters"]:
                config["handlers"][handler_name]["filters"].append("correlation_id")
        
        # Add correlation ID filter definition
        if "filters" not in config:
            config["filters"] = {}
        config["filters"]["correlation_id"] = {
            "()": "src.infrastructure.logger.CorrelationIdFilter"
        }
        
        # Apply config
        logging.config.dictConfig(config)
    else:
        # Fallback to basic config with correlation IDs
        _setup_basic_logging(log_level or "INFO")
    
    # Override log level if specified
    if log_level:
        logging.getLogger().setLevel(getattr(logging, log_level.upper()))


def _setup_basic_logging(level: str) -> None:
    """Set up basic logging configuration as fallback."""
    project_root = get_project_root()
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Create correlation ID filter
    correlation_filter = CorrelationIdFilter()
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s")
    )
    console_handler.addFilter(correlation_filter)
    
    file_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "finloom.log",
        maxBytes=10485760,  # 10MB
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonFormatter())
    file_handler.addFilter(correlation_filter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(
    name: str,
    context: Optional[dict[str, Any]] = None,
) -> logging.Logger | ContextAdapter:
    """
    Get a logger instance.
    
    Args:
        name: Logger name (typically module name).
        context: Optional context dict to add to all log messages.
    
    Returns:
        Logger instance, optionally wrapped with ContextAdapter.
    """
    logger = logging.getLogger(name)
    
    if context:
        return ContextAdapter(logger, context)
    
    return logger


def log_operation(
    logger: logging.Logger,
    operation: str,
    success: bool,
    duration_ms: Optional[float] = None,
    **kwargs: Any,
) -> None:
    """
    Log an operation result with structured data.
    
    Args:
        logger: Logger instance.
        operation: Name of the operation.
        success: Whether operation succeeded.
        duration_ms: Operation duration in milliseconds.
        **kwargs: Additional context to log.
    """
    extra_fields = {
        "operation": operation,
        "success": success,
    }
    if duration_ms is not None:
        extra_fields["duration_ms"] = duration_ms
    extra_fields.update(kwargs)
    
    level = logging.INFO if success else logging.ERROR
    message = f"Operation '{operation}' {'succeeded' if success else 'failed'}"
    
    logger.log(level, message, extra={"extra_fields": extra_fields})
