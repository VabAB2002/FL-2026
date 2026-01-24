"""
Logging setup for FinLoom SEC Data Pipeline.

Provides structured JSON logging and standard logging configuration.
"""

import json
import logging
import logging.config
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml

from .config import get_project_root, get_settings


class JsonFormatter(logging.Formatter):
    """
    JSON log formatter for structured logging.
    
    Outputs log records as JSON objects for easy parsing and analysis.
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
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
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
        
        # Apply config
        logging.config.dictConfig(config)
    else:
        # Fallback to basic config
        _setup_basic_logging(log_level or "INFO")
    
    # Override log level if specified
    if log_level:
        logging.getLogger().setLevel(getattr(logging, log_level.upper()))


def _setup_basic_logging(level: str) -> None:
    """Set up basic logging configuration as fallback."""
    project_root = get_project_root()
    logs_dir = project_root / "logs"
    logs_dir.mkdir(exist_ok=True)
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    
    file_handler = logging.handlers.RotatingFileHandler(
        logs_dir / "finloom.log",
        maxBytes=10485760,  # 10MB
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonFormatter())
    
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
