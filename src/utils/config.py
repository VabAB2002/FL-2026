"""
Unified configuration management for FinLoom SEC Data Pipeline.

This module provides:
- Environment-aware configuration (development, staging, production)
- YAML config loading with environment-specific overlays
- Environment variable overrides
- Pydantic models for type-safe access
- Feature flags management

Usage:
    from src.utils.config import get_config

    config = get_config()
    db_path = config.database_path
    is_prod = config.is_production
    flags = config.feature_flags
"""

import os
from enum import Enum
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# =============================================================================
# Environment Definition
# =============================================================================

class Environment(Enum):
    """Environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


# =============================================================================
# Pydantic Config Models (for type-safe access)
# =============================================================================

class CompanyConfig(BaseModel):
    """Configuration for a target company."""
    cik: str
    name: str
    ticker: str


class ExtractionConfig(BaseModel):
    """Configuration for data extraction."""
    form_types: list[str] = Field(default=["10-K"])
    start_year: int = Field(default=2014)
    end_year: int = Field(default=2024)
    sections: list[str] = Field(default=["item_1", "item_1a", "item_7", "item_8", "item_9a"])
    extract_all_xbrl_facts: bool = Field(default=True)


class SECApiConfig(BaseModel):
    """Configuration for SEC API access."""
    base_url: str = Field(default="https://www.sec.gov")
    edgar_base_url: str = Field(default="https://www.sec.gov/cgi-bin/browse-edgar")
    submissions_url: str = Field(default="https://data.sec.gov/submissions")
    rate_limit_per_second: float = Field(default=8.0)
    request_delay: float = Field(default=0.15)
    max_retries: int = Field(default=3)
    retry_delay_base: float = Field(default=2.0)
    user_agent: str = Field(default="FinLoom Data Pipeline")
    timeout: int = Field(default=30)


class S3Config(BaseModel):
    """Configuration for S3 backup."""
    bucket_name: str = Field(default="finloom-sec-data")
    raw_prefix: str = Field(default="raw/")
    processed_prefix: str = Field(default="processed/")
    database_prefix: str = Field(default="database/")
    region: str = Field(default="us-east-1")


class StorageConfig(BaseModel):
    """Configuration for data storage."""
    raw_data_path: str = Field(default="data/raw")
    processed_data_path: str = Field(default="data/processed")
    database_path: str = Field(default="data/database/finloom.duckdb")
    s3: S3Config = Field(default_factory=S3Config)


class DatabaseConfig(BaseModel):
    """Configuration for database."""
    pool_size: int = Field(default=2)
    timeout: int = Field(default=30)
    wal_enabled: bool = Field(default=True)


class ProcessingConfig(BaseModel):
    """Configuration for data processing."""
    max_workers: int = Field(default=4)
    batch_size: int = Field(default=100)
    enable_checkpoints: bool = Field(default=True)
    checkpoint_path: str = Field(default="data/checkpoints")


class LoggingConfig(BaseModel):
    """Configuration for logging."""
    level: str = Field(default="INFO")
    log_path: str = Field(default="logs")
    max_log_files: int = Field(default=30)
    log_format: str = Field(default="json")


class MonitoringConfig(BaseModel):
    """Configuration for monitoring."""
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9090)
    health_port: int = Field(default=8000)
    alerts_enabled: bool = Field(default=False)


class Neo4jConfig(BaseModel):
    """Configuration for Neo4j graph database."""
    uri: str = Field(default="bolt://localhost:7687")
    user: str = Field(default="neo4j")
    password: str = Field(default="finloom123")
    database: str = Field(default="neo4j")
    max_connection_pool_size: int = Field(default=50)
    connection_timeout: int = Field(default=30)
    max_transaction_retry_time: int = Field(default=30)


class Settings(BaseModel):
    """Main settings container."""
    companies: list[CompanyConfig] = Field(default_factory=list)
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)
    sec_api: SECApiConfig = Field(default_factory=SECApiConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    neo4j: Neo4jConfig = Field(default_factory=Neo4jConfig)
    features: dict[str, bool] = Field(default_factory=dict)


class EnvSettings(BaseSettings):
    """Environment variable settings."""
    sec_api_user_agent: str = Field(default="FinLoom contact@example.com")
    aws_access_key_id: Optional[str] = Field(default=None)
    aws_secret_access_key: Optional[str] = Field(default=None)
    aws_default_region: str = Field(default="us-east-1")
    s3_bucket_name: str = Field(default="finloom-sec-data")
    max_workers: int = Field(default=4)
    rate_limit_per_sec: float = Field(default=8.0)
    config_path: str = Field(default="config/settings.yaml")
    database_path: Optional[str] = Field(default=None)
    finloom_env: str = Field(default="development")
    log_level: Optional[str] = Field(default=None)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


# =============================================================================
# Unified AppConfig Class
# =============================================================================

class AppConfig:
    """
    Unified configuration manager for FinLoom.

    Combines:
    - Environment-aware configuration (dev/staging/prod)
    - YAML config loading with environment overlays
    - Environment variable overrides
    - Type-safe Pydantic settings

    Usage:
        config = get_config()
        print(config.database_path)
        print(config.is_production)
        print(config.feature_flags)
    """

    def __init__(self, env: Optional[str] = None):
        """
        Initialize unified configuration.

        Args:
            env: Environment name. Defaults to FINLOOM_ENV or 'development'.
        """
        # Determine environment
        self._env_name = env or os.getenv("FINLOOM_ENV", "development")
        try:
            self._environment = Environment(self._env_name)
        except ValueError:
            self._environment = Environment.DEVELOPMENT

        # Load environment variables
        self._env_settings = EnvSettings()

        # Raw config dict (for dot-notation access)
        self._config: dict[str, Any] = {}

        # Load and merge configs
        self._load_config()

        # Create typed settings object
        self._settings = self._create_settings()

    def _load_config(self) -> None:
        """Load configuration files with environment overlay."""
        config_dir = get_project_root() / "config"

        # Load base config
        base_path = config_dir / "settings.yaml"
        if base_path.exists():
            with open(base_path) as f:
                self._config = yaml.safe_load(f) or {}

        # Load environment-specific config and merge
        env_path = config_dir / f"settings.{self._environment.value}.yaml"
        if env_path.exists():
            with open(env_path) as f:
                env_config = yaml.safe_load(f) or {}
            self._deep_merge(self._config, env_config)

        # Apply environment variable overrides
        self._apply_env_overrides()

    def _deep_merge(self, base: dict, overlay: dict) -> None:
        """Deep merge overlay dict into base dict."""
        for key, value in overlay.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        # Database path
        if db_path := os.getenv("FINLOOM_DB_PATH"):
            self._set_nested("storage.database_path", db_path)
        elif self._env_settings.database_path:
            self._set_nested("storage.database_path", self._env_settings.database_path)

        # S3 bucket
        if s3_bucket := os.getenv("FINLOOM_S3_BUCKET"):
            self._set_nested("storage.s3.bucket_name", s3_bucket)

        # S3 region
        if s3_region := os.getenv("FINLOOM_S3_REGION"):
            self._set_nested("storage.s3.region", s3_region)

        # Log level
        if log_level := os.getenv("FINLOOM_LOG_LEVEL") or self._env_settings.log_level:
            self._set_nested("logging.level", log_level)

        # Rate limit
        if rate_limit := os.getenv("FINLOOM_SEC_RATE_LIMIT"):
            self._set_nested("sec_api.rate_limit_per_second", float(rate_limit))

        # Max workers
        if self._env_settings.max_workers:
            self._set_nested("processing.max_workers", self._env_settings.max_workers)

        # Neo4j configuration
        if neo4j_uri := os.getenv("NEO4J_URI"):
            self._set_nested("neo4j.uri", neo4j_uri)
        
        if neo4j_user := os.getenv("NEO4J_USER"):
            self._set_nested("neo4j.user", neo4j_user)
        
        if neo4j_password := os.getenv("NEO4J_PASSWORD"):
            self._set_nested("neo4j.password", neo4j_password)
        
        if neo4j_database := os.getenv("NEO4J_DATABASE"):
            self._set_nested("neo4j.database", neo4j_database)

    def _set_nested(self, path: str, value: Any) -> None:
        """Set nested dictionary value using dot notation."""
        keys = path.split(".")
        d = self._config
        for key in keys[:-1]:
            d = d.setdefault(key, {})
        d[keys[-1]] = value

    def _create_settings(self) -> Settings:
        """Create typed Settings object from config dict."""
        return Settings(**self._config)

    # -------------------------------------------------------------------------
    # Public Properties
    # -------------------------------------------------------------------------

    @property
    def environment(self) -> Environment:
        """Current environment."""
        return self._environment

    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self._environment == Environment.PRODUCTION

    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self._environment == Environment.DEVELOPMENT

    @property
    def is_staging(self) -> bool:
        """Check if running in staging."""
        return self._environment == Environment.STAGING

    @property
    def is_test(self) -> bool:
        """Check if running in test."""
        return self._environment == Environment.TEST

    @property
    def settings(self) -> Settings:
        """Get typed settings object."""
        return self._settings

    @property
    def database_path(self) -> Path:
        """Get absolute database path."""
        return get_absolute_path(self._settings.storage.database_path)

    @property
    def raw_data_path(self) -> Path:
        """Get absolute raw data path."""
        return get_absolute_path(self._settings.storage.raw_data_path)

    @property
    def processed_data_path(self) -> Path:
        """Get absolute processed data path."""
        return get_absolute_path(self._settings.storage.processed_data_path)

    @property
    def feature_flags(self) -> dict[str, bool]:
        """Get feature flags with environment-specific defaults."""
        # Default flags
        flags = {
            "extract_all_xbrl_facts": self._settings.extraction.extract_all_xbrl_facts,
            "async_downloads": True,
            "section_extraction": True,
            "table_extraction": False,
            "sentiment_analysis": False,
            "real_time_updates": False,
            "caching_enabled": False,
        }

        # Environment-specific defaults
        if self.is_production:
            flags.update({
                "async_downloads": True,
                "caching_enabled": True,
            })
        elif self.is_development:
            flags.update({
                "async_downloads": False,  # Easier debugging
            })

        # Config overrides
        if self._settings.features:
            flags.update(self._settings.features)

        return flags

    # -------------------------------------------------------------------------
    # Access Methods
    # -------------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key: Config key (e.g., 'storage.database_path').
            default: Default value if not found.

        Returns:
            Configuration value.
        """
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_database_config(self) -> dict[str, Any]:
        """Get database configuration dict."""
        return {
            "path": str(self.database_path),
            "pool_size": self._settings.database.pool_size if self.is_production else 2,
            "timeout": self._settings.database.timeout,
            "wal_enabled": self._settings.database.wal_enabled,
        }

    def get_monitoring_config(self) -> dict[str, Any]:
        """Get monitoring configuration dict."""
        return {
            "metrics_enabled": self._settings.monitoring.metrics_enabled,
            "metrics_port": self._settings.monitoring.metrics_port,
            "health_port": self._settings.monitoring.health_port,
            "alerts_enabled": self._settings.monitoring.alerts_enabled or self.is_production,
        }

    def get_sec_api_config(self) -> dict[str, Any]:
        """Get SEC API configuration dict."""
        return {
            "rate_limit": self._settings.sec_api.rate_limit_per_second,
            "timeout": self._settings.sec_api.timeout,
            "max_retries": self._settings.sec_api.max_retries,
            "user_agent": self._settings.sec_api.user_agent,
        }

    def get_neo4j_config(self) -> dict[str, Any]:
        """Get Neo4j configuration dict."""
        return {
            "uri": self._settings.neo4j.uri,
            "user": self._settings.neo4j.user,
            "password": self._settings.neo4j.password,
            "database": self._settings.neo4j.database,
            "max_connection_pool_size": self._settings.neo4j.max_connection_pool_size,
            "connection_timeout": self._settings.neo4j.connection_timeout,
            "max_transaction_retry_time": self._settings.neo4j.max_transaction_retry_time,
        }

    def validate(self) -> list[str]:
        """
        Validate configuration.

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        # Rate limit validation
        rate_limit = self._settings.sec_api.rate_limit_per_second
        if rate_limit <= 0 or rate_limit > 10:
            errors.append(f"Invalid SEC rate limit: {rate_limit} (must be 0-10)")

        # Production-specific validation
        if self.is_production:
            if not self._settings.storage.s3.bucket_name:
                errors.append("Production requires S3 bucket configuration")

        return errors


# =============================================================================
# Global Instances and Accessor Functions
# =============================================================================

_config: Optional[AppConfig] = None
_settings: Optional[Settings] = None
_env_settings: Optional[EnvSettings] = None


def get_project_root() -> Path:
    """Get the project root directory."""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "config").exists() and (parent / "src").exists():
            return parent
    return Path.cwd()


def get_absolute_path(relative_path: str) -> Path:
    """
    Convert a relative path to absolute path from project root.

    Args:
        relative_path: Path relative to project root.

    Returns:
        Absolute Path object.
    """
    project_root = get_project_root()
    path = Path(relative_path)
    if path.is_absolute():
        return path
    return project_root / path


def get_config(env: Optional[str] = None) -> AppConfig:
    """
    Get the unified configuration instance.

    This is the primary way to access configuration.

    Args:
        env: Optional environment override.

    Returns:
        AppConfig instance.
    """
    global _config
    if _config is None or env is not None:
        _config = AppConfig(env=env)
    return _config


# =============================================================================
# Backward Compatibility Functions
# =============================================================================

def load_config(config_path: Optional[str] = None) -> Settings:
    """
    Load configuration from YAML file.

    DEPRECATED: Use get_config().settings instead.
    """
    return get_config().settings


def get_settings() -> Settings:
    """
    Get the current settings instance.

    DEPRECATED: Use get_config().settings instead.
    """
    return get_config().settings


def get_env_settings() -> EnvSettings:
    """
    Get environment settings.

    DEPRECATED: Use get_config() instead.
    """
    global _env_settings
    if _env_settings is None:
        _env_settings = EnvSettings()
    return _env_settings
