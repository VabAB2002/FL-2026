"""
Configuration management for FinLoom SEC Data Pipeline.

Loads settings from YAML config files and environment variables.
"""

import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


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


class SECApiConfig(BaseModel):
    """Configuration for SEC API access."""
    base_url: str = Field(default="https://www.sec.gov")
    edgar_base_url: str = Field(default="https://www.sec.gov/cgi-bin/browse-edgar")
    submissions_url: str = Field(default="https://data.sec.gov/submissions")
    rate_limit_per_second: float = Field(default=8.0)
    request_delay: float = Field(default=0.15)
    max_retries: int = Field(default=3)
    retry_delay_base: float = Field(default=2.0)


class S3Config(BaseModel):
    """Configuration for S3 backup."""
    bucket_name: str = Field(default="finloom-sec-data")
    raw_prefix: str = Field(default="raw/")
    processed_prefix: str = Field(default="processed/")
    database_prefix: str = Field(default="database/")


class StorageConfig(BaseModel):
    """Configuration for data storage."""
    raw_data_path: str = Field(default="data/raw")
    processed_data_path: str = Field(default="data/processed")
    database_path: str = Field(default="data/database/finloom.duckdb")
    s3: S3Config = Field(default_factory=S3Config)


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


class Settings(BaseModel):
    """Main settings container."""
    companies: list[CompanyConfig] = Field(default_factory=list)
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)
    sec_api: SECApiConfig = Field(default_factory=SECApiConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


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

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


# Global settings instance
_settings: Optional[Settings] = None
_env_settings: Optional[EnvSettings] = None


def get_project_root() -> Path:
    """Get the project root directory."""
    # Navigate up from this file to find project root
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "config").exists() and (parent / "src").exists():
            return parent
    # Fallback to current working directory
    return Path.cwd()


def load_config(config_path: Optional[str] = None) -> Settings:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Optional path to config file. If not provided,
                     uses environment variable or default path.
    
    Returns:
        Settings object with loaded configuration.
    """
    global _settings, _env_settings
    
    # Load environment settings
    if _env_settings is None:
        _env_settings = EnvSettings()
    
    # Determine config path
    if config_path is None:
        config_path = _env_settings.config_path
    
    # Make path absolute relative to project root
    project_root = get_project_root()
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = project_root / config_file
    
    # Load YAML config
    if config_file.exists():
        with open(config_file, "r") as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {}
    
    # Create settings from config data
    _settings = Settings(**config_data)
    
    # Override with environment variables where applicable
    if _env_settings.database_path:
        _settings.storage.database_path = _env_settings.database_path
    if _env_settings.s3_bucket_name:
        _settings.storage.s3.bucket_name = _env_settings.s3_bucket_name
    if _env_settings.max_workers:
        _settings.processing.max_workers = _env_settings.max_workers
    if _env_settings.rate_limit_per_sec:
        _settings.sec_api.rate_limit_per_second = _env_settings.rate_limit_per_sec
    
    return _settings


def get_settings() -> Settings:
    """
    Get the current settings instance.
    
    Loads config if not already loaded.
    
    Returns:
        Settings object.
    """
    global _settings
    if _settings is None:
        load_config()
    return _settings


def get_env_settings() -> EnvSettings:
    """
    Get environment settings.
    
    Returns:
        EnvSettings object.
    """
    global _env_settings
    if _env_settings is None:
        _env_settings = EnvSettings()
    return _env_settings


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
