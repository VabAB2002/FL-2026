"""
Environment-specific configuration management.

Supports development, staging, and production environments.
"""

import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from ..utils.logger import get_logger

logger = get_logger("finloom.config.env_config")


class Environment(Enum):
    """Environment types."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"


class EnvironmentConfig:
    """
    Environment-specific configuration manager.
    
    Loads base config and overlays environment-specific settings.
    """
    
    def __init__(self, env: Optional[str] = None):
        """
        Initialize environment config.
        
        Args:
            env: Environment name (development, staging, production).
                 Defaults to FINLOOM_ENV or 'development'.
        """
        self.env_name = env or os.getenv('FINLOOM_ENV', 'development')
        try:
            self.environment = Environment(self.env_name)
        except ValueError:
            logger.warning(f"Unknown environment '{self.env_name}', using development")
            self.environment = Environment.DEVELOPMENT
        
        self.config: Dict[str, Any] = {}
        self._load_config()
        
        logger.info(f"Environment config loaded: {self.environment.value}")
    
    def _load_config(self):
        """Load configuration files."""
        # Find config directory
        config_dir = Path(__file__).parent.parent.parent / "config"
        
        # Load base config
        base_config_path = config_dir / "settings.yaml"
        if base_config_path.exists():
            with open(base_config_path) as f:
                self.config = yaml.safe_load(f) or {}
            logger.debug(f"Loaded base config: {base_config_path}")
        
        # Load environment-specific config
        env_config_path = config_dir / f"settings.{self.environment.value}.yaml"
        if env_config_path.exists():
            with open(env_config_path) as f:
                env_config = yaml.safe_load(f) or {}
            
            # Deep merge environment config
            self._deep_merge(self.config, env_config)
            logger.debug(f"Loaded environment config: {env_config_path}")
        else:
            logger.debug(f"No environment-specific config found: {env_config_path}")
        
        # Override with environment variables
        self._apply_env_overrides()
    
    def _deep_merge(self, base: Dict, overlay: Dict):
        """Deep merge overlay dict into base dict."""
        for key, value in overlay.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
    
    def _apply_env_overrides(self):
        """Apply environment variable overrides."""
        # Database path
        if db_path := os.getenv('FINLOOM_DB_PATH'):
            self._set_nested(self.config, 'storage.database_path', db_path)
        
        # S3 bucket
        if s3_bucket := os.getenv('FINLOOM_S3_BUCKET'):
            self._set_nested(self.config, 'storage.s3.bucket_name', s3_bucket)
        
        # S3 region
        if s3_region := os.getenv('FINLOOM_S3_REGION'):
            self._set_nested(self.config, 'storage.s3.region', s3_region)
        
        # Log level
        if log_level := os.getenv('FINLOOM_LOG_LEVEL'):
            self._set_nested(self.config, 'logging.level', log_level)
        
        # Rate limit
        if rate_limit := os.getenv('FINLOOM_SEC_RATE_LIMIT'):
            self._set_nested(self.config, 'sec_api.rate_limit', float(rate_limit))
        
        logger.debug("Applied environment variable overrides")
    
    def _set_nested(self, d: Dict, path: str, value: Any):
        """Set nested dictionary value using dot notation."""
        keys = path.split('.')
        for key in keys[:-1]:
            d = d.setdefault(key, {})
        d[keys[-1]] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value.
        
        Args:
            key: Config key in dot notation (e.g., 'storage.database_path').
            default: Default value if key not found.
        
        Returns:
            Configuration value.
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == Environment.PRODUCTION
    
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == Environment.DEVELOPMENT
    
    def is_staging(self) -> bool:
        """Check if running in staging."""
        return self.environment == Environment.STAGING
    
    def get_feature_flags(self) -> Dict[str, bool]:
        """Get environment-specific feature flags."""
        # Default feature flags
        flags = {
            "async_downloads": True,
            "section_extraction": True,
            "table_extraction": False,
            "sentiment_analysis": False,
            "real_time_updates": False,
            "caching_enabled": False,
        }
        
        # Production overrides
        if self.is_production():
            flags.update({
                "async_downloads": True,
                "section_extraction": True,
                "caching_enabled": True,
            })
        
        # Development overrides
        if self.is_development():
            flags.update({
                "async_downloads": False,  # Easier debugging
                "section_extraction": True,
            })
        
        # Check config overrides
        if config_flags := self.get('features'):
            flags.update(config_flags)
        
        return flags
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration."""
        return {
            "path": self.get('storage.database_path', 'data/finloom.duckdb'),
            "pool_size": self.get('database.pool_size', 5 if self.is_production() else 2),
            "timeout": self.get('database.timeout', 30),
            "wal_enabled": self.get('database.wal_enabled', True),
        }
    
    def get_monitoring_config(self) -> Dict[str, Any]:
        """Get monitoring configuration."""
        return {
            "metrics_enabled": self.get('monitoring.metrics_enabled', True),
            "metrics_port": self.get('monitoring.metrics_port', 9090),
            "health_port": self.get('monitoring.health_port', 8000),
            "alerts_enabled": self.get('monitoring.alerts_enabled', self.is_production()),
        }
    
    def get_sec_api_config(self) -> Dict[str, Any]:
        """Get SEC API configuration."""
        return {
            "rate_limit": self.get('sec_api.rate_limit', 8.0 if self.is_production() else 5.0),
            "timeout": self.get('sec_api.timeout', 30),
            "max_retries": self.get('sec_api.max_retries', 3),
            "user_agent": self.get('sec_api.user_agent', 'FinLoom Data Pipeline'),
        }
    
    def validate_config(self) -> List[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors (empty if valid).
        """
        errors = []
        
        # Required fields
        required = [
            'storage.database_path',
            'sec_api.rate_limit',
        ]
        
        for field in required:
            if self.get(field) is None:
                errors.append(f"Missing required config: {field}")
        
        # Production-specific validation
        if self.is_production():
            if not self.get('storage.s3.bucket_name'):
                errors.append("Production requires S3 bucket configuration")
            
            if not self.get('monitoring.alerts_enabled'):
                errors.append("Production should have alerts enabled")
        
        # Rate limit validation
        rate_limit = self.get('sec_api.rate_limit', 0)
        if rate_limit <= 0 or rate_limit > 10:
            errors.append(f"Invalid SEC rate limit: {rate_limit} (must be 0-10)")
        
        return errors


# Global instance
_env_config: Optional[EnvironmentConfig] = None


def get_env_config() -> EnvironmentConfig:
    """Get or create global environment config."""
    global _env_config
    if _env_config is None:
        _env_config = EnvironmentConfig()
    return _env_config


def get_current_environment() -> Environment:
    """Get current environment."""
    return get_env_config().environment


def is_production() -> bool:
    """Check if running in production."""
    return get_env_config().is_production()


def is_development() -> bool:
    """Check if running in development."""
    return get_env_config().is_development()
