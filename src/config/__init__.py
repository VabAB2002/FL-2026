"""
Configuration module - Backward Compatibility Layer.

DEPRECATED: This module is deprecated. Use src.utils.config instead.

The primary configuration API is now in src.utils.config.
This module provides backward-compatible imports for existing code.

Preferred usage:
    from src.utils.config import get_config
    config = get_config()

Legacy usage (still works but deprecated):
    from src.config import get_env_config  # Don't use this
    config = get_env_config()
"""

import warnings

# Import from unified config for backward compatibility
from ..utils.config import (
    AppConfig,
    Environment,
    get_config,
)

# Legacy imports from env_config (will be deprecated)
from .env_config import EnvironmentConfig, get_env_config, is_development, is_production

# Emit deprecation warning when importing legacy functions
warnings.warn(
    "Importing from src.config is deprecated. Use 'from src.utils.config import get_config' instead.",
    DeprecationWarning,
    stacklevel=2
)

__all__ = [
    # New unified API
    "get_config",
    "AppConfig",
    "Environment",
    # Legacy (deprecated)
    "EnvironmentConfig",
    "get_env_config",
    "is_production",
    "is_development",
]
