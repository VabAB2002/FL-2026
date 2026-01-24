"""Tests for configuration management."""

import pytest
from pathlib import Path

from src.utils.config import (
    Company,
    Settings,
    get_project_root,
    load_config,
    get_settings,
)


def test_get_project_root():
    """Test project root detection."""
    root = get_project_root()
    assert root.exists()
    assert (root / "src").exists() or (root / "config").exists()


def test_company_model():
    """Test Company configuration model."""
    company = Company(
        cik="0000320193",
        name="Apple Inc",
        ticker="AAPL",
    )
    assert company.cik == "0000320193"
    assert company.name == "Apple Inc"
    assert company.ticker == "AAPL"


def test_settings_defaults():
    """Test Settings default values."""
    settings = Settings()
    assert settings.extraction.form_types == ["10-K"]
    assert settings.extraction.start_year == 2014
    assert settings.extraction.end_year == 2024
    assert settings.sec_api.rate_limit_per_second == 8.0


def test_load_config():
    """Test loading configuration from file."""
    settings = load_config()
    assert settings is not None
    assert len(settings.companies) > 0
    assert settings.companies[0].ticker is not None


def test_get_settings_caches():
    """Test that get_settings caches result."""
    settings1 = get_settings()
    settings2 = get_settings()
    assert settings1 is settings2
