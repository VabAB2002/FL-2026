"""Pytest configuration and fixtures."""

import os
import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up environment for testing
os.environ.setdefault("SEC_API_USER_AGENT", "TestSuite test@example.com")


@pytest.fixture(scope="session", autouse=True)
def setup_logging():
    """Set up logging for tests."""
    from src.utils.logger import setup_logging
    setup_logging(log_level="WARNING")


@pytest.fixture(scope="session")
def project_root():
    """Get project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def sample_company():
    """Sample company data for testing."""
    return {
        "cik": "0000320193",
        "company_name": "Apple Inc",
        "ticker": "AAPL",
        "sic_code": "3571",
    }


@pytest.fixture
def sample_filing():
    """Sample filing data for testing."""
    from datetime import date
    return {
        "accession_number": "0000320193-24-000001",
        "cik": "0000320193",
        "form_type": "10-K",
        "filing_date": date(2024, 1, 15),
        "period_of_report": date(2023, 12, 31),
    }


@pytest.fixture
def sample_facts():
    """Sample XBRL facts for testing."""
    from datetime import date
    from decimal import Decimal
    
    return [
        {
            "accession_number": "0000320193-24-000001",
            "concept_name": "us-gaap:Assets",
            "value": Decimal("352583000000"),
            "unit": "USD",
            "period_type": "instant",
            "period_end": date(2023, 12, 31),
        },
        {
            "accession_number": "0000320193-24-000001",
            "concept_name": "us-gaap:Liabilities",
            "value": Decimal("290437000000"),
            "unit": "USD",
            "period_type": "instant",
            "period_end": date(2023, 12, 31),
        },
        {
            "accession_number": "0000320193-24-000001",
            "concept_name": "us-gaap:StockholdersEquity",
            "value": Decimal("62146000000"),
            "unit": "USD",
            "period_type": "instant",
            "period_end": date(2023, 12, 31),
        },
    ]
