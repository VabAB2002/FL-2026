"""Tests for data validation."""

import pytest
from datetime import date
from decimal import Decimal

from src.validation.schemas import Company, Filing, Fact, Section
from src.validation.data_quality import DataQualityChecker, ValidationResult


class TestCompanySchema:
    """Tests for Company schema."""
    
    def test_valid_company(self):
        """Test valid company creation."""
        company = Company(
            cik="320193",
            company_name="Apple Inc",
            ticker="AAPL",
        )
        assert company.cik == "0000320193"  # Should be zero-padded
        assert company.ticker == "AAPL"
    
    def test_cik_normalization(self):
        """Test CIK is normalized to 10 digits."""
        company = Company(cik="123", company_name="Test")
        assert company.cik == "0000000123"
    
    def test_ticker_uppercase(self):
        """Test ticker is uppercased."""
        company = Company(cik="123", company_name="Test", ticker="aapl")
        assert company.ticker == "AAPL"


class TestFilingSchema:
    """Tests for Filing schema."""
    
    def test_valid_filing(self):
        """Test valid filing creation."""
        filing = Filing(
            accession_number="0000320193-24-000001",
            cik="320193",
            form_type="10-K",
            filing_date=date(2024, 1, 15),
        )
        assert filing.accession_number == "0000320193-24-000001"
        assert filing.cik == "0000320193"
    
    def test_accession_number_format(self):
        """Test accession number validation."""
        # Valid format
        filing = Filing(
            accession_number="0000320193-24-000001",
            cik="123",
            form_type="10-K",
            filing_date=date(2024, 1, 1),
        )
        assert filing.accession_number == "0000320193-24-000001"
        
        # Without dashes - should be converted
        filing2 = Filing(
            accession_number="000032019324000001",
            cik="123",
            form_type="10-K",
            filing_date=date(2024, 1, 1),
        )
        assert "-" in filing2.accession_number
    
    def test_invalid_accession_number(self):
        """Test invalid accession number raises error."""
        with pytest.raises(ValueError):
            Filing(
                accession_number="invalid",
                cik="123",
                form_type="10-K",
                filing_date=date(2024, 1, 1),
            )


class TestFactSchema:
    """Tests for Fact schema."""
    
    def test_valid_numeric_fact(self):
        """Test valid numeric fact."""
        fact = Fact(
            accession_number="0000320193-24-000001",
            concept_name="us-gaap:Assets",
            value=Decimal("1000000"),
            unit="USD",
            period_type="instant",
            period_end=date(2024, 1, 1),
        )
        assert fact.value == Decimal("1000000")
    
    def test_valid_text_fact(self):
        """Test valid text fact."""
        fact = Fact(
            accession_number="0000320193-24-000001",
            concept_name="dei:DocumentType",
            value_text="10-K",
            period_type="instant",
        )
        assert fact.value_text == "10-K"
    
    def test_unrealistic_value_rejected(self):
        """Test unrealistic value raises error."""
        with pytest.raises(ValueError):
            Fact(
                accession_number="0000320193-24-000001",
                concept_name="us-gaap:Assets",
                value=Decimal("1e20"),  # Too large
                period_type="instant",
            )
    
    def test_must_have_value_or_text(self):
        """Test that either value or value_text is required."""
        with pytest.raises(ValueError):
            Fact(
                accession_number="0000320193-24-000001",
                concept_name="us-gaap:Assets",
                period_type="instant",
            )


class TestSectionSchema:
    """Tests for Section schema."""
    
    def test_valid_section(self):
        """Test valid section."""
        section = Section(
            accession_number="0000320193-24-000001",
            section_type="item_1",
            content_text="This is the business description...",
        )
        assert section.section_type == "item_1"
        assert section.word_count > 0
    
    def test_word_count_calculated(self):
        """Test word count is calculated."""
        section = Section(
            accession_number="0000320193-24-000001",
            section_type="item_1",
            content_text="One two three four five",
        )
        assert section.word_count == 5


class TestDataQualityChecker:
    """Tests for DataQualityChecker."""
    
    def test_validate_company_valid(self):
        """Test validation of valid company."""
        checker = DataQualityChecker()
        result = checker.validate_company({
            "cik": "123",
            "company_name": "Test Corp",
        })
        assert result.valid is True
        assert result.error_count == 0
    
    def test_validate_filing_date_consistency(self):
        """Test filing date validation."""
        checker = DataQualityChecker()
        
        # Valid dates
        result = checker.validate_filing({
            "accession_number": "0000320193-24-000001",
            "cik": "123",
            "form_type": "10-K",
            "filing_date": date(2024, 3, 1),
            "period_of_report": date(2023, 12, 31),
        })
        assert result.valid is True
    
    def test_validate_facts_missing_required(self):
        """Test validation flags missing required concepts."""
        checker = DataQualityChecker()
        
        # Facts without required concepts
        result = checker.validate_facts([
            {
                "accession_number": "0000320193-24-000001",
                "concept_name": "us-gaap:SomeOtherConcept",
                "value": Decimal("1000"),
                "period_type": "instant",
            }
        ])
        
        # Should have warnings about missing concepts
        assert result.warning_count > 0
    
    def test_validate_balance_sheet_equation(self):
        """Test balance sheet equation validation."""
        checker = DataQualityChecker(tolerance_percent=1.0)
        
        # Balanced sheet
        facts = [
            {
                "accession_number": "0000320193-24-000001",
                "concept_name": "us-gaap:Assets",
                "value": Decimal("1000000"),
                "period_type": "instant",
                "period_end": date(2024, 1, 1),
            },
            {
                "accession_number": "0000320193-24-000001",
                "concept_name": "us-gaap:Liabilities",
                "value": Decimal("600000"),
                "period_type": "instant",
                "period_end": date(2024, 1, 1),
            },
            {
                "accession_number": "0000320193-24-000001",
                "concept_name": "us-gaap:StockholdersEquity",
                "value": Decimal("400000"),
                "period_type": "instant",
                "period_end": date(2024, 1, 1),
            },
        ]
        
        result = checker.validate_facts(facts)
        
        # Should not have balance sheet errors (within tolerance)
        balance_errors = [i for i in result.issues if i.issue_type == "balance_sheet_imbalance"]
        assert len(balance_errors) == 0
