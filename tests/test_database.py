"""Tests for database operations."""

import pytest
import tempfile
from datetime import date
from pathlib import Path
from decimal import Decimal

from src.storage.database import Database


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        db = Database(str(db_path))
        db.initialize_schema()
        yield db
        db.close()


class TestDatabase:
    """Tests for Database class."""
    
    def test_initialize_schema(self, temp_db):
        """Test schema initialization."""
        # Schema should already be initialized by fixture
        # Verify tables exist
        result = temp_db.connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        
        table_names = [r[0] for r in result]
        assert "companies" in table_names
        assert "filings" in table_names
        assert "facts" in table_names
        assert "sections" in table_names
    
    def test_upsert_company(self, temp_db):
        """Test company upsert."""
        temp_db.upsert_company(
            cik="0000320193",
            company_name="Apple Inc",
            ticker="AAPL",
        )
        
        company = temp_db.get_company("0000320193")
        assert company is not None
        assert company["company_name"] == "Apple Inc"
        assert company["ticker"] == "AAPL"
    
    def test_upsert_company_update(self, temp_db):
        """Test company upsert updates existing."""
        # Insert
        temp_db.upsert_company(
            cik="0000320193",
            company_name="Apple Inc",
            ticker="AAPL",
        )
        
        # Update
        temp_db.upsert_company(
            cik="0000320193",
            company_name="Apple Inc Updated",
            ticker="AAPL",
        )
        
        company = temp_db.get_company("0000320193")
        assert company["company_name"] == "Apple Inc Updated"
    
    def test_upsert_filing(self, temp_db):
        """Test filing upsert."""
        # First insert company
        temp_db.upsert_company(
            cik="0000320193",
            company_name="Apple Inc",
        )
        
        # Insert filing
        temp_db.upsert_filing(
            accession_number="0000320193-24-000001",
            cik="0000320193",
            form_type="10-K",
            filing_date=date(2024, 1, 15),
            download_status="completed",
        )
        
        filing = temp_db.get_filing("0000320193-24-000001")
        assert filing is not None
        assert filing["form_type"] == "10-K"
        assert filing["download_status"] == "completed"
    
    def test_get_company_filings(self, temp_db):
        """Test getting filings for a company."""
        # Setup
        temp_db.upsert_company(cik="0000320193", company_name="Apple Inc")
        
        for i in range(3):
            temp_db.upsert_filing(
                accession_number=f"0000320193-24-00000{i+1}",
                cik="0000320193",
                form_type="10-K",
                filing_date=date(2024, 1, i + 1),
            )
        
        # Query
        filings = temp_db.get_company_filings("0000320193")
        assert len(filings) == 3
    
    def test_insert_fact(self, temp_db):
        """Test fact insertion."""
        # Setup
        temp_db.upsert_company(cik="0000320193", company_name="Apple Inc")
        temp_db.upsert_filing(
            accession_number="0000320193-24-000001",
            cik="0000320193",
            form_type="10-K",
            filing_date=date(2024, 1, 1),
        )
        
        # Insert fact
        fact_id = temp_db.insert_fact(
            accession_number="0000320193-24-000001",
            concept_name="us-gaap:Assets",
            value=Decimal("1000000"),
            unit="USD",
            period_type="instant",
            period_end=date(2024, 1, 1),
        )
        
        assert fact_id > 0
        
        # Verify
        facts = temp_db.get_facts("0000320193-24-000001")
        assert len(facts) == 1
        assert facts[0]["concept_name"] == "us-gaap:Assets"
    
    def test_insert_section(self, temp_db):
        """Test section insertion."""
        # Setup
        temp_db.upsert_company(cik="0000320193", company_name="Apple Inc")
        temp_db.upsert_filing(
            accession_number="0000320193-24-000001",
            cik="0000320193",
            form_type="10-K",
            filing_date=date(2024, 1, 1),
        )
        
        # Insert section
        section_id = temp_db.insert_section(
            accession_number="0000320193-24-000001",
            section_type="item_1",
            content_text="Apple Inc designs, manufactures...",
            word_count=100,
        )
        
        assert section_id > 0
        
        # Verify
        sections = temp_db.get_sections("0000320193-24-000001")
        assert len(sections) == 1
        assert sections[0]["section_type"] == "item_1"
    
    def test_update_filing_status(self, temp_db):
        """Test updating filing status."""
        # Setup
        temp_db.upsert_company(cik="0000320193", company_name="Apple Inc")
        temp_db.upsert_filing(
            accession_number="0000320193-24-000001",
            cik="0000320193",
            form_type="10-K",
            filing_date=date(2024, 1, 1),
        )
        
        # Update status
        temp_db.update_filing_status(
            accession_number="0000320193-24-000001",
            xbrl_processed=True,
            sections_processed=True,
        )
        
        # Verify
        filing = temp_db.get_filing("0000320193-24-000001")
        assert filing["xbrl_processed"] is True
        assert filing["sections_processed"] is True
    
    def test_log_processing(self, temp_db):
        """Test processing log insertion."""
        log_id = temp_db.log_processing(
            pipeline_stage="download",
            status="completed",
            accession_number="0000320193-24-000001",
            processing_time_ms=1500,
            records_processed=10,
        )
        
        assert log_id > 0
