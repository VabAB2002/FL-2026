"""Tests for file parsers."""

import pytest
import tempfile
from pathlib import Path

from src.parsers.section_parser import SectionParser, ExtractedSection, SECTION_DEFINITIONS
from src.parsers.table_parser import TableParser, ExtractedTable
from src.parsers.xbrl_parser import SimpleXBRLParser, XBRLFact


class TestSectionParser:
    """Tests for SectionParser class."""
    
    @pytest.fixture
    def sample_10k_html(self):
        """Create a sample 10-K HTML file."""
        html = """
        <!DOCTYPE html>
        <html>
        <head><title>10-K</title></head>
        <body>
        <h1>Form 10-K</h1>
        
        <h2>ITEM 1. BUSINESS</h2>
        <p>Our company designs, manufactures, and markets mobile communication 
        and media devices, personal computers, and portable digital music players.</p>
        <p>We also sell a variety of related software, services, accessories, 
        networking solutions, and third-party digital content and applications.</p>
        
        <h2>ITEM 1A. RISK FACTORS</h2>
        <p>The following discussion of risk factors contains forward-looking statements.</p>
        <p>Risk 1: Economic conditions may affect consumer spending.</p>
        <p>Risk 2: Competition in the technology sector is intense.</p>
        
        <h2>ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS</h2>
        <p>The following discussion should be read in conjunction with the 
        consolidated financial statements.</p>
        <p>Revenue increased 10% year over year due to strong product demand.</p>
        
        <h2>ITEM 8. FINANCIAL STATEMENTS</h2>
        <p>See accompanying financial statements.</p>
        
        <h2>ITEM 9A. CONTROLS AND PROCEDURES</h2>
        <p>Our management evaluated the effectiveness of our disclosure controls.</p>
        <p>Based on that evaluation, our CEO and CFO concluded that our controls were effective.</p>
        
        </body>
        </html>
        """
        
        with tempfile.TemporaryDirectory() as tmpdir:
            html_path = Path(tmpdir) / "10k.htm"
            html_path.write_text(html)
            yield html_path
    
    def test_section_definitions_exist(self):
        """Test that required section definitions exist."""
        required = ["item_1", "item_1a", "item_7", "item_8", "item_9a"]
        for section in required:
            assert section in SECTION_DEFINITIONS
    
    def test_parse_filing(self, sample_10k_html):
        """Test parsing a sample 10-K filing."""
        parser = SectionParser(priority_only=True)
        result = parser.parse_filing(sample_10k_html.parent, "0000000000-24-000001")
        
        assert result.success is True
        assert result.section_count >= 3  # Should find at least some sections
    
    def test_extract_item_1(self, sample_10k_html):
        """Test extracting Item 1 (Business)."""
        parser = SectionParser(priority_only=True)
        result = parser.parse_filing(sample_10k_html.parent, "0000000000-24-000001")
        
        item_1 = result.get_section("item_1")
        if item_1:
            assert "designs" in item_1.content_text.lower() or "manufactures" in item_1.content_text.lower()
    
    def test_extract_risk_factors(self, sample_10k_html):
        """Test extracting Item 1A (Risk Factors)."""
        parser = SectionParser(priority_only=True)
        result = parser.parse_filing(sample_10k_html.parent, "0000000000-24-000001")
        
        item_1a = result.get_section("item_1a")
        if item_1a:
            assert "risk" in item_1a.content_text.lower()
    
    def test_word_count(self, sample_10k_html):
        """Test word count calculation."""
        parser = SectionParser(priority_only=True)
        result = parser.parse_filing(sample_10k_html.parent, "0000000000-24-000001")
        
        for section in result.sections:
            assert section.word_count > 0
            assert section.word_count == len(section.content_text.split())


class TestTableParser:
    """Tests for TableParser class."""
    
    @pytest.fixture
    def sample_table_html(self):
        """Create a sample HTML with tables."""
        html = """
        <!DOCTYPE html>
        <html>
        <body>
        <h2>Balance Sheet</h2>
        <table>
            <thead>
                <tr><th>Item</th><th>2024</th><th>2023</th></tr>
            </thead>
            <tbody>
                <tr><td>Assets</td><td>$1,000,000</td><td>$900,000</td></tr>
                <tr><td>Liabilities</td><td>$600,000</td><td>$550,000</td></tr>
                <tr><td>Equity</td><td>$400,000</td><td>$350,000</td></tr>
            </tbody>
        </table>
        </body>
        </html>
        """
        
        with tempfile.TemporaryDirectory() as tmpdir:
            html_path = Path(tmpdir) / "table.html"
            html_path.write_text(html)
            yield html_path
    
    def test_extract_tables(self, sample_table_html):
        """Test extracting tables from HTML."""
        parser = TableParser()
        tables = parser.extract_tables(sample_table_html)
        
        assert len(tables) >= 1
    
    def test_table_structure(self, sample_table_html):
        """Test extracted table structure."""
        parser = TableParser()
        tables = parser.extract_tables(sample_table_html)
        
        if tables:
            table = tables[0]
            assert table.row_count >= 2
            assert table.column_count >= 2
            assert len(table.headers) > 0
    
    def test_financial_table_classification(self, sample_table_html):
        """Test table type classification."""
        parser = TableParser()
        tables = parser.extract_tables(sample_table_html)
        
        # Should classify as financial due to content
        if tables:
            table = tables[0]
            assert table.table_type == "financial"


class TestXBRLFact:
    """Tests for XBRLFact dataclass."""
    
    def test_fact_creation(self):
        """Test creating an XBRL fact."""
        from decimal import Decimal
        from datetime import date
        
        fact = XBRLFact(
            concept_name="us-gaap:Assets",
            concept_namespace="us-gaap",
            concept_local_name="Assets",
            value=Decimal("1000000"),
            value_text=None,
            unit="USD",
            decimals=-3,
            period_type="instant",
            period_start=None,
            period_end=date(2024, 12, 31),
        )
        
        assert fact.concept_name == "us-gaap:Assets"
        assert fact.value == Decimal("1000000")
    
    def test_fact_to_dict(self):
        """Test converting fact to dictionary."""
        from decimal import Decimal
        from datetime import date
        
        fact = XBRLFact(
            concept_name="us-gaap:Assets",
            concept_namespace="us-gaap",
            concept_local_name="Assets",
            value=Decimal("1000000"),
            value_text=None,
            unit="USD",
            decimals=-3,
            period_type="instant",
            period_start=None,
            period_end=date(2024, 12, 31),
        )
        
        d = fact.to_dict()
        
        assert d["concept_name"] == "us-gaap:Assets"
        assert d["value"] == Decimal("1000000")
        assert d["unit"] == "USD"


class TestSimpleXBRLParser:
    """Tests for SimpleXBRLParser class."""
    
    def test_parser_init(self):
        """Test parser initialization."""
        parser = SimpleXBRLParser()
        assert len(parser.core_concepts) > 0
    
    def test_core_concepts_defined(self):
        """Test core concepts are defined."""
        parser = SimpleXBRLParser()
        
        assert "us-gaap:Assets" in parser.core_concepts
        assert "us-gaap:Liabilities" in parser.core_concepts
        assert "us-gaap:NetIncomeLoss" in parser.core_concepts
