"""
Table extraction from SEC filings.

Extracts and structures tables from HTML documents.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

from ..utils.logger import get_logger

logger = get_logger("finloom.parsers.table")


@dataclass
class ExtractedTable:
    """Represents an extracted table from a filing."""
    table_index: int
    table_name: Optional[str]
    table_type: str  # financial, narrative, schedule, other
    headers: list[str]
    rows: list[list[str]]
    row_count: int
    column_count: int
    source_element: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "table_index": self.table_index,
            "table_name": self.table_name,
            "table_type": self.table_type,
            "headers": self.headers,
            "rows": self.rows,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "source_element": self.source_element,
        }
    
    @property
    def table_data(self) -> dict:
        """Get table as structured JSON."""
        return {
            "headers": self.headers,
            "data": [
                {self.headers[i] if i < len(self.headers) else f"col_{i}": cell
                 for i, cell in enumerate(row)}
                for row in self.rows
            ],
        }


class TableParser:
    """
    Parser for extracting tables from HTML filing documents.
    
    Identifies financial tables, narrative tables, and schedules.
    """
    
    # Keywords indicating financial tables
    FINANCIAL_KEYWORDS = [
        "balance sheet", "statement of operations", "income statement",
        "cash flow", "stockholders equity", "comprehensive income",
        "financial position", "assets", "liabilities", "revenue",
        "operating expenses", "net income", "earnings per share",
    ]
    
    # Keywords indicating schedule tables
    SCHEDULE_KEYWORDS = [
        "schedule", "exhibit", "note", "reconciliation",
        "maturity", "segment", "quarterly", "selected financial",
    ]
    
    def __init__(self) -> None:
        """Initialize table parser."""
        logger.info("Table parser initialized")
    
    def extract_tables(
        self,
        html_path: Path,
        min_rows: int = 2,
        min_cols: int = 2,
    ) -> list[ExtractedTable]:
        """
        Extract all tables from an HTML document.
        
        Args:
            html_path: Path to HTML file.
            min_rows: Minimum rows for a valid table.
            min_cols: Minimum columns for a valid table.
        
        Returns:
            List of ExtractedTable objects.
        """
        logger.debug(f"Extracting tables from {html_path.name}")
        
        try:
            with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception as e:
            logger.error(f"Failed to read file: {e}")
            return []
        
        soup = BeautifulSoup(content, "lxml")
        tables = []
        
        for idx, table_elem in enumerate(soup.find_all("table")):
            extracted = self._extract_table(table_elem, idx)
            
            if extracted and extracted.row_count >= min_rows and extracted.column_count >= min_cols:
                tables.append(extracted)
        
        logger.info(f"Extracted {len(tables)} tables from {html_path.name}")
        return tables
    
    def _extract_table(
        self,
        table_elem: Tag,
        index: int,
    ) -> Optional[ExtractedTable]:
        """Extract data from a single table element."""
        try:
            rows = []
            headers = []
            
            # Find header row (th elements or first tr)
            header_row = table_elem.find("thead")
            if header_row:
                for th in header_row.find_all(["th", "td"]):
                    headers.append(self._clean_cell_text(th.get_text()))
            
            # Find data rows
            tbody = table_elem.find("tbody") or table_elem
            for tr in tbody.find_all("tr"):
                cells = []
                for td in tr.find_all(["td", "th"]):
                    cells.append(self._clean_cell_text(td.get_text()))
                
                if cells:
                    # If no explicit headers, use first row
                    if not headers and not rows:
                        headers = cells
                    else:
                        rows.append(cells)
            
            if not rows:
                return None
            
            # Determine max columns
            max_cols = max(len(headers), max(len(row) for row in rows) if rows else 0)
            
            # Pad rows to equal length
            for row in rows:
                while len(row) < max_cols:
                    row.append("")
            
            # Determine table type
            table_type = self._classify_table(table_elem, headers, rows)
            
            # Try to extract table name from preceding text
            table_name = self._find_table_name(table_elem)
            
            return ExtractedTable(
                table_index=index,
                table_name=table_name,
                table_type=table_type,
                headers=headers,
                rows=rows,
                row_count=len(rows),
                column_count=max_cols,
                source_element=table_elem.get("id"),
            )
            
        except Exception as e:
            logger.debug(f"Failed to extract table {index}: {e}")
            return None
    
    def _clean_cell_text(self, text: str) -> str:
        """Clean and normalize cell text."""
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()
        
        # Remove common currency symbols for numeric comparison
        # but keep the value
        text = text.replace("\xa0", " ")  # Non-breaking space
        
        return text
    
    def _classify_table(
        self,
        table_elem: Tag,
        headers: list[str],
        rows: list[list[str]],
    ) -> str:
        """Classify table type based on content."""
        # Combine all text for keyword matching
        all_text = " ".join(headers).lower()
        all_text += " " + " ".join(" ".join(row) for row in rows[:5]).lower()
        
        # Check for financial keywords
        for keyword in self.FINANCIAL_KEYWORDS:
            if keyword in all_text:
                return "financial"
        
        # Check for schedule keywords
        for keyword in self.SCHEDULE_KEYWORDS:
            if keyword in all_text:
                return "schedule"
        
        # Check if mostly numeric (likely financial)
        numeric_count = 0
        total_count = 0
        for row in rows[:10]:
            for cell in row:
                total_count += 1
                if self._is_numeric(cell):
                    numeric_count += 1
        
        if total_count > 0 and numeric_count / total_count > 0.5:
            return "financial"
        
        return "narrative"
    
    def _is_numeric(self, text: str) -> bool:
        """Check if text represents a numeric value."""
        # Remove common formatting
        cleaned = text.replace(",", "").replace("$", "").replace("(", "-").replace(")", "")
        cleaned = cleaned.replace("%", "").strip()
        
        try:
            float(cleaned)
            return True
        except ValueError:
            return False
    
    def _find_table_name(self, table_elem: Tag) -> Optional[str]:
        """Find table name from preceding caption or heading."""
        # Check for caption element
        caption = table_elem.find("caption")
        if caption:
            return self._clean_cell_text(caption.get_text())
        
        # Look for preceding heading
        prev = table_elem.find_previous(["h1", "h2", "h3", "h4", "h5", "h6", "b", "strong"])
        if prev:
            text = self._clean_cell_text(prev.get_text())
            if len(text) < 200:  # Reasonable heading length
                return text
        
        return None
    
    def extract_financial_tables(
        self,
        html_path: Path,
    ) -> list[ExtractedTable]:
        """Extract only financial tables from a document."""
        all_tables = self.extract_tables(html_path)
        return [t for t in all_tables if t.table_type == "financial"]
