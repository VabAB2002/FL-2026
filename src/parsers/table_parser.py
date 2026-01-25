"""
Production-grade table extraction from SEC filings.

Features:
- Dual format: Structured JSON + Markdown
- Financial statement detection (Big 3)
- Complex table handling (merged cells, nested tables)
- Cell-level metadata extraction
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

from ..core.exceptions import TableParsingError
from ..utils.logger import get_logger

logger = get_logger("finloom.parsers.table")


@dataclass
class TableCell:
    """Represents a single table cell with metadata."""
    value: str
    numeric_value: Optional[float] = None
    rowspan: int = 1
    colspan: int = 1
    is_header: bool = False
    alignment: Optional[str] = None
    has_footnote: bool = False
    footnote_markers: list = field(default_factory=list)


@dataclass
class ExtractedTable:
    """Represents an extracted table with dual format and rich metadata."""
    table_index: int
    table_name: Optional[str]
    table_type: str  # financial, narrative, schedule, other
    
    # STRUCTURED FORMAT (for analysis)
    headers: list[dict]  # [{"text": "Assets", "level": 0, "colspan": 2}]
    cells: list[list[TableCell]]  # Cell-level metadata
    row_count: int
    column_count: int
    
    # FORMATTED (for RAG/LLMs)
    table_markdown: str
    table_caption: Optional[str] = None
    table_footnotes: list = field(default_factory=list)
    
    # METADATA
    is_financial_statement: bool = False
    table_category: Optional[str] = None  # balance_sheet, income_statement, etc.
    parent_table_id: Optional[int] = None
    
    # QUALITY
    extraction_quality: float = 1.0
    has_merged_cells: bool = False
    has_nested_tables: bool = False
    
    # SOURCE
    source_element: Optional[str] = None
    section_context: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "table_index": self.table_index,
            "table_name": self.table_name,
            "table_type": self.table_type,
            "headers": json.dumps(self.headers) if self.headers else None,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "table_data": self.get_structured_json(),
            "table_markdown": self.table_markdown,
            "table_caption": self.table_caption,
            "is_financial_statement": self.is_financial_statement,
            "table_category": self.table_category,
            "parent_table_id": self.parent_table_id,
            "footnote_refs": json.dumps(self.table_footnotes) if self.table_footnotes else None,
            "cell_metadata": self.get_cell_metadata_json(),
            "extraction_quality": self.extraction_quality,
            "source_element": self.source_element,
        }
    
    def get_structured_json(self) -> str:
        """Get table as structured JSON for analysis."""
        data = {
            "headers": self.headers,
            "rows": [
                [{"value": cell.value, "numeric": cell.numeric_value} for cell in row]
                for row in self.cells
            ]
        }
        return json.dumps(data)
    
    def get_cell_metadata_json(self) -> str:
        """Get cell-level metadata as JSON."""
        metadata = {
            "merged_cells": [],
            "footnotes": []
        }
        
        for row_idx, row in enumerate(self.cells):
            for col_idx, cell in enumerate(row):
                if cell.rowspan > 1 or cell.colspan > 1:
                    metadata["merged_cells"].append({
                        "row": row_idx,
                        "col": col_idx,
                        "rowspan": cell.rowspan,
                        "colspan": cell.colspan
                    })
                if cell.has_footnote:
                    metadata["footnotes"].append({
                        "row": row_idx,
                        "col": col_idx,
                        "markers": cell.footnote_markers
                    })
        
        return json.dumps(metadata)


class TableParser:
    """
    Production-grade parser for extracting tables from HTML filing documents.
    
    Features:
    - Dual format output (JSON + Markdown)
    - Financial statement detection
    - Complex table handling (merged cells, nested tables)
    - Cell-level metadata extraction
    """
    
    # Financial statement patterns (The "Big 3")
    FINANCIAL_STATEMENT_PATTERNS = {
        "balance_sheet": [
            r"(?i)consolidated\s+balance\s+sheets?",
            r"(?i)statements?\s+of\s+financial\s+position",
            r"(?i)statements?\s+of\s+condition",
        ],
        "income_statement": [
            r"(?i)consolidated\s+statements?\s+of\s+operations",
            r"(?i)consolidated\s+statements?\s+of\s+income",
            r"(?i)consolidated\s+statements?\s+of\s+earnings",
            r"(?i)statements?\s+of\s+comprehensive\s+income",
        ],
        "cash_flow": [
            r"(?i)consolidated\s+statements?\s+of\s+cash\s+flows?",
        ],
    }
    
    # Keywords indicating financial content
    FINANCIAL_KEYWORDS = [
        "balance sheet", "statement of operations", "income statement",
        "cash flow", "stockholders equity", "comprehensive income",
        "financial position", "assets", "liabilities", "revenue",
        "operating expenses", "net income", "earnings per share",
        "current assets", "long-term debt", "retained earnings",
    ]
    
    # Keywords indicating schedule tables
    SCHEDULE_KEYWORDS = [
        "schedule", "exhibit", "note", "reconciliation",
        "maturity", "segment", "quarterly", "selected financial",
        "fair value", "derivative", "stock-based compensation",
    ]
    
    # Layout table indicators (to filter out)
    LAYOUT_KEYWORDS = [
        "table of contents", "index", "page", "navigation",
    ]
    
    def __init__(self) -> None:
        """Initialize table parser."""
        logger.info("Production table parser initialized")
    
    def extract_tables(
        self,
        html_path: Path,
        section_context: Optional[str] = None,
        min_rows: int = 2,
        min_cols: int = 2,
    ) -> list[ExtractedTable]:
        """
        Extract all tables from an HTML document.
        
        Args:
            html_path: Path to HTML file.
            section_context: Section name (e.g., "item_8") for context.
            min_rows: Minimum rows for a valid table.
            min_cols: Minimum columns for a valid table.
        
        Returns:
            List of ExtractedTable objects.
        """
        logger.debug(f"Extracting tables from {html_path.name}")
        
        try:
            with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except OSError as e:
            logger.error(f"Failed to read file {html_path}: {e}")
            return []
        
        soup = BeautifulSoup(content, "lxml")
        tables = []
        
        for idx, table_elem in enumerate(soup.find_all("table")):
            # Filter out layout tables
            if self._is_layout_table(table_elem):
                logger.debug(f"Skipping layout table {idx}")
                continue
            
            extracted = self._extract_table(table_elem, idx, section_context)
            
            if extracted and extracted.row_count >= min_rows and extracted.column_count >= min_cols:
                tables.append(extracted)
        
        logger.info(f"Extracted {len(tables)} tables from {html_path.name}")
        return tables
    
    def extract_from_section_html(
        self,
        section_html: str,
        section_type: str,
        start_index: int = 0,
    ) -> list[ExtractedTable]:
        """
        Extract tables from section HTML content.
        
        Args:
            section_html: HTML content of section.
            section_type: Section identifier (e.g., "item_8").
            start_index: Starting table index.
        
        Returns:
            List of ExtractedTable objects.
        """
        soup = BeautifulSoup(section_html, "lxml")
        tables = []
        
        for idx, table_elem in enumerate(soup.find_all("table")):
            if self._is_layout_table(table_elem):
                continue
            
            extracted = self._extract_table(table_elem, start_index + idx, section_type)
            
            if extracted:
                tables.append(extracted)
        
        return tables
    
    def _is_layout_table(self, table_elem: Tag) -> bool:
        """Check if table is a layout table (should be filtered)."""
        # Check for layout indicators in class or ID
        classes = table_elem.get("class", [])
        table_id = table_elem.get("id", "")
        
        layout_indicators = ["layout", "navigation", "nav", "menu", "header", "footer", "toc"]
        
        for indicator in layout_indicators:
            if indicator in " ".join(classes).lower() or indicator in table_id.lower():
                return True
        
        # Check if table has only one cell (wrapper table)
        rows = table_elem.find_all("tr")
        if len(rows) == 1:
            cells = rows[0].find_all(["td", "th"])
            if len(cells) == 1:
                return True
        
        # Check for layout keywords in content
        text = table_elem.get_text().lower()[:200]
        for keyword in self.LAYOUT_KEYWORDS:
            if keyword in text:
                return True
        
        return False
    
    def _extract_table(
        self,
        table_elem: Tag,
        index: int,
        section_context: Optional[str] = None,
    ) -> Optional[ExtractedTable]:
        """Extract data from a single table element with full metadata."""
        try:
            # Check for nested tables
            has_nested = len(table_elem.find_all("table")) > 0
            
            # Extract caption and footnotes
            caption = self._extract_caption(table_elem)
            footnotes = self._extract_table_footnotes(table_elem)
            
            # Extract headers with hierarchy
            headers = self._extract_headers(table_elem)
            
            # Extract cells with metadata
            cells, row_count, col_count = self._extract_cells(table_elem, headers)
            
            if not cells or row_count == 0:
                return None
            
            # Check for merged cells
            has_merged = any(
                cell.rowspan > 1 or cell.colspan > 1
                for row in cells for cell in row
            )
            
            # Classify table
            table_type = self._classify_table(table_elem, headers, cells, caption)
            is_financial_stmt = self._is_financial_statement(caption, table_type)
            table_category = self._categorize_financial_table(caption) if is_financial_stmt else None
            
            # Generate markdown format
            markdown = self._generate_markdown(headers, cells, caption, footnotes)
            
            # Calculate extraction quality
            quality = self._calculate_quality(cells, has_merged, has_nested)
            
            # Find table name
            table_name = caption or self._find_table_name(table_elem)
            
            return ExtractedTable(
                table_index=index,
                table_name=table_name,
                table_type=table_type,
                headers=headers,
                cells=cells,
                row_count=row_count,
                column_count=col_count,
                table_markdown=markdown,
                table_caption=caption,
                table_footnotes=footnotes,
                is_financial_statement=is_financial_stmt,
                table_category=table_category,
                extraction_quality=quality,
                has_merged_cells=has_merged,
                has_nested_tables=has_nested,
                source_element=table_elem.get("id"),
                section_context=section_context,
            )
            
        except (ValueError, TypeError, AttributeError) as e:
            # Expected parsing issues with malformed table HTML
            logger.debug(f"Failed to extract table {index}: {e}")
            return None
        except Exception as e:
            # Unexpected error - log at warning level for investigation
            logger.warning(f"Unexpected error extracting table {index}: {type(e).__name__}: {e}")
            return None
    
    def _extract_caption(self, table_elem: Tag) -> Optional[str]:
        """Extract table caption."""
        caption = table_elem.find("caption")
        if caption:
            return self._clean_text(caption.get_text())
        return None
    
    def _extract_table_footnotes(self, table_elem: Tag) -> list:
        """Extract footnotes associated with table."""
        footnotes = []
        
        # Look for footnote markers in cells
        markers = set()
        for cell in table_elem.find_all(["td", "th"]):
            text = cell.get_text()
            # Find footnote markers: *, †, ‡, (1), [1], etc.
            found_markers = re.findall(r'[\*†‡§¶]|\(\d+\)|\[\d+\]', text)
            markers.update(found_markers)
        
        # Look for footnote text after table
        next_sibling = table_elem.find_next_sibling()
        if next_sibling and next_sibling.name in ['p', 'div']:
            text = next_sibling.get_text()
            for marker in markers:
                if marker in text:
                    footnotes.append({"marker": marker, "text": text})
        
        return footnotes
    
    def _extract_headers(self, table_elem: Tag) -> list[dict]:
        """Extract headers with hierarchy support."""
        headers = []
        
        # Check for thead
        thead = table_elem.find("thead")
        if thead:
            header_rows = thead.find_all("tr")
            if header_rows:
                # Handle multi-level headers
                for level, row in enumerate(header_rows):
                    for th in row.find_all(["th", "td"]):
                        headers.append({
                            "text": self._clean_text(th.get_text()),
                            "level": level,
                            "colspan": int(th.get("colspan", 1)),
                            "rowspan": int(th.get("rowspan", 1)),
                        })
        else:
            # Try first row as headers
            first_row = table_elem.find("tr")
            if first_row:
                for th in first_row.find_all(["th", "td"]):
                    headers.append({
                        "text": self._clean_text(th.get_text()),
                        "level": 0,
                        "colspan": int(th.get("colspan", 1)),
                        "rowspan": int(th.get("rowspan", 1)),
                    })
        
        return headers
    
    def _extract_cells(
        self,
        table_elem: Tag,
        headers: list[dict],
    ) -> tuple[list[list[TableCell]], int, int]:
        """Extract cells with full metadata."""
        cells = []
        max_cols = 0
        
        # Find data rows (skip header rows)
        tbody = table_elem.find("tbody")
        rows_to_process = tbody.find_all("tr") if tbody else table_elem.find_all("tr")
        
        # Skip header rows if no tbody
        if not tbody and headers:
            rows_to_process = rows_to_process[1:]
        
        for tr in rows_to_process:
            row_cells = []
            
            for td in tr.find_all(["td", "th"]):
                text = self._clean_text(td.get_text())
                numeric_val = self._extract_numeric_value(text)
                
                # Check for footnote markers
                footnote_markers = re.findall(r'[\*†‡§¶]|\(\d+\)|\[\d+\]', text)
                
                # Get cell alignment
                alignment = td.get("align") or td.get("style", "")
                
                cell = TableCell(
                    value=text,
                    numeric_value=numeric_val,
                    rowspan=int(td.get("rowspan", 1)),
                    colspan=int(td.get("colspan", 1)),
                    is_header=td.name == "th",
                    alignment=alignment if alignment else None,
                    has_footnote=len(footnote_markers) > 0,
                    footnote_markers=footnote_markers,
                )
                
                row_cells.append(cell)
            
            if row_cells:
                cells.append(row_cells)
                max_cols = max(max_cols, len(row_cells))
        
        return cells, len(cells), max_cols
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()
        # Remove non-breaking spaces
        text = text.replace("\xa0", " ")
        return text
    
    def _extract_numeric_value(self, text: str) -> Optional[float]:
        """Extract numeric value from text."""
        # Remove common formatting
        cleaned = text.replace(",", "").replace("$", "").replace("€", "").replace("£", "")
        cleaned = cleaned.replace("(", "-").replace(")", "")
        cleaned = cleaned.replace("%", "").strip()
        
        try:
            return float(cleaned)
        except ValueError:
            return None
    
    def _classify_table(
        self,
        table_elem: Tag,
        headers: list[dict],
        cells: list[list[TableCell]],
        caption: Optional[str],
    ) -> str:
        """Classify table type based on content."""
        # Combine text for analysis
        all_text = (caption or "").lower()
        all_text += " " + " ".join(h["text"] for h in headers).lower()
        
        # Add sample of cell content
        for row in cells[:5]:
            all_text += " " + " ".join(cell.value for cell in row).lower()
        
        # Check for financial keywords
        financial_score = sum(1 for kw in self.FINANCIAL_KEYWORDS if kw in all_text)
        schedule_score = sum(1 for kw in self.SCHEDULE_KEYWORDS if kw in all_text)
        
        if financial_score > schedule_score:
            return "financial"
        elif schedule_score > 0:
            return "schedule"
        
        # Check if mostly numeric (likely financial)
        numeric_count = sum(
            1 for row in cells[:10] for cell in row if cell.numeric_value is not None
        )
        total_count = sum(len(row) for row in cells[:10])
        
        if total_count > 0 and numeric_count / total_count > 0.5:
            return "financial"
        
        return "narrative"
    
    def _is_financial_statement(self, caption: Optional[str], table_type: str) -> bool:
        """Check if table is one of the Big 3 financial statements."""
        if not caption or table_type != "financial":
            return False
        
        caption_lower = caption.lower()
        
        for patterns in self.FINANCIAL_STATEMENT_PATTERNS.values():
            for pattern in patterns:
                if re.search(pattern, caption_lower):
                    return True
        
        return False
    
    def _categorize_financial_table(self, caption: Optional[str]) -> Optional[str]:
        """Categorize financial table (balance_sheet, income_statement, cash_flow)."""
        if not caption:
            return None
        
        caption_lower = caption.lower()
        
        for category, patterns in self.FINANCIAL_STATEMENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, caption_lower):
                    return category
        
        return None
    
    def _generate_markdown(
        self,
        headers: list[dict],
        cells: list[list[TableCell]],
        caption: Optional[str],
        footnotes: list,
    ) -> str:
        """Generate markdown representation of table."""
        lines = []
        
        # Add caption
        if caption:
            lines.append(f"**{caption}**\n")
        
        # Add headers
        if headers:
            header_texts = [h["text"] for h in headers if h["level"] == 0]
            if header_texts:
                lines.append("| " + " | ".join(header_texts) + " |")
                lines.append("|" + "|".join([" --- " for _ in header_texts]) + "|")
        
        # Add rows
        for row in cells:
            row_values = [cell.value for cell in row]
            lines.append("| " + " | ".join(row_values) + " |")
        
        # Add footnotes
        if footnotes:
            lines.append("")
            for fn in footnotes:
                lines.append(f"{fn['marker']} {fn.get('text', '')}")
        
        return "\n".join(lines)
    
    def _calculate_quality(
        self,
        cells: list[list[TableCell]],
        has_merged: bool,
        has_nested: bool,
    ) -> float:
        """Calculate extraction quality score."""
        quality = 1.0
        
        # Penalize merged cells (harder to parse)
        if has_merged:
            quality *= 0.95
        
        # Penalize nested tables
        if has_nested:
            quality *= 0.9
        
        # Check for empty cells
        total_cells = sum(len(row) for row in cells)
        empty_cells = sum(1 for row in cells for cell in row if not cell.value.strip())
        
        if total_cells > 0:
            empty_ratio = empty_cells / total_cells
            if empty_ratio > 0.3:
                quality *= 0.85
        
        return round(quality, 2)
    
    def _find_table_name(self, table_elem: Tag) -> Optional[str]:
        """Find table name from preceding heading."""
        # Look for preceding heading
        prev = table_elem.find_previous(["h1", "h2", "h3", "h4", "h5", "h6", "b", "strong"])
        if prev:
            text = self._clean_text(prev.get_text())
            if len(text) < 200:  # Reasonable heading length
                return text
        
        return None
    
    def extract_financial_statements(
        self,
        html_path: Path,
    ) -> dict[str, Optional[ExtractedTable]]:
        """
        Extract the Big 3 financial statements from a document.
        
        Returns:
            Dictionary with keys: balance_sheet, income_statement, cash_flow
        """
        all_tables = self.extract_tables(html_path, section_context="item_8")
        
        statements = {
            "balance_sheet": None,
            "income_statement": None,
            "cash_flow": None,
        }
        
        for table in all_tables:
            if table.is_financial_statement and table.table_category:
                statements[table.table_category] = table
        
        return statements
