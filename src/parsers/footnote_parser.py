"""
Footnote extraction and cross-reference system for SEC filings.

Features:
- Extract inline, section, table, and document-level footnotes
- Link footnotes to parent content (sections, tables)
- Build cross-reference graph
- Track footnote markers and references
"""

import re
import uuid
from dataclasses import dataclass, field
from typing import Optional

from bs4 import BeautifulSoup, Tag

from ..utils.logger import get_logger

logger = get_logger("finloom.parsers.footnote")


@dataclass
class ExtractedFootnote:
    """Represents an extracted footnote with linking metadata."""
    footnote_id: str
    marker: str  # "*", "1", "(a)", etc.
    footnote_text: str
    footnote_type: str  # inline, section, table, document
    
    # LINKS
    ref_links: list = field(default_factory=list)  # What this footnote references
    referenced_by: list = field(default_factory=list)  # What references this footnote
    
    # CONTEXT
    accession_number: Optional[str] = None
    section_id: Optional[int] = None
    table_id: Optional[int] = None
    position_in_text: Optional[int] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        import json
        return {
            "footnote_id": self.footnote_id,
            "accession_number": self.accession_number,
            "section_id": self.section_id,
            "table_id": self.table_id,
            "marker": self.marker,
            "footnote_text": self.footnote_text,
            "footnote_type": self.footnote_type,
            "ref_links": json.dumps(self.ref_links) if self.ref_links else None,
            "referenced_by": json.dumps(self.referenced_by) if self.referenced_by else None,
        }


class FootnoteParser:
    """
    Parser for extracting footnotes and cross-references from SEC filings.
    
    Handles:
    - Inline footnotes (superscript markers)
    - End-of-section footnotes
    - Table footnotes
    - Document-level notes (e.g., "Notes to Financial Statements")
    """
    
    # Common footnote marker patterns
    FOOTNOTE_MARKER_PATTERNS = [
        r'\*+',  # *, **, ***
        r'†+',   # †, ††
        r'‡+',   # ‡, ‡‡
        r'§+',   # §
        r'¶+',   # ¶
        r'\(\d+\)',  # (1), (2), etc.
        r'\[\d+\]',  # [1], [2], etc.
        r'\d+',  # 1, 2, 3 (when superscript)
        r'\([a-z]\)',  # (a), (b), (c)
        r'[a-z]',  # a, b, c (when superscript)
    ]
    
    # Cross-reference patterns
    CROSS_REF_PATTERNS = [
        (r"(?i)see\s+item\s+(\d+[A-Z]?)", "Item"),
        (r"(?i)refer\s+to\s+item\s+(\d+[A-Z]?)", "Item"),
        (r"(?i)discussed\s+in\s+item\s+(\d+[A-Z]?)", "Item"),
        (r"(?i)see\s+note\s+(\d+)", "Note"),
        (r"(?i)refer\s+to\s+note\s+(\d+)", "Note"),
        (r"(?i)see\s+part\s+(I{1,3}|IV)", "Part"),
        (r"(?i)see\s+table\s+(\d+)", "Table"),
    ]
    
    def __init__(self) -> None:
        """Initialize footnote parser."""
        logger.info("Footnote parser initialized")
        self.footnotes = []
    
    def extract_footnotes(
        self,
        sections: list,
        tables: list,
        accession_number: str,
    ) -> list[ExtractedFootnote]:
        """
        Extract all footnotes from sections and tables.
        
        Args:
            sections: List of ExtractedSection objects
            tables: List of ExtractedTable objects
            accession_number: Filing accession number
        
        Returns:
            List of ExtractedFootnote objects
        """
        all_footnotes = []
        
        # Extract from sections
        for section in sections:
            section_footnotes = self._extract_from_section(
                section, accession_number
            )
            all_footnotes.extend(section_footnotes)
        
        # Extract from tables
        for table in tables:
            table_footnotes = self._extract_from_table(
                table, accession_number
            )
            all_footnotes.extend(table_footnotes)
        
        # Build cross-reference graph
        all_footnotes = self._build_cross_reference_graph(all_footnotes)
        
        logger.info(f"Extracted {len(all_footnotes)} footnotes from {accession_number}")
        return all_footnotes
    
    def _extract_from_section(
        self,
        section,
        accession_number: str,
    ) -> list[ExtractedFootnote]:
        """Extract footnotes from a section."""
        footnotes = []
        
        # Extract inline footnotes (markers in text)
        inline_footnotes = self._extract_inline_footnotes(
            section.content_text,
            accession_number,
            section_id=getattr(section, 'id', None),
        )
        footnotes.extend(inline_footnotes)
        
        # Extract end-of-section footnotes
        if hasattr(section, 'content_html') and section.content_html:
            soup = BeautifulSoup(section.content_html, "lxml")
            end_footnotes = self._extract_end_footnotes(
                soup,
                accession_number,
                section_id=getattr(section, 'id', None),
            )
            footnotes.extend(end_footnotes)
        
        return footnotes
    
    def _extract_from_table(
        self,
        table,
        accession_number: str,
    ) -> list[ExtractedFootnote]:
        """Extract footnotes from a table."""
        footnotes = []
        
        # Table footnotes are usually in table.table_footnotes
        if hasattr(table, 'table_footnotes') and table.table_footnotes:
            for fn_data in table.table_footnotes:
                footnote = ExtractedFootnote(
                    footnote_id=str(uuid.uuid4()),
                    marker=fn_data.get("marker", ""),
                    footnote_text=fn_data.get("text", ""),
                    footnote_type="table",
                    accession_number=accession_number,
                    table_id=getattr(table, 'id', None),
                )
                footnotes.append(footnote)
        
        return footnotes
    
    def _extract_inline_footnotes(
        self,
        text: str,
        accession_number: str,
        section_id: Optional[int] = None,
    ) -> list[ExtractedFootnote]:
        """Extract inline footnote markers from text."""
        footnotes = []
        
        # Find all footnote markers
        markers_found = {}
        
        for pattern in self.FOOTNOTE_MARKER_PATTERNS:
            matches = re.finditer(pattern, text)
            for match in matches:
                marker = match.group(0)
                position = match.start()
                
                # Store unique markers with their first position
                if marker not in markers_found:
                    markers_found[marker] = position
        
        # Create footnote entries for each unique marker
        for marker, position in markers_found.items():
            # Try to extract footnote text (look for marker later in text)
            footnote_text = self._find_footnote_text(text, marker, position)
            
            if footnote_text:
                footnote = ExtractedFootnote(
                    footnote_id=str(uuid.uuid4()),
                    marker=marker,
                    footnote_text=footnote_text,
                    footnote_type="inline",
                    accession_number=accession_number,
                    section_id=section_id,
                    position_in_text=position,
                )
                footnotes.append(footnote)
        
        return footnotes
    
    def _find_footnote_text(
        self,
        text: str,
        marker: str,
        marker_position: int,
    ) -> Optional[str]:
        """Find the actual footnote text for a marker."""
        # Look for the marker appearing again later in the text
        # (footnotes usually appear at end of section)
        
        # Search in the last 20% of the text (where footnotes typically are)
        search_start = max(marker_position + 100, int(len(text) * 0.8))
        remaining_text = text[search_start:]
        
        # Find marker in remaining text
        escaped_marker = re.escape(marker)
        pattern = f"{escaped_marker}\\s+([^\\n]{{10,200}})"
        
        match = re.search(pattern, remaining_text)
        if match:
            return match.group(1).strip()
        
        return None
    
    def _extract_end_footnotes(
        self,
        soup: BeautifulSoup,
        accession_number: str,
        section_id: Optional[int] = None,
    ) -> list[ExtractedFootnote]:
        """Extract end-of-section footnotes from HTML."""
        footnotes = []
        
        # Look for common footnote container elements
        footnote_containers = soup.find_all(
            ["div", "p", "span"],
            class_=re.compile(r"(?i)footnote|note|reference")
        )
        
        for container in footnote_containers:
            text = container.get_text().strip()
            
            # Extract marker from start of text
            marker_match = re.match(r'^([\*†‡§¶]|\(\d+\)|\[\d+\]|\d+)', text)
            if marker_match:
                marker = marker_match.group(1)
                footnote_text = text[len(marker):].strip()
                
                if len(footnote_text) > 10:  # Minimum footnote length
                    footnote = ExtractedFootnote(
                        footnote_id=str(uuid.uuid4()),
                        marker=marker,
                        footnote_text=footnote_text,
                        footnote_type="section",
                        accession_number=accession_number,
                        section_id=section_id,
                    )
                    footnotes.append(footnote)
        
        return footnotes
    
    def _build_cross_reference_graph(
        self,
        footnotes: list[ExtractedFootnote],
    ) -> list[ExtractedFootnote]:
        """Build cross-reference graph between footnotes and content."""
        
        for footnote in footnotes:
            # Extract cross-references from footnote text
            cross_refs = self._extract_cross_references(footnote.footnote_text)
            
            if cross_refs:
                footnote.ref_links = cross_refs
        
        return footnotes
    
    def _extract_cross_references(self, text: str) -> list:
        """Extract cross-references from text."""
        cross_refs = []
        
        for pattern, ref_type in self.CROSS_REF_PATTERNS:
            matches = re.finditer(pattern, text)
            for match in matches:
                cross_refs.append({
                    "type": ref_type,
                    "target": f"{ref_type} {match.group(1)}",
                    "text": match.group(0),
                })
        
        # Deduplicate
        unique_refs = []
        seen = set()
        for ref in cross_refs:
            key = (ref["type"], ref["target"])
            if key not in seen:
                seen.add(key)
                unique_refs.append(ref)
        
        return unique_refs
    
    def extract_document_notes(
        self,
        html_path,
        accession_number: str,
    ) -> list[ExtractedFootnote]:
        """
        Extract document-level notes (e.g., "Notes to Financial Statements").
        
        These are different from regular footnotes - they're full sections.
        """
        from pathlib import Path
        
        try:
            with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception as e:
            logger.error(f"Failed to read file: {e}")
            return []
        
        soup = BeautifulSoup(content, "lxml")
        notes = []
        
        # Look for "Notes to Financial Statements" or similar headings
        note_patterns = [
            r"(?i)notes?\s+to\s+(consolidated\s+)?financial\s+statements?",
            r"(?i)notes?\s+to\s+(consolidated\s+)?financial\s+statements?",
        ]
        
        text = soup.get_text()
        
        for pattern in note_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                start_pos = match.start()
                # Extract surrounding context (next 500 chars)
                note_text = text[start_pos:start_pos+500]
                
                note = ExtractedFootnote(
                    footnote_id=str(uuid.uuid4()),
                    marker="",
                    footnote_text=note_text,
                    footnote_type="document",
                    accession_number=accession_number,
                    position_in_text=start_pos,
                )
                notes.append(note)
        
        logger.info(f"Extracted {len(notes)} document-level notes")
        return notes
    
    def link_footnotes_to_sections(
        self,
        footnotes: list[ExtractedFootnote],
        sections: list,
    ) -> list[ExtractedFootnote]:
        """Link footnotes to their parent sections."""
        
        for footnote in footnotes:
            if footnote.section_id:
                # Find the section
                for section in sections:
                    if hasattr(section, 'id') and section.id == footnote.section_id:
                        # Add back-reference
                        if footnote.section_id not in footnote.referenced_by:
                            footnote.referenced_by.append({
                                "type": "section",
                                "id": footnote.section_id,
                                "section_type": getattr(section, 'section_type', None),
                            })
        
        return footnotes
    
    def link_footnotes_to_tables(
        self,
        footnotes: list[ExtractedFootnote],
        tables: list,
    ) -> list[ExtractedFootnote]:
        """Link footnotes to their parent tables."""
        
        for footnote in footnotes:
            if footnote.table_id:
                # Find the table
                for table in tables:
                    if hasattr(table, 'id') and table.id == footnote.table_id:
                        # Add back-reference
                        if footnote.table_id not in footnote.referenced_by:
                            footnote.referenced_by.append({
                                "type": "table",
                                "id": footnote.table_id,
                                "table_name": getattr(table, 'table_name', None),
                            })
        
        return footnotes
