"""
10-K Section extraction from SEC filings.

Extracts key sections (Item 1, 1A, 7, 8, 9A, etc.) from 10-K HTML documents.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, NavigableString, Tag

from ..core.exceptions import SectionParsingError
from ..utils.logger import get_logger

logger = get_logger("finloom.parsers.section")


@dataclass
class ExtractedSection:
    """Represents an extracted section from a 10-K filing."""
    section_type: str           # item_1, item_1a, item_7, etc.
    section_number: str         # 1, 1A, 7, etc.
    section_title: str          # Full section title
    content_text: str           # Clean text content
    content_html: Optional[str] = None  # Original HTML
    word_count: int = 0
    character_count: int = 0
    paragraph_count: int = 0
    extraction_confidence: float = 0.0
    extraction_method: str = "regex"
    
    # NEW: Hierarchy metadata
    section_part: Optional[str] = None  # "Part I", "Part II", "Part III", "Part IV"
    parent_section_id: Optional[int] = None
    subsections: dict = field(default_factory=dict)  # {1: "Overview", 2: "Products"}
    
    # NEW: Content composition
    contains_tables: int = 0
    contains_lists: int = 0
    contains_footnotes: int = 0
    
    # NEW: Cross-references
    cross_references: list = field(default_factory=list)  # [{"target": "Item 7", "text": "See Item 7"}]
    
    # NEW: Structure
    page_numbers: dict = field(default_factory=dict)  # {"start": 10, "end": 25}
    heading_hierarchy: list = field(default_factory=list)  # ["Business", "Products", "iPhone"]
    
    # NEW: Quality metadata
    extraction_quality: float = 0.0
    extraction_issues: list = field(default_factory=list)
    
    def __post_init__(self):
        if self.content_text:
            self.word_count = len(self.content_text.split())
            self.character_count = len(self.content_text)
            self.paragraph_count = len([p for p in self.content_text.split("\n\n") if p.strip()])
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
        import json
        return {
            "section_type": self.section_type,
            "section_number": self.section_number,
            "section_title": self.section_title,
            "content_text": self.content_text,
            "content_html": self.content_html,
            "word_count": self.word_count,
            "character_count": self.character_count,
            "paragraph_count": self.paragraph_count,
            "extraction_confidence": self.extraction_confidence,
            "extraction_method": self.extraction_method,
            # NEW fields
            "section_part": self.section_part,
            "parent_section_id": self.parent_section_id,
            "subsections": json.dumps(self.subsections) if self.subsections else None,
            "contains_tables": self.contains_tables,
            "contains_lists": self.contains_lists,
            "contains_footnotes": self.contains_footnotes,
            "cross_references": json.dumps(self.cross_references) if self.cross_references else None,
            "page_numbers": json.dumps(self.page_numbers) if self.page_numbers else None,
            "heading_hierarchy": json.dumps(self.heading_hierarchy) if self.heading_hierarchy else None,
            "extraction_quality": self.extraction_quality,
            "extraction_issues": json.dumps(self.extraction_issues) if self.extraction_issues else None,
        }


# 10-K Section definitions with regex patterns
# Complete set of all 15+ Item sections with improved end patterns
SECTION_DEFINITIONS = {
    "item_1": {
        "number": "1",
        "title": "Business",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+1\.?\s*[-—–]?\s*business\b(?!\s+acquired)",
            r"(?i)part\s+i\s*[-—–]?\s*item\s+1\.?\s*[-—–]?\s*business",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+1a[\.\s]",
            r"(?i)^\s*item\s+1b[\.\s]",
            r"(?i)^\s*item\s+2[\.\s]",
        ],
        "min_words": 1000,
    },
    "item_1a": {
        "number": "1A",
        "title": "Risk Factors",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+1a\.?\s*[-—–]?\s*risk\s+factors",
            r"(?i)risk\s+factors\s*$",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+1b[\.\s]",
            r"(?i)^\s*item\s+1c[\.\s]",
            r"(?i)^\s*item\s+2[\.\s]",
        ],
        "min_words": 2000,
    },
    "item_1b": {
        "number": "1B",
        "title": "Unresolved Staff Comments",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+1b\.?\s*[-—–]?\s*unresolved\s+staff\s+comments",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+1c[\.\s]",
            r"(?i)^\s*item\s+2[\.\s]",
        ],
        "min_words": 10,
    },
    "item_1c": {
        "number": "1C",
        "title": "Cybersecurity",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+1c\.?\s*[-—–]?\s*cybersecurity",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+2[\.\s]",
        ],
        "min_words": 200,
    },
    "item_2": {
        "number": "2",
        "title": "Properties",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+2\.?\s*[-—–]?\s*properties",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+3[\.\s]",
        ],
        "min_words": 100,
    },
    "item_3": {
        "number": "3",
        "title": "Legal Proceedings",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+3\.?\s*[-—–]?\s*legal\s+proceedings",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+4[\.\s]",
        ],
        "min_words": 50,
    },
    "item_4": {
        "number": "4",
        "title": "Mine Safety Disclosures",
        "part": "Part I",
        "patterns": [
            r"(?i)item\s+4\.?\s*[-—–]?\s*mine\s+safety",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+5[\.\s]",
            r"(?i)^\s*part\s+ii\b",
        ],
        "min_words": 10,
    },
    "item_5": {
        "number": "5",
        "title": "Market for Registrant's Common Equity",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+5\.?\s*[-—–]?\s*market\s+for",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+6[\.\s]",
        ],
        "min_words": 200,
    },
    "item_6": {
        "number": "6",
        "title": "Reserved/Selected Financial Data",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+6\.?\s*[-—–]?\s*selected\s+financial\s+data",
            r"(?i)item\s+6\.?\s*[-—–]?\s*\[reserved\]",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+7[\.\s]",
        ],
        "min_words": 10,
    },
    "item_7": {
        "number": "7",
        "title": "Management's Discussion and Analysis",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+7\.?\s*[-—–]?\s*management.{0,5}s?\s+discussion",
            r"(?i)management.{0,5}s\s+discussion\s+and\s+analysis",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+7a[\.\s]",
            r"(?i)^\s*item\s+8[\.\s]",
        ],
        "min_words": 5000,
    },
    "item_7a": {
        "number": "7A",
        "title": "Quantitative and Qualitative Disclosures About Market Risk",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+7a\.?\s*[-—–]?\s*quantitative",
            r"(?i)item\s+7a\.?\s*[-—–]?\s*market\s+risk",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+8[\.\s]",
        ],
        "min_words": 500,
    },
    "item_8": {
        "number": "8",
        "title": "Financial Statements and Supplementary Data",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+8\.?\s*[-—–]?\s*financial\s+statements",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+9[\.\s]",
            r"(?i)^\s*part\s+iii\b",
        ],
        "min_words": 10000,
    },
    "item_9": {
        "number": "9",
        "title": "Changes in and Disagreements with Accountants",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+9\.?\s*[-—–]?\s*changes\s+in\s+and\s+disagreements",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+9a[\.\s]",
            r"(?i)^\s*item\s+9b[\.\s]",
        ],
        "min_words": 50,
    },
    "item_9a": {
        "number": "9A",
        "title": "Controls and Procedures",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+9a\.?\s*[-—–]?\s*controls\s+and\s+procedures",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+9b[\.\s]",
            r"(?i)^\s*item\s+9c[\.\s]",
            r"(?i)^\s*item\s+10[\.\s]",
            r"(?i)^\s*part\s+iii\b",
        ],
        "min_words": 500,
    },
    "item_9b": {
        "number": "9B",
        "title": "Other Information",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+9b\.?\s*[-—–]?\s*other\s+information",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+9c[\.\s]",
            r"(?i)^\s*item\s+10[\.\s]",
            r"(?i)^\s*part\s+iii\b",
        ],
        "min_words": 10,
    },
    "item_9c": {
        "number": "9C",
        "title": "Disclosure Regarding Foreign Jurisdictions",
        "part": "Part II",
        "patterns": [
            r"(?i)item\s+9c\.?\s*[-—–]?\s*disclosure\s+regarding\s+foreign",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+10[\.\s]",
            r"(?i)^\s*part\s+iii\b",
        ],
        "min_words": 10,
    },
    "item_10": {
        "number": "10",
        "title": "Directors, Executive Officers and Corporate Governance",
        "part": "Part III",
        "patterns": [
            r"(?i)item\s+10\.?\s*[-—–]?\s*directors",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+11[\.\s]",
        ],
        "min_words": 500,
    },
    "item_11": {
        "number": "11",
        "title": "Executive Compensation",
        "part": "Part III",
        "patterns": [
            r"(?i)item\s+11\.?\s*[-—–]?\s*executive\s+compensation",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+12[\.\s]",
        ],
        "min_words": 1000,
    },
    "item_12": {
        "number": "12",
        "title": "Security Ownership of Certain Beneficial Owners and Management",
        "part": "Part III",
        "patterns": [
            r"(?i)item\s+12\.?\s*[-—–]?\s*security\s+ownership",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+13[\.\s]",
        ],
        "min_words": 200,
    },
    "item_13": {
        "number": "13",
        "title": "Certain Relationships and Related Transactions",
        "part": "Part III",
        "patterns": [
            r"(?i)item\s+13\.?\s*[-—–]?\s*certain\s+relationships",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+14[\.\s]",
        ],
        "min_words": 200,
    },
    "item_14": {
        "number": "14",
        "title": "Principal Accountant Fees and Services",
        "part": "Part III",
        "patterns": [
            r"(?i)item\s+14\.?\s*[-—–]?\s*principal\s+accountant",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+15[\.\s]",
            r"(?i)^\s*part\s+iv\b",
        ],
        "min_words": 100,
    },
    "item_15": {
        "number": "15",
        "title": "Exhibits and Financial Statement Schedules",
        "part": "Part IV",
        "patterns": [
            r"(?i)item\s+15\.?\s*[-—–]?\s*exhibits",
        ],
        "end_patterns": [
            r"(?i)^\s*item\s+16[\.\s]",
            r"(?i)^\s*signatures?\s*$",
        ],
        "min_words": 100,
    },
    "item_16": {
        "number": "16",
        "title": "Form 10-K Summary",
        "part": "Part IV",
        "patterns": [
            r"(?i)item\s+16\.?\s*[-—–]?\s*form\s+10-k\s+summary",
        ],
        "end_patterns": [
            r"(?i)^\s*signatures?\s*$",
        ],
        "min_words": 10,
    },
}

# Priority sections to extract (as per plan)
PRIORITY_SECTIONS = ["item_1", "item_1a", "item_7", "item_8", "item_9a"]


@dataclass
class SectionParseResult:
    """Result of section extraction from a filing."""
    success: bool
    accession_number: str
    sections: list[ExtractedSection] = field(default_factory=list)
    error_message: Optional[str] = None
    parse_time_ms: float = 0.0
    
    @property
    def section_count(self) -> int:
        return len(self.sections)
    
    def get_section(self, section_type: str) -> Optional[ExtractedSection]:
        for section in self.sections:
            if section.section_type == section_type:
                return section
        return None


class SectionParser:
    """
    Parser for extracting 10-K sections from HTML filings.
    
    Uses regex patterns to identify section boundaries and extracts clean text.
    """
    
    def __init__(
        self,
        priority_only: bool = True,
        preserve_html: bool = False,
        max_section_chars: int = 5_000_000,
    ) -> None:
        """
        Initialize section parser.
        
        Args:
            priority_only: If True, only extract priority sections.
            preserve_html: If True, also store original HTML.
            max_section_chars: Maximum characters per section.
        """
        self.priority_only = priority_only
        self.preserve_html = preserve_html
        self.max_section_chars = max_section_chars
        
        if priority_only:
            self.sections_to_extract = PRIORITY_SECTIONS
        else:
            self.sections_to_extract = list(SECTION_DEFINITIONS.keys())
        
        logger.info(
            f"Section parser initialized. Extracting: {self.sections_to_extract}"
        )
    
    def parse_filing(
        self,
        filing_path: Path,
        accession_number: str,
    ) -> SectionParseResult:
        """
        Parse sections from a 10-K filing.
        
        Args:
            filing_path: Path to filing directory or HTML file.
            accession_number: Filing accession number.
        
        Returns:
            SectionParseResult with extracted sections.
        """
        import time
        start_time = time.time()
        
        logger.info(f"Parsing sections for {accession_number}")
        
        # Find the primary HTML document
        html_file = self._find_primary_document(filing_path)
        
        if not html_file:
            return SectionParseResult(
                success=False,
                accession_number=accession_number,
                error_message="No HTML document found",
            )
        
        try:
            # Read and parse HTML
            with open(html_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            
            soup = BeautifulSoup(content, "lxml")
            
            # Get text content with position mapping
            text_content = self._extract_text(soup)
            
            # Extract each section
            sections = []
            for section_type in self.sections_to_extract:
                section = self._extract_section(
                    text_content,
                    soup,
                    section_type,
                )
                if section:
                    sections.append(section)
            
            elapsed_ms = (time.time() - start_time) * 1000
            
            logger.info(
                f"Extracted {len(sections)} sections from {accession_number} "
                f"in {elapsed_ms:.0f}ms"
            )
            
            return SectionParseResult(
                success=True,
                accession_number=accession_number,
                sections=sections,
                parse_time_ms=elapsed_ms,
            )
            
        except (OSError, ValueError, AttributeError) as e:
            # Expected parsing failures (file issues, malformed HTML)
            elapsed_ms = (time.time() - start_time) * 1000
            logger.warning(f"Failed to parse sections for {accession_number}: {e}")

            return SectionParseResult(
                success=False,
                accession_number=accession_number,
                error_message=str(e),
                parse_time_ms=elapsed_ms,
            )
        except Exception as e:
            # Unexpected error - log at error level
            elapsed_ms = (time.time() - start_time) * 1000
            logger.error(f"Unexpected error parsing sections for {accession_number}: {type(e).__name__}: {e}")

            return SectionParseResult(
                success=False,
                accession_number=accession_number,
                error_message=f"Unexpected error: {type(e).__name__}: {e}",
                parse_time_ms=elapsed_ms,
            )
    
    def _find_primary_document(self, filing_path: Path) -> Optional[Path]:
        """Find the primary HTML document in a filing."""
        if filing_path.is_file():
            return filing_path
        
        # Look for common primary document patterns
        patterns = [
            "*10-k*.htm",
            "*10k*.htm",
            "*annual*.htm",
            "*.htm",
        ]
        
        for pattern in patterns:
            files = list(filing_path.glob(pattern))
            # Filter out exhibit files
            files = [f for f in files if "ex" not in f.name.lower()[:3]]
            if files:
                # Return largest file (usually the main document)
                files.sort(key=lambda x: x.stat().st_size, reverse=True)
                return files[0]
        
        return None
    
    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract clean text from HTML while preserving structure."""
        # Remove script and style elements
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        
        # Get text with separator
        text = soup.get_text(separator="\n")
        
        # Clean up whitespace while preserving paragraph breaks
        lines = []
        for line in text.split("\n"):
            line = line.strip()
            if line:
                lines.append(line)
            elif lines and lines[-1]:  # Add blank line for paragraph break
                lines.append("")
        
        return "\n".join(lines)
    
    def _extract_section(
        self,
        text_content: str,
        soup: BeautifulSoup,
        section_type: str,
    ) -> Optional[ExtractedSection]:
        """Extract a specific section from the document with rich metadata."""
        definition = SECTION_DEFINITIONS.get(section_type)
        if not definition:
            return None

        # Find ALL matches and pick the one with the most content
        # This handles TOC entries vs actual section content
        candidates = []
        min_words = definition.get("min_words", 100)

        for pattern in definition["patterns"]:
            for match in re.finditer(pattern, text_content, re.MULTILINE):
                candidate_start = match.end()

                # Find section end for this candidate
                candidate_end = len(text_content)
                for end_pattern in definition["end_patterns"]:
                    end_match = re.search(end_pattern, text_content[candidate_start:], re.MULTILINE)
                    if end_match:
                        candidate_end = candidate_start + end_match.start()
                        break

                # Calculate section length
                candidate_text = text_content[candidate_start:candidate_end].strip()
                word_count = len(candidate_text.split())

                # Only consider if it meets minimum threshold (10% of min_words)
                if word_count >= min_words * 0.1:
                    candidates.append({
                        'start': candidate_start,
                        'end': candidate_end,
                        'word_count': word_count,
                        'pattern': pattern,
                    })

        if not candidates:
            logger.debug(f"Section {section_type} not found (no valid candidates)")
            return None

        # Pick the candidate with the most words (actual content, not TOC)
        best = max(candidates, key=lambda x: x['word_count'])
        start_pos = best['start']
        end_pos = best['end']
        matched_pattern = best['pattern']
        extraction_confidence = 0.9
        issues = []

        logger.debug(f"Section {section_type}: found {len(candidates)} candidates, "
                    f"best has {best['word_count']} words")

        # Extract section content
        section_text = text_content[start_pos:end_pos].strip()
        
        # Validate section length
        min_words = definition.get("min_words", 100)
        actual_words = len(section_text.split())
        
        if actual_words < min_words * 0.1:
            logger.debug(f"Section {section_type} too short ({actual_words} words, min {min_words})")
            return None
        
        if actual_words < min_words:
            issues.append(f"Section shorter than expected (got {actual_words}, expected {min_words}+)")
            extraction_confidence *= 0.8
        
        # Limit section size
        if len(section_text) > self.max_section_chars:
            section_text = section_text[:self.max_section_chars]
            extraction_confidence *= 0.8
            issues.append("Section truncated due to length")
        
        # Extract metadata
        contains_tables = len(re.findall(r"(?i)<table", str(soup)))
        contains_lists = len(re.findall(r"(?i)^[\s•\-\*]\s*\w", section_text, re.MULTILINE))
        contains_footnotes = len(re.findall(r"[\*†‡§¶]|\(\d+\)|\[\d+\]", section_text))
        
        # Extract cross-references
        cross_references = self._extract_cross_references(section_text)
        
        # Extract heading hierarchy
        heading_hierarchy = self._extract_heading_hierarchy(section_text)
        
        # Clean section text
        section_text = self._clean_section_text(section_text)
        
        if not section_text or len(section_text) < 100:
            logger.debug(f"Section {section_type} too short or empty after cleaning")
            return None
        
        # Extract HTML if requested
        section_html = None
        if self.preserve_html:
            section_html = self._extract_section_html(soup, start_pos, end_pos)
        
        # Calculate extraction quality
        extraction_quality = extraction_confidence
        if actual_words < min_words * 0.5:
            extraction_quality *= 0.7
        if not cross_references and section_type in ["item_7", "item_8"]:
            extraction_quality *= 0.95  # Slight penalty, but not critical
        
        return ExtractedSection(
            section_type=section_type,
            section_number=definition["number"],
            section_title=definition["title"],
            content_text=section_text,
            content_html=section_html,
            extraction_confidence=extraction_confidence,
            extraction_method="regex",
            # NEW metadata
            section_part=definition.get("part"),
            contains_tables=contains_tables,
            contains_lists=contains_lists,
            contains_footnotes=contains_footnotes,
            cross_references=cross_references,
            heading_hierarchy=heading_hierarchy,
            extraction_quality=extraction_quality,
            extraction_issues=issues,
        )
    
    def _clean_section_text(self, text: str) -> str:
        """Clean and normalize section text."""
        # Remove excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        
        # Remove page numbers and headers
        text = re.sub(r"\n\s*\d+\s*\n", "\n", text)
        text = re.sub(r"(?i)\n\s*table\s+of\s+contents\s*\n", "\n", text)
        
        # Remove form headers
        text = re.sub(r"(?i)form\s+10-k\s*\n", "", text)
        
        return text.strip()
    
    def _extract_cross_references(self, text: str) -> list:
        """Extract cross-references to other sections."""
        cross_refs = []
        
        # Patterns for cross-references
        patterns = [
            (r"(?i)see\s+item\s+(\d+[A-Z]?)", "Item"),
            (r"(?i)refer\s+to\s+item\s+(\d+[A-Z]?)", "Item"),
            (r"(?i)discussed\s+in\s+item\s+(\d+[A-Z]?)", "Item"),
            (r"(?i)see\s+note\s+(\d+)", "Note"),
            (r"(?i)refer\s+to\s+note\s+(\d+)", "Note"),
            (r"(?i)see\s+part\s+(I{1,3}|IV)", "Part"),
        ]
        
        for pattern, ref_type in patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                cross_refs.append({
                    "target": f"{ref_type} {match.group(1)}",
                    "text": match.group(0),
                    "position": match.start()
                })
        
        # Deduplicate while preserving order
        seen = set()
        unique_refs = []
        for ref in cross_refs:
            key = (ref["target"], ref["text"])
            if key not in seen:
                seen.add(key)
                unique_refs.append({"target": ref["target"], "text": ref["text"]})
        
        return unique_refs
    
    def _extract_heading_hierarchy(self, text: str) -> list:
        """Extract heading hierarchy from section text."""
        headings = []
        
        # Pattern for potential headings (short lines, possibly in caps or title case)
        lines = text.split("\n")
        for i, line in enumerate(lines):
            line = line.strip()
            # Check if line looks like a heading:
            # - Short (< 100 chars)
            # - No ending punctuation (except colon)
            # - Followed by content or blank line
            if (len(line) > 5 and len(line) < 100 and 
                not line.endswith(('.', ',', ';')) and
                (line.isupper() or line.istitle() or line.endswith(':'))):
                # Check if followed by content
                if i + 1 < len(lines) and (not lines[i+1].strip() or len(lines[i+1]) > 50):
                    headings.append(line.rstrip(':'))
        
        # Limit to top 10 headings
        return headings[:10]
    
    def _extract_section_html(
        self,
        soup: BeautifulSoup,
        start_pos: int,
        end_pos: int,
    ) -> Optional[str]:
        """
        Extract HTML fragment between start and end positions.
        
        This is an approximation - tries to find HTML elements that contain
        the text range. Not perfect but sufficient for table extraction.
        """
        try:
            # Get all text from soup to find positions
            full_text = soup.get_text()
            
            # Safety check
            if start_pos >= len(full_text) or end_pos > len(full_text):
                return None
            
            # Extract the text segment
            target_text = full_text[start_pos:end_pos]
            
            # Find all elements that might contain this text
            # We'll return a simplified HTML that includes tables and structure
            body = soup.find('body') or soup
            
            # Return the HTML string of the body (or whole soup)
            # The table parser will find <table> tags within this
            return str(body)
            
        except (ValueError, AttributeError, IndexError) as e:
            # Expected issues with HTML extraction from malformed content
            logger.debug(f"Could not extract HTML: {e}")
            return None


class InlineXBRLSectionParser(SectionParser):
    """
    Section parser optimized for Inline XBRL documents.
    
    Uses XBRL tags to identify section boundaries where available.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logger.info("Inline XBRL section parser initialized")
    
    def _extract_section(
        self,
        text_content: str,
        soup: BeautifulSoup,
        section_type: str,
    ) -> Optional[ExtractedSection]:
        """Extract section using XBRL tags if available, with rich metadata."""
        definition = SECTION_DEFINITIONS.get(section_type)
        if not definition:
            return None
        
        # Try to find XBRL-tagged section first
        section_element = self._find_xbrl_section(soup, section_type)
        
        if section_element:
            section_text = section_element.get_text(separator="\n")
            section_text = self._clean_section_text(section_text)
            
            if section_text and len(section_text) >= 100:
                section_html = str(section_element) if self.preserve_html else None
                
                # Extract metadata
                contains_tables = len(section_element.find_all("table"))
                contains_lists = len(re.findall(r"(?i)^[\s•\-\*]\s*\w", section_text, re.MULTILINE))
                contains_footnotes = len(re.findall(r"[\*†‡§¶]|\(\d+\)|\[\d+\]", section_text))
                cross_references = self._extract_cross_references(section_text)
                heading_hierarchy = self._extract_heading_hierarchy(section_text)
                
                return ExtractedSection(
                    section_type=section_type,
                    section_number=definition["number"],
                    section_title=definition["title"],
                    content_text=section_text,
                    content_html=section_html,
                    extraction_confidence=0.95,
                    extraction_method="xbrl_tag",
                    # NEW metadata
                    section_part=definition.get("part"),
                    contains_tables=contains_tables,
                    contains_lists=contains_lists,
                    contains_footnotes=contains_footnotes,
                    cross_references=cross_references,
                    heading_hierarchy=heading_hierarchy,
                    extraction_quality=0.95,
                )
        
        # Fall back to regex-based extraction
        return super()._extract_section(text_content, soup, section_type)
    
    def _find_xbrl_section(
        self,
        soup: BeautifulSoup,
        section_type: str,
    ) -> Optional[Tag]:
        """Find XBRL-tagged section element."""
        # Common XBRL section names
        xbrl_names = {
            "item_1": ["BusinessDescriptionAndBasisOfPresentationTextBlock", "NatureOfOperations", "BusinessDescriptionTextBlock"],
            "item_1a": ["RiskFactorsTextBlock"],
            "item_1b": ["UnresolvedStaffCommentsTextBlock"],
            "item_2": ["PropertiesTextBlock"],
            "item_3": ["LegalProceedingsTextBlock"],
            "item_7": ["ManagementsDiscussionAndAnalysisOfFinancialConditionAndResultsOfOperationsTextBlock"],
            "item_7a": ["QuantitativeAndQualitativeDisclosuresAboutMarketRiskTextBlock"],
            "item_8": ["FinancialStatementsAndSupplementaryDataTextBlock"],
            "item_9a": ["ControlsAndProceduresTextBlock"],
            "item_10": ["DirectorsExecutiveOfficersAndCorporateGovernanceTextBlock"],
            "item_11": ["ExecutiveCompensationTextBlock"],
            "item_12": ["SecurityOwnershipOfCertainBeneficialOwnersAndManagementTextBlock"],
        }
        
        names = xbrl_names.get(section_type, [])
        
        for name in names:
            # Look for ix:nonnumeric or div with matching name
            element = soup.find(attrs={"name": re.compile(name, re.I)})
            if element:
                return element
            
            # Look for continuation elements
            element = soup.find("ix:continuation", attrs={"name": re.compile(name, re.I)})
            if element:
                return element
        
        return None
