"""
10-K Section extraction from SEC filings.

Extracts key sections (Item 1, 1A, 7, 8, 9A, etc.) from 10-K HTML documents.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, NavigableString, Tag

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
    
    def __post_init__(self):
        if self.content_text:
            self.word_count = len(self.content_text.split())
            self.character_count = len(self.content_text)
            self.paragraph_count = len([p for p in self.content_text.split("\n\n") if p.strip()])
    
    def to_dict(self) -> dict:
        """Convert to dictionary for database insertion."""
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
        }


# 10-K Section definitions with regex patterns
SECTION_DEFINITIONS = {
    "item_1": {
        "number": "1",
        "title": "Business",
        "patterns": [
            r"(?i)item\s+1\.?\s*[-—–]?\s*business\b(?!\s+acquired)",
            r"(?i)part\s+i\s*[-—–]?\s*item\s+1\.?\s*[-—–]?\s*business",
        ],
        "end_patterns": [
            r"(?i)item\s+1a\b",
            r"(?i)item\s+1b\b",
            r"(?i)item\s+2\b",
        ],
    },
    "item_1a": {
        "number": "1A",
        "title": "Risk Factors",
        "patterns": [
            r"(?i)item\s+1a\.?\s*[-—–]?\s*risk\s+factors",
            r"(?i)risk\s+factors\s*$",
        ],
        "end_patterns": [
            r"(?i)item\s+1b\b",
            r"(?i)item\s+2\b",
        ],
    },
    "item_1b": {
        "number": "1B",
        "title": "Unresolved Staff Comments",
        "patterns": [
            r"(?i)item\s+1b\.?\s*[-—–]?\s*unresolved\s+staff\s+comments",
        ],
        "end_patterns": [
            r"(?i)item\s+1c\b",
            r"(?i)item\s+2\b",
        ],
    },
    "item_2": {
        "number": "2",
        "title": "Properties",
        "patterns": [
            r"(?i)item\s+2\.?\s*[-—–]?\s*properties",
        ],
        "end_patterns": [
            r"(?i)item\s+3\b",
        ],
    },
    "item_3": {
        "number": "3",
        "title": "Legal Proceedings",
        "patterns": [
            r"(?i)item\s+3\.?\s*[-—–]?\s*legal\s+proceedings",
        ],
        "end_patterns": [
            r"(?i)item\s+4\b",
        ],
    },
    "item_5": {
        "number": "5",
        "title": "Market for Registrant's Common Equity",
        "patterns": [
            r"(?i)item\s+5\.?\s*[-—–]?\s*market\s+for",
        ],
        "end_patterns": [
            r"(?i)item\s+6\b",
        ],
    },
    "item_6": {
        "number": "6",
        "title": "Selected Financial Data",
        "patterns": [
            r"(?i)item\s+6\.?\s*[-—–]?\s*selected\s+financial\s+data",
            r"(?i)item\s+6\.?\s*[-—–]?\s*\[reserved\]",
        ],
        "end_patterns": [
            r"(?i)item\s+7\b",
        ],
    },
    "item_7": {
        "number": "7",
        "title": "Management's Discussion and Analysis",
        "patterns": [
            r"(?i)item\s+7\.?\s*[-—–]?\s*management.{0,5}s?\s+discussion",
            r"(?i)management.{0,5}s\s+discussion\s+and\s+analysis",
        ],
        "end_patterns": [
            r"(?i)item\s+7a\b",
            r"(?i)item\s+8\b",
        ],
    },
    "item_7a": {
        "number": "7A",
        "title": "Quantitative and Qualitative Disclosures About Market Risk",
        "patterns": [
            r"(?i)item\s+7a\.?\s*[-—–]?\s*quantitative",
            r"(?i)item\s+7a\.?\s*[-—–]?\s*market\s+risk",
        ],
        "end_patterns": [
            r"(?i)item\s+8\b",
        ],
    },
    "item_8": {
        "number": "8",
        "title": "Financial Statements and Supplementary Data",
        "patterns": [
            r"(?i)item\s+8\.?\s*[-—–]?\s*financial\s+statements",
        ],
        "end_patterns": [
            r"(?i)item\s+9\b",
        ],
    },
    "item_9": {
        "number": "9",
        "title": "Changes in and Disagreements with Accountants",
        "patterns": [
            r"(?i)item\s+9\.?\s*[-—–]?\s*changes\s+in\s+and\s+disagreements",
        ],
        "end_patterns": [
            r"(?i)item\s+9a\b",
            r"(?i)item\s+9b\b",
        ],
    },
    "item_9a": {
        "number": "9A",
        "title": "Controls and Procedures",
        "patterns": [
            r"(?i)item\s+9a\.?\s*[-—–]?\s*controls\s+and\s+procedures",
        ],
        "end_patterns": [
            r"(?i)item\s+9b\b",
            r"(?i)item\s+10\b",
            r"(?i)part\s+iii\b",
        ],
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
            
        except Exception as e:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.error(f"Failed to parse sections for {accession_number}: {e}")
            
            return SectionParseResult(
                success=False,
                accession_number=accession_number,
                error_message=str(e),
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
        """Extract a specific section from the document."""
        definition = SECTION_DEFINITIONS.get(section_type)
        if not definition:
            return None
        
        # Find section start
        start_pos = None
        matched_pattern = None
        extraction_confidence = 0.0
        
        for pattern in definition["patterns"]:
            match = re.search(pattern, text_content)
            if match:
                start_pos = match.end()
                matched_pattern = pattern
                extraction_confidence = 0.9
                break
        
        if start_pos is None:
            logger.debug(f"Section {section_type} not found")
            return None
        
        # Find section end
        end_pos = len(text_content)
        for end_pattern in definition["end_patterns"]:
            match = re.search(end_pattern, text_content[start_pos:])
            if match:
                end_pos = start_pos + match.start()
                break
        
        # Extract section content
        section_text = text_content[start_pos:end_pos].strip()
        
        # Limit section size
        if len(section_text) > self.max_section_chars:
            section_text = section_text[:self.max_section_chars]
            extraction_confidence *= 0.8
        
        # Clean section text
        section_text = self._clean_section_text(section_text)
        
        if not section_text or len(section_text) < 100:
            logger.debug(f"Section {section_type} too short or empty")
            return None
        
        # Extract HTML if requested
        section_html = None
        if self.preserve_html:
            section_html = self._extract_section_html(soup, definition)
        
        return ExtractedSection(
            section_type=section_type,
            section_number=definition["number"],
            section_title=definition["title"],
            content_text=section_text,
            content_html=section_html,
            extraction_confidence=extraction_confidence,
            extraction_method="regex",
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
    
    def _extract_section_html(
        self,
        soup: BeautifulSoup,
        definition: dict,
    ) -> Optional[str]:
        """Extract HTML for a section (simplified approach)."""
        # This is a simplified approach - full HTML extraction is complex
        # due to varied document structures
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
        """Extract section using XBRL tags if available."""
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
                
                return ExtractedSection(
                    section_type=section_type,
                    section_number=definition["number"],
                    section_title=definition["title"],
                    content_text=section_text,
                    content_html=section_html,
                    extraction_confidence=0.95,
                    extraction_method="xbrl_tag",
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
            "item_1": ["BusinessDescriptionAndBasisOfPresentationTextBlock", "NatureOfOperations"],
            "item_1a": ["RiskFactorsTextBlock"],
            "item_7": ["ManagementsDiscussionAndAnalysisOfFinancialConditionAndResultsOfOperations"],
            "item_8": ["FinancialStatementsAndSupplementaryData"],
            "item_9a": ["ControlsAndProcedures"],
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
