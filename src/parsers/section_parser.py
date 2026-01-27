"""
10-K Section extraction from SEC filings.

Extracts key sections (Item 1, 1A, 7, 8, 9A, etc.) from 10-K HTML documents.
Uses the unstructured library for robust HTML parsing.
"""

import re
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Add unstructured library to path
UNSTRUCTURED_PATH = Path(__file__).parent.parent.parent / "unstructured-main"
if str(UNSTRUCTURED_PATH) not in sys.path:
    sys.path.insert(0, str(UNSTRUCTURED_PATH))

# Import unstructured library components
from unstructured.partition.html import partition_html
from unstructured.staging.base import elements_to_md
from unstructured.documents.elements import Element

from ..config.extraction_config import get_section_extraction_config
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
    content_markdown: Optional[str] = None  # Markdown content for RAG
    word_count: int = 0
    character_count: int = 0
    paragraph_count: int = 0
    extraction_confidence: float = 0.0
    extraction_method: str = "unstructured"

    # Hierarchy metadata
    section_part: Optional[str] = None  # "Part I", "Part II", "Part III", "Part IV"
    parent_section_id: Optional[int] = None
    subsections: dict = field(default_factory=dict)  # {1: "Overview", 2: "Products"}

    # Content composition
    contains_tables: int = 0
    contains_lists: int = 0
    contains_footnotes: int = 0

    # Cross-references
    cross_references: list = field(default_factory=list)  # [{"target": "Item 7", "text": "See Item 7"}]

    # Structure
    page_numbers: dict = field(default_factory=dict)  # {"start": 10, "end": 25}
    heading_hierarchy: list = field(default_factory=list)  # ["Business", "Products", "iPhone"]

    # Quality metadata
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
            "content_markdown": self.content_markdown,
            "word_count": self.word_count,
            "character_count": self.character_count,
            "paragraph_count": self.paragraph_count,
            "extraction_confidence": self.extraction_confidence,
            "extraction_method": self.extraction_method,
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


@dataclass
class FullMarkdownResult:
    """Result of full document markdown extraction with embedded section markers."""
    full_markdown: str                    # Complete document with section markers
    sections_found: list[str]             # List of detected section IDs
    word_count: int                       # Total word count
    character_count: int                  # Total character count
    extraction_quality: float             # Overall quality score (0-1)
    sections: list[ExtractedSection]      # Extracted sections (for sections table)

    def to_dict(self) -> dict:
        """Convert to dictionary for database storage."""
        return {
            "full_markdown": self.full_markdown,
            "markdown_word_count": self.word_count,
            "sections_found": len(self.sections_found),
        }


# 10-K Section definitions
# Complete set of all 15+ Item sections
SECTION_DEFINITIONS = {
    "item_1": {
        "number": "1",
        "title": "Business",
        "part": "Part I",
    },
    "item_1a": {
        "number": "1A",
        "title": "Risk Factors",
        "part": "Part I",
    },
    "item_1b": {
        "number": "1B",
        "title": "Unresolved Staff Comments",
        "part": "Part I",
    },
    "item_1c": {
        "number": "1C",
        "title": "Cybersecurity",
        "part": "Part I",
    },
    "item_2": {
        "number": "2",
        "title": "Properties",
        "part": "Part I",
    },
    "item_3": {
        "number": "3",
        "title": "Legal Proceedings",
        "part": "Part I",
    },
    "item_4": {
        "number": "4",
        "title": "Mine Safety Disclosures",
        "part": "Part I",
    },
    "item_5": {
        "number": "5",
        "title": "Market for Registrant's Common Equity",
        "part": "Part II",
    },
    "item_6": {
        "number": "6",
        "title": "Reserved/Selected Financial Data",
        "part": "Part II",
    },
    "item_7": {
        "number": "7",
        "title": "Management's Discussion and Analysis",
        "part": "Part II",
    },
    "item_7a": {
        "number": "7A",
        "title": "Quantitative and Qualitative Disclosures About Market Risk",
        "part": "Part II",
    },
    "item_8": {
        "number": "8",
        "title": "Financial Statements and Supplementary Data",
        "part": "Part II",
    },
    "item_9": {
        "number": "9",
        "title": "Changes in and Disagreements with Accountants",
        "part": "Part II",
    },
    "item_9a": {
        "number": "9A",
        "title": "Controls and Procedures",
        "part": "Part II",
    },
    "item_9b": {
        "number": "9B",
        "title": "Other Information",
        "part": "Part II",
    },
    "item_9c": {
        "number": "9C",
        "title": "Disclosure Regarding Foreign Jurisdictions",
        "part": "Part II",
    },
    "item_10": {
        "number": "10",
        "title": "Directors, Executive Officers and Corporate Governance",
        "part": "Part III",
    },
    "item_11": {
        "number": "11",
        "title": "Executive Compensation",
        "part": "Part III",
    },
    "item_12": {
        "number": "12",
        "title": "Security Ownership of Certain Beneficial Owners and Management",
        "part": "Part III",
    },
    "item_13": {
        "number": "13",
        "title": "Certain Relationships and Related Transactions",
        "part": "Part III",
    },
    "item_14": {
        "number": "14",
        "title": "Principal Accountant Fees and Services",
        "part": "Part III",
    },
    "item_15": {
        "number": "15",
        "title": "Exhibits and Financial Statement Schedules",
        "part": "Part IV",
    },
    "item_16": {
        "number": "16",
        "title": "Form 10-K Summary",
        "part": "Part IV",
    },
}

# Priority sections to extract
PRIORITY_SECTIONS = ["item_1", "item_1a", "item_7", "item_8", "item_9a"]

# Pattern to detect SEC section headers in element text
SEC_ITEM_PATTERN = re.compile(
    r'''(?ix)                           # case insensitive, verbose
    ^\s*                                # leading whitespace
    (?:part\s+[IVX]+\s*[-—–]?\s*)?     # optional "Part I -" prefix
    item\s+                             # required "Item "
    (\d+[A-C]?)                         # capture: section number (1, 1A, 7A, 9C, etc.)
    \.?\s*                              # optional period and whitespace
    [-—–]?\s*                           # optional separator dash
    (.*)                                # capture: section title (rest of line)
    $
    ''',
    re.IGNORECASE | re.VERBOSE
)


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

    Uses the unstructured library for robust HTML parsing that handles
    format variations across different companies.
    """

    def __init__(
        self,
        priority_only: bool = True,
        preserve_html: bool = False,
        config: Optional['SectionExtractionConfig'] = None,
    ) -> None:
        """
        Initialize section parser.

        Args:
            priority_only: If True, only extract priority sections.
            preserve_html: If True, also store original HTML for tables.
            config: Extraction configuration (uses default if not provided).
        """
        self.priority_only = priority_only
        self.preserve_html = preserve_html
        self.config = config or get_section_extraction_config()

        if priority_only:
            self.sections_to_extract = PRIORITY_SECTIONS
        else:
            self.sections_to_extract = list(SECTION_DEFINITIONS.keys())

        logger.info(
            f"Section parser initialized with unstructured. Extracting: {self.sections_to_extract}"
        )

    def parse_filing(
        self,
        filing_path: Path,
        accession_number: str,
    ) -> SectionParseResult:
        """
        Parse sections from a 10-K filing using unstructured library.

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
            sections = self.extract_sections(html_file)

            elapsed_ms = (time.time() - start_time) * 1000

            logger.info(
                f"Extracted {len(sections)} sections from {accession_number} "
                f"in {elapsed_ms:.0f}ms using unstructured"
            )

            return SectionParseResult(
                success=True,
                accession_number=accession_number,
                sections=sections,
                parse_time_ms=elapsed_ms,
            )

        except SectionParsingError as e:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.warning(f"Failed to parse sections for {accession_number}: {e}")

            return SectionParseResult(
                success=False,
                accession_number=accession_number,
                error_message=str(e),
                parse_time_ms=elapsed_ms,
            )
        except Exception as e:
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

    # =========================================================================
    # Unstructured-based extraction
    # =========================================================================

    def _detect_section_header(self, element: Element) -> Optional[tuple[str, str, str]]:
        """
        Detect if an element is an SEC section header.

        Args:
            element: An unstructured Element object

        Returns:
            tuple of (section_id, section_number, section_title) or None
            e.g., ("item_1a", "1A", "Risk Factors")
        """
        # Check Title and UncategorizedText elements (SEC inline XBRL uses UncategorizedText)
        if element.category not in ("Title", "UncategorizedText"):
            return None

        text = element.text.strip() if element.text else ""
        if not text:
            return None

        # For UncategorizedText, only check short text that looks like headers
        # (avoid processing long paragraphs)
        if element.category == "UncategorizedText" and len(text) > 200:
            return None

        match = SEC_ITEM_PATTERN.match(text)
        if not match:
            return None

        section_number = match.group(1).upper()  # e.g., "1A"
        section_title = match.group(2).strip()   # e.g., "Risk Factors"
        section_id = f"item_{section_number.lower()}"  # e.g., "item_1a"

        # Validate section_id exists in our definitions
        if section_id not in SECTION_DEFINITIONS:
            logger.debug(f"Unknown section detected: {section_id}")
            return None

        return (section_id, section_number, section_title)

    def _group_elements_by_section(
        self,
        elements: list[Element]
    ) -> dict[str, tuple[str, str, list[Element]]]:
        """
        Group elements between SEC section headers.

        Returns dict mapping section_id to (section_number, section_title, list of elements).
        """
        sections = {}
        current_section_id = None
        current_section_number = None
        current_section_title = None
        current_elements = []

        for element in elements:
            # Check if this is a section header
            header_info = self._detect_section_header(element)

            if header_info:
                # Save previous section if exists and has content
                if current_section_id and current_elements:
                    # Keep longest version if duplicate section_id
                    if current_section_id in sections:
                        existing_len = sum(len(e.text or '') for e in sections[current_section_id][2])
                        new_len = sum(len(e.text or '') for e in current_elements)
                        if new_len > existing_len:
                            sections[current_section_id] = (
                                current_section_number,
                                current_section_title,
                                current_elements
                            )
                    else:
                        sections[current_section_id] = (
                            current_section_number,
                            current_section_title,
                            current_elements
                        )

                # Start new section
                section_id, section_number, section_title = header_info
                current_section_id = section_id
                current_section_number = section_number
                current_section_title = section_title
                current_elements = [element]  # Include header element
            else:
                # Add to current section
                if current_section_id:
                    current_elements.append(element)

        # Save final section
        if current_section_id and current_elements:
            if current_section_id in sections:
                existing_len = sum(len(e.text or '') for e in sections[current_section_id][2])
                new_len = sum(len(e.text or '') for e in current_elements)
                if new_len > existing_len:
                    sections[current_section_id] = (
                        current_section_number,
                        current_section_title,
                        current_elements
                    )
            else:
                sections[current_section_id] = (
                    current_section_number,
                    current_section_title,
                    current_elements
                )

        return sections

    def _elements_to_extracted_section(
        self,
        section_id: str,
        section_number: str,
        section_title: str,
        elements: list[Element],
    ) -> Optional[ExtractedSection]:
        """
        Convert grouped elements to an ExtractedSection object.
        """
        if not elements:
            return None

        # Get section definition
        section_def = SECTION_DEFINITIONS.get(section_id, {})

        # Use extracted title or fall back to definition
        title = section_title or section_def.get('title', '')

        # Concatenate text content from all elements
        text_parts = []
        html_parts = []
        table_count = 0
        list_count = 0

        for el in elements:
            if el.text:
                text_parts.append(el.text)

            # Count tables and extract HTML
            if el.category == "Table":
                table_count += 1
                if hasattr(el, 'metadata') and hasattr(el.metadata, 'text_as_html'):
                    if el.metadata.text_as_html:
                        html_parts.append(el.metadata.text_as_html)

            # Count list items
            if el.category == "ListItem":
                list_count += 1

        content_text = "\n\n".join(text_parts)
        content_html = "\n".join(html_parts) if html_parts else None

        # Generate markdown from elements
        try:
            content_markdown = elements_to_md(elements)
        except Exception as e:
            logger.debug(f"Failed to generate markdown for {section_id}: {e}")
            content_markdown = content_text

        # Clean the content
        content_text = self._clean_section_text(content_text)

        # Calculate word count
        actual_words = len(content_text.split())

        # Skip if too short (likely TOC entry)
        min_words = self.config.get_min_words(section_id)
        min_threshold = int(min_words * self.config.candidate_threshold)

        if actual_words < min_threshold:
            logger.debug(
                f"Skipping {section_id}: {actual_words} words < {min_threshold} threshold"
            )
            return None

        # Calculate confidence
        extraction_confidence = 0.95  # Higher base confidence for unstructured
        issues = []

        if actual_words < min_words * self.config.short_threshold:
            issues.append(f"Section shorter than expected ({actual_words} vs {min_words})")
            extraction_confidence *= self.config.short_section_penalty

        # Truncate if too long
        if len(content_text) > self.config.max_section_chars:
            content_text = content_text[:self.config.max_section_chars]
            extraction_confidence *= self.config.truncated_section_penalty
            issues.append("Section truncated due to length")

        # Extract metadata
        cross_references = self._extract_cross_references(content_text)
        heading_hierarchy = self._extract_heading_hierarchy(content_text)
        contains_footnotes = len(re.findall(r'[\*†‡§¶]|\(\d+\)|\[\d+\]', content_text))

        # Calculate quality score
        extraction_quality = self.config.calculate_quality_score(
            actual_words=actual_words,
            expected_words=min_words,
            has_references=bool(cross_references),
            section_type=section_id
        )

        return ExtractedSection(
            section_type=section_id,
            section_number=section_number or section_def.get('number', ''),
            section_title=title,
            content_text=content_text,
            content_html=content_html if self.preserve_html else None,
            content_markdown=content_markdown,
            extraction_method='unstructured',
            extraction_confidence=extraction_confidence,
            section_part=section_def.get('part'),
            contains_tables=table_count,
            contains_lists=list_count,
            contains_footnotes=contains_footnotes,
            cross_references=cross_references,
            heading_hierarchy=heading_hierarchy,
            extraction_quality=extraction_quality,
            extraction_issues=issues,
        )

    def extract_sections(self, html_path: Path) -> list[ExtractedSection]:
        """
        Extract sections using the unstructured library.

        This method:
        1. Parses HTML into elements using partition_html()
        2. Groups elements by SEC section headers
        3. Converts each group to an ExtractedSection

        Args:
            html_path: Path to the HTML filing document.

        Returns:
            List of ExtractedSection objects.
        """
        try:
            logger.debug(f"Parsing HTML with unstructured: {html_path}")

            # Step 1: Parse HTML into elements
            elements = partition_html(filename=str(html_path))

            logger.debug(f"Extracted {len(elements)} elements from HTML")

            # Step 2: Group elements by SEC section headers
            section_groups = self._group_elements_by_section(elements)

            logger.debug(f"Found {len(section_groups)} sections: {list(section_groups.keys())}")

            # Step 3: Convert to ExtractedSection objects
            extracted_sections = []

            for section_id, (section_number, section_title, section_elements) in section_groups.items():
                # Only extract sections we're configured to extract
                if self.priority_only and section_id not in self.sections_to_extract:
                    continue

                section = self._elements_to_extracted_section(
                    section_id=section_id,
                    section_number=section_number,
                    section_title=section_title,
                    elements=section_elements,
                )

                if section:
                    extracted_sections.append(section)

            logger.info(f"Extracted {len(extracted_sections)} sections using unstructured method")
            return extracted_sections

        except Exception as e:
            logger.error(f"Failed to extract sections: {e}")
            raise SectionParsingError(f"Extraction failed: {e}")

    # =========================================================================
    # Helper methods
    # =========================================================================

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
            # - Short (between min and max heading length)
            # - No ending punctuation (except colon)
            # - Followed by content or blank line
            if (len(line) > self.config.min_heading_length and
                len(line) < self.config.max_heading_length and
                not line.endswith(('.', ',', ';')) and
                (line.isupper() or line.istitle() or line.endswith(':'))):
                # Check if followed by content
                if i + 1 < len(lines) and (not lines[i+1].strip() or len(lines[i+1]) > 50):
                    headings.append(line.rstrip(':'))

        # Limit to top 10 headings
        return headings[:10]

    # =========================================================================
    # Full Markdown Extraction with Section Markers
    # =========================================================================

    def extract_full_markdown(
        self,
        html_path: Path,
        accession_number: str = "",
        ticker: str = "",
    ) -> FullMarkdownResult:
        """
        Extract full document markdown with embedded section markers.

        This method:
        1. Parses HTML using unstructured
        2. Detects all sections
        3. Generates full markdown with section markers
        4. Returns both full markdown AND extracted sections

        Args:
            html_path: Path to the HTML filing document
            accession_number: Filing accession number (for document header)
            ticker: Company ticker (for document header)

        Returns:
            FullMarkdownResult with full_markdown and extracted sections
        """
        try:
            # Step 1: Parse HTML into elements
            elements = partition_html(filename=str(html_path))

            # Step 2: Generate full markdown from all elements
            full_markdown = elements_to_md(elements)

            # Step 3: Group elements by section for extraction
            section_groups = self._group_elements_by_section(elements)

            # Step 4: Build document header
            header_lines = []
            if ticker or accession_number:
                header_lines.append(f"<!-- DOCUMENT: {ticker} 10-K -->")
            if accession_number:
                header_lines.append(f"<!-- ACCESSION: {accession_number} -->")
            header_lines.append("")

            # Step 5: Embed section markers
            marked_markdown = self._embed_section_markers_from_groups(
                full_markdown,
                section_groups,
            )

            # Add header
            if header_lines:
                marked_markdown = "\n".join(header_lines) + marked_markdown

            # Step 6: Convert groups to ExtractedSection objects
            extracted_sections = []
            for section_id, (section_number, section_title, section_elements) in section_groups.items():
                section = self._elements_to_extracted_section(
                    section_id=section_id,
                    section_number=section_number,
                    section_title=section_title,
                    elements=section_elements,
                )
                if section:
                    extracted_sections.append(section)

            # Calculate metrics
            word_count = len(marked_markdown.split())
            character_count = len(marked_markdown)
            sections_found = list(section_groups.keys())

            # Calculate quality (based on priority sections found)
            priority_sections = {'item_1', 'item_1a', 'item_7', 'item_8', 'item_9a'}
            found_priority = set(sections_found) & priority_sections
            extraction_quality = len(found_priority) / len(priority_sections) if priority_sections else 0

            logger.info(
                f"Extracted full markdown: {word_count:,} words, "
                f"{len(sections_found)} sections, quality={extraction_quality:.2f}"
            )

            return FullMarkdownResult(
                full_markdown=marked_markdown,
                sections_found=sections_found,
                word_count=word_count,
                character_count=character_count,
                extraction_quality=extraction_quality,
                sections=extracted_sections,
            )

        except Exception as e:
            logger.error(f"Failed to extract full markdown: {e}")
            raise SectionParsingError(f"Full markdown extraction failed: {e}")

    def _embed_section_markers_from_groups(
        self,
        markdown_text: str,
        section_groups: dict,
    ) -> str:
        """
        Embed section markers into the markdown text.

        Inserts HTML comments like:
        <!-- SECTION: item_1a -->
        <!-- TITLE: Risk Factors -->

        before each detected section for RAG citation support.
        """
        # Find section headers in the markdown and add markers
        # This is a simplified approach - we mark known section patterns

        result = markdown_text

        for section_id, (section_number, section_title, elements) in section_groups.items():
            section_def = SECTION_DEFINITIONS.get(section_id, {})
            title = section_title or section_def.get('title', '')

            # Create marker
            marker = f"\n<!-- SECTION: {section_id} -->\n"
            if title:
                marker += f"<!-- TITLE: {title} -->\n"

            # Find the section header in markdown
            # Pattern to find "Item N" at start of line
            pattern = re.compile(
                rf'(?m)^(\s*#*\s*Item\s+{re.escape(section_number)})',
                re.IGNORECASE
            )

            match = pattern.search(result)
            if match:
                result = result[:match.start()] + marker + result[match.start():]

        return result
