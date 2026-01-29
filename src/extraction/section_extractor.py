"""
Regex-based section extraction from SEC filing markdown.

Handles standard and non-standard section formats across different companies.
"""

from __future__ import annotations

import logging
import re
from typing import Pattern

logger = logging.getLogger(__name__)


class SectionExtractor:
    """Extract sections from full markdown using multi-pattern regex."""

    # Standard ITEM patterns (most common)
    STANDARD_PATTERNS = {
        "ITEM 1": [
            re.compile(r"(?:^|\n)\s*ITEM\s+1[\.\s]+Business", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*ITEM\s+1[\.\s]*\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 1A": [
            re.compile(r"(?:^|\n)\s*ITEM\s+1A[\.\s]+Risk\s+Factors", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*ITEM\s+1A[\.\s]*\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 1B": [
            re.compile(r"(?:^|\n)\s*ITEM\s+1B[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 1C": [
            re.compile(r"(?:^|\n)\s*ITEM\s+1C[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 2": [
            re.compile(r"(?:^|\n)\s*ITEM\s+2[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 7": [
            re.compile(r"(?:^|\n)\s*ITEM\s+7[\.\s]+Management", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*ITEM\s+7[\.\s]+\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 7A": [
            re.compile(r"(?:^|\n)\s*ITEM\s+7A[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 8": [
            re.compile(r"(?:^|\n)\s*ITEM\s+8[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 9": [
            re.compile(r"(?:^|\n)\s*ITEM\s+9[\.\s]+Changes", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*ITEM\s+9[\.\s]+\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 9A": [
            re.compile(r"(?:^|\n)\s*ITEM\s+9A[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 9B": [
            re.compile(r"(?:^|\n)\s*ITEM\s+9B[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 9C": [
            re.compile(r"(?:^|\n)\s*ITEM\s+9C[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 10": [
            re.compile(r"(?:^|\n)\s*ITEM\s+10[\.\s]+Directors", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*ITEM\s+10[\.\s]*\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 11": [
            re.compile(r"(?:^|\n)\s*ITEM\s+11[\.\s]+Executive\s+Compensation", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*ITEM\s+11[\.\s]*", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 12": [
            re.compile(r"(?:^|\n)\s*ITEM\s+12[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 13": [
            re.compile(r"(?:^|\n)\s*ITEM\s+13[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 14": [
            re.compile(r"(?:^|\n)\s*ITEM\s+14[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 15": [
            re.compile(r"(?:^|\n)\s*ITEM\s+15[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 16": [
            re.compile(r"(?:^|\n)\s*ITEM\s+16[\.\s]", re.IGNORECASE | re.MULTILINE),
        ],
    }

    # Non-standard patterns (for companies like INTC that use custom headings)
    NONSTANDARD_PATTERNS = {
        "ITEM 1": [
            re.compile(r"(?:^|\n)\s*#+\s*Overview\s*\n", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*#+\s*Our\s+Business\s*\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 1A": [
            re.compile(r"(?:^|\n)\s*#+\s*Risk\s+Factors\s*\n", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 7": [
            re.compile(r"(?:^|\n)\s*#+\s*Management.*Discussion\s+and\s+Analysis", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 10": [
            re.compile(r"(?:^|\n)\s*#+\s*Information\s+About.*Executive\s+Officers", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*#+\s*Executive\s+Officers", re.IGNORECASE | re.MULTILINE),
            re.compile(r"(?:^|\n)\s*#+\s*Directors.*Executive\s+Officers", re.IGNORECASE | re.MULTILINE),
        ],
        "ITEM 11": [
            re.compile(r"(?:^|\n)\s*#+\s*Executive\s+Compensation\s*\n", re.IGNORECASE | re.MULTILINE),
        ],
    }

    # Next section markers to find boundaries
    ALL_ITEM_PATTERNS = [
        re.compile(r"(?:^|\n)\s*ITEM\s+\d+[A-C]?[\.\s]", re.IGNORECASE | re.MULTILINE),
        re.compile(r"(?:^|\n)\s*#+\s*(?:Overview|Risk Factors|Management|Executive|Information About)", re.IGNORECASE | re.MULTILINE),
    ]

    def __init__(self):
        """Initialize section extractor."""
        self.stats = {"standard": 0, "nonstandard": 0, "crossref": 0, "failed": 0}

    def extract_section(self, full_markdown: str, item: str) -> str | None:
        """
        Extract section from full markdown.

        Tries multiple strategies:
        1. Standard ITEM patterns
        2. Non-standard heading patterns
        3. Cross-reference index mapping

        Args:
            full_markdown: Complete filing markdown
            item: Item number (e.g., "ITEM 1", "ITEM 10")

        Returns:
            Section text or None if not found
        """
        if not full_markdown or not item:
            return None

        # Normalize item format
        item = item.upper().strip()
        
        # Minimum length (allows "Refer to Item X" and "incorporated by reference")
        min_length = 15

        # Try standard patterns first
        section = self._extract_standard_item(full_markdown, item)
        if section and len(section) > min_length:
            self.stats["standard"] += 1
            logger.debug(f"Extracted {item} using standard pattern ({len(section)} chars)")
            return section

        # Try non-standard patterns
        section = self._extract_nonstandard_item(full_markdown, item)
        if section and len(section) > min_length:
            self.stats["nonstandard"] += 1
            logger.debug(f"Extracted {item} using non-standard pattern ({len(section)} chars)")
            return section

        # Try cross-reference index
        section = self._extract_via_crossref(full_markdown, item)
        if section and len(section) > min_length:
            self.stats["crossref"] += 1
            logger.debug(f"Extracted {item} using cross-reference ({len(section)} chars)")
            return section

        self.stats["failed"] += 1
        logger.warning(f"Failed to extract {item} with any pattern")
        return None

    def _extract_standard_item(self, markdown: str, item: str) -> str | None:
        """Extract using standard ITEM patterns."""
        patterns = self.STANDARD_PATTERNS.get(item, [])
        
        for pattern in patterns:
            match = pattern.search(markdown)
            if match:
                start = match.start()
                # Find next section boundary
                end = self._find_next_section_boundary(markdown, start + len(match.group(0)))
                if end:
                    return markdown[start:end].strip()
                else:
                    # No next section, take rest of document (up to reasonable limit)
                    return markdown[start:start + 100000].strip()
        
        return None

    def _extract_nonstandard_item(self, markdown: str, item: str) -> str | None:
        """Extract using non-standard patterns for custom formats."""
        patterns = self.NONSTANDARD_PATTERNS.get(item, [])
        
        for pattern in patterns:
            match = pattern.search(markdown)
            if match:
                start = match.start()
                # Find next major section
                end = self._find_next_section_boundary(markdown, start + len(match.group(0)))
                if end:
                    return markdown[start:end].strip()
                else:
                    return markdown[start:start + 100000].strip()
        
        return None

    def _extract_via_crossref(self, markdown: str, item: str) -> str | None:
        """
        Extract using cross-reference index mapping.
        
        Some companies (like INTC) provide a "Form 10-K Cross-Reference Index"
        that maps their custom section names to standard Item numbers.
        """
        # Look for cross-reference index
        crossref_pattern = re.compile(
            r"(?:Form 10-K )?Cross-Reference Index",
            re.IGNORECASE
        )
        
        match = crossref_pattern.search(markdown)
        if not match:
            return None
        
        # Extract the cross-reference table (next 5000 chars)
        crossref_start = match.start()
        crossref_section = markdown[crossref_start:crossref_start + 5000]
        
        # Parse the mapping for this item
        # Look for patterns like: "Item 10 ... page X ... Overview" or "Item 10|Overview"
        item_num = item.replace("ITEM ", "").strip()
        mapping_pattern = re.compile(
            rf"Item\s+{re.escape(item_num)}[^\n]*?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            re.IGNORECASE
        )
        
        mapping_match = mapping_pattern.search(crossref_section)
        if mapping_match:
            section_title = mapping_match.group(1).strip()
            logger.debug(f"Found cross-ref mapping: {item} -> {section_title}")
            
            # Now search for that section title in the document
            title_pattern = re.compile(
                rf"(?:^|\n)\s*#+\s*{re.escape(section_title)}\s*\n",
                re.IGNORECASE | re.MULTILINE
            )
            
            title_match = title_pattern.search(markdown)
            if title_match:
                start = title_match.start()
                end = self._find_next_section_boundary(markdown, start + len(title_match.group(0)))
                if end:
                    return markdown[start:end].strip()
                else:
                    return markdown[start:start + 100000].strip()
        
        return None

    def _find_next_section_boundary(self, markdown: str, start_pos: int) -> int | None:
        """
        Find the start of the next section after start_pos.
        
        Args:
            markdown: Full markdown text
            start_pos: Position to start searching from
        
        Returns:
            Position of next section start, or None if not found
        """
        # Search for next ITEM marker or major heading
        for pattern in self.ALL_ITEM_PATTERNS:
            match = pattern.search(markdown, start_pos)
            if match:
                return match.start()
        
        return None

    def get_stats(self) -> dict[str, int]:
        """Get extraction statistics."""
        return self.stats.copy()

    def reset_stats(self) -> None:
        """Reset extraction statistics."""
        self.stats = {"standard": 0, "nonstandard": 0, "crossref": 0, "failed": 0}
