"""
Orchestrates section retrieval with 3-tier fallback strategy.

Tier 1: Database (filing_sections table)
Tier 2: Regex extraction from full_markdown
Tier 3: LLM-based section finding (lazy loaded)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.extraction.section_extractor import SectionExtractor

if TYPE_CHECKING:
    from src.storage.database import Database

logger = logging.getLogger(__name__)


class SectionRetriever:
    """
    Retrieve sections using adaptive 3-tier strategy.
    
    Strategy:
    1. Check filing_sections table (fast, free)
    2. Regex extraction from full_markdown (fast, free)
    3. LLM section finder (slow, costs $0.01, rare)
    """

    # Minimum length for a valid section (chars)
    MIN_SUBSTANTIAL_LENGTH = 1000  # Tier 1 threshold
    MIN_VALID_LENGTH = 15  # Tier 2/3 threshold (allows "Refer to Item X" and "incorporated by reference")

    def __init__(self, db: Database):
        """
        Initialize section retriever.
        
        Args:
            db: Database connection
        """
        self.db = db
        self.regex_extractor = SectionExtractor()
        self._llm_finder = None  # Lazy load only if needed
        
        # Statistics tracking
        self.stats = {
            "db_hits": 0,
            "db_misses": 0,
            "regex_hits": 0,
            "regex_misses": 0,
            "llm_hits": 0,
            "llm_misses": 0,
        }
        
        # Cache full_markdown per filing to avoid repeated queries
        self._markdown_cache: dict[str, str] = {}

    def get_section(self, accession_number: str, item: str) -> str | None:
        """
        Get section text using 3-tier fallback.
        
        Args:
            accession_number: Filing accession number
            item: Item number (e.g., "ITEM 1", "ITEM 10")
        
        Returns:
            Section text or None if not found in any tier
        """
        # Normalize item format
        item = item.upper().strip()
        
        logger.debug(f"Retrieving {item} for {accession_number}")
        
        # Tier 1: Database
        section = self._get_from_database(accession_number, item)
        if section and len(section) > self.MIN_SUBSTANTIAL_LENGTH:
            self.stats["db_hits"] += 1
            logger.debug(f"Tier 1 (DB) hit: {item} ({len(section)} chars)")
            return section
        
        self.stats["db_misses"] += 1
        logger.debug(f"Tier 1 (DB) miss: {item}")
        
        # Tier 2: Regex extraction from full_markdown
        full_markdown = self._get_full_markdown(accession_number)
        if not full_markdown:
            logger.warning(f"No full_markdown found for {accession_number}")
            return None
        
        section = self.regex_extractor.extract_section(full_markdown, item)
        if section and len(section) > self.MIN_VALID_LENGTH:
            self.stats["regex_hits"] += 1
            logger.info(f"Tier 2 (Regex) hit: {item} ({len(section)} chars)")
            return section
        
        self.stats["regex_misses"] += 1
        logger.debug(f"Tier 2 (Regex) miss: {item}")
        
        # Tier 3: LLM section finder (lazy loaded)
        section = self._get_via_llm(full_markdown, item)
        if section and len(section) > self.MIN_VALID_LENGTH:
            self.stats["llm_hits"] += 1
            logger.info(f"Tier 3 (LLM) hit: {item} ({len(section)} chars)")
            return section
        
        self.stats["llm_misses"] += 1
        logger.warning(f"Tier 3 (LLM) miss: {item} - section not found in any tier")
        return None

    def get_multiple_sections(
        self, accession_number: str, items: list[str]
    ) -> dict[str, str | None]:
        """
        Get multiple sections for a filing.
        
        Args:
            accession_number: Filing accession number
            items: List of item numbers
        
        Returns:
            Dictionary mapping item -> section text (or None)
        """
        results = {}
        for item in items:
            results[item] = self.get_section(accession_number, item)
        return results

    def _get_from_database(self, accession_number: str, item: str) -> str | None:
        """
        Get section from filing_sections table.
        
        Args:
            accession_number: Filing accession number
            item: Item number
        
        Returns:
            Section markdown or None
        """
        try:
            result = self.db.connection.execute(
                """
                SELECT markdown
                FROM filing_sections
                WHERE accession_number = ? AND item = ?
                """,
                [accession_number, item],
            ).fetchone()
            
            return result[0] if result else None
        
        except Exception as e:
            logger.error(f"Database query failed for {accession_number} {item}: {e}")
            return None

    def _get_full_markdown(self, accession_number: str) -> str | None:
        """
        Get full_markdown from filings table (with caching).
        
        Args:
            accession_number: Filing accession number
        
        Returns:
            Full markdown text or None
        """
        # Check cache first
        if accession_number in self._markdown_cache:
            return self._markdown_cache[accession_number]
        
        try:
            result = self.db.connection.execute(
                """
                SELECT full_markdown
                FROM filings
                WHERE accession_number = ?
                """,
                [accession_number],
            ).fetchone()
            
            if result and result[0]:
                markdown = result[0]
                self._markdown_cache[accession_number] = markdown
                return markdown
            
            return None
        
        except Exception as e:
            logger.error(f"Failed to get full_markdown for {accession_number}: {e}")
            return None

    def _get_via_llm(self, full_markdown: str, item: str) -> str | None:
        """
        Get section using LLM section finder (Tier 3).
        
        Only used as last resort for truly non-standard formats.
        Lazy loads the LLM finder to avoid unnecessary imports.
        
        Args:
            full_markdown: Full filing markdown
            item: Item number
        
        Returns:
            Section text or None
        """
        # Lazy load LLM finder
        if self._llm_finder is None:
            try:
                from src.extraction.llm_section_finder import LLMSectionFinder
                self._llm_finder = LLMSectionFinder()
                logger.info("LLM section finder loaded (Tier 3 activated)")
            except ImportError:
                logger.warning("LLM section finder not available (module not found)")
                return None
            except Exception as e:
                logger.error(f"Failed to initialize LLM section finder: {e}")
                return None
        
        try:
            return self._llm_finder.find_section(full_markdown, item)
        except Exception as e:
            logger.error(f"LLM section finding failed for {item}: {e}")
            return None

    def get_stats(self) -> dict[str, int]:
        """
        Get retrieval statistics.
        
        Returns:
            Dictionary with hit/miss counts per tier
        """
        total_requests = (
            self.stats["db_hits"] +
            self.stats["db_misses"]
        )
        
        return {
            **self.stats,
            "total_requests": total_requests,
            "db_hit_rate": (
                self.stats["db_hits"] / total_requests * 100
                if total_requests > 0 else 0
            ),
            "regex_hit_rate": (
                self.stats["regex_hits"] / self.stats["db_misses"] * 100
                if self.stats["db_misses"] > 0 else 0
            ),
            "llm_usage": self.stats["llm_hits"] + self.stats["llm_misses"],
        }

    def print_stats(self) -> None:
        """Print formatted statistics."""
        stats = self.get_stats()
        
        print("\n" + "=" * 70)
        print("SECTION RETRIEVAL STATISTICS")
        print("=" * 70)
        
        print(f"\nTotal Requests: {stats['total_requests']}")
        
        print(f"\nTier 1 (Database):")
        print(f"  Hits: {self.stats['db_hits']}")
        print(f"  Misses: {self.stats['db_misses']}")
        print(f"  Hit Rate: {stats['db_hit_rate']:.1f}%")
        
        print(f"\nTier 2 (Regex):")
        print(f"  Hits: {self.stats['regex_hits']}")
        print(f"  Misses: {self.stats['regex_misses']}")
        if self.stats['db_misses'] > 0:
            print(f"  Success Rate: {stats['regex_hit_rate']:.1f}%")
        
        print(f"\nTier 3 (LLM):")
        print(f"  Hits: {self.stats['llm_hits']}")
        print(f"  Misses: {self.stats['llm_misses']}")
        print(f"  Total LLM Calls: {stats['llm_usage']}")
        
        # Regex extractor stats
        regex_stats = self.regex_extractor.get_stats()
        if sum(regex_stats.values()) > 0:
            print(f"\nRegex Pattern Success:")
            print(f"  Standard patterns: {regex_stats['standard']}")
            print(f"  Non-standard patterns: {regex_stats['nonstandard']}")
            print(f"  Cross-reference: {regex_stats['crossref']}")
            print(f"  Failed: {regex_stats['failed']}")
        
        print("=" * 70)

    def reset_stats(self) -> None:
        """Reset all statistics."""
        self.stats = {
            "db_hits": 0,
            "db_misses": 0,
            "regex_hits": 0,
            "regex_misses": 0,
            "llm_hits": 0,
            "llm_misses": 0,
        }
        self.regex_extractor.reset_stats()
        self._markdown_cache.clear()

    def clear_cache(self) -> None:
        """Clear the markdown cache."""
        self._markdown_cache.clear()
