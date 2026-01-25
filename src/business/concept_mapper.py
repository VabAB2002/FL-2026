"""
Concept Mapper: Translates variable XBRL concepts to standardized metrics.

This is the core of the normalization system, similar to Bloomberg's internal
mapping layer. It handles the complexity of different companies using different
XBRL concepts for the same financial metric.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from ..storage.repositories import (
    DuckDBConceptMappingRepository,
    DuckDBFactRepository,
    get_fact_repository,
    get_mapping_repository,
)
from ..utils.logger import get_logger

if TYPE_CHECKING:
    from ..storage.database import Database

logger = get_logger("finloom.business.mapper")


@dataclass
class NormalizedMetric:
    """A normalized financial metric."""
    metric_id: str
    metric_value: Decimal
    source_concept: str
    confidence_score: float
    fiscal_year: int
    fiscal_quarter: Optional[int] = None


@dataclass
class MappingRule:
    """A concept mapping rule."""
    metric_id: str
    concept_name: str
    priority: int
    confidence_score: float
    applies_to_industry: Optional[str] = None


class ConceptMapper:
    """
    Maps XBRL concepts to standardized metrics using priority/fallback logic.

    Example:
        mapper = ConceptMapper()
        normalized = mapper.normalize_filing('0000320193-24-000123')
        # Returns: [NormalizedMetric(metric_id='revenue', value=391000000000, ...)]

    Or with explicit database (for backward compatibility):
        mapper = ConceptMapper(db)
    """

    def __init__(
        self,
        db: Optional["Database"] = None,
        fact_repo: Optional[DuckDBFactRepository] = None,
        mapping_repo: Optional[DuckDBConceptMappingRepository] = None,
    ):
        """
        Initialize mapper with repositories.

        Args:
            db: Optional Database instance (for backward compatibility)
            fact_repo: Optional FactRepository (uses singleton if not provided)
            mapping_repo: Optional MappingRepository (uses singleton if not provided)
        """
        # Use provided repositories or get singletons
        self._fact_repo = fact_repo or get_fact_repository(db)
        self._mapping_repo = mapping_repo or get_mapping_repository(db)

        # For backward compatibility
        self.db = db

        self.mappings = self._load_mappings()
        logger.info(f"Loaded {len(self.mappings)} metric mappings")
    
    def _load_mappings(self) -> dict[str, list[MappingRule]]:
        """Load all concept mappings from database, grouped by metric."""
        mappings_data = self._mapping_repo.get_all_mappings()
        
        # Group by metric_id
        mappings: dict[str, list[MappingRule]] = {}
        for row in mappings_data:
            metric_id = row['metric_id']
            if metric_id not in mappings:
                mappings[metric_id] = []

            mappings[metric_id].append(MappingRule(
                metric_id=metric_id,
                concept_name=row['concept_name'],
                priority=row['priority'],
                confidence_score=row['confidence_score'] or 1.0,
                applies_to_industry=row['applies_to_industry']
            ))

        # Sort by priority
        for metric_id in mappings:
            mappings[metric_id].sort(key=lambda r: r.priority)

        return mappings

    def normalize_filing(
        self,
        accession_number: str,
        company_industry: Optional[str] = None
    ) -> list[NormalizedMetric]:
        """
        Normalize a filing to standardized metrics.

        Uses priority/fallback logic:
        1. Try the highest priority concept (priority=1)
        2. If not found, try the next priority (priority=2)
        3. Continue until a value is found or all options exhausted

        Args:
            accession_number: Filing accession number
            company_industry: Optional industry code for industry-specific mappings

        Returns:
            List of normalized metrics extracted from the filing
        """
        # Get all facts for this filing using repository
        facts = self._fact_repo.get_facts_for_filing(accession_number)
        
        if not facts:
            logger.warning(f"No facts found for {accession_number}")
            return []
        
        # Build lookup for quick access
        facts_by_concept = self._build_facts_lookup(facts)
        
        # Get fiscal year from facts
        fiscal_year = self._extract_fiscal_year(facts)
        if not fiscal_year:
            logger.warning(f"Could not determine fiscal year for {accession_number}")
            return []
        
        # Normalize each metric
        normalized = []
        for metric_id, rules in self.mappings.items():
            # Filter rules by industry if applicable
            applicable_rules = [
                r for r in rules
                if r.applies_to_industry is None or r.applies_to_industry == company_industry
            ]
            
            # Try each rule in priority order
            for rule in applicable_rules:
                value = self._find_value(facts_by_concept, rule.concept_name)
                
                if value is not None:
                    normalized.append(NormalizedMetric(
                        metric_id=metric_id,
                        metric_value=value,
                        source_concept=rule.concept_name,
                        confidence_score=rule.confidence_score,
                        fiscal_year=fiscal_year
                    ))
                    break  # Found it, stop trying alternatives
        
        logger.info(f"Normalized {len(normalized)} metrics from {accession_number}")
        return normalized
    
    def _build_facts_lookup(self, facts: list[dict]) -> dict[str, list[dict]]:
        """Build concept name -> facts lookup for fast access."""
        lookup = {}
        for fact in facts:
            concept = fact['concept_name']
            if concept not in lookup:
                lookup[concept] = []
            lookup[concept].append(fact)
        return lookup
    
    def _find_value(
        self,
        facts_by_concept: dict[str, list[dict]],
        concept_name: str
    ) -> Optional[Decimal]:
        """
        Find the appropriate value for a concept.
        
        Selection logic:
        1. Prefer facts without dimensions (consolidated data)
        2. Prefer the latest period
        3. For balance sheet items (instant), use period_end
        4. For income statement items (duration), use the fiscal year period
        """
        if concept_name not in facts_by_concept:
            return None
        
        facts = facts_by_concept[concept_name]
        
        # Filter out facts with dimensions (segment data)
        consolidated = [f for f in facts if f['dimensions'] is None or f['dimensions'] == '{}']
        if not consolidated:
            consolidated = facts  # Fall back to all facts if no consolidated
        
        # Filter out text facts, keep only numeric
        numeric = [f for f in consolidated if f['value'] is not None]
        if not numeric:
            return None
        
        # Get the fact with the latest period_end
        sorted_facts = sorted(
            numeric,
            key=lambda f: f['period_end'] if f['period_end'] else date(1900, 1, 1),
            reverse=True
        )
        
        if sorted_facts:
            return Decimal(str(sorted_facts[0]['value']))
        
        return None
    
    def _extract_fiscal_year(self, facts: list[dict]) -> Optional[int]:
        """Extract fiscal year from facts."""
        # Look for period_end dates
        dates = [f['period_end'] for f in facts if f['period_end']]
        if dates:
            # Get the latest date and extract year
            latest = max(dates)
            return latest.year
        return None
    
    def reload_mappings(self) -> None:
        """Reload mappings from database (useful after updates)."""
        self.mappings = self._load_mappings()
        logger.info(f"Reloaded {len(self.mappings)} metric mappings")


# Convenience function for getting a mapper instance
def get_concept_mapper() -> ConceptMapper:
    """Get a ConceptMapper instance with default repositories."""
    return ConceptMapper()
