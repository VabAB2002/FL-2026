"""
Extraction Configuration

Centralized configuration for extraction, chunking, and quality scoring.
All magic numbers and thresholds are defined here for easy tuning.
"""

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SectionExtractionConfig:
    """Configuration for section extraction from 10-K filings."""
    
    # Section minimum word counts (by section type)
    # These define how much content is expected for each section
    min_words: Dict[str, int] = field(default_factory=lambda: {
        "item_1": 1000,      # Business - substantial description expected
        "item_1a": 2000,     # Risk Factors - typically very long
        "item_1b": 10,       # Unresolved Staff Comments - usually brief
        "item_1c": 200,      # Cybersecurity - moderate length
        "item_2": 100,       # Properties - brief descriptions
        "item_3": 50,        # Legal Proceedings - brief if any
        "item_4": 10,        # Mine Safety Disclosures - brief
        "item_5": 200,       # Market for Registrant's Common Equity
        "item_6": 10,        # [Reserved]
        "item_7": 5000,      # MD&A - very long analysis
        "item_7a": 500,      # Quantitative and Qualitative Disclosures
        "item_8": 10000,     # Financial Statements - longest section
        "item_9": 50,        # Changes in Accounting Disagreements
        "item_9a": 500,      # Controls and Procedures
        "item_9b": 10,       # Other Information
        "item_9c": 10,       # Disclosure Foreign Jurisdictions
        "item_10": 500,      # Directors and Officers
        "item_11": 1000,     # Executive Compensation
        "item_12": 200,      # Security Ownership
        "item_13": 200,      # Certain Relationships
        "item_14": 100,      # Principal Accountant Fees
        "item_15": 100,      # Exhibits
        "item_16": 10,       # Form 10-K Summary
    })
    
    # Maximum section size (characters) to prevent memory issues
    max_section_chars: int = 5_000_000  # 5 million characters
    
    # Minimum text length after cleaning (characters)
    min_text_length: int = 100
    
    # Extraction confidence values
    base_confidence: float = 0.9              # Starting confidence for good match
    xbrl_tag_confidence: float = 0.95         # Higher confidence for XBRL-tagged sections
    
    # Confidence penalties (multipliers)
    short_section_penalty: float = 0.8        # Section shorter than expected
    truncated_section_penalty: float = 0.8    # Section truncated due to length
    missing_references_penalty: float = 0.95  # Missing cross-references (slight)
    very_short_penalty: float = 0.7           # Section much shorter than expected
    
    # Length thresholds for penalties (fraction of min_words)
    short_threshold: float = 1.0              # Below 100% of min_words
    very_short_threshold: float = 0.5         # Below 50% of min_words
    candidate_threshold: float = 0.1          # Minimum 10% to consider as candidate
    
    # Heading detection
    min_heading_length: int = 5               # Minimum chars for heading
    max_heading_length: int = 100             # Maximum chars for heading

    # Unstructured extraction settings
    unstructured_confidence_base: float = 0.95  # Higher confidence for unstructured

    # Lenient validation settings
    lenient_validation: bool = True           # Warn instead of reject on missing sections
    min_sections_required: int = 1            # Minimum sections to consider success
    warn_on_missing_priority: bool = True     # Log warning for missing priority sections

    # Priority sections for validation
    priority_sections: list = field(default_factory=lambda: [
        "item_1", "item_1a", "item_7", "item_8", "item_9a"
    ])

    def get_min_words(self, section_type: str) -> int:
        """Get minimum word count for a section type."""
        return self.min_words.get(section_type, 100)  # Default to 100 if not found
    
    def calculate_quality_score(
        self,
        actual_words: int,
        expected_words: int,
        has_references: bool,
        section_type: str
    ) -> float:
        """
        Calculate extraction quality score.
        
        Args:
            actual_words: Actual word count extracted
            expected_words: Expected minimum word count
            has_references: Whether section has cross-references
            section_type: Type of section (for reference checking)
        
        Returns:
            Quality score (0.0 to 1.0)
        """
        quality = self.base_confidence
        
        # Penalize if much shorter than expected
        if actual_words < expected_words * self.very_short_threshold:
            quality *= self.very_short_penalty
        
        # Slight penalty for missing cross-references in important sections
        if not has_references and section_type in ["item_7", "item_8"]:
            quality *= self.missing_references_penalty
        
        return quality


@dataclass
class ChunkingConfig:
    """Configuration for semantic chunking (RAG preparation)."""
    
    # Token counts for chunks
    target_chunk_size: int = 750      # Target size in tokens
    min_chunk_size: int = 500         # Minimum acceptable size
    max_chunk_size: int = 1000        # Maximum size before splitting
    overlap_size: int = 100           # Overlap between chunks for context
    
    # Text thresholds
    min_paragraph_length: int = 100   # Minimum chars for paragraph
    min_chunk_text_length: int = 100  # Minimum chars for chunk
    
    # Heading detection
    min_heading_length: int = 5       # Minimum chars for heading
    max_heading_length: int = 100     # Maximum chars for heading
    
    def should_create_chunk(self, token_count: int, is_last: bool = False) -> bool:
        """Determine if a chunk should be created based on size."""
        if is_last:
            return token_count > 0  # Always create last chunk if has content
        return token_count >= self.min_chunk_size


@dataclass
class QualityScoringConfig:
    """Configuration for quality scoring of filings."""
    
    # Starting score (out of 100)
    base_score: float = 100.0
    
    # Penalties (points deducted)
    missing_concept_penalty: float = 10.0     # Per missing required concept
    balance_sheet_penalty_multiplier: float = 2.0  # Multiplier for imbalance percentage
    max_balance_sheet_penalty: float = 20.0   # Maximum penalty for imbalance
    incomplete_balance_penalty: float = 10.0  # Penalty for incomplete data
    
    duplicate_fact_penalty: float = 5.0       # Per duplicate fact
    max_duplicate_penalty: float = 20.0       # Maximum penalty for duplicates
    
    max_null_value_penalty: float = 10.0      # Maximum penalty for null values
    
    # Thresholds
    balance_sheet_tolerance: float = 1.0      # 1% tolerance for balance sheet
    null_value_threshold: float = 10.0        # % of null values before penalty
    
    # Grade thresholds (out of 100)
    grade_a_threshold: float = 90.0
    grade_b_threshold: float = 80.0
    grade_c_threshold: float = 70.0
    grade_d_threshold: float = 60.0
    # Below D threshold = F
    
    def calculate_missing_concepts_penalty(self, missing_count: int) -> float:
        """Calculate penalty for missing required concepts."""
        return missing_count * self.missing_concept_penalty
    
    def calculate_balance_sheet_penalty(self, difference_pct: float) -> float:
        """Calculate penalty for balance sheet imbalance."""
        if difference_pct <= self.balance_sheet_tolerance:
            return 0.0
        
        penalty = difference_pct * self.balance_sheet_penalty_multiplier
        return min(penalty, self.max_balance_sheet_penalty)
    
    def calculate_duplicate_penalty(self, duplicate_count: int) -> float:
        """Calculate penalty for duplicate facts."""
        penalty = duplicate_count * self.duplicate_fact_penalty
        return min(penalty, self.max_duplicate_penalty)
    
    def calculate_null_value_penalty(self, null_pct: float) -> float:
        """Calculate penalty for null values."""
        if null_pct <= self.null_value_threshold:
            return 0.0
        return min(null_pct, self.max_null_value_penalty)
    
    def score_to_grade(self, score: float) -> str:
        """Convert numeric score to letter grade."""
        if score >= self.grade_a_threshold:
            return 'A'
        elif score >= self.grade_b_threshold:
            return 'B'
        elif score >= self.grade_c_threshold:
            return 'C'
        elif score >= self.grade_d_threshold:
            return 'D'
        else:
            return 'F'


# Global configuration instances
# These can be customized per environment or loaded from YAML
SECTION_EXTRACTION_CONFIG = SectionExtractionConfig()
CHUNKING_CONFIG = ChunkingConfig()
QUALITY_SCORING_CONFIG = QualityScoringConfig()


def get_section_extraction_config() -> SectionExtractionConfig:
    """Get the global section extraction configuration."""
    return SECTION_EXTRACTION_CONFIG


def get_chunking_config() -> ChunkingConfig:
    """Get the global chunking configuration."""
    return CHUNKING_CONFIG


def get_quality_scoring_config() -> QualityScoringConfig:
    """Get the global quality scoring configuration."""
    return QUALITY_SCORING_CONFIG


# For future enhancement: Load from YAML
def load_extraction_config_from_yaml(yaml_path: str) -> None:
    """
    Load extraction configuration from YAML file.
    
    Future enhancement to allow configuration tuning without code changes.
    
    Example YAML:
    ```yaml
    section_extraction:
      max_section_chars: 5000000
      base_confidence: 0.9
      min_words:
        item_1: 1000
        item_7: 5000
    
    chunking:
      target_chunk_size: 750
      overlap_size: 100
    
    quality_scoring:
      missing_concept_penalty: 10.0
      grade_a_threshold: 90.0
    ```
    """
    # TODO: Implement YAML loading
    # import yaml
    # with open(yaml_path) as f:
    #     config = yaml.safe_load(f)
    #     # Update global configs
    pass
