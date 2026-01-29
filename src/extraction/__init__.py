"""
Entity extraction module for SEC filings.

Provides:
- SpaCy-based Named Entity Recognition with custom financial patterns
- LLM-based extraction for people, risk factors, and complex entities
- Validation filters for improved accuracy
- Adaptive section retrieval with regex and LLM fallback
"""

# Always available (no heavy dependencies)
from src.extraction.llm_extractor import (
    ExtractionResult,
    LLMExtractor,
    LLMProvider,
    PersonExtraction,
    RiskFactorExtraction,
)
from src.extraction.section_extractor import SectionExtractor
from src.extraction.section_retriever import SectionRetriever

__all__ = [
    "LLMExtractor",
    "LLMProvider",
    "PersonExtraction",
    "RiskFactorExtraction",
    "ExtractionResult",
    "SectionExtractor",
    "SectionRetriever",
]

# Optional SpaCy-based extractor (only if spacy is installed)
try:
    from src.extraction.entity_extractor import FinancialEntityExtractor
    from src.extraction.entity_validators import filter_entities, is_valid_cardinal, is_valid_date
    
    __all__.extend([
        "FinancialEntityExtractor",
        "filter_entities",
        "is_valid_cardinal",
        "is_valid_date",
    ])
except ImportError:
    pass  # SpaCy not installed, skip
