"""
SEC filing readers and extractors module.

Combines XBRL parsing, LLM-based extraction, and section finding.
"""

from .xbrl_reader import XBRLParser, XBRLParseResult, XBRLFact
from .ai_reader import LLMExtractor, PersonExtraction, RiskFactorExtraction, ExtractionResult
from .entity_finder import FinancialEntityExtractor
from .section_extractor import SectionExtractor
from .section_finder import SectionRetriever

__all__ = [
    # XBRL parsing
    "XBRLParser",
    "XBRLParseResult",
    "XBRLFact",
    # LLM extraction
    "LLMExtractor",
    "PersonExtraction",
    "RiskFactorExtraction",
    "ExtractionResult",
    # Entity extraction
    "FinancialEntityExtractor",
    # Section finding
    "SectionExtractor",
    "SectionRetriever",
]
