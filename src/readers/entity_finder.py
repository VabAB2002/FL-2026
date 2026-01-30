"""
SpaCy-based Named Entity Recognition for financial documents.

Extracts standard entities (PERSON, ORG, GPE, MONEY, DATE) and custom
financial entities (METRIC, RISK) using transformer model + pattern rules.
"""

from __future__ import annotations

import logging
from typing import Any

import spacy
from spacy.language import Language
from spacy.tokens import Doc

from src.readers.entity_checks import filter_entities

logger = logging.getLogger(__name__)


class FinancialEntityExtractor:
    """Extract entities using SpaCy with custom financial patterns."""

    def __init__(self, model_name: str = "en_core_web_trf"):
        """
        Initialize entity extractor.

        Args:
            model_name: SpaCy model to use (default: transformer for max accuracy)
        """
        self.model_name = model_name
        self.nlp = self._load_model()
        self._add_financial_patterns()
        logger.info(f"FinancialEntityExtractor initialized with {model_name}")

    def _load_model(self) -> Language:
        """Load SpaCy model, downloading if necessary."""
        try:
            nlp = spacy.load(self.model_name)
            logger.info(f"Loaded SpaCy model: {self.model_name}")
            return nlp
        except OSError:
            logger.info(f"Downloading SpaCy model: {self.model_name}...")
            spacy.cli.download(self.model_name)
            nlp = spacy.load(self.model_name)
            logger.info(f"Downloaded and loaded: {self.model_name}")
            return nlp

    def _add_financial_patterns(self) -> None:
        """Add custom entity ruler for financial terms."""
        if "entity_ruler" not in self.nlp.pipe_names:
            ruler = self.nlp.add_pipe("entity_ruler", before="ner")

            # Financial metrics patterns
            patterns = [
                # Revenue metrics
                {"label": "METRIC", "pattern": [{"LOWER": "revenue"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "revenues"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "net"}, {"LOWER": "revenue"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "total"}, {"LOWER": "revenue"}]},
                # Profitability metrics
                {"label": "METRIC", "pattern": [{"LOWER": "ebitda"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "net"}, {"LOWER": "income"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "net"}, {"LOWER": "earnings"}]},
                {
                    "label": "METRIC",
                    "pattern": [{"LOWER": "earnings"}, {"LOWER": "per"}, {"LOWER": "share"}],
                },
                {"label": "METRIC", "pattern": [{"LOWER": "eps"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "operating"}, {"LOWER": "income"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "gross"}, {"LOWER": "profit"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "gross"}, {"LOWER": "margin"}]},
                # Asset metrics
                {"label": "METRIC", "pattern": [{"LOWER": "total"}, {"LOWER": "assets"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "total"}, {"LOWER": "equity"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "shareholders"}, {"LOWER": "equity"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "book"}, {"LOWER": "value"}]},
                # Liability metrics
                {"label": "METRIC", "pattern": [{"LOWER": "total"}, {"LOWER": "debt"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "long"}, {"LOWER": "term"}, {"LOWER": "debt"}]},
                {
                    "label": "METRIC",
                    "pattern": [{"LOWER": "current"}, {"LOWER": "liabilities"}],
                },
                # Cash flow metrics
                {"label": "METRIC", "pattern": [{"LOWER": "cash"}, {"LOWER": "flow"}]},
                {"label": "METRIC", "pattern": [{"LOWER": "free"}, {"LOWER": "cash"}, {"LOWER": "flow"}]},
                {
                    "label": "METRIC",
                    "pattern": [{"LOWER": "operating"}, {"LOWER": "cash"}, {"LOWER": "flow"}],
                },
                # Valuation metrics
                {"label": "METRIC", "pattern": [{"LOWER": "market"}, {"LOWER": "cap"}]},
                {
                    "label": "METRIC",
                    "pattern": [{"LOWER": "market"}, {"LOWER": "capitalization"}],
                },
                {"label": "METRIC", "pattern": [{"LOWER": "p"}, {"LOWER": "/"}, {"LOWER": "e"}]},
                # Risk factors
                {"label": "RISK", "pattern": [{"LOWER": "market"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "credit"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "operational"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "liquidity"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "regulatory"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "compliance"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "cybersecurity"}, {"LOWER": "risk"}]},
                {"label": "RISK", "pattern": [{"LOWER": "reputational"}, {"LOWER": "risk"}]},
            ]

            ruler.add_patterns(patterns)
            logger.info(f"Added {len(patterns)} financial entity patterns")

    def extract_entities(
        self, text: str, max_length: int | None = None
    ) -> list[dict[str, Any]]:
        """
        Extract entities from text.

        Args:
            text: Input text to process
            max_length: Maximum text length (SpaCy has 1M char limit)

        Returns:
            List of entity dicts with type, text, start, end positions
        """
        # Handle SpaCy's max length limit
        if max_length is None:
            max_length = self.nlp.max_length

        # Truncate if necessary
        text_to_process = text[:max_length] if len(text) > max_length else text

        # Process with SpaCy
        doc: Doc = self.nlp(text_to_process)

        # Extract entities
        entities = []
        for ent in doc.ents:
            entities.append(
                {
                    "type": ent.label_,
                    "text": ent.text,
                    "start": ent.start_char,
                    "end": ent.end_char,
                }
            )

        return entities

    def extract_from_section(
        self, section_text: str, section_type: str
    ) -> dict[str, Any]:
        """
        Extract entities from a filing section.

        Args:
            section_text: Text content of the section
            section_type: Section identifier (e.g., 'item_1', 'item_1a')

        Returns:
            Dict with section_type, total_entities, entities_by_type, raw_entities
        """
        entities = self.extract_entities(section_text)
        
        # Filter noisy entities (phone numbers, frequency words, etc.)
        entities = filter_entities(entities)

        # Group entities by type for easy analysis
        grouped: dict[str, list[dict[str, Any]]] = {}
        for entity in entities:
            ent_type = entity["type"]
            if ent_type not in grouped:
                grouped[ent_type] = []
            grouped[ent_type].append(entity)

        return {
            "section_type": section_type,
            "total_entities": len(entities),
            "entities_by_type": grouped,
            "raw_entities": entities,
        }

    def get_entity_summary(self, extraction_result: dict[str, Any]) -> dict[str, int]:
        """
        Get entity count summary from extraction result.

        Args:
            extraction_result: Result from extract_from_section()

        Returns:
            Dict mapping entity type to count
        """
        return {
            entity_type: len(entities)
            for entity_type, entities in extraction_result["entities_by_type"].items()
        }
