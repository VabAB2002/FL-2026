"""
LLM-based entity extraction for SEC filings.

Extracts structured data (people, risk factors) from filing sections
using various LLM providers (DeepSeek, Kimi, GPT-4o, etc.).
"""

from __future__ import annotations

import json
import logging
import os
from enum import Enum
from typing import Any

from openai import OpenAI
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# =============================================================================
# Output Schemas
# =============================================================================


class PersonExtraction(BaseModel):
    """Extracted person/executive information."""

    name: str = Field(..., description="Full name of the person")
    role: str = Field(..., description="Job title or role (CEO, CFO, Director, etc.)")
    start_date: str | None = Field(
        default=None, description="Start date if mentioned (YYYY-MM-DD or year)"
    )


class RiskFactorExtraction(BaseModel):
    """Extracted risk factor information."""

    category: str = Field(
        ...,
        description="Risk category (operational, financial, regulatory, market, cybersecurity, legal, competitive, other)",
    )
    severity: int = Field(
        ..., ge=1, le=5, description="Severity rating: 1=low, 2=moderate, 3=significant, 4=high, 5=critical"
    )
    description: str = Field(..., description="Brief description of the risk (max 200 chars)")


class ExtractionResult(BaseModel):
    """Complete extraction result for a section."""

    people: list[PersonExtraction] = Field(default_factory=list)
    risk_factors: list[RiskFactorExtraction] = Field(default_factory=list)
    extraction_success: bool = True
    error_message: str | None = None


# =============================================================================
# LLM Provider Enum
# =============================================================================


class LLMProvider(str, Enum):
    """Supported LLM providers."""

    DEEPSEEK = "deepseek"
    KIMI = "kimi"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"


# =============================================================================
# LLM Extractor
# =============================================================================


class LLMExtractor:
    """Extract structured entities from text using LLMs."""

    # Provider configurations
    PROVIDER_CONFIGS = {
        LLMProvider.DEEPSEEK: {
            "base_url": "https://api.deepseek.com/v1",
            "model": "deepseek-chat",
            "env_var": "DEEPSEEK_API_KEY",
        },
        LLMProvider.KIMI: {
            "base_url": "https://api.moonshot.cn/v1",
            "model": "kimi-k2.5",
            "env_var": "KIMI_API_KEY",
        },
        LLMProvider.OPENAI: {
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "env_var": "OPENAI_API_KEY",
        },
    }

    def __init__(self, provider: LLMProvider = LLMProvider.DEEPSEEK):
        """
        Initialize LLM extractor.

        Args:
            provider: LLM provider to use
        """
        self.provider = provider
        config = self.PROVIDER_CONFIGS[provider]

        # Get API key from environment
        api_key = os.getenv(config["env_var"])
        if not api_key:
            raise ValueError(
                f"API key not found. Set {config['env_var']} environment variable."
            )

        # Initialize OpenAI client (works with OpenAI-compatible APIs)
        self.client = OpenAI(
            api_key=api_key,
            base_url=config["base_url"],
        )
        self.model = config["model"]

        logger.info(f"LLMExtractor initialized with provider: {provider.value}, model: {self.model}")

    def extract_people(self, text: str, max_tokens: int = 1000) -> list[PersonExtraction]:
        """
        Extract people/executives from text.

        Args:
            text: Section text (typically Item 1 - Business)
            max_tokens: Maximum tokens for response

        Returns:
            List of extracted people
        """
        prompt = self._build_person_extraction_prompt(text)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precise entity extraction system for SEC filings. Extract only factual information explicitly stated in the text. Output valid JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
                temperature=0,  # Deterministic output
                response_format={"type": "json_object"},
            )

            # Parse response
            content = response.choices[0].message.content
            if not content:
                logger.warning("Empty response from LLM")
                return []

            data = json.loads(content)
            people_data = data.get("people", [])

            # Validate with Pydantic
            people = [PersonExtraction(**p) for p in people_data]
            logger.info(f"Extracted {len(people)} people")
            return people

        except Exception as e:
            logger.error(f"Person extraction failed: {e}")
            return []

    def extract_risk_factors(
        self, text: str, max_tokens: int = 2000
    ) -> list[RiskFactorExtraction]:
        """
        Extract risk factors from text.

        Args:
            text: Section text (typically Item 1A - Risk Factors)
            max_tokens: Maximum tokens for response

        Returns:
            List of extracted risk factors
        """
        prompt = self._build_risk_extraction_prompt(text)

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precise risk analysis system for SEC filings. Extract and categorize risk factors. Output valid JSON only.",
                    },
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
                temperature=0,
                response_format={"type": "json_object"},
            )

            # Parse response
            content = response.choices[0].message.content
            if not content:
                logger.warning("Empty response from LLM")
                return []

            data = json.loads(content)
            risks_data = data.get("risk_factors", [])

            # Validate with Pydantic
            risks = [RiskFactorExtraction(**r) for r in risks_data]
            logger.info(f"Extracted {len(risks)} risk factors")
            return risks

        except Exception as e:
            logger.error(f"Risk extraction failed: {e}")
            return []

    def extract_from_section(
        self, section_text: str, section_type: str
    ) -> ExtractionResult:
        """
        Extract entities from a filing section.

        Args:
            section_text: Text content of the section
            section_type: Section identifier (e.g., 'item_10', 'item_1a', 'item_1')

        Returns:
            Extraction result with people and/or risk factors
            
        Note:
            - Item 10 (Directors, Executive Officers) is preferred for people
            - Item 1 (Business) is used as fallback when Item 10 is incorporated by reference
            - Item 1A (Risk Factors) is used for risk extraction
        """
        result = ExtractionResult()

        try:
            # Extract people from Item 10 (Directors, Officers) or Item 1 (Business)
            if ("item_10" in section_type.lower() or 
                ("item_1" in section_type.lower() and "1a" not in section_type.lower())):
                logger.debug(f"Extracting people from {section_type}")
                result.people = self.extract_people(section_text)

            # Extract risks from Item 1A (Risk Factors)
            if "1a" in section_type.lower() or "risk" in section_type.lower():
                logger.debug(f"Extracting risks from {section_type}")
                result.risk_factors = self.extract_risk_factors(section_text)

            result.extraction_success = True

        except Exception as e:
            logger.error(f"Extraction failed for {section_type}: {e}")
            result.extraction_success = False
            result.error_message = str(e)

        return result

    def _build_person_extraction_prompt(self, text: str) -> str:
        """Build prompt for person extraction."""
        return f"""Extract all people mentioned with their roles from this SEC filing text.

Focus on:
- Executive officers (CEO, CFO, COO, etc.)
- Board members and directors
- Key management personnel

For each person, extract:
- Full name
- Role/title
- Start date if mentioned (format: YYYY-MM-DD or just year)

Only extract people explicitly mentioned. Do not infer or hallucinate.

Text:
{text[:15000]}

Output JSON format:
{{
  "people": [
    {{"name": "John Doe", "role": "CEO", "start_date": "2020"}},
    {{"name": "Jane Smith", "role": "CFO", "start_date": null}}
  ]
}}"""

    def _build_risk_extraction_prompt(self, text: str) -> str:
        """Build prompt for risk factor extraction."""
        return f"""Extract and categorize all risk factors from this SEC filing Risk Factors section.

For each risk, extract:
- Category: Choose ONE: operational, financial, regulatory, market, cybersecurity, legal, competitive, other
- Severity: Rate 1-5 (1=low, 2=moderate, 3=significant, 4=high, 5=critical)
- Description: Brief summary (max 200 chars)

Guidelines:
- Focus on major risks only (top 10-15)
- Be specific about categories
- Consider impact and likelihood for severity
- Keep descriptions concise

Text:
{text[:30000]}

Output JSON format:
{{
  "risk_factors": [
    {{"category": "regulatory", "severity": 4, "description": "Changes in regulations could materially impact operations"}},
    {{"category": "market", "severity": 3, "description": "Economic downturn could reduce demand for products"}}
  ]
}}"""
