"""
Graph schema definitions using Pydantic.

Defines 8 node types and 12 relationship types for SEC filing data:
- Company, Person, Filing, Section, FinancialMetric, RiskFactor, BusinessSegment, Event
- Relationships connect these nodes to represent filing structure and business relationships
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field

# =============================================================================
# Node Models (8 required)
# =============================================================================


class CompanyNode(BaseModel):
    """
    Company entity node.
    
    Represents a public company that files with the SEC.
    """

    cik: str = Field(..., description="Central Index Key (unique identifier)")
    name: str = Field(..., description="Official company name")
    ticker: str = Field(..., description="Stock ticker symbol")
    sector: str | None = Field(default=None, description="Business sector")
    sic_code: str | None = Field(default=None, description="Standard Industrial Classification code")
    fiscal_year_end: str | None = Field(
        default=None, description="Fiscal year end (MMDD format)"
    )


class PersonNode(BaseModel):
    """
    Person entity node (executives, directors, key management).
    
    Represents individuals mentioned in SEC filings.
    """

    name: str = Field(..., description="Full name")
    role: str = Field(..., description="Position/title (CEO, CFO, Director, etc.)")
    tenure_start: date | None = Field(default=None, description="Start date of tenure")
    tenure_end: date | None = Field(default=None, description="End date of tenure (if no longer active)")
    aliases: list[str] = Field(
        default_factory=list, description="Name variations and aliases"
    )


class FilingNode(BaseModel):
    """
    SEC filing document node.
    
    Represents a single filing (10-K, 10-Q, etc.).
    """

    accession_number: str = Field(..., description="Unique SEC accession number")
    form_type: str = Field(..., description="Form type (10-K, 10-Q, 8-K, etc.)")
    filing_date: date = Field(..., description="Date filed with SEC")
    fiscal_period: str | None = Field(
        default=None, description="Fiscal period (FY, Q1, Q2, Q3, Q4)"
    )
    document_count: int = Field(default=0, description="Number of documents in filing")


class SectionNode(BaseModel):
    """
    Filing section node.
    
    Represents a specific section within a filing (Item 1, Item 1A, etc.).
    """

    section_type: str = Field(
        ..., description="Section identifier (item_1, item_1a, item_7, etc.)"
    )
    content_summary: str | None = Field(
        default=None, description="Brief summary of section content"
    )
    word_count: int = Field(default=0, description="Word count of section")
    markdown_hash: str | None = Field(
        default=None, description="Hash of markdown content for deduplication"
    )


class FinancialMetricNode(BaseModel):
    """
    Financial metric/fact node.
    
    Represents a single financial data point extracted from XBRL.
    """

    concept_name: str = Field(..., description="XBRL concept name (e.g., Revenue, Assets)")
    value: float = Field(..., description="Numeric value")
    unit: str = Field(..., description="Unit of measurement (USD, shares, etc.)")
    period_start: date | None = Field(default=None, description="Period start date")
    period_end: date | None = Field(default=None, description="Period end date")
    context_ref: str | None = Field(
        default=None, description="XBRL context reference for traceability"
    )


class RiskFactorNode(BaseModel):
    """
    Risk factor node.
    
    Represents a risk disclosed in Item 1A.
    """

    category: str = Field(
        ..., description="Risk category (operational, financial, regulatory, etc.)"
    )
    severity: int = Field(..., ge=1, le=5, description="Severity rating (1=low, 5=critical)")
    description: str = Field(..., description="Risk description text")
    first_mentioned_date: date | None = Field(
        default=None, description="First date this risk appeared in filings"
    )


class BusinessSegmentNode(BaseModel):
    """
    Business segment node.
    
    Represents a business unit, product line, or geographic segment.
    """

    name: str = Field(..., description="Segment name")
    revenue: float | None = Field(default=None, description="Segment revenue (USD)")
    geography: str | None = Field(
        default=None, description="Geographic region (if geographic segment)"
    )
    product_line: str | None = Field(
        default=None, description="Product/service line (if product segment)"
    )


class EventNode(BaseModel):
    """
    Business event node.
    
    Represents significant events (acquisitions, restructuring, lawsuits, etc.).
    """

    event_type: str = Field(
        ...,
        description="Event type (acquisition, divestiture, lawsuit, restructuring, etc.)",
        alias="type",
    )
    event_date: date = Field(..., description="Event date", alias="date")
    description: str = Field(..., description="Event description")
    impact: str | None = Field(
        default=None, description="Business impact assessment (positive, negative, neutral)"
    )
    related_entities: list[str] = Field(
        default_factory=list, description="Names of other entities involved"
    )


# =============================================================================
# Relationship Model
# =============================================================================


class Relationship(BaseModel):
    """
    Generic relationship model for graph edges.
    
    Relationship types include:
    1. FILED: Company -> Filing
    2. HAS_EXECUTIVE: Company -> Person
    3. HAS_SEGMENT: Company -> BusinessSegment
    4. CONTAINS_SECTION: Filing -> Section
    5. REPORTS_METRIC: Filing -> FinancialMetric
    6. DISCLOSES_RISK: Filing -> RiskFactor
    7. EXPERIENCED: Company -> Event
    8. COMPETES_WITH: Company -> Company
    9. MOVED_TO: Person -> Company (executive transitions)
    10. CHANGED_FROM: FinancialMetric -> FinancialMetric (period-over-period)
    11. OPERATES_IN: Company -> BusinessSegment
    12. AFFECTS: RiskFactor -> BusinessSegment
    """

    from_node: str = Field(..., description="Source node ID or label")
    to_node: str = Field(..., description="Target node ID or label")
    relationship_type: str = Field(
        ..., description="Relationship type (FILED, HAS_EXECUTIVE, etc.)"
    )
    properties: dict = Field(
        default_factory=dict, description="Additional relationship properties"
    )


# =============================================================================
# Relationship Type Constants
# =============================================================================

# Standard relationship types used in the graph
RELATIONSHIP_TYPES = {
    "FILED": "Company filed a document",
    "HAS_EXECUTIVE": "Company has an executive/director",
    "HAS_SEGMENT": "Company has a business segment",
    "CONTAINS_SECTION": "Filing contains a section",
    "REPORTS_METRIC": "Filing reports a financial metric",
    "DISCLOSES_RISK": "Filing discloses a risk factor",
    "EXPERIENCED": "Company experienced an event",
    "COMPETES_WITH": "Company competes with another company",
    "MOVED_TO": "Person transitioned to another company",
    "CHANGED_FROM": "Metric changed from previous period",
    "OPERATES_IN": "Company operates in a business segment",
    "AFFECTS": "Risk factor affects a business segment",
}

# Node labels for Cypher queries
NODE_LABELS = {
    "COMPANY": "Company",
    "PERSON": "Person",
    "FILING": "Filing",
    "SECTION": "Section",
    "FINANCIAL_METRIC": "FinancialMetric",
    "RISK_FACTOR": "RiskFactor",
    "BUSINESS_SEGMENT": "BusinessSegment",
    "EVENT": "Event",
}
