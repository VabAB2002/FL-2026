"""
Graph database module for FinLoom.

Provides Neo4j integration with:
- Schema definitions for nodes and relationships
- Client wrapper for database operations
- Graph construction and XBRL import
- Community detection (Leiden) and summarization
- Type-safe models using Pydantic
"""

from src.graph.graph_connector import Neo4jClient
from src.graph.schema import (
    BusinessSegmentNode,
    CompanyNode,
    EventNode,
    FilingNode,
    FinancialMetricNode,
    PersonNode,
    Relationship,
    RiskFactorNode,
    SectionNode,
)

# Lazy imports for modules with heavy dependencies (graphdatascience, openai)
# Use: from src.graph.find_communities import CommunityDetector
# Use: from src.graph.summarization import CommunitySummarizer

__all__ = [
    "Neo4jClient",
    "CommunityDetector",
    "CommunitySummarizer",
    "CompanyNode",
    "PersonNode",
    "FilingNode",
    "SectionNode",
    "FinancialMetricNode",
    "RiskFactorNode",
    "BusinessSegmentNode",
    "EventNode",
    "Relationship",
]
