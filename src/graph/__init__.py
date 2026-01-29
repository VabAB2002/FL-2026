"""
Graph database module for FinLoom.

Provides Neo4j integration with:
- Schema definitions for nodes and relationships
- Client wrapper for database operations
- Graph construction and XBRL import
- Community detection (Leiden) and summarization
- Type-safe models using Pydantic
"""

from src.graph.community_detection import CommunityDetector
from src.graph.neo4j_client import Neo4jClient
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
from src.graph.summarization import CommunitySummarizer

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
