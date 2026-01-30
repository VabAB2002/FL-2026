"""
GPT-4o community summarization for the knowledge graph.

Generates structured summaries for each detected community,
capturing themes, companies, and time periods.
"""

from __future__ import annotations

import json
import os
from collections import Counter

from openai import OpenAI

from src.graph.graph_connector import Neo4jClient
from src.infrastructure.logger import get_logger

logger = get_logger("finloom.graph.summarization")

_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
_DEFAULT_MODEL = "deepseek-chat"

# Keys to extract from Neo4j node properties for summarization
_NODE_DISPLAY_KEYS = ("name", "ticker", "concept_name", "category", "section_type", "description")


class CommunitySummarizer:
    """Generate LLM summaries for graph communities using DeepSeek-V3.2."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = _DEFAULT_MODEL,
        base_url: str = _DEEPSEEK_BASE_URL,
    ):
        """
        Initialize summarizer.

        Args:
            api_key: DeepSeek API key. Falls back to DEEPSEEK_API_KEY env var.
            model: Model name (default: deepseek-chat for V3.2).
            base_url: API base URL.
        """
        resolved_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not resolved_key:
            raise ValueError(
                "DeepSeek API key required. Pass api_key or set DEEPSEEK_API_KEY env var."
            )

        self.client = OpenAI(api_key=resolved_key, base_url=base_url)
        self.model = model
        logger.info(f"CommunitySummarizer initialized (model={model}, base_url={base_url})")

    def summarize_community(
        self,
        community_id: int,
        nodes: list[dict],
        relationships: list[dict] | None = None,
    ) -> dict:
        """
        Generate a structured summary for a community.

        Args:
            community_id: Community ID
            nodes: List of community member nodes (from Neo4j)
            relationships: Optional relationship type distribution

        Returns:
            Summary dict with title, description, themes, time_period, companies, member_count
        """
        type_dist = self._get_node_type_distribution(nodes)
        node_descriptions = self._format_nodes(nodes, max_nodes=30)

        rel_section = ""
        if relationships:
            rel_lines = [f"  - {r['rel_type']}: {r['count']}" for r in relationships[:10]]
            rel_section = f"\nRelationship types:\n" + "\n".join(rel_lines)

        prompt = f"""Summarize this cluster of financial entities from SEC filings.

Community ID: {community_id}
Total nodes: {len(nodes)}
Node type distribution: {type_dist}
{rel_section}
Sample nodes:
{node_descriptions}

Return a JSON object with these fields:
{{
  "title": "Short descriptive title (max 10 words)",
  "description": "2-3 sentence summary of what connects these entities",
  "themes": ["theme1", "theme2", "theme3"],
  "time_period": "YYYY-YYYY or null if not identifiable",
  "companies": ["TICKER1", "TICKER2"] or []
}}"""

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.3,
            )

            summary = json.loads(response.choices[0].message.content)
            summary["community_id"] = community_id
            summary["member_count"] = len(nodes)
            return summary

        except Exception as e:
            logger.error(f"Summarization failed for community {community_id}: {e}")
            return {
                "community_id": community_id,
                "title": f"Community {community_id}",
                "description": f"Summary generation failed: {e}",
                "themes": [],
                "time_period": None,
                "companies": [],
                "member_count": len(nodes),
            }

    def save_summary(
        self, neo4j_client: Neo4jClient, community_id: int, summary: dict
    ) -> None:
        """
        Persist community summary back to Neo4j on member nodes.

        Args:
            neo4j_client: Neo4j client
            community_id: Community ID
            summary: Summary dict to store
        """
        query = """
        MATCH (n) WHERE n.community = $community_id
        SET n.community_summary = $summary_json
        """
        neo4j_client.execute_write(
            query,
            {
                "community_id": community_id,
                "summary_json": json.dumps(summary),
            },
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_nodes(nodes: list[dict], max_nodes: int = 30) -> str:
        """Format node list for the LLM prompt."""
        lines = []
        for node in nodes[:max_nodes]:
            node_type = node.get("types", ["Unknown"])[0]
            props = CommunitySummarizer._extract_display_props(node.get("n", {}))
            lines.append(f"- {node_type}: {props}")
        return "\n".join(lines)

    @staticmethod
    def _extract_display_props(node_props: dict) -> str:
        """Extract human-readable properties from a node."""
        parts = []
        for key in _NODE_DISPLAY_KEYS:
            if key in node_props:
                val = node_props[key]
                if isinstance(val, str) and len(val) > 120:
                    val = val[:120] + "..."
                parts.append(f"{key}={val}")
        return ", ".join(parts) if parts else str(node_props)[:80]

    @staticmethod
    def _get_node_type_distribution(nodes: list[dict]) -> dict[str, int]:
        """Count node types in a community."""
        types = [node.get("types", ["Unknown"])[0] for node in nodes]
        return dict(Counter(types))
