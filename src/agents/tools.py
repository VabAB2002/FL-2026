"""Tool definitions for the FinLoom agent."""

from __future__ import annotations

from typing import Annotated, Optional

from langchain_core.tools import tool

from src.infrastructure.logger import get_logger

from .context import AgentContext

logger = get_logger("finloom.agents.tools")


def _format_results(results: list[dict], max_per_result: int = 300) -> str:
    """Format retrieval results into text the LLM can read."""
    if not results:
        return "No results found."

    lines: list[str] = []
    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        ticker = meta.get("ticker", "N/A")
        company = meta.get("company_name", "N/A")
        section = meta.get("section_title", "N/A")
        date = meta.get("filing_date", "N/A")
        score = r.get("score", 0)
        content = r.get("content", "")[:max_per_result]

        lines.append(
            f"[{i}] {company} ({ticker}) | {section} | {date} | "
            f"score={score:.3f}\n{content}..."
        )

    return "\n\n".join(lines)


def create_tools(context: AgentContext) -> list:
    """Create LangChain tools with access to shared context.

    Uses closure pattern so tools have clean signatures (required by
    LangGraph) while still accessing shared DB/retriever instances.
    """

    @tool
    def vector_search(
        query: Annotated[str, "Search query text"],
        top_k: Annotated[int, "Number of results to return"] = 10,
        ticker: Annotated[Optional[str], "Filter by ticker (e.g. 'AAPL')"] = None,
    ) -> str:
        """Semantic search across SEC filing chunks using vector embeddings.

        Best for conceptual queries, finding similar content, or broad exploration.
        """
        try:
            filters = {"ticker": ticker} if ticker else None
            results = context.hybrid_retriever.vector_search.search(
                query=query, top_k=top_k, filters=filters
            )
            return _format_results(results)
        except Exception as e:
            logger.error(f"vector_search error: {e}")
            return f"Error: {e}"

    @tool
    def keyword_search(
        query: Annotated[str, "Keywords or phrase to search"],
        top_k: Annotated[int, "Number of results"] = 10,
        ticker: Annotated[Optional[str], "Filter by ticker"] = None,
    ) -> str:
        """Exact keyword/phrase search using BM25 ranking.

        Best for specific terms, legal phrases, exact quotes.
        """
        try:
            filter_str = f"ticker = '{ticker}'" if ticker else None
            results = context.hybrid_retriever.keyword_search.search(
                query=query, top_k=top_k, filters=filter_str
            )
            return _format_results(results)
        except Exception as e:
            logger.error(f"keyword_search error: {e}")
            return f"Error: {e}"

    @tool
    def graph_search(
        entity_name: Annotated[str, "Company name or ticker (e.g. 'AAPL')"],
        top_k: Annotated[int, "Number of results"] = 10,
    ) -> str:
        """Search for content related to a specific entity using the knowledge graph.

        Best for finding all content about a company or executive.
        """
        try:
            gs = context.hybrid_retriever.graph_search
            if gs is None:
                return "Graph search unavailable (Neo4j not connected)."
            results = gs.search_by_entity(entity_name=entity_name, top_k=top_k)
            return _format_results(results)
        except Exception as e:
            logger.error(f"graph_search error: {e}")
            return f"Error: {e}"

    @tool
    def multi_hop_search(
        query: Annotated[str, "Complex query requiring multi-hop reasoning"],
        top_k: Annotated[int, "Number of final results"] = 10,
        max_hops: Annotated[int, "Maximum reasoning hops (1-3)"] = 2,
    ) -> str:
        """Multi-hop retrieval traversing related passages across filings.

        Best for cross-company comparisons, causal analysis, or queries
        requiring connected information from multiple sections or companies.
        Note: first call loads a large passage graph (~5 s startup).
        """
        try:
            results = context.hoprag_retriever.retrieve(
                query=query, top_k=top_k, max_hops=max_hops
            )
            return _format_results(results)
        except Exception as e:
            logger.error(f"multi_hop_search error: {e}")
            return f"Error: {e}"

    @tool
    def get_financial_metric(
        ticker: Annotated[str, "Company ticker (e.g. 'AAPL')"],
        metric: Annotated[
            str,
            "One of: revenue, assets, liabilities, equity, net_income",
        ] = "revenue",
    ) -> str:
        """Retrieve XBRL financial metrics from the structured database.

        Returns historical values for the specified metric.
        Best for quantitative questions about specific financial numbers.
        """
        try:
            # Resolve ticker → CIK
            row = context.db.connection.execute(
                "SELECT cik FROM companies WHERE ticker = ?", [ticker]
            ).fetchone()
            if not row:
                return f"Company with ticker '{ticker}' not found in database."
            cik = row[0]

            df = context.db.analytics.get_key_financials(cik=cik)
            if df.empty:
                return f"No financial data found for {ticker} (CIK {cik})."

            metric_col = {
                "revenue": "revenue",
                "assets": "total_assets",
                "liabilities": "total_liabilities",
                "equity": "equity",
                "net_income": "net_income",
            }.get(metric.lower())

            if not metric_col or metric_col not in df.columns:
                available = "revenue, assets, liabilities, equity, net_income"
                return f"Unknown metric '{metric}'. Available: {available}"

            lines = [f"{ticker} — {metric.upper()}:"]
            for _, row in df.iterrows():
                val = row[metric_col]
                period = row.get("period_of_report", "?")
                if val is not None and str(val) != "None":
                    lines.append(f"  {period}: ${float(val):,.0f}")
                else:
                    lines.append(f"  {period}: N/A")

            return "\n".join(lines) if len(lines) > 1 else f"No {metric} data for {ticker}."
        except Exception as e:
            logger.error(f"get_financial_metric error: {e}")
            return f"Error: {e}"

    @tool
    def compare_metrics(
        tickers: Annotated[list[str], "List of tickers to compare (e.g. ['AAPL', 'MSFT'])"],
        metric: Annotated[str, "Metric to compare (revenue, assets, etc.)"] = "revenue",
    ) -> str:
        """Compare a financial metric across multiple companies.

        Returns side-by-side values for each company.
        Best for cross-company numerical comparison.
        """
        try:
            metric_col = {
                "revenue": "revenue",
                "assets": "total_assets",
                "liabilities": "total_liabilities",
                "equity": "equity",
                "net_income": "net_income",
            }.get(metric.lower())

            if not metric_col:
                return f"Unknown metric '{metric}'. Use: revenue, assets, liabilities, equity, net_income"

            lines = [f"Comparison — {metric.upper()}:\n"]
            for ticker in tickers:
                row = context.db.connection.execute(
                    "SELECT cik FROM companies WHERE ticker = ?", [ticker]
                ).fetchone()
                if not row:
                    lines.append(f"{ticker}: not found in database")
                    continue

                df = context.db.analytics.get_key_financials(cik=row[0])
                if df.empty or metric_col not in df.columns:
                    lines.append(f"{ticker}: no data")
                    continue

                lines.append(f"{ticker}:")
                for _, r in df.iterrows():
                    val = r[metric_col]
                    period = r.get("period_of_report", "?")
                    if val is not None and str(val) != "None":
                        lines.append(f"  {period}: ${float(val):,.0f}")

            return "\n".join(lines)
        except Exception as e:
            logger.error(f"compare_metrics error: {e}")
            return f"Error: {e}"

    @tool
    def search_by_section(
        section: Annotated[
            str,
            "Section ID: item_1 (Business), item_1a (Risk Factors), "
            "item_7 (MD&A), item_8 (Financials)",
        ],
        query: Annotated[str, "Search query within the section"],
        top_k: Annotated[int, "Number of results"] = 10,
        ticker: Annotated[Optional[str], "Filter by ticker"] = None,
    ) -> str:
        """Search within a specific SEC filing section.

        Filters results to the specified section for targeted analysis.
        Best for questions about specific topics within a known section.
        """
        try:
            results = context.hybrid_retriever.retrieve(
                query=query, top_k=top_k * 3, ticker=ticker
            )

            # Normalize section ID: "item_1a" -> "item 1a", "ITEM 1A" -> "item 1a"
            def _normalize_section(s: str) -> str:
                return s.lower().replace("_", " ").strip()

            target = _normalize_section(section)

            filtered = [
                r
                for r in results
                if _normalize_section(r["metadata"].get("section_item", "")) == target
            ]
            if not filtered:
                return f"No results found in section '{section}' for query '{query}'."
            return _format_results(filtered[:top_k])
        except Exception as e:
            logger.error(f"search_by_section error: {e}")
            return f"Error: {e}"

    return [
        vector_search,
        keyword_search,
        graph_search,
        multi_hop_search,
        get_financial_metric,
        compare_metrics,
        search_by_section,
    ]
