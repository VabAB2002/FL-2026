"""Main FinLoom agent using LangGraph ReAct pattern."""

from __future__ import annotations

import os

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from src.infrastructure.logger import get_logger
from src.retrieval.query_router import (
    DecomposedQuery,
    QueryRouter,
    QueryType,
    detect_companies,
)

from .context import AgentContext
from .prompts import SYSTEM_PROMPT, build_system_prompt
from .tools import create_tools

logger = get_logger("finloom.agents.agent")


class FinLoomAgent:
    """LangGraph ReAct agent for SEC filing analysis.

    Orchestrates retrieval tools and financial analytics to answer
    complex queries about SEC filings. Uses pre-query enrichment
    to classify and decompose complex queries before the ReAct loop.
    """

    def __init__(
        self,
        context: AgentContext,
        model: str = "deepseek-chat",
        temperature: float = 0.1,
        max_iterations: int = 15,
    ):
        api_key = os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY environment variable not set")

        self.context = context
        self.max_iterations = max_iterations

        self.llm = ChatOpenAI(
            model=model,
            temperature=temperature,
            openai_api_key=api_key,
            openai_api_base="https://api.deepseek.com",
        )

        self.tools = create_tools(context)

        # No static prompt — we inject a dynamic one per query
        self.agent = create_react_agent(
            model=self.llm,
            tools=self.tools,
        )

        self._router = QueryRouter(api_key=api_key, model=model)

        logger.info(
            f"FinLoomAgent ready: model={model}, "
            f"{len(self.tools)} tools, max_iterations={max_iterations}"
        )

    def _enrich_query(self, question: str) -> tuple[str, dict]:
        """Pre-agent query enrichment: classify and optionally decompose.

        Returns:
            (system_prompt, metadata) — metadata contains routing
            and decomposition info for logging/debugging.
        """
        try:
            decision = self._router.route(question)

            meta: dict = {
                "query_type": decision.query_type.value,
                "confidence": decision.confidence,
                "routing_reasoning": decision.reasoning,
            }

            if decision.query_type == QueryType.SIMPLE_FACT:
                decomposed = DecomposedQuery(
                    original_query=question,
                    query_type=QueryType.SIMPLE_FACT,
                    companies=sorted(detect_companies(question)),
                    sub_queries=[],
                    synthesis_hint="",
                    reasoning="Simple query",
                )
            else:
                decomposed = self._router.decompose(question, decision)
                meta["sub_queries"] = decomposed.sub_queries
                meta["synthesis_hint"] = decomposed.synthesis_hint
                logger.info(
                    f"Decomposed into {len(decomposed.sub_queries)} sub-queries: "
                    f"{decomposed.sub_queries}"
                )

            prompt = build_system_prompt(decomposed)
            return prompt, meta

        except Exception as e:
            logger.warning(f"Query enrichment failed: {e}, using default prompt")
            return SYSTEM_PROMPT, {"query_type": "unknown", "error": str(e)}

    def query(self, question: str) -> dict:
        """Run a financial analysis query through the agent.

        Returns:
            {
                "answer": str,
                "steps": list[dict],   # tool calls and observations
                "iterations": int,
                "success": bool,
                "query_type": str,
                "sub_queries": list[str],
            }
        """
        logger.info(f"Query: {question}")

        try:
            # Phase 1: Pre-agent enrichment
            system_prompt, enrichment_meta = self._enrich_query(question)

            logger.info(
                f"Query type: {enrichment_meta.get('query_type')} | "
                f"Sub-queries: {len(enrichment_meta.get('sub_queries', []))}"
            )

            # Phase 2: Run ReAct agent with enriched prompt
            result = self.agent.invoke(
                {
                    "messages": [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=question),
                    ]
                },
                config={"recursion_limit": self.max_iterations * 2},
            )

            messages = result["messages"]

            # Extract reasoning steps
            steps: list[dict] = []
            for msg in messages:
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tc in msg.tool_calls:
                        steps.append(
                            {"type": "action", "tool": tc["name"], "args": tc["args"]}
                        )
                elif msg.type == "tool":
                    preview = (
                        msg.content[:200] + "..."
                        if len(msg.content) > 200
                        else msg.content
                    )
                    steps.append({"type": "observation", "content": preview})

            answer = messages[-1].content if messages else "No response generated."
            iterations = sum(1 for s in steps if s["type"] == "action")

            logger.info(f"Completed in {iterations} tool calls")
            return {
                "answer": answer,
                "steps": steps,
                "iterations": iterations,
                "success": True,
                "query_type": enrichment_meta.get("query_type"),
                "sub_queries": enrichment_meta.get("sub_queries", []),
            }

        except Exception as e:
            logger.error(f"Agent query failed: {e}", exc_info=True)
            return {
                "answer": f"Error: {e}",
                "steps": [],
                "iterations": 0,
                "success": False,
            }
