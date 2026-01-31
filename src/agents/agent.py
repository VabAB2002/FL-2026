"""Main FinLoom agent using LangGraph ReAct pattern."""

from __future__ import annotations

import os

from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from src.infrastructure.logger import get_logger

from .context import AgentContext
from .prompts import SYSTEM_PROMPT
from .tools import create_tools

logger = get_logger("finloom.agents.agent")


class FinLoomAgent:
    """LangGraph ReAct agent for SEC filing analysis.

    Orchestrates retrieval tools and financial analytics to answer
    complex queries about SEC filings.
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
        self.agent = create_react_agent(
            model=self.llm,
            tools=self.tools,
            prompt=SYSTEM_PROMPT,
        )

        logger.info(
            f"FinLoomAgent ready: model={model}, "
            f"{len(self.tools)} tools, max_iterations={max_iterations}"
        )

    def query(self, question: str) -> dict:
        """Run a financial analysis query through the agent.

        Returns:
            {
                "answer": str,
                "steps": list[dict],   # tool calls and observations
                "iterations": int,
                "success": bool,
            }
        """
        logger.info(f"Query: {question}")

        try:
            result = self.agent.invoke(
                {"messages": [HumanMessage(content=question)]},
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
            }

        except Exception as e:
            logger.error(f"Agent query failed: {e}", exc_info=True)
            return {
                "answer": f"Error: {e}",
                "steps": [],
                "iterations": 0,
                "success": False,
            }
