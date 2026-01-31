"""CLI entry point for the FinLoom agent."""

import argparse
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from src.infrastructure.logger import get_logger, setup_logging

setup_logging()
logger = get_logger("finloom.agents.cli")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="FinLoom AI Agent — SEC filing analysis"
    )
    parser.add_argument("query", help="Financial analysis query")
    parser.add_argument(
        "--max-iterations", type=int, default=15, help="Max ReAct iterations (default: 15)"
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Show intermediate reasoning steps"
    )
    parser.add_argument("--db-path", help="Override DuckDB path")
    parser.add_argument(
        "--passage-graph",
        default="data/passage_graph.pkl",
        help="Passage graph path (default: data/passage_graph.pkl)",
    )
    args = parser.parse_args()

    if not os.getenv("DEEPSEEK_API_KEY"):
        print("Error: DEEPSEEK_API_KEY not set", file=sys.stderr)
        return 1

    if not os.getenv("OPENAI_API_KEY"):
        logger.warning("OPENAI_API_KEY not set — vector search will fail")

    from .agent import FinLoomAgent
    from .context import AgentContext

    context: AgentContext | None = None
    try:
        context = AgentContext(
            db_path=args.db_path,
            passage_graph_path=args.passage_graph,
        )
        agent = FinLoomAgent(context=context, max_iterations=args.max_iterations)

        print(f"\n{'=' * 80}")
        print(f"Query: {args.query}")
        print(f"{'=' * 80}\n")

        result = agent.query(args.query)

        if args.verbose and result["steps"]:
            print("Reasoning Steps:")
            print("-" * 80)
            for i, step in enumerate(result["steps"], 1):
                if step["type"] == "action":
                    print(f"\n  [{i}] TOOL: {step['tool']}")
                    print(f"      Args: {step['args']}")
                else:
                    print(f"\n  [{i}] OBSERVATION:")
                    print(f"      {step['content']}")
            print("\n" + "=" * 80)

        print("\nAnswer:")
        print("-" * 80)
        print(result["answer"])
        print(f"\n{'=' * 80}")
        print(f"Tool calls: {result['iterations']} | Success: {result['success']}")

        return 0 if result["success"] else 1

    except KeyboardInterrupt:
        print("\nInterrupted.")
        return 130
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)
        print(f"\nError: {e}", file=sys.stderr)
        return 1
    finally:
        if context is not None:
            context.close()


if __name__ == "__main__":
    sys.exit(main())
