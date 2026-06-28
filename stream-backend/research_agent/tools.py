from __future__ import annotations

import os
from typing import Any

from langchain_core.tools import tool


@tool
def think_tool(thought: str) -> str:
    """Record a private reasoning checkpoint before taking an action."""
    return f"Thought recorded: {thought}"


@tool
def tavily_search(query: str, max_results: int = 5) -> dict[str, Any]:
    """Search the web with Tavily when TAVILY_API_KEY is configured."""
    api_key = os.getenv("TAVILY_API_KEY")
    if not api_key:
        return {
            "query": query,
            "results": [
                {
                    "title": "Tavily not configured",
                    "content": "Set TAVILY_API_KEY to enable live Tavily search.",
                    "url": None,
                }
            ],
        }

    try:
        from tavily import TavilyClient

        client = TavilyClient(api_key=api_key)
        return client.search(query=query, max_results=max_results)
    except Exception as exc:  # pragma: no cover - depends on external service
        return {"query": query, "error": str(exc), "results": []}

