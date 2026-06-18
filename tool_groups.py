"""
Tool Grouping Layer for LLM Token Efficiency.

Group definitions live in tool_catalog.json so Python, Worker, docs, and smoke
checks can share one machine-readable catalog.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any


_CATALOG_PATH = Path(__file__).with_name("tool_catalog.json")


def _load_catalog() -> dict[str, Any]:
    with _CATALOG_PATH.open(encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict) or not isinstance(data.get("groups"), dict):
        raise RuntimeError("tool_catalog.json must contain a groups object")
    return data


TOOL_CATALOG = _load_catalog()
TOOL_GROUPS: dict[str, dict[str, Any]] = TOOL_CATALOG["groups"]


def register_grouped_tools(server, handler_registry: dict) -> None:
    """Register domain-grouped meta-tools on the FastMCP server.

    Each meta-tool accepts:
      - action: str (required) โ€” which sub-action to invoke
      - params: dict (optional) โ€” parameters for the sub-action (e.g. ticker, period, etc.)

    ``handler_registry`` maps handler function name -> function (see
    ``yfmcp.app.build_handler_registry``). This replaces the 111 individual tool
    registrations with 10 grouped tools, reducing LLM token overhead.
    """

    def _make_handler(gn, registry):
        async def handler(action: str, params: dict | None = None) -> str:
            return await _route_grouped_call(gn, action, params or {}, registry)
        handler.__name__ = gn
        handler.__qualname__ = gn
        return handler

    for group_name, group_def in TOOL_GROUPS.items():
        handler = _make_handler(group_name, handler_registry)
        server.tool(
            name=group_name,
            description=group_def["description"],
        )(handler)


def get_all_grouped_action_names() -> list[str]:
    """Return a flat list of all action names across all groups."""
    names = []
    for group_def in TOOL_GROUPS.values():
        names.extend(group_def["actions"].keys())
    return names


def get_group_for_action(action: str) -> str | None:
    """Given an action name, return which group it belongs to."""
    for group_name, group_def in TOOL_GROUPS.items():
        if action in group_def["actions"]:
            return group_name
    return None

