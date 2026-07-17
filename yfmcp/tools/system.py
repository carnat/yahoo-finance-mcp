"""Public-safe server availability and schema identity tools."""

from __future__ import annotations

import datetime
import hashlib
import json
import os

from yfmcp.app import yfinance_server
from yfmcp.envelope import SERVER_VERSION
from yfmcp.schemas import _MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA, _SIMPLE_OUTPUT_SCHEMA


def _tool_inventory() -> tuple[list[str], list[dict]]:
    try:
        tools = yfinance_server._tool_manager._tools
    except Exception:
        tools = {}
    names = sorted(tools.keys())
    specs: list[dict] = []
    for name in names:
        tool = tools[name]
        entry: dict = {"name": name, "description": str(getattr(tool, "description", "") or "")}
        for attr in ("parameters", "input_schema", "output_schema"):
            value = getattr(tool, attr, None)
            if value is None:
                continue
            if hasattr(value, "model_dump"):
                value = value.model_dump()
            if isinstance(value, (dict, list, str, int, float, bool)):
                entry[attr] = value
        specs.append(entry)
    return names, specs


def _public_metadata() -> dict:
    names, specs = _tool_inventory()
    manifest_hash = hashlib.sha256(json.dumps(names, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    schema_hash = hashlib.sha256(json.dumps(specs, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
    tool_mode = os.environ.get("TOOL_MODE", "expanded").lower()
    return {
        "status": "ok",
        "serverVersion": SERVER_VERSION,
        "toolCount": len(names),
        "manifestVersion": os.environ.get("MANIFEST_VERSION", "1"),
        "manifestHash": manifest_hash,
        "schemaHash": schema_hash,
        "runtimeHash": hashlib.sha256(f"{SERVER_VERSION}|{schema_hash}|{tool_mode}".encode("utf-8")).hexdigest()[:16],
        "toolMode": tool_mode,
        "envelopeSchemaVersion": "2026-07-08",
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
        "privacyScope": "public_market_data_only",
    }


@yfinance_server.tool(name="health_check", output_schema=_SIMPLE_OUTPUT_SCHEMA, description="Return public-safe MCP availability and schema identity metadata.")
async def health_check() -> str:
    return json.dumps(_public_metadata())


@yfinance_server.tool(name="get_manifest_diagnostics", output_schema=_MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA, description="Return public-safe MCP schema identity metadata for connector freshness checks.")
async def get_manifest_diagnostics() -> str:
    return json.dumps(_public_metadata())
