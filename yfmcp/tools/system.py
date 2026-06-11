"""System domain: server health and manifest diagnostics tools.

Registers two tools on the shared ``yfinance_server``:
- ``health_check``: runtime health metadata (version, tool count, manifest hash).
- ``get_manifest_diagnostics``: deployment diagnostics (build SHA, deploy time,
  canonical/deprecated counts, connector-staleness advisory).
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os

from yfmcp.app import TOOL_ALIASES, yfinance_server
from yfmcp.envelope import SERVER_VERSION

_SIMPLE_OUTPUT_SCHEMA: dict = {"type": "object", "properties": {}, "additionalProperties": True}

_MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "toolCount": {"type": "number"},
        "manifestVersion": {"type": ["string", "null"]},
        "manifestHash": {"type": ["string", "null"]},
        "buildSha": {"type": ["string", "null"]},
        "deployedAt": {"type": ["string", "null"]},
        "privacyScope": {"type": "string"},
        "canonicalToolCount": {"type": "number"},
        "deprecatedAliasCount": {"type": "number"},
        "publicSchemaGeneratedAt": {"type": ["string", "null"]},
        "workerSchemaGeneratedAt": {"type": ["string", "null"]},
        "manifestMismatch": {"type": ["boolean", "null"]},
        "staleConnectorWarning": {"type": ["string", "null"]},
    },
}


@yfinance_server.tool(name="health_check", output_schema=_SIMPLE_OUTPUT_SCHEMA, description="Return runtime health metadata.")
async def health_check() -> str:
    try:
        tool_count = len(yfinance_server._tool_manager._tools)
    except Exception:
        tool_count = len(TOOL_ALIASES) + 50
    tool_names = sorted(TOOL_ALIASES.keys())
    manifest_hash = hashlib.sha256(json.dumps(tool_names).encode("utf-8")).hexdigest()[:16]
    manifest_version = os.environ.get("MANIFEST_VERSION", "1")
    deployed_at = os.environ.get("DEPLOYED_AT", datetime.datetime.utcnow().isoformat() + "Z")
    runtime_hash = hashlib.sha256((SERVER_VERSION + str(tool_count)).encode("utf-8")).hexdigest()[:16]
    deprecated_alias_count = len(
        {
            "get_tps_inputs",
            "get_eqf_bracket",
            "get_adv_gate",
            "get_dc134_options_scan",
            "get_china_revenue_pct",
            "get_geographic_revenue",
            "get_filing_text_search",
            "get_filing_document",
        }
    )
    return json.dumps({
        "serverVersion": SERVER_VERSION,
        "buildSha": os.environ.get("BUILD_SHA", "unknown"),
        "toolCount": tool_count,
        "canonicalToolCount": max(tool_count - deprecated_alias_count, 0),
        "deprecatedAliasCount": deprecated_alias_count,
        "manifestVersion": manifest_version,
        "manifestHash": manifest_hash,
        "schemaHash": manifest_hash,
        "runtimeHash": runtime_hash,
        "deployedAt": deployed_at,
        "generatedAt": datetime.datetime.utcnow().isoformat() + "Z",
        "privacyScope": "public_market_data_only",
    })


@yfinance_server.tool(name="get_manifest_diagnostics", output_schema=_MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA, description="Return deployment and manifest diagnostics: tool counts, manifest version, hash, build SHA, deploy timestamp, privacy scope, and connector-staleness advisory.")
async def get_manifest_diagnostics() -> str:
    try:
        tool_count = len(yfinance_server._tool_manager._tools)
    except Exception:
        tool_count = len(TOOL_ALIASES) + 50
    tool_names = sorted(TOOL_ALIASES.keys())
    manifest_hash = hashlib.sha256(json.dumps(tool_names).encode("utf-8")).hexdigest()[:16]
    manifest_version = os.environ.get("MANIFEST_VERSION", None)
    deployed_at = os.environ.get("DEPLOYED_AT", None)
    deprecated_alias_set = {
        "get_tps_inputs",
        "get_eqf_bracket",
        "get_adv_gate",
        "get_dc134_options_scan",
        "get_china_revenue_pct",
        "get_geographic_revenue",
        "get_filing_text_search",
        "get_filing_document",
    }
    deprecated_alias_count = len(deprecated_alias_set)
    canonical_tool_count = max(tool_count - deprecated_alias_count, 0)
    worker_schema_generated_at = datetime.datetime.utcnow().isoformat() + "Z"
    return json.dumps({
        "toolCount": tool_count,
        "manifestVersion": manifest_version,
        "manifestHash": manifest_hash,
        "buildSha": os.environ.get("BUILD_SHA", "unknown"),
        "deployedAt": deployed_at,
        "privacyScope": "public_market_data_only",
        "canonicalToolCount": canonical_tool_count,
        "deprecatedAliasCount": deprecated_alias_count,
        "publicSchemaGeneratedAt": None,
        "workerSchemaGeneratedAt": worker_schema_generated_at,
        "manifestMismatch": None,
        "staleConnectorWarning": "ChatGPT connector schema may lag the deployed Worker schema. Direct Worker tools/list and get_manifest_diagnostics are source of truth.",
        "serverVersion": SERVER_VERSION,
    })
