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
from yfmcp.envelope import BUILD_DATE, SERVER_VERSION
from yfmcp.schemas import _MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA, _SIMPLE_OUTPUT_SCHEMA


def _tool_counts() -> tuple[int, int, int, list[str]]:
    try:
        names = sorted(yfinance_server._tool_manager._tools.keys())
    except Exception:
        names = sorted(TOOL_ALIASES.keys())
    deprecated_alias_count = len([name for name in names if name in TOOL_ALIASES])
    canonical_tool_count = max(len(names) - deprecated_alias_count, 0)
    canonical_names = [name for name in names if name not in TOOL_ALIASES]
    return len(names), canonical_tool_count, deprecated_alias_count, canonical_names


@yfinance_server.tool(name="health_check", output_schema=_SIMPLE_OUTPUT_SCHEMA, description="Return runtime health metadata.")
async def health_check() -> str:
    tool_count, canonical_tool_count, deprecated_alias_count, canonical_names = _tool_counts()
    manifest_hash = hashlib.sha256(json.dumps(canonical_names).encode("utf-8")).hexdigest()[:16]
    manifest_version = os.environ.get("MANIFEST_VERSION", "1")
    deployed_at = os.environ.get("DEPLOYED_AT", datetime.datetime.utcnow().isoformat() + "Z")
    runtime_hash = hashlib.sha256((SERVER_VERSION + str(canonical_tool_count)).encode("utf-8")).hexdigest()[:16]
    return json.dumps({
        "serverVersion": SERVER_VERSION,
        "buildDate": BUILD_DATE,
        "buildSha": os.environ.get("BUILD_SHA", "unknown"),
        "toolCount": tool_count,
        "canonicalToolCount": canonical_tool_count,
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
    tool_count, canonical_tool_count, deprecated_alias_count, canonical_names = _tool_counts()
    manifest_hash = hashlib.sha256(json.dumps(canonical_names).encode("utf-8")).hexdigest()[:16]
    manifest_version = os.environ.get("MANIFEST_VERSION", None)
    deployed_at = os.environ.get("DEPLOYED_AT", None)
    worker_schema_generated_at = datetime.datetime.utcnow().isoformat() + "Z"
    return json.dumps({
        "toolCount": tool_count,
        "manifestVersion": manifest_version,
        "manifestHash": manifest_hash,
        "buildSha": os.environ.get("BUILD_SHA", "unknown"),
        "buildDate": BUILD_DATE,
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
