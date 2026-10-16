"""Parity cases: A1 (2.9.0) health_check.credentialTransport.

The Worker reports how the calling /mcp request presented its credential (header, path or none). The local stdio
server has no HTTP request, so it reports null; the Worker does the same outside a request context. Both runtimes
publish the same output-schema entry and the same sentence in the tool description.

Same shape as the authorizationHeaderPresent handling (see scripts/test_manifest_diagnostics.py), which this mirrors.
"""

from __future__ import annotations

import asyncio
import json

from scripts.parity.harness import Case

import server  # noqa: F401,E402
from yfmcp.schemas import _MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA  # noqa: E402

CASES: list[Case] = []

DESCRIPTION_SENTENCE = (
    "credentialTransport reports how the calling request presented its credential "
    "(header, path or none), never the credential itself."
)
SCHEMA_ENTRY = {"type": ["string", "null"], "enum": ["header", "path", "none", None]}


def _py_health_check() -> dict:
    return json.loads(asyncio.run(server.health_check()))["credentialTransport"]


CASES += [
    Case(
        name="health.credential_transport.is_null_outside_an_http_request", ids=["A1"],
        py=_py_health_check,
        ts={"js": "return JSON.parse(await m.tools.callTool('health_check', {})).credentialTransport;"},
        expect=None),
    Case(
        name="health.credential_transport.schema_entry_and_description",
        ids=["A1"],
        py=lambda: _py_contract(),
        ts={"js": "const t = m.tools.TOOLS.find((x) => x.name === 'health_check');"
                  "return { schema: t.outputSchema.properties.credentialTransport,"
                  " hasSentence: t.description.endsWith(args[0]),"
                  " additionalProperties: t.outputSchema.additionalProperties };",
            "args": [DESCRIPTION_SENTENCE]},
        expect={"schema": SCHEMA_ENTRY, "hasSentence": True, "additionalProperties": False}),
]


def _py_contract() -> dict:
    # The registered schema, not the listing's: FastMCP wraps a listed output schema in a {"result": string} object.
    tool = next(t for t in asyncio.run(server.yfinance_server.list_tools()) if t.name == "health_check")
    return {
        "schema": _MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA["properties"]["credentialTransport"],
        "hasSentence": tool.description.endswith(DESCRIPTION_SENTENCE),
        "additionalProperties": _MANIFEST_DIAGNOSTICS_OUTPUT_SCHEMA["additionalProperties"],
    }
