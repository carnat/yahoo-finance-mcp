#!/usr/bin/env python3
"""Hard live smoke for deployed Worker -> EdgarTools sidecar structured facts.

This script intentionally has no ALLOW_NETWORK_SKIP path. It is the deploy gate
that proves the deployed Worker can reach the deployed sidecar.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request


MCP_URL = os.environ.get("MCP_URL", "https://yahoo-finance-mcp.artinatw.workers.dev/mcp").strip()
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-sec-facts-sidecar-smoke/1.0)"


def rpc(method: str, params: dict | None = None, req_id: int = 1) -> dict:
    payload: dict = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=180) as resp:
        return json.loads(resp.read())


def call_tool(name: str, arguments: dict, req_id: int) -> dict:
    resp = rpc("tools/call", {"name": name, "arguments": arguments}, req_id=req_id)
    if resp.get("error"):
        raise AssertionError(f"{name} JSON-RPC error: {resp['error']}")
    text = ((((resp.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"{name} returned non-JSON text: {text!r}") from exc


def data(payload: dict) -> dict:
    if payload.get("ok") is True and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def main() -> int:
    if not MCP_URL:
        raise AssertionError("MCP_URL is required for the hard SEC facts sidecar smoke gate")

    health = data(call_tool("health_check", {}, 1))
    if health.get("structuredFactProvider") != "edgartools_sidecar":
        raise AssertionError(f"structuredFactProvider not active: {health}")
    if health.get("structuredFactProviderHealth") != "OK":
        raise AssertionError(f"structuredFactProviderHealth not OK: {health}")

    geo = data(call_tool("extract_geographic_revenue", {
        "ticker": "AAPL",
        "region": "Greater China",
        "filing_type": "10-K",
        "period": "latest",
    }, 2))
    if geo.get("status") != "FOUND":
        raise AssertionError(f"AAPL Greater China sidecar smoke did not find revenue: {geo}")
    for field in ("accessionNumber", "filingDate", "documentUrl"):
        if not geo.get(field):
            raise AssertionError(f"AAPL Greater China missing {field}: {geo}")
    value = geo.get("value")
    value_pct = geo.get("valuePct")
    if not isinstance(value, (int, float)) or value <= 0:
        raise AssertionError(f"AAPL Greater China value must be positive numeric: {geo}")
    if not isinstance(value_pct, (int, float)) or not (1 <= value_pct <= 80):
        raise AssertionError(f"AAPL Greater China valuePct out of sane range: {geo}")
    print("PASS deployed Worker -> EdgarTools sidecar SEC facts smoke")
    return 0


if __name__ == "__main__":
    sys.exit(main())
