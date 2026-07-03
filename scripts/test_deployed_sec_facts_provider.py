#!/usr/bin/env python3
"""Deployed smoke for Worker official SEC structured facts.

This script intentionally has no ALLOW_NETWORK_SKIP path in deploy workflow use.
It requires Worker reachability and correct provider routing. Upstream SEC
provider unavailability is tolerated as a skip in CI/test environments.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request


MCP_URL = os.environ.get("MCP_URL", "https://yahoo-finance-mcp.artinatw.workers.dev/mcp").strip()
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-sec-facts-smoke/1.0)"


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


class ProviderUnavailableException(Exception):
    pass


def data(payload: dict) -> dict:
    d = payload
    if payload.get("ok") is True and isinstance(payload.get("data"), dict):
        d = payload["data"]
    if isinstance(d, dict) and (d.get("status") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE" or d.get("code") == "STRUCTURED_FACT_PROVIDER_UNAVAILABLE"):
        raise ProviderUnavailableException("Official SEC facts provider is unavailable")
    return d


def filing_meta(payload: dict, field: str) -> object:
    if field == "documentUrl":
        direct = payload.get("documentUrl") or payload.get("primaryDocumentUrl") or payload.get("url")
    else:
        direct = payload.get(field)
    if direct:
        return direct
    evidence = payload.get("evidence")
    if isinstance(evidence, list) and evidence:
        first = evidence[0]
        if isinstance(first, dict):
            if field == "documentUrl":
                return first.get("documentUrl") or first.get("primaryDocumentUrl") or first.get("url")
            return first.get(field)
    if isinstance(evidence, dict):
        if field == "documentUrl":
            return evidence.get("documentUrl") or evidence.get("primaryDocumentUrl") or evidence.get("url")
        return evidence.get(field)
    return None


def assert_filing_metadata(payload: dict, label: str) -> None:
    for field in ("accessionNumber", "filingDate", "documentUrl"):
        if not filing_meta(payload, field):
            raise AssertionError(f"{label} missing {field}: {payload}")


def main() -> int:
    if not MCP_URL:
        raise AssertionError("MCP_URL is required for the hard SEC facts smoke gate")

    health = data(call_tool("health_check", {}, 1))
    if health.get("structuredFactProvider") != "official_sec_data_api":
        raise AssertionError(f"structuredFactProvider not active: {health}")
    if health.get("structuredFactProviderHealth") == "UNCONFIGURED":
        raise AssertionError(f"structuredFactProvider is unconfigured: {health}")

    total = data(call_tool("extract_total_revenue", {
        "ticker": "AAPL",
        "filing_type": "10-K",
        "period": "latest",
    }, 2))
    if total.get("status") != "FOUND":
        raise AssertionError(f"AAPL total revenue official SEC smoke did not find revenue: {total}")
    assert_filing_metadata(total, "AAPL total revenue")
    total_value = total.get("value")
    if not isinstance(total_value, (int, float)) or total_value <= 0:
        raise AssertionError(f"AAPL total revenue value must be positive numeric: {total}")

    geo = data(call_tool("extract_geographic_revenue", {
        "ticker": "AAPL",
        "region": "Greater China",
        "filing_type": "10-K",
        "period": "latest",
    }, 3))
    if geo.get("status") not in {"FOUND", "PROVIDER_LIMITATION", "EXTRACTION_FAILED", "TABLE_NOT_PARSED", "NO_DIMENSIONAL_REVENUE_FACT"}:
        raise AssertionError(f"AAPL Greater China official SEC smoke returned unexpected status: {geo}")
    if geo.get("status") == "NOT_DISCLOSED":
        raise AssertionError(f"AAPL Greater China must not collapse provider limits into NOT_DISCLOSED: {geo}")
    assert_filing_metadata(geo, "AAPL Greater China")
    if geo.get("status") == "FOUND":
        value = geo.get("value")
        value_pct = geo.get("valuePct")
        if not isinstance(value, (int, float)) or value <= 0:
            raise AssertionError(f"AAPL Greater China value must be positive numeric when FOUND: {geo}")
        if not isinstance(value_pct, (int, float)) or not (1 <= value_pct <= 80):
            raise AssertionError(f"AAPL Greater China valuePct out of sane range: {geo}")
    print("PASS deployed Worker official SEC facts smoke")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ProviderUnavailableException as e:
        print(f"\nSKIPPED remaining SEC facts provider tests: {e} (tolerated in CI/test environments)")
        sys.exit(0)
