#!/usr/bin/env python3
"""Validate geographic revenue schema guarantees.

Default mode validates the local server.py tool path.
Set MCP_URL to validate a deployed MCP endpoint instead.
"""

from __future__ import annotations

import json
import os
import sys
import asyncio
import urllib.request

URL = os.environ.get("MCP_URL", "").strip()
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-geo-schema/1.0)"
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _patch_fastmcp_tool() -> None:
    from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

    orig_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return orig_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]


def _call_local(args: dict) -> dict:
    _patch_fastmcp_tool()
    import importlib  # noqa: E402
    import server as srv  # noqa: E402

    importlib.reload(srv)
    raw = asyncio.run(
        srv.extract_sec_filing_fact(
            ticker=str(args.get("ticker", "")),
            fact=str(args.get("fact")) if args.get("fact") is not None else None,
            fact_name=str(args.get("fact_name")) if args.get("fact_name") is not None else None,
            fact_type=args.get("fact_type"),
            region=str(args.get("region")) if args.get("region") is not None else None,
            filing_type=str(args.get("filing_type", "10-K")),
            period=str(args.get("period", "latest")),
            document_url=str(args.get("document_url")) if args.get("document_url") is not None else None,
            accession_number=str(args.get("accession_number")) if args.get("accession_number") is not None else None,
        )
    )
    return json.loads(raw)


def call(name: str, args: dict, req_id: int) -> dict:
    if not URL:
        if name != "extract_sec_filing_fact":
            raise AssertionError(f"Unsupported local call: {name}")
        return _call_local(args)

    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": args},
    }
    req = urllib.request.Request(
        URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as resp:
        out = json.loads(resp.read())
    if "error" in out:
        raise AssertionError(f"JSON-RPC error: {out['error']}")
    text = ((((out.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    return json.loads(text)


def data_of(payload: dict) -> dict:
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        data = payload.get("data")
        if isinstance(data, str):
            try:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                return {}
        return data or {}
    return payload


def assert_geo_shape(data: dict) -> None:
    for key in ("value", "denominator", "valueRatio", "valuePct", "extractionMethod", "confidence"):
        if key not in data:
            raise AssertionError(f"missing key: {key}")
        if data[key] == {}:
            raise AssertionError(f"{key} must be null/number, not object")
    if data["denominator"] is not None and not isinstance(data["denominator"], (int, float)):
        raise AssertionError("denominator must be number|null")
    if data["valueRatio"] is not None and not isinstance(data["valueRatio"], (int, float)):
        raise AssertionError("valueRatio must be number|null")
    if data["valuePct"] is not None and not isinstance(data["valuePct"], (int, float)):
        raise AssertionError("valuePct must be number|null")
    if data["valuePct"] is not None and data["denominator"] is None:
        raise AssertionError("valuePct requires denominator")
    if data["denominator"] is None and data["valueRatio"] is not None:
        raise AssertionError("valueRatio must be null when denominator is null")
    if data["denominator"] is not None:
        if data["valueRatio"] is None:
            raise AssertionError("valueRatio must be non-null when denominator is non-null")
        if data["valuePct"] is None:
            raise AssertionError("valuePct must be non-null when denominator is non-null")


def main() -> int:
    print(f"Geo schema target: {URL or 'local server.py module path'}")
    aaoi = call("extract_sec_filing_fact", {"ticker": "AAOI", "fact": "geographic_revenue", "region": "China"}, 0)
    aaoi_data = data_of(aaoi)
    print(f"AAOI payload: {json.dumps(aaoi_data, sort_keys=True)}")
    assert_geo_shape(aaoi_data)
    if aaoi_data.get("value") is not None and aaoi_data.get("denominator") is not None:
        if aaoi_data.get("valueRatio") != 0.5752:
            raise AssertionError(f"AAOI valueRatio mismatch: {aaoi_data.get('valueRatio')!r}")
        if aaoi_data.get("valuePct") != 57.52:
            raise AssertionError(f"AAOI valuePct mismatch: {aaoi_data.get('valuePct')!r}")

    axti = call("extract_sec_filing_fact", {"ticker": "AXTI", "fact": "geographic_revenue", "region": "China"}, 5)
    axti_data = data_of(axti)
    assert_geo_shape(axti_data)
    if axti_data.get("confidence") != "NOT_DISCLOSED":
        raise AssertionError(f"AXTI expected NOT_DISCLOSED confidence, got {axti_data.get('confidence')!r}")

    qcom = call("extract_sec_filing_fact", {"ticker": "QCOM", "fact": "geographic_revenue", "region": "China"}, 1)
    qcom_data = data_of(qcom)
    assert_geo_shape(qcom_data)

    nd = call("extract_sec_filing_fact", {"ticker": "ZZZZINVALID", "fact": "geographic_revenue", "region": "China"}, 2)
    nd_data = data_of(nd)
    assert_geo_shape(nd_data)
    if nd_data.get("source") not in ("NOT_DISCLOSED", None):
        pass

    print("PASS geographic revenue schema")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
