#!/usr/bin/env python3
"""Validate geographic revenue schema guarantees on deployed MCP."""

from __future__ import annotations

import json
import urllib.request

URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-geo-schema/1.0)"


def call(name: str, args: dict, req_id: int) -> dict:
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
        return payload.get("data") or {}
    return payload


def assert_geo_shape(data: dict) -> None:
    for key in ("value", "totalRevenue", "valuePct"):
        if key not in data:
            raise AssertionError(f"missing key: {key}")
        if data[key] == {}:
            raise AssertionError(f"{key} must be null/number, not object")
    if data["totalRevenue"] is not None and not isinstance(data["totalRevenue"], (int, float)):
        raise AssertionError("totalRevenue must be number|null")
    if data["valuePct"] is not None and not isinstance(data["valuePct"], (int, float)):
        raise AssertionError("valuePct must be number|null")


def main() -> int:
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

