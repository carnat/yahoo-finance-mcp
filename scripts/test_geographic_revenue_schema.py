#!/usr/bin/env python3
"""Validate geographic revenue schema guarantees.

Mode selection:
1. If GEO_SCHEMA_TARGET is set ("remote" or "local"), obey it.
2. Else if MCP_URL is set, default to remote mode.
3. Else default to local mode.

Remote mode: calls deployed Worker JSON-RPC tools/call; does not import
server.py and does not require the Python mcp package.

Local mode: imports server.py; stubs FastMCP when mcp package is absent.
"""

from __future__ import annotations

import json
import os
import sys
import asyncio
import types
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_GEO_SCHEMA_TARGET = os.environ.get("GEO_SCHEMA_TARGET", "").strip().lower()
_MCP_URL = os.environ.get("MCP_URL", "").strip()

if _GEO_SCHEMA_TARGET == "remote":
    _MODE = "remote"
elif _GEO_SCHEMA_TARGET == "local":
    _MODE = "local"
elif _MCP_URL:
    _MODE = "remote"
else:
    _MODE = "local"

URL = _MCP_URL if _MODE == "remote" else ""
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-geo-schema/1.0)"


def _ensure_mcp_available() -> None:
    """Install a minimal FastMCP stub into sys.modules when the real mcp package is absent."""
    try:
        from mcp.server.fastmcp import FastMCP  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]

            def _decorator(fn):  # type: ignore[return]
                return fn

            return _decorator

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


def _patch_fastmcp_tool() -> None:
    from mcp.server.fastmcp import FastMCP as _FastMCP  # noqa: E402

    if getattr(_FastMCP, "_output_schema_patched", False):
        return
    orig_tool = _FastMCP.tool

    def _patched_tool(self, name=None, output_schema=None, **kwargs):  # type: ignore[override]
        return orig_tool(self, name=name, **kwargs)

    _FastMCP.tool = _patched_tool  # type: ignore[method-assign]
    _FastMCP._output_schema_patched = True  # type: ignore[attr-defined]


def _call_local(name: str, args: dict) -> dict:
    _ensure_mcp_available()
    _patch_fastmcp_tool()
    import importlib  # noqa: E402
    import sys  # noqa: E402
    import server as srv  # noqa: E402, F401 (may be first import)

    for _mod in ("yfmcp.app", "yfmcp.tools.system"):
        if _mod in sys.modules:
            importlib.reload(sys.modules[_mod])
    importlib.reload(srv)
    if name == "extract_sec_filing_fact":
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
    elif name == "extract_geographic_revenue":
        raw = asyncio.run(
            srv.extract_geographic_revenue(
                ticker=str(args.get("ticker", "")),
                region=str(args.get("region", "")),
                filing_type=str(args.get("filing_type", "10-K")),
                period=str(args.get("period", "latest")),
            )
        )
    else:
        raise AssertionError(f"Unsupported local call: {name}")
    return json.loads(raw)


def call(name: str, args: dict, req_id: int) -> dict:
    if _MODE == "local":
        return _call_local(name, args)

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
    print(f"Geo schema mode: {_MODE}  target: {URL or 'local server.py module path'}")
    # Phase 1/2 tool: extract_sec_filing_fact
    aaoi = call("extract_sec_filing_fact", {"ticker": "AAOI", "fact": "geographic_revenue", "region": "China"}, 0)
    aaoi_data = data_of(aaoi)
    print(f"AAOI (extract_sec_filing_fact) payload: {json.dumps(aaoi_data, sort_keys=True)}")
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

    # Phase 3 tool: extract_geographic_revenue (remote mode only — local mode is tested in test_phase3_extractors.py)
    if _MODE == "remote":
        aaoi_geo = call("extract_geographic_revenue", {"ticker": "AAOI", "region": "China", "filing_type": "10-K", "period": "latest"}, 10)
        aaoi_geo_data = data_of(aaoi_geo)
        print(f"AAOI (extract_geographic_revenue) payload: {json.dumps(aaoi_geo_data, sort_keys=True)}")
        assert_geo_shape(aaoi_geo_data)
        for field in ("factType", "evidence", "warnings"):
            if field not in aaoi_geo_data:
                raise AssertionError(f"extract_geographic_revenue missing field: {field}")
        print("  PASS extract_geographic_revenue AAOI schema")

        axti_geo = call("extract_geographic_revenue", {"ticker": "AXTI", "region": "China", "filing_type": "10-K", "period": "latest"}, 11)
        axti_geo_data = data_of(axti_geo)
        assert_geo_shape(axti_geo_data)
        print("  PASS extract_geographic_revenue AXTI schema")

        missing_region = call("extract_geographic_revenue", {"ticker": "AAPL", "region": "Atlantis", "filing_type": "10-K", "period": "latest"}, 12)
        missing_region_data = data_of(missing_region)
        assert_geo_shape(missing_region_data)
        if missing_region_data.get("value") is not None:
            raise AssertionError(f"Atlantis region should have null value, got: {missing_region_data.get('value')!r}")
        print("  PASS extract_geographic_revenue missing-region schema")

    print("PASS geographic revenue schema")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
