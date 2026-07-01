#!/usr/bin/env python3
"""Post-deploy live discovery/callability checks for the deployed MCP endpoint."""

import argparse
import json
import sys
import urllib.request
from typing import Any

from live_smoke_utils import resolve_option_expiration

DEFAULT_URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
_UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-live-discovery/1.0)"
_BAD_CALL_ERRORS = ("unknown tool", "method not found", "unregistered dispatch")
_DYNAMIC_OPTION_EXPIRATION = "_DYNAMIC_OPTION_EXPIRATION_"


def _rpc(url: str, method: str, params: dict[str, Any] | None = None, req_id: int = 1) -> dict[str, Any]:
    payload = {"jsonrpc": "2.0", "id": req_id, "method": method}
    if params is not None:
        payload["params"] = params
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": _UA},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def _default_for(name: str, schema: dict[str, Any], runtime_defaults: dict[str, Any]) -> Any:
    if name in runtime_defaults:
        return runtime_defaults[name]
    if "default" in schema:
        return schema["default"]
    if "enum" in schema and schema["enum"]:
        return schema["enum"][0]
    if "oneOf" in schema:
        for opt in schema["oneOf"]:
            if opt.get("type") == "string":
                return "AAPL"
            if opt.get("type") == "array":
                return ["AAPL"]
    t = schema.get("type")
    if isinstance(t, list):
        t = next((x for x in t if x != "null"), "string")
    if t == "boolean":
        return False
    if t == "number" or t == "integer":
        return 1
    if t == "array":
        return []

    lookup = {
        "ticker": "AAPL",
        "query": "Apple",
        "period": "1mo",
        "interval": "1d",
        "financial_type": "income_stmt",
        "holder_type": "major_holders",
        "expiration_date": _DYNAMIC_OPTION_EXPIRATION,
        "option_type": "calls",
        "recommendation_type": "recommendations",
        "screener_name": "day_gainers",
        "fact_type": "geographic_revenue",
        "fact_name": "total_revenue",
        "region": "China",
        "window_label": "T-7",
        "section_name": "Item 1",
        "document_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/aapl-20230930x10k.htm",
        "accession_number": "0000320193-23-000106",
        "io_pt": 250,
    }
    return lookup.get(name, "sample")


def _args_for(tool: dict[str, Any], runtime_defaults: dict[str, Any]) -> dict[str, Any]:
    schema = tool.get("inputSchema", {}) or {}
    props = schema.get("properties", {}) or {}
    required = schema.get("required", []) or []
    args: dict[str, Any] = {}
    for name in required:
        args[name] = _default_for(name, props.get(name, {}), runtime_defaults)
    return args


def _assert_any(tool_names: set[str], group: tuple[str, ...], errors: list[str]) -> None:
    if not any(name in tool_names for name in group):
        errors.append(f"missing expected discovery tool(s): one of {group!r}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify deployed MCP discovery and callability")
    parser.add_argument("--url", default=DEFAULT_URL)
    args = parser.parse_args()

    errors: list[str] = []
    runtime_defaults: dict[str, Any] = {}
    listed = _rpc(args.url, "tools/list")
    if "error" in listed:
        print(f"FAIL tools/list error: {listed['error']}", file=sys.stderr)
        return 1
    tools = (((listed.get("result") or {}).get("tools")) or [])
    tool_names = {str(t.get("name")) for t in tools if isinstance(t, dict)}
    if not tool_names:
        print("FAIL discovery returned 0 tools", file=sys.stderr)
        return 1

    _assert_any(tool_names, ("list_sec_filings", "list_sec_company_filings"), errors)
    _assert_any(tool_names, ("get_filing_outline", "get_sec_filing_outline"), errors)
    _assert_any(tool_names, ("get_filing_section", "get_sec_filing_section"), errors)
    _assert_any(tool_names, ("list_filing_tables", "list_sec_filing_tables"), errors)
    _assert_any(tool_names, ("get_filing_table", "get_sec_filing_table"), errors)
    _assert_any(tool_names, ("extract_filing_fact", "extract_sec_filing_fact"), errors)
    _assert_any(tool_names, ("get_options_summary", "summarize_options_flow"), errors)

    if "health_check" not in tool_names:
        errors.append("missing health_check in discovery")

    if "get_option_chain" in tool_names:
        expiry = resolve_option_expiration(args.url, "AAPL", user_agent=_UA, req_id=9000)
        if expiry:
            runtime_defaults["expiration_date"] = expiry
            print(f"Resolved AAPL option expiration for live discovery: {expiry}")
        else:
            print("WARN could not resolve AAPL option expiration; get_option_chain callability will be skipped")

    option_tool = next((t for t in tools if isinstance(t, dict) and t.get("name") == "get_option_chain"), None)
    if option_tool is None:
        errors.append("missing get_option_chain in discovery")
    else:
        props = (((option_tool.get("inputSchema") or {}).get("properties")) or {})
        for field in ("max_contracts", "min_open_interest", "min_volume"):
            if field not in props:
                errors.append(f"get_option_chain schema missing field: {field}")

    for idx, tool in enumerate(tools, start=1):
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name", ""))
        if not name:
            continue
        tool_args = _args_for(tool, runtime_defaults)
        if tool_args.get("expiration_date") == _DYNAMIC_OPTION_EXPIRATION:
            continue
        call = _rpc(args.url, "tools/call", {"name": name, "arguments": tool_args}, req_id=idx + 1000)
        if "error" in call:
            msg = json.dumps(call["error"]).lower()
            if any(bad in msg for bad in _BAD_CALL_ERRORS):
                errors.append(f"{name}: discovery-listed tool is not callable ({call['error']})")
            continue
        text = ""
        try:
            text = str((((call.get("result") or {}).get("content") or [{}])[0] or {}).get("text", ""))
        except Exception:
            text = ""
        low = text.lower()
        if any(bad in low for bad in _BAD_CALL_ERRORS):
            errors.append(f"{name}: discovery-listed tool returned non-callable error")

    if errors:
        print("FAIL live discovery checks:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(f"PASS live discovery checks ({len(tool_names)} tools listed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
