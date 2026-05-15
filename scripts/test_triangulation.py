#!/usr/bin/env python3
"""Triangulation tests: cross-validate public event tools against each other.

Checks:
  1. search_company_news, get_company_press_releases, get_sec_recent_events,
     get_public_event_timeline, verify_company_event all respond without error.
  2. get_public_event_timeline count >= count of sec items for the same ticker.
  3. verify_company_event returns CONFIRMED or PARTIAL (not NOT_FOUND) when a
     known recent 8-K term is queried for a high-volume ticker.
  4. Duplicate dedup: two items with the same duplicateGroupId do not appear in
     get_public_event_timeline for the same date.
"""

from __future__ import annotations

import json
import sys
import urllib.request

URL = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-triangulation/1.0)"
TICKER = "AAPL"


def rpc(name: str, args: dict, req_id: int = 1) -> dict:
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
        body = json.loads(resp.read())
    if "error" in body:
        raise RuntimeError(f"{name} JSON-RPC error: {body['error']}")
    text = ((((body.get("result") or {}).get("content") or [{}])[0]) or {}).get("text", "")
    return json.loads(text)


def unwrap(payload: dict) -> dict:
    """Unwrap MCP envelope if present."""
    if isinstance(payload, dict) and "ok" in payload and "data" in payload:
        return payload.get("data") or {}
    return payload


def main() -> int:
    failures: list[str] = []

    # 1. All 5 tools respond without error
    tools_under_test = [
        ("search_company_news", {"ticker": TICKER, "query": "earnings", "max_results": 5}),
        ("get_company_press_releases", {"ticker": TICKER, "max_results": 5}),
        ("get_sec_recent_events", {"ticker": TICKER, "filing_type": "8-K", "max_results": 5}),
        ("get_public_event_timeline", {"ticker": TICKER, "max_results": 10}),
        ("verify_company_event", {"ticker": TICKER, "event_query": "quarterly results"}),
    ]
    results: dict[str, dict] = {}
    for i, (tool, args) in enumerate(tools_under_test, start=1):
        try:
            raw = rpc(tool, args, i)
            data = unwrap(raw)
            results[tool] = data
            if data.get("error"):
                failures.append(f"{tool} returned error: {data.get('message')}")
            else:
                print(f"  PASS  {tool} responded ok")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{tool}: {exc}")

    # 2. Timeline count >= sec items for same ticker
    timeline = results.get("get_public_event_timeline", {})
    sec_events = results.get("get_sec_recent_events", {})
    timeline_count = timeline.get("count", 0)
    sec_count = sec_events.get("count", 0)
    if not failures and timeline_count < sec_count:
        failures.append(
            f"Timeline count ({timeline_count}) < SEC events count ({sec_count}) — "
            "timeline should include SEC items"
        )
    else:
        print(f"  PASS  timeline_count ({timeline_count}) >= sec_count ({sec_count})")

    # 3. verify_company_event returns CONFIRMED or PARTIAL for known query
    verify = results.get("verify_company_event", {})
    status = verify.get("verificationStatus", "")
    if not failures and status not in ("CONFIRMED", "PARTIAL"):
        failures.append(
            f"verify_company_event returned {status!r} for 'quarterly results' on {TICKER}; "
            "expected CONFIRMED or PARTIAL"
        )
    else:
        print(f"  PASS  verify_company_event status={status!r}")

    # 4. No duplicate duplicateGroupId in timeline items
    items = timeline.get("items", [])
    seen_ids: set[str] = set()
    dups: list[str] = []
    for item in items:
        gid = item.get("duplicateGroupId", "")
        if gid and gid in seen_ids:
            dups.append(gid)
        if gid:
            seen_ids.add(gid)
    if dups:
        failures.append(f"Duplicate duplicateGroupId found in timeline: {dups[:5]}")
    else:
        print(f"  PASS  no duplicate duplicateGroupId in {len(items)} timeline items")

    if failures:
        print("\nFAILURES:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        return 1

    print("\nPASS — all triangulation checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
