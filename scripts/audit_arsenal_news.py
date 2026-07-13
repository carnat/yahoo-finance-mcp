#!/usr/bin/env python3
"""Produce a non-blocking, source-isolated news-quality audit for Arsenal.

The audit records response contracts and compact evidence metadata.  It does
not decide whether a provider is "better", retry failures, or change registry
state; reviewers use the artifact to prioritize follow-up work.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
from pathlib import Path
from typing import Any, Callable

try:  # Supports both ``python scripts/...`` and module-based regression tests.
    from live_smoke_utils import call_tool, extract_data, is_error_payload
except ModuleNotFoundError:
    from scripts.live_smoke_utils import call_tool, extract_data, is_error_payload


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCOPE = ROOT / "scripts" / "ir_page_scope.json"
DEFAULT_ENDPOINT = "https://yahoo-finance-mcp.artinatw.workers.dev/mcp"
USER_AGENT = "yahoo-finance-mcp-arsenal-news-audit/1.0"
SOURCES = ("yahoo_finance_news", "finnhub")


def load_tickers(path: Path) -> list[str]:
    scope = json.loads(path.read_text(encoding="utf-8"))
    tickers = scope.get("tickers") if isinstance(scope, dict) else None
    if not isinstance(tickers, list) or len(tickers) != 20:
        raise ValueError("Arsenal scope must contain exactly 20 tickers")
    normalized = [str(ticker).strip().upper() for ticker in tickers]
    if any(not ticker for ticker in normalized) or len(set(normalized)) != len(normalized):
        raise ValueError("Arsenal tickers must be non-empty and unique")
    return normalized


def _titles(items: list[dict[str, Any]]) -> set[str]:
    return {
        str(item.get("title") or "").strip().casefold()
        for item in items
        if str(item.get("title") or "").strip()
    }


def summarize_response(ticker: str, source: str, payload: Any) -> dict[str, Any]:
    """Turn a provider response into a compact, stable audit row."""
    if not isinstance(payload, dict) or is_error_payload(payload):
        return {
            "ticker": ticker,
            "source": source,
            "callState": "CALL_FAILED",
            "responseStatus": None,
            "sourceStatus": None,
            "itemCount": 0,
            "uniqueTitles": [],
            "evidenceClasses": {},
            "coverage": None,
        }
    data = extract_data(payload)
    if not isinstance(data, dict):
        return {
            "ticker": ticker,
            "source": source,
            "callState": "MALFORMED_RESPONSE",
            "responseStatus": None,
            "sourceStatus": None,
            "itemCount": 0,
            "uniqueTitles": [],
            "evidenceClasses": {},
            "coverage": None,
        }
    items = [item for item in data.get("items") or [] if isinstance(item, dict)]
    evidence_classes = collections.Counter(
        str(item.get("evidenceClass") or "UNKNOWN") for item in items
    )
    titles = sorted(_titles(items))
    source_status = ((data.get("sourceStatus") or {}).get(source) or {})
    return {
        "ticker": ticker,
        "source": source,
        "callState": "COMPLETED",
        "responseStatus": data.get("status"),
        "sourceStatus": source_status.get("status"),
        "itemCount": len(items),
        "uniqueTitles": titles,
        "evidenceClasses": dict(sorted(evidence_classes.items())),
        "coverage": data.get("coverage"),
    }


def build_ticker_result(ticker: str, responses: dict[str, Any]) -> dict[str, Any]:
    providers = {source: summarize_response(ticker, source, responses.get(source)) for source in SOURCES}
    yahoo_titles = set(providers["yahoo_finance_news"]["uniqueTitles"])
    finnhub_titles = set(providers["finnhub"]["uniqueTitles"])
    union = yahoo_titles | finnhub_titles
    return {
        "ticker": ticker,
        "providers": providers,
        "comparison": {
            "sharedTitleCount": len(yahoo_titles & finnhub_titles),
            "combinedUniqueTitleCount": len(union),
            "titleOverlapRatio": round(len(yahoo_titles & finnhub_titles) / len(union), 3) if union else None,
        },
    }


def build_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    providers: dict[str, dict[str, Any]] = {}
    for source in SOURCES:
        rows = [result["providers"][source] for result in results]
        states = collections.Counter(str(row.get("sourceStatus") or row.get("callState")) for row in rows)
        providers[source] = {
            "totalItems": sum(int(row.get("itemCount") or 0) for row in rows),
            "tickersWithItems": sum(1 for row in rows if row.get("itemCount")),
            "sourceStatuses": dict(sorted(states.items())),
        }
    return {
        "tickerCount": len(results),
        "providers": providers,
        "comparison": {
            "aggregateSharedTitleCount": sum(result["comparison"]["sharedTitleCount"] for result in results),
            "aggregateCombinedUniqueTitleCount": sum(result["comparison"]["combinedUniqueTitleCount"] for result in results),
        },
    }


def run_audit(
    tickers: list[str], endpoint: str, lookback_days: int, max_results: int,
    caller: Callable[..., dict[str, Any]] = call_tool,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    for index, ticker in enumerate(tickers):
        responses: dict[str, Any] = {}
        for source_index, source in enumerate(SOURCES):
            try:
                responses[source] = caller(
                    endpoint,
                    "get_company_news",
                    {"ticker": ticker, "sources": [source], "lookback_days": lookback_days, "max_results": max_results},
                    req_id=5100 + index * len(SOURCES) + source_index,
                    user_agent=USER_AGENT,
                    timeout=45,
                    retries=2,
                )
            except Exception:
                # Provider and transport failures are a useful audit finding, not a failed release gate.
                responses[source] = {"ok": False}
        results.append(build_ticker_result(ticker, responses))
    return {
        "schemaVersion": "2026-07-13",
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "endpoint": endpoint,
        "scope": "Arsenal 20",
        "results": results,
        "summary": build_summary(results),
        "reviewNote": "Audit-only: compare compact source evidence and coverage; do not treat provider gaps as evidence of absence.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--max-results", type=int, default=10)
    parser.add_argument("--output", type=Path, help="optional JSON artifact path")
    args = parser.parse_args()
    if args.lookback_days < 1 or args.max_results < 1:
        raise ValueError("lookback-days and max-results must be positive")
    artifact = run_audit(load_tickers(args.scope), args.endpoint, args.lookback_days, args.max_results)
    rendered = json.dumps(artifact, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
        print(f"WROTE audit artifact: {args.output}")
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
