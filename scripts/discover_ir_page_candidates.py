#!/usr/bin/env python3
"""Build a Git-reviewable queue of bounded official IR-page candidates.

This script never promotes a source. It only writes ``status: candidate``
records for websites obtained from Yahoo's public company profile endpoint.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import urllib.parse
import urllib.request
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCOPE = ROOT / "scripts" / "ir_page_scope.json"
DEFAULT_REGISTRY = ROOT / "worker" / "src" / "company-ir-page-registry.json"
MAX_BYTES = 750 * 1024
DISCOVERY_PATHS = ("/", "/investor-relations", "/investors", "/investor", "/newsroom", "/news", "/press-releases")
BLOCKED_HOSTS = {"facebook.com", "instagram.com", "linkedin.com", "twitter.com", "x.com", "youtube.com"}
USER_AGENT = "yahoo-finance-mcp-ir-discovery/1.0"


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain an object")
    return value


def validate_scope(scope: dict[str, Any]) -> list[str]:
    if scope.get("schemaVersion") != "2026-07-10":
        raise ValueError("scope schemaVersion must be 2026-07-10")
    tickers = scope.get("tickers")
    if not isinstance(tickers, list) or len(tickers) != 20:
        raise ValueError("scope must contain exactly 20 Arsenal tickers")
    normalized = [str(ticker).upper().strip() for ticker in tickers]
    if any(not ticker for ticker in normalized) or len(set(normalized)) != len(normalized):
        raise ValueError("scope tickers must be non-empty and unique")
    return normalized


def validate_registry(registry: dict[str, Any]) -> list[dict[str, Any]]:
    if registry.get("schemaVersion") != "2026-07-10":
        raise ValueError("registry schemaVersion must be 2026-07-10")
    sources = registry.get("sources")
    if not isinstance(sources, list):
        raise ValueError("registry sources must be a list")
    seen: set[str] = set()
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("registry entries must be objects")
        ticker = str(source.get("ticker") or "").upper()
        status = source.get("status")
        canonical = str(source.get("canonicalUrl") or "")
        hosts = source.get("allowedHosts")
        if not ticker or status not in {"candidate", "approved", "disabled"}:
            raise ValueError("registry entries require ticker and valid status")
        if ticker in seen:
            raise ValueError(f"duplicate registry ticker: {ticker}")
        seen.add(ticker)
        if canonical:
            parsed = urllib.parse.urlparse(canonical)
            if parsed.scheme != "https" or not parsed.hostname:
                raise ValueError(f"{ticker} canonicalUrl must be HTTPS")
        if not isinstance(hosts, list) or any(not isinstance(host, str) or not host for host in hosts):
            raise ValueError(f"{ticker} allowedHosts must be a non-empty string list")
    return sources


def fetch_text(url: str) -> tuple[str, str, str] | None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/json,application/rss+xml,application/atom+xml"})
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            content_type = response.headers.get_content_type().lower()
            final_url = response.geturl()
            raw = response.read(MAX_BYTES + 1)
    except Exception:
        return None
    if len(raw) > MAX_BYTES:
        return None
    return final_url, content_type, raw.decode("utf-8", errors="replace")


def public_company_identity(ticker: str) -> tuple[str, str] | None:
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(ticker)}?modules=price,summaryProfile,assetProfile"
    fetched = fetch_text(url)
    if not fetched:
        return None
    _final_url, _content_type, body = fetched
    try:
        price = json.loads(body)["quoteSummary"]["result"][0]
        profile = price.get("summaryProfile") or price.get("assetProfile") or {}
        company = str((price.get("price") or {}).get("longName") or (price.get("price") or {}).get("shortName") or ticker)
        website = str(profile.get("website") or "")
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    parsed = urllib.parse.urlparse(website)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    return company, website


def root_host(host: str) -> str:
    value = host.lower().removeprefix("www.")
    for prefix in ("investors.", "investor.", "ir.", "newsroom.", "news."):
        if value.startswith(prefix):
            return value[len(prefix):]
    return value


def candidate_for(ticker: str) -> dict[str, Any] | None:
    identity = public_company_identity(ticker)
    if not identity:
        return None
    issuer, website = identity
    parsed = urllib.parse.urlparse(website)
    root = root_host(parsed.hostname or "")
    hosts = [root, f"investors.{root}", f"investor.{root}", f"ir.{root}"]
    for host in hosts:
        if host in BLOCKED_HOSTS:
            continue
        for path in DISCOVERY_PATHS:
            fetched = fetch_text(f"https://{host}{path}")
            if not fetched:
                continue
            final_url, content_type, text = fetched
            final = urllib.parse.urlparse(final_url)
            if final.scheme != "https" or not final.hostname or root_host(final.hostname) != root:
                continue
            lower = text[:200_000].lower()
            is_structured = content_type in {"application/json", "application/feed+json", "application/rss+xml", "application/atom+xml"}
            is_newsroom = any(marker in lower for marker in ("press release", "newsroom", "investor relations", "application/ld+json"))
            if not (is_structured or is_newsroom):
                continue
            return {
                "ticker": ticker,
                "issuerName": issuer,
                "status": "candidate",
                "adapter": "structured" if is_structured else "html_article",
                "canonicalUrl": final_url,
                "allowedHosts": [final.hostname.lower()],
                "allowedPathPrefixes": [final.path or "/"],
                "candidateReason": "bounded company-domain discovery found a structured feed/API or newsroom page",
                "discoveredAt": dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z"),
                "reviewedBy": null,
                "reviewedAt": null,
                "revalidateAfter": null
            }
    return None


def refresh(scope: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    tickers = validate_scope(scope)
    sources = validate_registry(registry)
    existing = {str(source["ticker"]).upper(): source for source in sources}
    refreshed: list[dict[str, Any]] = []
    for ticker in tickers:
        current = existing.get(ticker)
        if current and current.get("status") in {"approved", "disabled"}:
            refreshed.append(current)
            continue
        candidate = candidate_for(ticker)
        if candidate:
            refreshed.append(candidate)
        elif current:
            refreshed.append(current)
    return {
        "schemaVersion": "2026-07-10",
        "registryVersion": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d.%H%M"),
        "sources": sorted(refreshed, key=lambda source: str(source["ticker"]))
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", type=Path, default=DEFAULT_SCOPE)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--write", action="store_true", help="write refreshed candidate records to the registry")
    parser.add_argument("--validate-only", action="store_true", help="validate scope and registry without network access")
    args = parser.parse_args()
    scope = load_json(args.scope)
    registry = load_json(args.registry)
    validate_scope(scope)
    validate_registry(registry)
    if args.validate_only:
        print(f"PASS IR page registry validation ({len(registry['sources'])} sources, 20 scope tickers)")
        return 0
    refreshed = refresh(scope, registry)
    if args.write:
        args.registry.write_text(json.dumps(refreshed, indent=2) + "\n", encoding="utf-8")
        print(f"WROTE IR page registry ({len(refreshed['sources'])} sources)")
    else:
        print(json.dumps(refreshed, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
