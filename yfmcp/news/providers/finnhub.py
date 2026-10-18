"""Finnhub company-news provider: the API constant and _collect_finnhub_events.

Adapter contract (unchanged by the move): each ``_collect_*_events`` keeps its current signature and
return value. The return value is the items plus the per-source status/diagnostics; warnings are
appended to the caller's ``warnings`` list. There is no provider registry yet (2.15.1 is a pure
restructure; the registry and the off switch are part 2).
"""

import asyncio
import datetime
import json
import os
import urllib.parse as _urlparse
import urllib.request as _urlrequest
import urllib.error as _urlerror

from yfmcp import url_policy as _url_policy
from yfmcp.util import _to_iso_utc
from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT
from yfmcp.news.shared import (
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _short_text,
    _within_date_window,
)


_FINNHUB_NEWS_API = "https://finnhub.io/api/v1/company-news"


async def _collect_finnhub_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> tuple[list[dict], list[dict], bool, dict]:
    warnings: list[dict] = []
    items: list[dict] = []
    diagnostics = {
        "rawCount": 0,
        "retrievedCount": 0,
        "acceptedCount": 0,
        "collectedCount": 0,
        "rejectedCount": 0,
        "rejectionCounts": {},
        "attempted": True,
        "completed": False,
    }

    def _reject(reason: str) -> None:
        diagnostics["rejectedCount"] += 1
        diagnostics["rejectionCounts"][reason] = diagnostics["rejectionCounts"].get(reason, 0) + 1

    api_key = os.environ.get("FINNHUB_API_KEY") or os.environ.get("FINNHUB_TOKEN")
    if not api_key:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub company-news source is not configured; skipped.",
            "severity": "warning",
        })
        diagnostics["attempted"] = False
        return items, warnings, False, diagnostics

    from_day = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=lookback_days or 14)
    ).strftime("%Y-%m-%d")
    to_day = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    query = _urlparse.urlencode({
        "symbol": ticker.upper(),
        "from": from_day,
        "to": to_day,
    })
    url = f"{_FINNHUB_NEWS_API}?{query}"
    loop = asyncio.get_event_loop()

    def _fetch() -> list[dict]:
        req = _urlrequest.Request(
            url,
            headers={"User-Agent": _PROVIDER_USER_AGENT, "X-Finnhub-Token": api_key},
        )
        with _url_policy.urlopen(req, timeout=20) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    try:
        raw_items = await loop.run_in_executor(None, _url_policy.in_context(_fetch))
    except _urlerror.HTTPError as exc:
        if exc.code in (401, 403):
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": f"Finnhub auth error: HTTP {exc.code}",
                "severity": "warning",
            })
        elif exc.code == 429:
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": "Finnhub rate limited: HTTP 429",
                "severity": "warning",
            })
        else:
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": f"Finnhub source unavailable: {exc}",
                "severity": "warning",
            })
        return items, warnings, False, diagnostics
    except Exception as exc:
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": f"Finnhub source unavailable: {exc}",
            "severity": "warning",
        })
        return items, warnings, False, diagnostics

    if not isinstance(raw_items, list):
        warnings.append({
            "code": "SOURCE_UNAVAILABLE",
            "message": "Finnhub provider changed: unexpected response format",
            "severity": "warning",
        })
        return items, warnings, False, diagnostics

    diagnostics["rawCount"] = len(raw_items)
    diagnostics["retrievedCount"] = len(raw_items)
    ticker_u = ticker.upper()
    for row in raw_items:
        if not isinstance(row, dict):
            _reject("INVALID_ROW")
            continue
        title = str(row.get("headline") or "").strip()
        if not title:
            _reject("MISSING_HEADLINE")
            continue
        summary = str(row.get("summary") or "").strip()
        original_source = str(row.get("source") or "").strip() or None
        item_url = str(row.get("url") or "").strip() or None
        published_at = _to_iso_utc(row.get("datetime"))
        duplicate_group_id = _make_duplicate_group_id(ticker_u, title, published_at, None, item_url)
        if duplicate_group_id is None:
            warnings.append({
                "code": "DEDUPE_WEAK_KEY",
                "message": "Weak dedupe key for at least one item.",
                "severity": "warning",
            })
        blob = f"{title} {summary}".upper()
        item = {
            "title": title,
            "source": "finnhub",
            "originalSource": original_source,
            "sourceType": "company_news",
            "publishedAt": published_at,
            "retrievedAt": retrieved_at,
            "url": item_url,
            "issuer": None,
            "tickers": [ticker_u],
            "eventType": _event_type_from_keywords(f"{title} {summary}"),
            "summary": _short_text(summary or title, 240),
            "evidenceText": _short_text(summary or title, 180),
            "confidence": "MEDIUM" if item_url else "LOW",
            "tickerRelevance": "HIGH" if ticker_u in blob else "LOW",
            "duplicateGroupId": duplicate_group_id,
        }
        if not _within_date_window(
            item.get("publishedAt"),
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
        ):
            _reject("DATE_OUTSIDE_WINDOW")
            continue
        diagnostics["acceptedCount"] += 1
        if len(items) < max_results:
            items.append(item)
    diagnostics["collectedCount"] = len(items)
    diagnostics["completed"] = True
    return items, warnings, True, diagnostics
