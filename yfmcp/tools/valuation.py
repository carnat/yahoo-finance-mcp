"""Valuation MCP tools.

Phase 5 unit 7: extracted from server.py as a pure move (no behaviour change).
Covers ``get_valuation_snapshot``, ``get_historical_valuation_context`` and ``reconcile_metric_sources``
with their helpers (daily history, periodic reports, cover-page share counts, Yahoo statement rows) and the
module constants only they use. The arithmetic stays in ``yfmcp.valuation``, ``yfmcp.valuation_history`` and
``yfmcp.metric_reconciliation``.

Shared dependencies are imported from ``yfmcp.*``; this module never imports ``server`` (no import cycle).
``yfmcp.tools.evidence_pack`` does not import this module, and this module never imports it. ``server.py``
re-exports the names that remaining server code, grouped routing, scripts and tests still look up as
``server.<name>``.
"""

import asyncio
import datetime
import json
import math
from typing import Annotated

import pandas as pd
import yfinance as yf
from pydantic import Field

from yfmcp import companyfacts_coverage as _cfc
from yfmcp import metric_reconciliation as _mr
from yfmcp import valuation as _vl
from yfmcp import valuation_history as _vh
from yfmcp.app import yfinance_server
from yfmcp.clients.edgar import (
    _edgar_get_company_facts,
    _edgar_get_html,
    _get_submissions_for_ticker,
    _load_edgar_tickers,
    _resolve_cik_for_ticker,
)
from yfmcp.clients.endpoints import sec_archive_document_url
from yfmcp.clients.yahoo import _valuation_market_inputs
from yfmcp.evidence import AUTHORITY_BOUNDARY as _AUTHORITY_BOUNDARY
from yfmcp.tools.exposure import (
    _earnings_release_picks,
    _read_release_text,
    extract_capital_structure,
    extract_dilution_bridge,
)
from yfmcp.tools.fundamentals import _peer_valuation_error
from yfmcp.util import _unwrap_payload


# ── Historical valuation context and metric reconciliation (2.5.4) ────────

def _daily_history_sync(ticker: str, from_date: str) -> dict | None:
    """Daily closes (Yahoo split-adjusted), split events and the quote currency from from_date to now."""
    company = yf.Ticker(ticker)
    df = company.history(start=from_date, interval="1d", auto_adjust=False, actions=True)
    if df is None or getattr(df, "empty", True):
        return None
    bars: list[dict] = []
    splits: list[dict] = []
    for idx, row in df.iterrows():
        date = idx.strftime("%Y-%m-%d")
        close = row.get("Close")
        if close is not None and math.isfinite(float(close)):
            bars.append({"date": date, "close": float(close)})
        ratio = row.get("Stock Splits")
        if ratio is not None and math.isfinite(float(ratio)) and float(ratio) > 0:
            splits.append({"date": date, "ratio": float(ratio)})
    meta = getattr(company, "history_metadata", None) or {}
    return {"bars": bars, "splits": splits, "currency": meta.get("currency")}


async def _daily_history(ticker: str, from_date: str) -> dict | None:
    try:
        return await asyncio.to_thread(_daily_history_sync, ticker, from_date)
    except Exception:  # noqa: BLE001 - reported as unread
        return None


# Cover pages sit at the top of a periodic report with the header contexts; the first 900 KB is read, not
# the whole report (ASTS's 10-Q is 3.5 MB, its class counts end near 194 KB).
_COVER_READ_MAX_BYTES = 900_000


_PERIODIC_REPORT_FORMS = {"10-K", "10-Q", "20-F", "40-F"}


def _periodic_reports(subs: dict | None, cik_padded: str | None) -> list[dict]:
    """The filer's periodic reports in SEC's recent submissions, with their primary documents."""
    if not subs or not cik_padded:
        return []
    recent = (subs.get("filings") or {}).get("recent") or {}

    def at(key: str, i: int) -> str:
        values = recent.get(key) or []
        return str(values[i] or "") if i < len(values) and values[i] is not None else ""

    out = []
    for i, form in enumerate(recent.get("form") or []):
        form_upper = str(form or "").upper()
        acc, doc = at("accessionNumber", i), at("primaryDocument", i)
        if form_upper not in _PERIODIC_REPORT_FORMS or not acc or not doc:
            continue
        out.append({"accessionNumber": acc, "form": form_upper, "filed": at("filingDate", i), "reportDate": at("reportDate", i) or None,
                    "documentUrl": sec_archive_document_url(int(cik_padded), acc.replace('-', ''), doc)})
    return out


async def _cover_counts_for(companyfacts: dict, reports: list[dict], dates: list[str]) -> dict:
    """Per-class cover-page share counts from the periodic reports companyfacts cannot give a point-in-time count for."""
    out: dict = {}
    for n in _vh.cover_reads_needed(companyfacts, reports, dates):
        url = n["documentUrl"]
        try:
            html = await _edgar_get_html(url, max_bytes=_COVER_READ_MAX_BYTES)
        except Exception:  # noqa: BLE001 - reported as unread
            html = None
        out[n["accessionNumber"]] = {**_vh.cover_share_counts(html), "documentUrl": url} if html else {"status": "NOT_READ", "value": None, "documentUrl": url}
    return out


async def _valuation_history_for(ticker: str, dates: list[str] | None, from_date: str) -> dict:
    upper = ticker.upper()
    cik_padded = await _resolve_cik_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        return {"ticker": upper, "status": code, "code": code, "retryable": not ticker_index, **_AUTHORITY_BOUNDARY}
    facts, history = await asyncio.gather(_edgar_get_company_facts(cik_padded), _daily_history(ticker, from_date), return_exceptions=True)
    facts = None if isinstance(facts, BaseException) else facts
    history = None if isinstance(history, BaseException) else history
    if not facts:
        return {"ticker": upper, "status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True, **_AUTHORITY_BOUNDARY}
    if not history or not history["bars"]:
        return {"ticker": upper, "status": "PRICE_HISTORY_NOT_AVAILABLE", "code": "PRICE_HISTORY_NOT_AVAILABLE", "retryable": True, **_AUTHORITY_BOUNDARY}
    use_dates = dates if dates is not None else _vh.valuation_dates(None, history["bars"][-1]["date"])[0]
    _, currency = _vh.taxonomy_of(facts)
    price_currency = _vl._major_price(1, history["currency"])[1] if history["currency"] else None
    warnings: list[dict] = []
    # One FX series per reporting currency the dates use (2.5.32, F-029: NBIS reported in RUB, then USD).
    fx_by_currency: dict[str, dict] = {}
    if price_currency:
        for c in _vh.reporting_currencies(facts, use_dates):
            if c == price_currency:
                continue
            pair = f"{c}{price_currency}=X"
            fx_history = await _daily_history(pair, from_date)
            fx_by_currency[c] = {"pair": pair, "bars": (fx_history or {}).get("bars") or []}
    fx = fx_by_currency.get(currency) if currency else None
    ratio = None
    if _vh.foreign_filer(facts):
        try:
            market = await _valuation_market_inputs(ticker)
        except Exception:  # noqa: BLE001 - no ratio without Yahoo's count
            market = None
        sec = _vh.latest_share_count(facts)
        quoted = (market or {}).get("sharesOutstanding")
        r = _vl.ads_ratio(sec, quoted) if sec is not None and quoted is not None else None
        if r is not None and r != 1:
            ratio = r
            warnings.append({"code": "ADR_RATIO_APPLIED", "message": (
                f"SEC counts {sec} ordinary shares; Yahoo counts {quoted} quoted shares, {r} ordinary shares each. Share counts are divided by "
                f"{r} at every date."), "severity": "info"})
    try:
        _, subs = await _get_submissions_for_ticker(ticker)
    except Exception:  # noqa: BLE001 - reported below
        subs = None
    reports = _periodic_reports(subs, cik_padded)
    cover_counts = await _cover_counts_for(facts, reports, use_dates)
    periodic_filings = [{k: r[k] for k in ("accessionNumber", "form", "filed", "reportDate")} for r in reports]
    out = _vh.historical_valuation({"ticker": ticker, "dates": use_dates, "companyfacts": facts, "bars": history["bars"],
                                    "priceCurrency": history["currency"], "splits": history["splits"], "fx": fx, "fxByCurrency": fx_by_currency, "adsRatio": ratio,
                                    "coverCounts": cover_counts, "periodicFilings": periodic_filings})
    if not subs:
        warnings.append({"code": "SUBMISSIONS_NOT_AVAILABLE", "message": (
            "SEC submissions could not be read, so no cover page was read; filers without an undimensioned share count have no market cap. Retry."),
            "severity": "warning"})
    out["warnings"] = warnings
    return out


@yfinance_server.tool(
    name="get_historical_valuation_context",
    description="Market cap, enterprise value and EV/Revenue, EV/EBITDA, P/E and P/S as they stood at each date, for a ticker and caller-named peers on the same dates. Point in time: SEC companyfacts filed on or before each date; the close on or before it with later split adjustments undone; balances at the latest balance date then filed; LTM (LFY + YTD - prior YTD) and LFY denominators with their periods, concepts and filings; reporting currency converted at that date's FX close; ADR share counts divided by ordinary shares per ADS. Shares are point in time: the cover-page or balance-sheet count, else the filing's per-class cover counts summed; a weighted-average count never sets market cap (POINT_IN_TIME_SHARES_UNRESOLVED). EBITDA is computed (operating income + D&A). Untagged or unfiled figures leave their multiples null with a status; status is OK only when all eight multiples are usable (coreStatus covers market cap and EV, coverage lists the rest); peer medians are unweighted. No multiple is selected. Evidence only.",
)
async def get_historical_valuation_context(ticker: str, dates: list[str] | None = None, peers: list[str] | None = None) -> str:
    requested = None
    if dates:
        requested, error = _vh.valuation_dates([str(d).strip() for d in dates], None)
        if error:
            return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": error})
    peer_list = list(dict.fromkeys(p for p in (str(x).strip().upper() for x in (peers or [])) if p and p != ticker.upper()))
    if len(peer_list) > 5:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "At most 5 peers."})
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    earliest = requested[0] if requested else f"{int(today[:4]) - 5}{today[4:]}"
    from_date = (datetime.date.fromisoformat(earliest[:4] + ("-02-28" if earliest[4:] == "-02-29" else earliest[4:])) - datetime.timedelta(days=15)).isoformat()
    subject = await _valuation_history_for(ticker, requested, from_date)
    final_dates = requested if requested else [str(p["date"]) for p in subject.get("points") or []]
    peer_results: list[dict] = []
    # Three at a time keeps SEC and Yahoo request rates modest.
    for i in range(0, len(peer_list), 3):
        peer_results += await asyncio.gather(*(_valuation_history_for(p, final_dates or None, from_date) for p in peer_list[i:i + 3]))
    return json.dumps({
        "ticker": ticker.upper(),
        "status": str(subject.get("status") or "NOT_AVAILABLE"),
        "dates": final_dates,
        "subject": subject,
        "peers": peer_results,
        "peerMedians": _vh.peer_medians(final_dates, [p for p in peer_results if isinstance(p.get("points"), list)]) if peer_list and final_dates else [],
        "notes": [
            "Peers are the caller's; their medians are unweighted and count only peers with that multiple on that date. Each peer's denominators are its own fiscal periods, named in its points.",
            "The latest date's close can be intraday while the market is open.",
        ],
        **_AUTHORITY_BOUNDARY,
    })


_RELEASE_READ_MAX = 4


def _yahoo_statement_rows_sync(ticker: str, annual: bool, balance: bool) -> list[dict]:
    """Yahoo statement columns as rows keyed by camelCase line item: {"date": "2026-06-30", "totalRevenue": ...}."""
    company = yf.Ticker(ticker)
    if balance:
        df = company.balance_sheet if annual else company.quarterly_balance_sheet
    else:
        df = company.income_stmt if annual else company.quarterly_income_stmt
    if df is None or getattr(df, "empty", True):
        return []
    rows = []
    for col in df.columns:
        row: dict = {"date": pd.Timestamp(col).strftime("%Y-%m-%d")}
        for name, value in df[col].items():
            if value is None or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
                continue
            key = str(name).replace(" ", "")
            row[key[:1].lower() + key[1:]] = float(value)
        rows.append(row)
    return rows


@yfinance_server.tool(
    name="reconcile_metric_sources",
    description="One metric for one period as each source states it: SEC XBRL as first filed and as latest filed (a difference is a restatement), the issuer's earnings release (only sentences naming the metric, an amount and the period's scope) and Yahoo's statement row, each compared with the latest SEC value with difference, percentage and tolerance (the larger of tolerance_pct and half the release's last stated digit). Status AGREED requires latest SEC plus at least one independent matching provider; PARTIAL covers a missing SEC baseline or only one provider; otherwise CONFLICT or NOT_FOUND, with source evidence. Metrics: revenue, net_income, operating_income, eps_diluted, cash_and_equivalents. Evidence only.",
)
async def reconcile_metric_sources(ticker: str, metric: str, period: Annotated[str, Field(description=_mr.RECONCILE_PERIOD_DESCRIPTION)] = "latest", tolerance_pct: float = _mr.DEFAULT_TOLERANCE_PCT) -> str:
    upper = ticker.upper()
    if metric not in _mr.METRICS:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"metric must be one of: {', '.join(_mr.METRICS)}"})
    # A period it cannot read is an input error before any fetch, not ok:true with INVALID_PERIOD (2.26.5, F60).
    if not _mr.is_reconcile_period(period):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": _mr.RECONCILE_PERIOD_HELP})
    # Q3e (3.2.1): only a finite number is clamped to 0-10; NaN, an infinity, a numeric string or anything else is the
    # 0.5 default, as in the Worker (min/max before the finiteness check turned NaN into 0 and +inf into 10).
    if isinstance(tolerance_pct, (int, float)) and not isinstance(tolerance_pct, bool) and math.isfinite(tolerance_pct):
        tolerance = min(10.0, max(0.0, float(tolerance_pct)))
    else:
        tolerance = _mr.DEFAULT_TOLERANCE_PCT
    cik_padded, subs = await _get_submissions_for_ticker(ticker)
    if not cik_padded:
        ticker_index = await _load_edgar_tickers()
        code = "NO_SEC_REGISTRANT" if ticker_index else "SEC_LOOKUP_UNAVAILABLE"
        return json.dumps({"ticker": upper, "metric": metric, "status": code, "code": code, "retryable": not ticker_index})
    facts = await _edgar_get_company_facts(cik_padded)
    if not facts:
        return json.dumps({"ticker": upper, "metric": metric, "status": "COMPANYFACTS_NOT_AVAILABLE", "code": "COMPANYFACTS_NOT_AVAILABLE", "retryable": True})
    # The latest annual report may be missing from companyfacts (2.5.30, F-015): said on every result, found or not.
    coverage_warning = _cfc.latest_annual_coverage_warning(_cfc.annual_filings_from_submissions(subs), facts)
    resolved = _mr.resolve_period(facts, metric, period)
    if resolved["status"] != "OK":
        return json.dumps({"ticker": upper, "metric": metric, **resolved, **({"warnings": [coverage_warning]} if coverage_warning else {})})
    cik_int = int(cik_padded)
    end = str(resolved["periodEnd"])
    # Results releases (Item 2.02 8-Ks and 8-K/As) filed within 100 days after the period ended. The newest
    # _RELEASE_READ_MAX are read, so a later correction is never crowded out by earlier filings; they are kept
    # oldest first, and the latest with a scoped figure wins (an early Item 2.02 8-K, as ASTS filed on
    # 2026-07-15, is not always the results release).
    in_window = sorted((p for p in (_earnings_release_picks(subs, cik_int, 40, True) if subs else [])
                        if p["filingDate"] > end and (datetime.date.fromisoformat(p["filingDate"]) - datetime.date.fromisoformat(end)).days <= 100),
                       key=lambda p: (p["filingDate"], p["accessionNumber"]))
    candidates = in_window[-_RELEASE_READ_MAX:]
    not_read = in_window[:len(in_window) - len(candidates)]
    annual = resolved["periodType"] == "ANNUAL"
    instant = _mr.METRICS[metric]["kind"] == "instant"

    async def read_releases() -> list[dict]:
        read = []
        for c in candidates:
            try:
                r = {**await _read_release_text(cik_int, c["accessionNumber"], c["primaryUrl"]), "filingDate": c["filingDate"], "accessionNumber": c["accessionNumber"]}
            except Exception:  # noqa: BLE001 - reported as unread
                r = {"status": "NOT_READ", "url": c["primaryUrl"], "filingDate": c["filingDate"], "accessionNumber": c["accessionNumber"], "text": None}
            read.append(r)
        return read

    async def yahoo_rows() -> list[dict] | None:
        try:
            return await asyncio.to_thread(_yahoo_statement_rows_sync, ticker, annual, instant)
        except Exception:  # noqa: BLE001 - reported as unread
            return None

    releases, rows = await asyncio.gather(read_releases(), yahoo_rows())
    out = _mr.metric_reconciliation(ticker=ticker, metric=metric, period=resolved, companyfacts=facts, releases=releases, yahoo_rows=rows,
                                    tolerance_pct=tolerance)
    warnings = []
    unread = [r for r in releases if r["status"] == "NOT_READ"]
    if unread:
        warnings.append({"code": "RELEASE_TEXT_NOT_AVAILABLE", "message": f"{len(unread)} earnings release(s) could not be read from SEC; retry.", "severity": "warning"})
    if rows is None:
        warnings.append({"code": "YAHOO_NOT_AVAILABLE", "message": "Yahoo's statements could not be read; retry.", "severity": "warning"})
    out["releaseCandidates"] = {"inWindow": len(in_window), "read": len(candidates),
                                "notRead": [{"filingDate": p["filingDate"], "accessionNumber": p["accessionNumber"], "form": p["form"]} for p in not_read]}
    if not_read:
        warnings.append({"code": "OLDER_RELEASE_CANDIDATES_NOT_READ", "message": (
            f"{len(not_read)} older Item 2.02 filing(s) in the 100-day window were not read; the {len(candidates)} newest were."), "severity": "info"})
    if coverage_warning:
        warnings.append(coverage_warning)
    out["retryable"] = bool(unread) or rows is None
    out["warnings"] = warnings
    return json.dumps(out)


# ── Valuation snapshot and peer multiples (2.4.0) ─────────────────────────
# Mirrors the Worker; the arithmetic lives in yfmcp.valuation.


_SEC_UNAVAILABLE_STATUSES = {"TICKER_NOT_FOUND", "FILING_NOT_FOUND_TRY_OTHER_TYPE", "FILING_TEXT_NOT_AVAILABLE"}


@yfinance_server.tool(
    name="get_valuation_snapshot",
    description="Valuation context for one ticker at the current price or one you supply: diluted shares from the SEC dilution bridge at that price, the filing's period-end cash and debt, equity and enterprise value (in-the-money convertibles leave the debt), EV/Revenue, EV/EBITDA and P/E on trailing results and Yahoo's current and next fiscal-year consensus with analyst counts, and ATM capacity. Falls back to Yahoo figures for non-USD or non-SEC listings. Mechanical context, not a price target; nothing is back-solved.",
)
async def get_valuation_snapshot(
    ticker: str,
    price: float | None = None,
    filing_type: str = "latest",
    include_sec_filings: bool = True,
) -> str:
    if price is not None:
        try:
            price = float(price)
        except (TypeError, ValueError):
            price = math.nan
        if not math.isfinite(price) or price <= 0:
            return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "price must be a positive number when supplied."})
    try:
        market = await _valuation_market_inputs(ticker)
    except Exception as exc:
        # As compare_peer_valuations (F57): an unknown ticker is TICKER_NOT_FOUND, and no provider URL is shown (3.2.1).
        failure = _peer_valuation_error(ticker.upper(), str(exc))
        message = failure["message"] if failure["code"] == "TICKER_NOT_FOUND" else f"Yahoo quote summary failed for {ticker.upper()}: {failure['message']}"
        return json.dumps({"error": True, "code": failure["code"], "message": message})
    quote = price if price is not None else market["price"]
    sec_warnings: list[dict] = []
    bridge = capital = None
    if quote is not None and include_sec_filings is not False:
        major, major_currency = _vl._major_price(quote, market["currency"])
        if major_currency is None or major_currency == "USD":
            b_raw, c_raw = await asyncio.gather(
                extract_dilution_bridge(ticker=ticker, price=major, currency="USD", filing_type=filing_type, include_atm=True),
                extract_capital_structure(ticker=ticker, filing_type=filing_type, include_funding_statements=False),
            )
            b, c = _unwrap_payload(b_raw), _unwrap_payload(c_raw)
            bridge = b if b.get("basis") == "MECHANICAL_COMPANY_DISCLOSED" else None
            capital = c if c.get("basis") == "COMPANY_DISCLOSED" else None
            status = str(b.get("status") or b.get("code") or c.get("status") or "")
            if bridge is None or capital is None:
                sec_warnings.append({
                    "code": "SEC_FILINGS_UNAVAILABLE",
                    "message": f"SEC filings were not read ({status}); Yahoo figures are used." if status in _SEC_UNAVAILABLE_STATUSES else "SEC filings could not be read in full; Yahoo figures fill the gaps.",
                    "severity": "info",
                })
        else:
            sec_warnings.append({"code": "NON_USD_LISTING", "message": f"SEC filings are read only for USD listings; {ticker.upper()} is quoted in {market['currency']}, so Yahoo figures are used.", "severity": "info"})
    return json.dumps(_vl.valuation_snapshot(ticker.upper(), market, price, bridge, capital, sec_warnings))
