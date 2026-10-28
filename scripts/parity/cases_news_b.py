"""Parity cases: News domain, rules BR-NWS-191 .. BR-NWS-280 (modernization Phase 2).

The News code will be MOVED (pure refactor, byte-identical output) into yfmcp/news.py and worker/src/news.ts.
These cases pin what both runtimes do TODAY, so the move can be proven safe.

Conventions (same as the other cases_*.py files)
- `equal` cases carry `expect` taken from the code (the legacy code is the oracle, not the rule card).
- `pinned` cases record a Python/Worker divergence as it is today; `decision` starts with "FINDING:" and names the
  difference. No winner is chosen here.
- "Finding (Q7, for triage)" comments mark behaviour both runtimes share that looks wrong.

Determinism
- Both clocks are fixed at 2026-10-15T12:00:00Z. Python: `server.datetime` and `server._utc_now_iso` are patched; the
  Worker: `Date` is replaced for the duration of the case body (`new Date()` and `Date.now()` both).
- Provider key variables are removed unless a case sets them; Python process caches the case touches are restored.
- Neither runtime reaches the network. Yahoo (yfinance / curl_cffi) is not covered by the harness stubs, so the Python
  side patches `server._yahoo_search_news` (the news search feed) and `server.yf.Ticker` (company info).

Credential-shaped values below (provider keys, tokens) are clearly fake placeholders, not copied from any source.

Techniques worth knowing when reading or extending this file
- `_scenario` / `_multi_scenario` run one or several tool calls under the shared stubs; `_collector_case` calls the Yahoo collector
  directly with a literal identity (Python `_collect_yahoo_events`, Worker `collectYahooEvents`).
- Approved company IR pages: the shipped registry only holds candidates. Python reads a temporary registry file
  (`server._COMPANY_IR_PAGE_REGISTRY_PATH`); the Worker's registry is a bundled constant, so the case body briefly wraps
  `Array.prototype.find` to upgrade the IQE.L entry in place (isolated bundle copy, restored in `finally`).
- Python-only helpers with no reachable Worker counterpart (BR-NWS-235, BR-NWS-250) are pinned with a placeholder Worker side
  (`return null`), marked FINDING in `decision`.
- A redirect to an off-list host (BR-NWS-208) cannot be driven: both harness stubs answer with the requested URL.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import datetime as dt
import json
import os
import re
import tempfile
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from scripts.parity.harness import WORKER, Case
from scripts.parity.cases_news_a import _REGISTRY_BUNDLE_JS

# Registers the tools (server.py imports the yfmcp modules) the way the existing suites do.
import server  # noqa: F401,E402
from yfmcp import url_policy  # noqa: E402
from yfmcp.app import registered_tool_fn  # noqa: E402
from yfmcp import capital_structure as cs  # noqa: E402
from yfmcp import cache as pycache  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402
from yfmcp.tools import news as news_tools  # noqa: E402
from yfmcp.news import shared as news_shared  # noqa: E402
from yfmcp.news.providers import company_ir_page as ir_page_provider, finnhub as finnhub_provider, marketaux as marketaux_provider, sec as sec_provider, yahoo as yahoo_provider  # noqa: E402
from yfmcp.tools import provider_gaps  # noqa: E402

CASES: list[Case] = []

# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Shared scenario machinery
# ═══════════════════════════════════════════════════════════════════════════════════════════════

NOW_ISO = "2026-10-15T12:00:00Z"
NOW_PARTS = (2026, 9, 15, 12, 0, 0)  # Date.UTC month index
FAKE_FINNHUB_KEY = "fake-finnhub-key-0000000000"  # placeholder, same shape as a Finnhub key; never a real credential
FAKE_MARKETAUX_TOKEN = "fake-marketaux-token-000000"  # placeholder
FAKE_ALPHA_KEY = "FAKEALPHAKEY0000"  # placeholder
PROVIDER_ENV_VARS = (
    "FINNHUB_API_KEY", "FINNHUB_TOKEN", "ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY",
    "MARKETAUX_API_TOKEN", "MARKETAUX_API_KEY",
    "NEWS_SOURCES_DISABLED",  # 2.16.0 operator off switch: never inherited from the host environment
)

YAHOO_AUTH_ROUTES = {
    "https://fc.yahoo.com": {"headers": {"set-cookie": "A3=fake-cookie; Path=/"}},
    "https://query1.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
    "https://query2.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
}
YAHOO_SEARCH_PREFIX = "https://query1.finance.yahoo.com/v1/finance/search?*"
YAHOO_SUMMARY_PREFIX = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*"
FINNHUB_NEWS_PREFIX = "https://finnhub.io/api/v1/company-news?*"
MARKETAUX_PREFIX = "https://api.marketaux.com/v1/news/all?*"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def _epoch(iso: str) -> int:
    return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


def _days_ago(days: float, hour: int = 9) -> str:
    """ISO UTC timestamp `days` whole days before the fixed clock, at `hour`:00:00Z."""
    day = (dt.datetime(2026, 10, 15) - dt.timedelta(days=days)).date()
    return f"{day.isoformat()}T{hour:02d}:00:00Z"


class _FrozenDatetime(dt.datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: D102
        base = dt.datetime(2026, 10, 15, 12, 0, 0, tzinfo=dt.timezone.utc)
        return base if tz is None else base.astimezone(tz)


_DT_SHIM = types.SimpleNamespace(**{k: getattr(dt, k) for k in dir(dt) if not k.startswith("__")})
_DT_SHIM.datetime = _FrozenDatetime


class _FakeTicker:
    """yf.Ticker stand-in: `.info` is the dict, or raises when `info` is an Exception."""

    def __init__(self, info):
        self._info = info

    @property
    def info(self):
        if isinstance(self._info, Exception):
            raise self._info
        return copy.deepcopy(self._info)


def _env_scope(env: dict | None):
    """Python process env with every provider variable removed, then `env` applied (restored on exit)."""
    stack = contextlib.ExitStack()
    stack.enter_context(patch.dict(os.environ, {}, clear=False))
    for var in PROVIDER_ENV_VARS:
        os.environ.pop(var, None)
    os.environ.update(env or {})
    return stack


@contextlib.contextmanager
def _py_news_env(*, news=None, info=None, env=None, submissions=None, clock=True, gnw_cache_clear=True):
    """Everything a Python news tool needs: fixed clock, provider env, Yahoo feed/profile stubs, SEC submissions."""
    with contextlib.ExitStack() as stack:
        stack.enter_context(_env_scope(env))
        if clock:
            for clock_owner in (news_tools, news_shared, ir_page_provider, finnhub_provider, marketaux_provider):
                stack.enter_context(patch.object(clock_owner, "datetime", _DT_SHIM))
            stack.enter_context(patch.object(news_tools, "_utc_now_iso", lambda: NOW_ISO))
        stack.enter_context(patch.object(yahoo_provider, "_yahoo_search_news", lambda ticker: copy.deepcopy(news or [])))
        stack.enter_context(patch.object(server.yf, "Ticker", lambda ticker, *a, **k: _FakeTicker(info if info is not None else {})))
        if submissions is not None:
            stub_submissions = AsyncMock(return_value=submissions)
            for submissions_owner in (news_tools, sec_provider):
                stack.enter_context(patch.object(submissions_owner, "_get_submissions_for_ticker", stub_submissions))
        saved = dict(pycache._tool_cache._store)
        saved_html = dict(edgar._EDGAR_HTML_CACHE)
        # The SEC ticker index is process state: start every case unloaded (a no-CIK ticker reads it; 2.16.0).
        saved_tickers = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT)
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
        if gnw_cache_clear:
            for key in [k for k in pycache._tool_cache._store if k.startswith("gnw_rss:")]:
                del pycache._tool_cache._store[key]
        edgar._EDGAR_HTML_CACHE.clear()
        try:
            yield
        finally:
            pycache._tool_cache._store.clear()
            pycache._tool_cache._store.update(saved)
            edgar._EDGAR_HTML_CACHE.clear()
            edgar._EDGAR_HTML_CACHE.update(saved_html)
            edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = saved_tickers


def _worker_js(body: str, env: dict | None = None, clock: bool = True) -> str:
    """Worker case body: provider env set, `Date` fixed (restored afterwards)."""
    env_js = f"m.response.setWorkerEnv({json.dumps(env or {})});"
    if not clock:
        return f"{env_js}\n{body}"
    y, mo, d, h, mi, s = NOW_PARTS
    return f"""
{env_js}
let __now = Date.UTC({y}, {mo}, {d}, {h}, {mi}, {s});
const RealDate = Date;
globalThis.Date = class extends RealDate {{
  constructor(...a) {{ if (a.length === 0) super(__now); else super(...a); }}
  static now() {{ return __now; }}
}};
try {{
{body}
}} finally {{ globalThis.Date = RealDate; }}
"""


def _summary_route(info: dict | None, website: str | None = None) -> dict:
    """quoteSummary body for the Worker's company identity lookup from the same info the Python side gets."""
    info = info or {}
    price = {k: v for k, v in (("shortName", info.get("shortName")), ("longName", info.get("longName")),
                                ("exchangeName", info.get("exchange"))) if v is not None}
    profile = {"website": website} if website else {}
    return {"json": {"quoteSummary": {"result": [{"price": price, "summaryProfile": profile, "assetProfile": {}}], "error": None}}}


def _yahoo_routes(news=None, info=None, website=None, identity_fails=False) -> dict:
    routes = dict(YAHOO_AUTH_ROUTES)
    routes[YAHOO_SEARCH_PREFIX] = {"json": {"news": copy.deepcopy(news or [])}}
    routes[YAHOO_SUMMARY_PREFIX] = {"status": 500, "body": "{}"} if identity_fails else _summary_route(info, website)
    return routes


def _sec_routes(cik: int, ticker: str, name: str, recent: dict) -> dict:
    return {
        SEC_TICKERS_URL: {"json": {"0": {"cik_str": cik, "ticker": ticker, "title": name}}},
        f"https://data.sec.gov/submissions/CIK{cik:010d}.json": {"json": {"name": name, "filings": {"recent": recent}}},
    }


def _py_sec(cik: int, name: str, recent: dict):
    return (f"{cik:010d}", {"name": name, "filings": {"recent": recent}})


def _yn(title, *, publisher="Example Wire", link=None, when=None, **extra):
    """A Yahoo search-feed news row (title, publisher, link, providerPublishTime epoch seconds)."""
    row = {"title": title, "publisher": publisher, "link": link if link is not None else "https://example.com/" + str(abs(hash(title)) % 10**6),
           "providerPublishTime": _epoch(when or _days_ago(1))}
    row.update(extra)
    return row


def _scenario(name, ids, py_call, ts_call, *, news=None, info=None, website=None, env=None, submissions=None, routes=None,
              identity_fails=False, select=None, ts_prelude="", **kw) -> Case:
    """One tool call on both runtimes under the shared stubs.

    py_call: zero-argument callable returning the Python tool coroutine (JSON text result).
    ts_call: JavaScript expression awaiting the Worker tool function (JSON text result).
    """
    def py():
        with _py_news_env(news=news, info=info, env=env, submissions=submissions):
            return json.loads(asyncio.run(py_call()))

    all_routes = {**_yahoo_routes(news, info, website, identity_fails), **(routes or {})}
    js = _worker_js(f"{ts_prelude}\nreturn JSON.parse(await {ts_call});", env)
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True}, routes=all_routes, select=select, **kw)


# ----------------------------------------------------------------------------------------------------- views

def _norm_ts(value):
    """Deep copy with '...:00.000Z' timestamps shortened to '...:00Z' (BR-NWS-278 is pinned on its own)."""
    if isinstance(value, str) and value.endswith(".000Z"):
        return value[:-5] + "Z"
    if isinstance(value, dict):
        return {k: _norm_ts(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_norm_ts(v) for v in value]
    return value


def _unwrap(v):
    """A tool-envelope result (Worker callTool, Python failure) reduced to its data, or {'error': code, 'message': text}."""
    if isinstance(v, dict) and "ok" in v and "meta" in v and "data" in v:
        if v["ok"]:
            return v["data"]
        err = v.get("error") or {}
        return {"error": err.get("code"), "message": err.get("message")}
    return v


def _item_rows(v, keys=("title", "source", "matchBasis", "issuer", "confidence", "tickerRelevance")):
    return [[i.get(k) for k in keys] for i in v["items"]]


def _status_rows(v, source, keys=("status", "rawCount", "acceptedCount", "returnedCount", "rejectedCount", "rejectionCounts")):
    row = v["sourceStatus"].get(source) or {}
    return {k: row.get(k) for k in keys if k in row}


def _warning_rows(v):
    return [[w["code"], w.get("message")] for w in v["warnings"]]


def _warning_codes(v):
    return [w["code"] for w in v["warnings"]]


AMD_INFO = {"shortName": "Advanced Micro Devices, Inc.", "longName": "Advanced Micro Devices, Inc.", "exchange": "NMS"}
AEHR_INFO = {"shortName": "Aehr Test Systems", "longName": "Aehr Test Systems, Inc.", "exchange": "NMS"}
YAHOO_NEWS_VIEW = lambda v: {  # noqa: E731
    "items": _item_rows(v), "news": _status_rows(v, "yahoo_finance_news"), "warnings": _warning_codes(v),
    "status": v.get("status"), "coverage": v["sourceCoverage"],
}

# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Yahoo news feed: identity gate, match ranking, content type, date window, issuer wire releases
# BR-NWS-243 / 244 / 245 / 262 (relevance gate), 227 / 247 (issuer wire), 280 (content type), 199 (feed size)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_AMD_FEED = [
    _yn("AMD beats estimates", publisher="Reuters", link="https://example.com/amd-beats", when=_days_ago(5)),
    _yn("Advanced Micro Devices ships MI400", publisher="Reuters", link="https://example.com/amd-mi400", when=_days_ago(4)),
    _yn("Intel gains on foundry hopes", publisher="Reuters", link="https://example.com/intel", when=_days_ago(3)),
    _yn("Advanced Micro Devices expands partnership", publisher="Reuters", link="", when=_days_ago(2)),
]
AMD_IDENTITY = {"status": "RESOLVED", "companyName": "Advanced Micro Devices, Inc.",
                "aliases": ["advanced micro devices", "advanced micro devices inc"], "acronyms": ["AMD"], "exchange": "NMS"}


def _collector_case(name, ids, ticker, news, identity, *, feed="news", lookback=14, max_results=10, view=None, **kw) -> Case:
    """The Yahoo collector called directly with a literal identity (Python _collect_yahoo_events, Worker collectYahooEvents)."""
    def py():
        py_identity = {**identity, "aliases": tuple(identity["aliases"]), "acronyms": tuple(identity["acronyms"]), "ticker": ticker}
        with _py_news_env(news=news, info={}):
            items, warnings, used, diagnostics = asyncio.run(server._collect_yahoo_events(
                ticker, retrieved_at=NOW_ISO, max_results=max_results, lookback_days=lookback, feed=feed,
                identity=py_identity, include_diagnostics=True))
        return {"items": items, "warnings": warnings, "used": used, "diagnostics": diagnostics}

    js = _worker_js(f"""
const r = await m.yahooFinance.collectYahooEvents({json.dumps(ticker)}, {max_results}, {json.dumps(NOW_ISO)}, {json.dumps(identity)}, "", "", {lookback}, {json.dumps(feed)});
return JSON.parse(JSON.stringify(r));""")
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True}, routes=_yahoo_routes(news, {}), select=view, **kw)


def _collector_view(v):
    return {
        "items": [[i["title"], i["source"], i.get("matchBasis"), i.get("issuer"), i.get("confidence"), i.get("tickerRelevance")] for i in v["items"]],
        "used": v["used"], "accepted": v["diagnostics"]["acceptedCount"], "raw": v["diagnostics"]["rawCount"],
        "rejections": v["diagnostics"]["rejectionCounts"], "identityStatus": v["diagnostics"]["identityStatus"],
        "warnings": [w["code"] for w in v["warnings"]],
    }


CASES += [
    # Tool level (card 245 / 243): both AMD items survive; the final list is newest first (the dedupe step sorts by
    # publishedAt), then re-ordered by tickerRelevance, which is HIGH for every accepted Yahoo item.
    # No URL means confidence LOW. Non-matching headlines are IDENTITY_MISMATCH.
    _scenario(
        "news.yahoo.accepted_items_are_newest_first_with_url_driven_confidence", ["BR-NWS-243", "BR-NWS-245", "BR-NWS-262"],
        lambda: server.get_company_news("AMD", 10, 14, ["yahoo_finance_news"]),
        'm.yahooFinance.getCompanyNews("AMD", 10, 14, ["yahoo_finance_news"])',
        news=_AMD_FEED, info=AMD_INFO, select=YAHOO_NEWS_VIEW,
        expect={
            "items": [["Advanced Micro Devices expands partnership", "yahoo_finance_news", "ISSUER_NAME", None, "LOW", "HIGH"],
                      ["Advanced Micro Devices ships MI400", "yahoo_finance_news", "ISSUER_NAME", None, "MEDIUM", "HIGH"],
                      ["AMD beats estimates", "yahoo_finance_news", "TICKER_TOKEN", None, "MEDIUM", "HIGH"]],
            "news": {"status": "OK", "rawCount": 4, "acceptedCount": 3, "returnedCount": 3, "rejectedCount": 1,
                     "rejectionCounts": {"IDENTITY_MISMATCH": 1}},
            "warnings": [], "status": None, "coverage": "FULL",
        }),
    # Collector level (card 245): the ticker-token match (rank 0) is listed before the newer issuer-name matches (rank 1).
    _collector_case(
        "news.yahoo.collector_orders_by_match_rank_then_newest", ["BR-NWS-243", "BR-NWS-245"], "AMD", _AMD_FEED, AMD_IDENTITY,
        view=_collector_view,
        expect={
            "items": [["AMD beats estimates", "yahoo_finance_news", "TICKER_TOKEN", None, "MEDIUM", "HIGH"],
                      ["Advanced Micro Devices expands partnership", "yahoo_finance_news", "ISSUER_NAME", None, "LOW", "HIGH"],
                      ["Advanced Micro Devices ships MI400", "yahoo_finance_news", "ISSUER_NAME", None, "MEDIUM", "HIGH"]],
            "used": True, "accepted": 3, "raw": 4, "rejections": {"IDENTITY_MISMATCH": 1}, "identityStatus": "RESOLVED", "warnings": [],
        }),
]


def _multi_scenario(name, ids, calls, *, news=None, info=None, website=None, env=None, submissions=None, routes=None,
                    identity_fails=False, select=None, ts_prelude="", **kw) -> Case:
    """Several tool calls in one case. calls = [(label, python_call, worker_expression)]; the result maps label -> payload.

    Python caches the calls touch are shared across them, as the Worker's module caches are within one isolate.
    """
    def py():
        out = {}
        with _py_news_env(news=news, info=info, env=env, submissions=submissions):
            for label, py_call, _ts in calls:
                out[label] = json.loads(asyncio.run(py_call()))
        return out

    body = ts_prelude + "\nconst out = {};\n" + "\n".join(
        f"out[{json.dumps(label)}] = JSON.parse(await {ts});" for label, _py, ts in calls) + "\nreturn out;"
    all_routes = {**_yahoo_routes(news, info, website, identity_fails), **(routes or {})}
    return Case(name=name, ids=ids, py=py, ts={"js": _worker_js(body, env), "isolate": True}, routes=all_routes,
                select=(lambda v: {k: select(p) for k, p in v.items()}) if select else None, **kw)


def _selector_view(v):
    news = v["sourceStatus"].get("yahoo_finance_news") or {}
    return {"items": [[i["title"], i["source"]] for i in v["items"]], "sourcesUsed": v["meta"]["sourcesUsed"],
            "coverage": v["sourceCoverage"], "status": v.get("status"),
            "news": {k: news[k] for k in ("acceptedCount", "returnedCount") if k in news},
            "sourceStatus": {k: s.get("status") for k, s in v["sourceStatus"].items()}}


_SEL_FEED = [_yn(f"AMD story {i}", when=_days_ago(i + 1), contentType="STORY") for i in range(5)] + [
    _yn("AMD Announces Release One", publisher="Business Wire", when=_days_ago(1), contentType="PRESS_RELEASE"),
    _yn("Advanced Micro Devices Announces Release Two", publisher="GlobeNewswire", when=_days_ago(2), contentType="PRESS_RELEASE"),
]
_STORIES = [["AMD story 0", "yahoo_finance_news"], ["AMD story 1", "yahoo_finance_news"], ["AMD story 2", "yahoo_finance_news"],
            ["AMD story 3", "yahoo_finance_news"], ["AMD story 4", "yahoo_finance_news"]]
_LEGACY_ITEMS = [["AMD story 0", "yahoo_finance_news"], ["AMD Announces Release One", "yahoo_finance_press_releases"],
                 ["AMD story 1", "yahoo_finance_news"], ["Advanced Micro Devices Announces Release Two", "yahoo_finance_press_releases"],
                 ["AMD story 2", "yahoo_finance_news"], ["AMD story 3", "yahoo_finance_news"], ["AMD story 4", "yahoo_finance_news"]]

CASES += [
    # BR-NWS-198: the selected Yahoo source names decide which items are kept. The legacy aggregate returns the same 7 items
    # in both runtimes. yahoo_finance_news agrees since 2.17.0 (B-G1): both runtimes read each row's contentType, so
    # PRESS_RELEASE rows are relabelled yahoo_finance_press_releases and dropped from a news-only selection (accepted 7,
    # returned 5). company_ir agrees since 2.19.0 (R-news Batch B, A-G10 / B-G2): both runtimes read approved company IR registry
    # entries only (none for AMD, so NOT_REGISTERED and nothing; Python used to serve it from the Yahoo feed).
    _multi_scenario(
        "news.yahoo.source_selector_item_sets", ["BR-NWS-198", "BR-NWS-246"],
        [(label, (lambda s=srcs: server.get_company_news("AMD", 10, 14, s)),
          f"m.yahooFinance.getCompanyNews(\"AMD\", 10, 14, {json.dumps(srcs)})")
         for label, srcs in (("yahoo_finance_news", ["yahoo_finance_news"]), ("yahoo_finance", ["yahoo_finance"]),
                             ("company_ir", ["company_ir"]), ("yahoo_finance_press_releases", ["yahoo_finance_press_releases"]))],
        news=_SEL_FEED, info=AMD_INFO, select=_selector_view,
        expect={
            "yahoo_finance_news": {"items": _STORIES, "sourcesUsed": ["yahoo_finance_news"], "coverage": "FULL", "status": None,
                                   "news": {"acceptedCount": 7, "returnedCount": 5}, "sourceStatus": {"yahoo_finance_news": "OK"}},
            "yahoo_finance": {"items": _LEGACY_ITEMS, "sourcesUsed": ["yahoo_finance"], "coverage": "FULL", "status": None,
                              "news": {}, "sourceStatus": {"yahoo_finance": "OK"}},
            "company_ir": {"items": [], "sourcesUsed": [], "coverage": "PARTIAL", "status": "PROVIDER_ERROR",
                           "news": {}, "sourceStatus": {"company_ir": "NOT_REGISTERED"}},
            "yahoo_finance_press_releases": {
                "items": [["AMD Announces Release One", "yahoo_finance_press_releases"],
                          ["Advanced Micro Devices Announces Release Two", "yahoo_finance_press_releases"]],
                "sourcesUsed": ["yahoo_finance_press_releases"], "coverage": "FULL", "status": None, "news": {},
                "sourceStatus": {"yahoo_finance_press_releases": "OK"}},
        }),
]


def _kept(title, source="yahoo_finance_news", basis="TICKER_TOKEN", issuer=None, confidence="MEDIUM", relevance="HIGH"):
    return [title, source, basis, issuer, confidence, relevance]


def _collected(items, *, raw, rejections, identity="RESOLVED", used=True, warnings=()):
    return {"items": items, "used": used, "accepted": len(items), "raw": raw, "rejections": rejections,
            "identityStatus": identity, "warnings": list(warnings)}


# --- exchange-prefix compatibility of a ticker mention (BR-NWS-217, BR-NWS-218) --------------------------------------
# The headline is repeated as summary and evidence text, so every mention of the ticker is checked. The company name never
# appears in these headlines, so only the ticker-token test can accept them.
_ZETA = {"status": "RESOLVED", "companyName": "Zeta Holdings Corp", "aliases": ["zeta holdings"], "acronyms": [], "exchange": "NMS"}
_PREFIX_FEED = [_yn(t, when=_days_ago(1)) for t in [
    "Shares of ABC (TSX: ABC) rose", "Nasdaq: ABC climbs", "ABC wins contract", "ABC (OTC: ABC) quoted",
    "Acme (TSXV: ABC) announces results", "Listing on NYSE: ABC planned", "Rally in ASX: ABC shares", "Zeta (CVE: ABC) update", "LSE: ABC dips",
    "Rally (TSX: ABC) continues"]]

CASES += [
    # A NASDAQ (NMS) company: a bare mention, 'Nasdaq:' and an unknown prefix ('OTC:') match; TSXV / NYSE / ASX / CVE / LSE
    # prefixes are incompatible (the first headline also has a bare mention, so it matches).
    _collector_case(
        "news.yahoo.exchange_prefix_incompatible_for_nasdaq_company", ["BR-NWS-217", "BR-NWS-218", "BR-NWS-243"], "ABC", _PREFIX_FEED, _ZETA,
        view=_collector_view,
        expect=_collected([_kept("Shares of ABC (TSX: ABC) rose"), _kept("Nasdaq: ABC climbs"), _kept("ABC wins contract"),
                           _kept("ABC (OTC: ABC) quoted")], raw=10, rejections={"IDENTITY_MISMATCH": 6})),
    # Unknown company exchange: any word-boundary token match counts, whatever the prefix.
    _collector_case(
        "news.yahoo.exchange_prefix_ignored_when_company_exchange_unknown", ["BR-NWS-217", "BR-NWS-218"], "ABC", _PREFIX_FEED,
        {**_ZETA, "exchange": None}, view=_collector_view,
        expect=_collected([_kept(t["title"]) for t in _PREFIX_FEED], raw=10, rejections={})),
    # Until 2.17.0 a company whose exchange reads 'NYSE American' normalised to NYSE in both runtimes (NYSE was tested first), so
    # an 'AMEX:' mention was rejected and 'NYSE:' accepted. R-news Batch A (B-G4): the shared normaliser tests NYSE American /
    # AMEX / exact ASE before NYSE, so 'AMEX:' is accepted and 'NYSE:' rejected. 'NYSE American:' is accepted because its prefix
    # token 'American' is unknown.
    _collector_case(
        "news.yahoo.exchange_nyse_american_company_reads_as_nyse", ["BR-NWS-217"], "ABC",
        [_yn(t, when=_days_ago(1)) for t in ["NYSE American: ABC lifts", "AMEX: ABC lifts", "NYSE: ABC lifts2"]],
        {**_ZETA, "exchange": "NYSE American"}, view=_collector_view, expect=_collected([_kept("NYSE American: ABC lifts"), _kept("AMEX: ABC lifts")], raw=3, rejections={"IDENTITY_MISMATCH": 1})),
    # 'GS' is a NASDAQ code. Since 2.17.0 (B-G4) it counts only as a whole token or an exact code; in 'LSE GS' it is a whole
    # token, so the string still reads as NASDAQ in both runtimes (the substring case is the next one).
    _collector_case(
        "news.yahoo.exchange_gs_substring_reads_as_nasdaq", ["BR-NWS-217"], "ABC",
        [_yn(t, when=_days_ago(1)) for t in ["Nasdaq: ABC climbs", "NYSE: ABC slips"]],
        {**_ZETA, "exchange": "LSE GS"}, view=_collector_view,
        expect=_collected([_kept("Nasdaq: ABC climbs")], raw=2, rejections={"IDENTITY_MISMATCH": 1})),
    # 2.17.0 (B-G4): GS / NMS / NGM / NCM inside another word ('GSX', 'XNMS') are not NASDAQ codes, so the company exchange is
    # unknown and no prefix is incompatible (before, Python read them as NASDAQ and rejected the 'NYSE:' mention).
    _collector_case(
        "news.yahoo.exchange_nasdaq_codes_count_only_as_whole_tokens", ["BR-NWS-217"], "ABC",
        [_yn(t, when=_days_ago(1)) for t in ["Nasdaq: ABC climbs", "NYSE: ABC slips"]],
        {**_ZETA, "exchange": "GSX XNMS"}, view=_collector_view, expect=_collected([_kept("Nasdaq: ABC climbs"), _kept("NYSE: ABC slips")], raw=2, rejections={})),
]

# --- issuer acronym match and identity-unavailable gate (BR-NWS-243, BR-NWS-244, BR-NWS-245) ---------------------------
_ACRONYM_ID = {"status": "RESOLVED", "companyName": "Advanced Micro Devices", "aliases": ["advanced micro devices"],
               "acronyms": ["AMD"], "exchange": "NMS"}
_UNAVAILABLE_ID = {"status": "UNAVAILABLE", "companyName": None, "aliases": [], "acronyms": [], "exchange": None}
CASES += [
    # Ranks: ticker token 0, issuer name 1, issuer acronym 2 (case-insensitive word match), then newest first within a rank.
    _collector_case(
        "news.yahoo.collector_acronym_match_is_the_lowest_rank", ["BR-NWS-244", "BR-NWS-245"], "XYZ",
        [_yn("AMD to launch chip", when=_days_ago(1)), _yn("Advanced Micro Devices chip", when=_days_ago(2)),
         _yn("amd lowercase mention", when=_days_ago(1)), _yn("Unrelated headline", when=_days_ago(1)),
         _yn("XYZ also moves", when=_days_ago(3))],
        _ACRONYM_ID, view=_collector_view,
        expect=_collected([_kept("XYZ also moves"), _kept("Advanced Micro Devices chip", basis="ISSUER_NAME"),
                           _kept("AMD to launch chip", basis="ISSUER_ACRONYM"), _kept("amd lowercase mention", basis="ISSUER_ACRONYM")],
                          raw=5, rejections={"IDENTITY_MISMATCH": 1})),
    # Identity unavailable: only ticker-token matches pass; the others are counted as IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND.
    _collector_case(
        "news.yahoo.collector_identity_unavailable_keeps_only_ticker_tokens", ["BR-NWS-243", "BR-NWS-245"], "AMD", _AMD_FEED, _UNAVAILABLE_ID,
        view=_collector_view,
        expect=_collected([_kept("AMD beats estimates")], raw=4, rejections={"IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND": 3},
                          identity="UNAVAILABLE")),
    # Tool level: the Yahoo profile lookup fails, so SOURCE_IDENTITY_UNAVAILABLE is raised, the identity source is
    # IDENTITY_UNAVAILABLE and the result is PARTIAL.
    _scenario(
        "news.yahoo.tool_identity_unavailable_is_partial_with_warning", ["BR-NWS-243", "BR-NWS-245", "BR-NWS-196"],
        lambda: server.get_company_news("AMD", 10, 14, ["yahoo_finance_news"]),
        'm.yahooFinance.getCompanyNews("AMD", 10, 14, ["yahoo_finance_news"])',
        news=_AMD_FEED, info=RuntimeError("profile unavailable"), identity_fails=True,
        select=lambda v: {**YAHOO_NEWS_VIEW(v), "identity": v["sourceStatus"].get("yahoo_finance_identity", {}).get("status"),
                          "action": v["coverage"]["recommendedNextAction"]},
        expect={
            "items": [_kept("AMD beats estimates")],
            "news": {"status": "OK", "rawCount": 4, "acceptedCount": 1, "returnedCount": 1, "rejectedCount": 3,
                     "rejectionCounts": {"IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND": 3}},
            "warnings": ["SOURCE_IDENTITY_UNAVAILABLE"], "status": "PARTIAL", "coverage": "PARTIAL",
            "identity": "IDENTITY_UNAVAILABLE", "action": "CHECK_OFFICIAL_RELEASES"}),
]

# --- issuer-own wire press releases (BR-NWS-227, BR-NWS-247) ---------------------------------------------------------
_AAPL_ID = {"status": "RESOLVED", "companyName": "Apple Inc.", "aliases": ["apple inc", "apple"], "acronyms": [], "exchange": "NMS"}
_AEHR_ID = {"status": "RESOLVED", "companyName": "Aehr Test Systems, Inc.", "aliases": ["aehr test systems", "aehr test systems inc"],
            "acronyms": ["ATS"], "exchange": "NMS"}
CASES += [
    # Card 247: another issuer's release that mentions Apple is NOT_ISSUER_WIRE_RELEASE; the issuer's own is kept with issuer
    # = company name. The wire pattern is anchored at the start of the publisher name; MT Newswires is deliberately not a wire.
    _collector_case(
        "news.yahoo.press_release_feed_keeps_only_issuer_own_wire_releases", ["BR-NWS-247", "BR-NWS-227"], "AAPL",
        [_yn("Qualcomm renews license with Apple", publisher="Business Wire", when=_days_ago(1)),
         _yn("Apple announces new iPhone", publisher="GlobeNewswire", when=_days_ago(1)),
         _yn("AAPL: Apple announces dividend", publisher="PR Newswire", when=_days_ago(1)),
         _yn("Apple announces services update", publisher="MT Newswires", when=_days_ago(1)),
         _yn("Apple", publisher="Cision PR Newswire", when=_days_ago(1)),
         _yn("Apple Inc. expands", publisher="EQS Newswire", when=_days_ago(1)),
         _yn("Apple opens store", publisher="Reuters via Business Wire", when=_days_ago(1)),
         _yn("Applesauce recall", publisher="Business Wire", when=_days_ago(1))],
        _AAPL_ID, feed="press_releases", view=_collector_view,
        expect=_collected([
            _kept("AAPL: Apple announces dividend", "yahoo_finance_press_releases", "TICKER_TOKEN", "Apple Inc."),
            _kept("Apple announces new iPhone", "yahoo_finance_press_releases", "ISSUER_NAME", "Apple Inc."),
            _kept("Apple", "yahoo_finance_press_releases", "ISSUER_NAME", "Apple Inc."),
            _kept("Apple Inc. expands", "yahoo_finance_press_releases", "ISSUER_NAME", "Apple Inc.")],
            raw=8, rejections={"IDENTITY_MISMATCH": 1, "NOT_ISSUER_WIRE_RELEASE": 3})),
    # Card 227: AEHR's ACCESS Newswire release is kept (issuer = company name); QuickLogic's GlobeNewswire release is not.
    _collector_case(
        "news.yahoo.press_release_feed_aehr_sonoma_order_vs_quicklogic", ["BR-NWS-227"], "AEHR",
        [_yn("Aehr Test Systems Receives Sonoma Order", publisher="ACCESS Newswire", when=_days_ago(1)),
         _yn("QuickLogic partners with Aehr", publisher="GlobeNewswire", when=_days_ago(1))],
        _AEHR_ID, feed="press_releases", view=_collector_view,
        expect=_collected([_kept("Aehr Test Systems Receives Sonoma Order", "yahoo_finance_press_releases", "TICKER_TOKEN",
                                 "Aehr Test Systems, Inc.")], raw=2, rejections={"NOT_ISSUER_WIRE_RELEASE": 1})),
    # yahooItemIssuer / _yahoo_item_issuer: the title must equal, or start with, an alias or ticker; only press-release items.
    Case(name="news.yahoo.issuer_lead_rules",
         ids=["BR-NWS-247", "BR-NWS-227"],
         py=lambda: [server._yahoo_item_issuer(item, {**_AAPL_ID, "aliases": tuple(_AAPL_ID["aliases"])}) for item in [
             {"source": "yahoo_finance_press_releases", "title": "Apple", "tickers": ["AAPL"]},
             {"source": "yahoo_finance_press_releases", "title": "Apple, Inc. announces", "tickers": ["AAPL"]},
             {"source": "yahoo_finance_press_releases", "title": "Applesauce", "tickers": ["AAPL"]},
             {"source": "yahoo_finance_press_releases", "title": "AAPL: update", "tickers": ["AAPL"]},
             {"source": "yahoo_finance_press_releases", "title": "Update from Apple", "tickers": ["AAPL"]},
             {"source": "yahoo_finance_news", "title": "Apple announces", "tickers": ["AAPL"]}]],
         ts={"js": """
const items = [
  {source: "yahoo_finance_press_releases", title: "Apple", tickers: ["AAPL"]},
  {source: "yahoo_finance_press_releases", title: "Apple, Inc. announces", tickers: ["AAPL"]},
  {source: "yahoo_finance_press_releases", title: "Applesauce", tickers: ["AAPL"]},
  {source: "yahoo_finance_press_releases", title: "AAPL: update", tickers: ["AAPL"]},
  {source: "yahoo_finance_press_releases", title: "Update from Apple", tickers: ["AAPL"]},
  {source: "yahoo_finance_news", title: "Apple announces", tickers: ["AAPL"]}];
return items.map(i => m.yahooFinance.yahooItemIssuer(i, args[0]));""", "args": [_AAPL_ID]},
         expect=["Apple Inc.", "Apple Inc.", None, "Apple Inc.", None, None]),
]


# --- date window, content type, summary text, feed size (BR-NWS-212, 244, 280, 256, 199, 246) ------------------------
_WINDOW_FEED = [
    _yn("AMD story on the cutoff day", when=_days_ago(14)),
    _yn("AMD story the day before the cutoff", when=_days_ago(15)),
    _yn("AMD article from long ago", when=_days_ago(200), contentType="ARTICLE"),
    _yn("AMD story fresh", when=_days_ago(1)),
    {**_yn("AMD story without a timestamp"), "providerPublishTime": None},
]
_VIDEO_FEED = [
    _yn("AMD video interview", when=_days_ago(2), contentType="VIDEO"),
    _yn("AMD story fresh", when=_days_ago(1), contentType="STORY"),
]
_LONG_SUMMARY = ("Advanced Micro Devices summary sentence one. " * 12).strip()  # 539 characters

CASES += [
    # Cutoff = 2026-10-15 minus 14 days = 2026-10-01, compared on the first 10 characters: the cutoff day is kept, the day
    # before is rejected, as are an old item and an item without a timestamp (DATE_WINDOW).
    _collector_case(
        "news.yahoo.date_window_keeps_cutoff_day_and_rejects_older_and_undated", ["BR-NWS-212", "BR-NWS-244", "BR-NWS-245"], "AMD", _WINDOW_FEED,
        AMD_IDENTITY, view=_collector_view,
        expect=_collected([_kept("AMD story fresh"), _kept("AMD story on the cutoff day")], raw=5, rejections={"DATE_WINDOW": 3})),
    # BR-NWS-280 / 244: a VIDEO row is rejected (CONTENT_TYPE) in both runtimes; the Worker's getNews() passes contentType
    # through since 2.17.0 (B-G1).
    _collector_case(
        "news.yahoo.content_type_video_filter_fires_in_both", ["BR-NWS-280", "BR-NWS-244"], "AMD", _VIDEO_FEED, AMD_IDENTITY,
        view=_collector_view, expect=_collected([_kept("AMD story fresh")], raw=2, rejections={"CONTENT_TYPE": 1})),
    # BR-NWS-246 card: 5 accepted items, one with contentType PRESS_RELEASE, selection yahoo_finance_news: the PRESS_RELEASE row is
    # labelled yahoo_finance_press_releases and dropped (returned 4, accepted 5) in both runtimes (the Worker since 2.17.0).
    _scenario(
        "news.yahoo.press_release_row_in_news_selection_is_dropped_in_both", ["BR-NWS-246", "BR-NWS-198"],
        lambda: server.get_company_news("AMD", 10, 14, ["yahoo_finance_news"]),
        'm.yahooFinance.getCompanyNews("AMD", 10, 14, ["yahoo_finance_news"])',
        news=[_yn(f"AMD story {i}", when=_days_ago(i + 1), contentType="STORY") for i in range(4)]
        + [_yn("AMD Announces Release", publisher="Business Wire", when=_days_ago(6), contentType="PRESS_RELEASE")],
        info=AMD_INFO, select=_selector_view,
        expect={"items": [["AMD story 0", "yahoo_finance_news"], ["AMD story 1", "yahoo_finance_news"], ["AMD story 2", "yahoo_finance_news"],
                          ["AMD story 3", "yahoo_finance_news"]],
                "sourcesUsed": ["yahoo_finance_news"], "coverage": "FULL", "status": None,
                "news": {"acceptedCount": 5, "returnedCount": 4}, "sourceStatus": {"yahoo_finance_news": "OK"}}),
    # BR-NWS-256 on a Yahoo row with a 539-character summary: both runtimes keep summary[:240] / evidenceText[:180] (the Worker's
    # getNews() passes the summary through since 2.17.0, B-G1 / call #13).
    _collector_case(
        "news.yahoo.summary_text_is_carried_in_both", ["BR-NWS-256", "BR-NWS-280"], "AMD",
        [_yn("AMD summary check", when=_days_ago(1), summary=_LONG_SUMMARY)], AMD_IDENTITY,
        view=lambda v: [[len(i["summary"]), len(i["evidenceText"])] for i in v["items"]], expect=[[240, 180]]),
    # Call #13 (2.17.0): the summary is for display; identity and ticker matching read the title only, so a row whose title names
    # neither the ticker nor the company is rejected even when its summary does, in both runtimes.
    _collector_case(
        "news.yahoo.summary_mention_alone_does_not_match_the_issuer", ["BR-NWS-256", "BR-NWS-243"], "AMD",
        [_yn("Chip stocks rally on demand", when=_days_ago(1), summary="AMD and Advanced Micro Devices peers gained."),
         _yn("AMD beats estimates", when=_days_ago(2), summary="Quarterly results.")], AMD_IDENTITY,
        view=_collector_view, expect=_collected([_kept("AMD beats estimates")], raw=2, rejections={"IDENTITY_MISMATCH": 1})),
]


def _search_request_py():
    """Arguments `server._yahoo_search_news` passes to yf.Search (mapped to the Worker's query-parameter names)."""
    calls = []

    class FakeSearch:
        def __init__(self, *args, **kwargs):
            calls.append((args, kwargs))
            self.news = []

    with patch.object(server.yf, "Search", FakeSearch):
        server._yahoo_search_news("AAPL")
    (args, kwargs), = calls
    return {"q": args[0], "quotesCount": kwargs["max_results"], "newsCount": kwargs["news_count"],
            "enableFuzzyQuery": kwargs["enable_fuzzy_query"]}


CASES += [
    # BR-NWS-199: the Yahoo search feed is asked for 20 news rows, no quotes, no fuzzy query.
    Case(name="news.yahoo.feed_request_asks_for_20_news_rows_and_no_quotes", ids=["BR-NWS-199"],
         py=_search_request_py,
         ts={"js": """
const urls = [];
const realFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => { urls.push(typeof input === "string" ? input : input.url); return realFetch(input, init); };
try {
  await m.yahooFinance.getNews("AAPL");
} finally { globalThis.fetch = realFetch; }
const u = new URL(urls.find(x => x.includes("/finance/search")));
return {q: u.searchParams.get("q"), quotesCount: Number(u.searchParams.get("quotesCount")), newsCount: Number(u.searchParams.get("newsCount")),
        enableFuzzyQuery: u.searchParams.get("enableFuzzyQuery") === "true"};
""", "isolate": True},
         routes=_yahoo_routes([], {}),
         expect={"q": "AAPL", "quotesCount": 0, "newsCount": 20, "enableFuzzyQuery": False}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Finnhub company news: acceptance, truncation, limits, HTTP outcomes
# BR-NWS-192 / 194 / 196 / 212 / 213 / 219 / 220 / 248 / 262 / 277
# ═══════════════════════════════════════════════════════════════════════════════════════════════

FINNHUB_ENV = {"FINNHUB_API_KEY": FAKE_FINNHUB_KEY}


def _frow(i, ticker="AAPL", days=1, text=None, **kw):
    """A Finnhub company-news row dated `days` before the fixed clock."""
    row = {"headline": text if text is not None else f"{ticker} headline {i}", "summary": f"summary {i}", "source": "Wire",
           "url": f"https://example.com/{i}", "datetime": _epoch(_days_ago(days, hour=i % 24))}
    row.update(kw)
    return row


def _finnhub(rows, status=200, body=None):
    return {FINNHUB_NEWS_PREFIX: {"json": rows} if body is None else {"status": status, "body": body}}


def _fin_counts(v):
    keys = ("status", "acceptedCount", "collectedCount", "returnedCount", "truncatedCount", "hasMoreAccepted")
    return {k: v["sourceStatus"]["finnhub"][k] for k in keys}


def _fin_truncation_view(v):
    return {"returned": len(v["items"]), "finnhub": _fin_counts(v), "truncatedSources": v["coverage"]["truncatedSources"],
            "action": v["coverage"]["recommendedNextAction"], "state": v["coverage"]["state"], "status": v.get("status"),
            # F15: the top-level sourceCoverage follows coverage.state.
            "sourceCoverage": v["sourceCoverage"]}


_ROWS_10_DAYS = [_frow(i, days=1 + (i % 10), text=f"Story number {i}") for i in range(120)]

CASES += [
    # Cards 219 / 220 / 192: 40 accepted rows, pool 30 (max_results 10 x 3), 10 returned after the relevance cut. The status
    # counts what was returned (10), so truncatedCount is 30, not the 10 the card states; nothing failed, so the next action
    # is RETRY_TRUNCATED_SOURCE.
    _scenario(
        "news.finnhub.truncated_source_recommends_retry_truncated_source", ["BR-NWS-192", "BR-NWS-219", "BR-NWS-220"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([_frow(i, days=1 + (i % 10)) for i in range(40)]), select=_fin_truncation_view,
        expect={"returned": 10,
                "finnhub": {"status": "OK", "acceptedCount": 40, "collectedCount": 30, "returnedCount": 10, "truncatedCount": 30,
                            "hasMoreAccepted": True},
                "truncatedSources": [{"source": "finnhub", "acceptedCount": 40, "returnedCount": 10, "truncatedCount": 30}],
                "action": "RETRY_TRUNCATED_SOURCE", "state": "FULL", "status": None, "sourceCoverage": "FULL"}),
    _scenario(
        "news.finnhub.25_accepted_rows_10_returned_truncated_15", ["BR-NWS-192", "BR-NWS-220"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([_frow(i, days=1 + (i % 10)) for i in range(25)]), select=_fin_truncation_view,
        expect={"returned": 10,
                "finnhub": {"status": "OK", "acceptedCount": 25, "collectedCount": 25, "returnedCount": 10, "truncatedCount": 15,
                            "hasMoreAccepted": True},
                "truncatedSources": [{"source": "finnhub", "acceptedCount": 25, "returnedCount": 10, "truncatedCount": 15}],
                "action": "RETRY_TRUNCATED_SOURCE", "state": "FULL", "status": None, "sourceCoverage": "FULL"}),
    # #19 (R-news Batch C, decisions 10 and 18): when truncatedSources is non-empty and the tool returns fewer items than the
    # caller's effective max_results, the evidence was cut by the pool cap or dedupe, not by the caller, so coverage.state is
    # PARTIAL. The two cases above return a full output (10 of 10), so the caller's limit cut them and they stay FULL. Here 40
    # accepted rows carry 4 distinct stories on one day: the pool of 30 dedupes to 4 items, so 4 < 10 are returned.
    _scenario(
        "news.coverage.truncation_by_dedupe_below_max_results_is_partial", ["BR-NWS-192", "BR-NWS-220", "BR-NWS-260"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([_frow(i, days=1, text=f"Same story {i % 4}") for i in range(40)]), select=_fin_truncation_view,
        expect={"returned": 4,
                   "finnhub": {"status": "OK", "acceptedCount": 40, "collectedCount": 30, "returnedCount": 4, "truncatedCount": 36,
                               "hasMoreAccepted": True},
                   "truncatedSources": [{"source": "finnhub", "acceptedCount": 40, "returnedCount": 4, "truncatedCount": 36}],
                   "action": "RETRY_TRUNCATED_SOURCE", "state": "PARTIAL", "status": None, "sourceCoverage": "PARTIAL"}),
    # #19 / decisions 10 and 18, the pool cap: search_company_news filters a pool of 100. Of 120 accepted rows only the first 100
    # are searched, so 'number 11' finds row 11 but not rows 110-119: 1 of max 10 returned, coverage PARTIAL.
    _scenario(
        "news.coverage.search_pool_cap_truncation_below_max_results_is_partial", ["BR-NWS-192", "BR-NWS-260"],
        lambda: server.search_company_news("AAPL", "number 11", "", "", ["finnhub"], 10),
        'm.yahooFinance.searchCompanyNews("AAPL", "number 11", "", "", ["finnhub"], 10)',
        env=FINNHUB_ENV, routes=_finnhub(_ROWS_10_DAYS), select=_fin_truncation_view,
        expect={"returned": 1,
                   "finnhub": {"status": "OK", "acceptedCount": 120, "collectedCount": 100, "returnedCount": 1, "truncatedCount": 119,
                               "hasMoreAccepted": True},
                   "truncatedSources": [{"source": "finnhub", "acceptedCount": 120, "returnedCount": 1, "truncatedCount": 119}],
                   "action": "RETRY_TRUNCATED_SOURCE", "state": "PARTIAL", "status": None, "sourceCoverage": "PARTIAL"}),
    # Decision 18: a mixed output that fills max_results stays FULL although Finnhub is in truncatedSources. 6 Yahoo items (today,
    # the newest) and 4 of Finnhub's 40 accepted rows fill the 10 slots (all HIGH relevance), so the caller's limit cut Finnhub.
    # A full output never turns PARTIAL, in either runtime.
    _scenario(
        "news.coverage.mixed_output_filling_max_results_stays_full_with_a_truncated_source", ["BR-NWS-192", "BR-NWS-260"],
        lambda: server.get_company_news("AAPL", 10, 14, ["yahoo_finance_news", "finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["yahoo_finance_news", "finnhub"])',
        news=[_yn(f"AAPL unveils {w} product line", when=_days_ago(0, hour=1 + i), link=f"https://example.com/y{i}")
              for i, w in enumerate(("alpha", "bravo", "charlie", "delta", "echo", "foxtrot"))],
        info={"shortName": "Apple Inc.", "longName": "Apple Inc.", "exchange": "NMS"},
        env=FINNHUB_ENV, routes=_finnhub([_frow(i, days=1 + (i % 10), text=f"AAPL finnhub story {i}") for i in range(40)]),
        select=lambda v: {"returned": len(v["items"]), "sources": sorted({i["source"] for i in v["items"]}),
                          "truncatedSources": v["coverage"]["truncatedSources"], "state": v["coverage"]["state"],
                          "action": v["coverage"]["recommendedNextAction"], "sourceCoverage": v["sourceCoverage"]},
        expect={"returned": 10, "sources": ["finnhub", "yahoo_finance_news"],
                "truncatedSources": [{"source": "finnhub", "acceptedCount": 40, "returnedCount": 4, "truncatedCount": 36}],
                "state": "FULL", "action": "RETRY_TRUNCATED_SOURCE", "sourceCoverage": "FULL"}),
    # Card 192: a retryable failure (RATE_LIMITED / TIMEOUT) sets the next action to RETRY_RETRYABLE_SOURCES, ahead of CHECK_OFFICIAL_RELEASES and
    # RETRY_TRUNCATED_SOURCE.
    _scenario(
        "news.finnhub.rate_limit_outranks_other_next_actions", ["BR-NWS-192", "BR-NWS-197"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(None, 429, "slow down"),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "warnings": _warning_rows(v), "status": v.get("status"),
                          "coverage": v["coverage"]},
        expect={"finnhub": {"status": "RATE_LIMITED", "rawCount": 0, "filteredCount": 0},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub rate limited: HTTP 429"]], "status": "SOURCE_LIMITED_NOT_FOUND",
                "coverage": {"state": "PARTIAL", "failedSources": [{"source": "finnhub", "status": "RATE_LIMITED", "attempted": True, "reasonCode": None}],
                             "skippedSources": [], "truncatedSources": [], "recommendedNextAction": "RETRY_RETRYABLE_SOURCES"}}),
    # No key: UNCONFIGURED, nothing attempted, PARTIAL coverage.
    _scenario(
        "news.finnhub.missing_key_is_unconfigured_and_not_attempted", ["BR-NWS-219", "BR-NWS-196"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        routes=_finnhub([_frow(1)]),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "warnings": _warning_rows(v), "status": v.get("status"),
                          "sourceCoverage": v["sourceCoverage"], "attempted": v["attemptedSources"]},
        expect={"finnhub": {"status": "UNCONFIGURED"},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub company-news source is not configured; skipped."]],
                "status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL", "attempted": []}),
    # HTTP outcomes. One message template per status in both runtimes (R-news Batch C, decision 11): Python's `HTTP <n>` wording,
    # and no internal FINNHUB_* tags (the Worker said 'FINNHUB_AUTH_ERROR:401' and 'FINNHUB_PROVIDER_CHANGED: expected array, got
    # object' until 2.20.0). The statuses, inferred from the message prefixes, are unchanged.
    _scenario(
        "news.finnhub.http_401_is_auth_error_with_the_http_status_message", ["BR-NWS-219"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(None, 401, "invalid token"),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "warnings": _warning_rows(v), "status": v.get("status")},
        expect={"finnhub": {"status": "AUTH_ERROR", "rawCount": 0, "filteredCount": 0},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub auth error: HTTP 401"]], "status": "SOURCE_LIMITED_NOT_FOUND"}),
    _scenario(
        "news.finnhub.http_500_is_provider_error_with_the_http_status_message", ["BR-NWS-219"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(None, 500, "boom"),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "status": v.get("status"), "warnings": _warning_rows(v)},
        expect={"finnhub": {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub source unavailable: HTTP 500"]], "status": "SOURCE_LIMITED_NOT_FOUND"}),
    _scenario(
        "news.finnhub.non_array_payload_is_provider_changed_with_the_unexpected_format_message", ["BR-NWS-219"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub({"error": "changed"}),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "warnings": _warning_rows(v), "status": v.get("status")},
        expect={"finnhub": {"status": "PROVIDER_CHANGED", "rawCount": 0, "filteredCount": 0},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub provider changed: unexpected response format"]], "status": "SOURCE_LIMITED_NOT_FOUND"}),
    # Row acceptance (card 220): a non-object row is INVALID_ROW, an empty headline MISSING_HEADLINE, rows older than the
    # lookback or without a timestamp DATE_OUTSIDE_WINDOW. Confidence is MEDIUM with a URL, LOW without; tickerRelevance is
    # HIGH only when the ticker is a substring of headline + summary (LOW rows stay, ranked after the HIGH ones).
    _scenario(
        "news.finnhub.row_acceptance_rejection_reasons_confidence_and_relevance", ["BR-NWS-219", "BR-NWS-220", "BR-NWS-262", "BR-NWS-260"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub(["notadict", {"headline": "", "datetime": _epoch(_days_ago(1))},
                         {"headline": "AAPL old row", "datetime": _epoch(_days_ago(40)), "url": "https://e.com/1"},
                         {"headline": "No timestamp row"},
                         _frow(1, text="AAPL big news", summary="x"), _frow(2, text="Sector moves", summary="nothing", url=""),
                         _frow(3, text="Sector moves again", summary="holds aapl exposure")]),
        select=lambda v: {"items": _item_rows(v, ("title", "confidence", "tickerRelevance", "url")), "finnhub": _status_rows(v, "finnhub"),
                          "warnings": _warning_codes(v)},
        expect={"items": [["Sector moves again", "MEDIUM", "HIGH", "https://example.com/3"],
                          ["AAPL big news", "MEDIUM", "HIGH", "https://example.com/1"],
                          ["Sector moves", "LOW", "LOW", None]],
                "finnhub": {"status": "OK", "rawCount": 7, "acceptedCount": 3, "returnedCount": 3, "rejectedCount": 4,
                            "rejectionCounts": {"INVALID_ROW": 1, "MISSING_HEADLINE": 1, "DATE_OUTSIDE_WINDOW": 2}},
                "warnings": []}),
]


# --- result-count and lookback clamping (BR-NWS-194, BR-NWS-277, BR-NWS-275) ------------------------------------------
def _limit_view(v):
    return {"returned": len(v["items"]), **{k: v["sourceStatus"]["finnhub"][k] for k in ("status", "acceptedCount", "collectedCount", "truncatedCount")}}


def _limit_call(max_results, lookback):
    return (lambda: server.get_company_news("AAPL", max_results, lookback, ["finnhub"]),
            f'm.yahooFinance.getCompanyNews("AAPL", {max_results}, {lookback}, ["finnhub"])')


_ROWS_5_DAYS = [_frow(i, days=5, text=f"Story {i}") for i in range(3)]

CASES += [
    # Card 194: max_results 500 becomes 100 (the pool is min(100, 3 x max)); negative becomes 1; lookback above 3650 is clamped,
    # not rejected.
    _multi_scenario(
        "news.limits.max_results_clamped_to_1_through_100", ["BR-NWS-194", "BR-NWS-275", "BR-NWS-277"],
        [("500", *_limit_call(500, 14)), ("-3", *_limit_call(-3, 14)), ("5", *_limit_call(5, 14)), ("lookback_huge", *_limit_call(5, 99999))],
        env=FINNHUB_ENV, routes=_finnhub(_ROWS_10_DAYS), select=_limit_view,
        expect={"500": {"returned": 100, "status": "OK", "acceptedCount": 120, "collectedCount": 100, "truncatedCount": 20},
                "-3": {"returned": 1, "status": "OK", "acceptedCount": 120, "collectedCount": 3, "truncatedCount": 119},
                "5": {"returned": 5, "status": "OK", "acceptedCount": 120, "collectedCount": 15, "truncatedCount": 115},
                "lookback_huge": {"returned": 5, "status": "OK", "acceptedCount": 120, "collectedCount": 15, "truncatedCount": 115}}),
    # Negative or tiny lookbacks become 1 day (rows are 5 days old); 7 days keeps them.
    _multi_scenario(
        "news.limits.lookback_days_negative_becomes_one_day", ["BR-NWS-194", "BR-NWS-277"],
        [("1", *_limit_call(5, 1)), ("-2", *_limit_call(5, -2)), ("7", *_limit_call(5, 7))],
        env=FINNHUB_ENV, routes=_finnhub(_ROWS_5_DAYS), select=_limit_view,
        expect={"1": {"returned": 0, "status": "EMPTY_RESULT", "acceptedCount": 0, "collectedCount": 0, "truncatedCount": 0},
                "-2": {"returned": 0, "status": "EMPTY_RESULT", "acceptedCount": 0, "collectedCount": 0, "truncatedCount": 0},
                "7": {"returned": 3, "status": "OK", "acceptedCount": 3, "collectedCount": 3, "truncatedCount": 0}}),
    # Card 194 / 277: 0 clamps to 1 in both runtimes (R-news Batch C, B-G7, decision 13); only a missing value means the default.
    _multi_scenario(
        "news.limits.zero_clamps_to_one", ["BR-NWS-194", "BR-NWS-277"],
        [("max_results_0", *_limit_call(0, 14)), ("lookback_0", *_limit_call(5, 0))],
        env=FINNHUB_ENV, routes=_finnhub(_ROWS_10_DAYS), select=_limit_view,
        expect={"max_results_0": {"returned": 1, "status": "OK", "acceptedCount": 120, "collectedCount": 3, "truncatedCount": 119},
                   "lookback_0": {"returned": 5, "status": "OK", "acceptedCount": 12, "collectedCount": 12, "truncatedCount": 7}}),
]

# --- date window with explicit dates: the 14-day lookback is always applied (BR-NWS-212, 213, 248) --------------------
_WINDOW_ROWS = [_frow(1, days=45, text="Story old"), _frow(2, days=20, text="Story mid"), _frow(3, days=3, text="Story fresh"),
                _frow(4, days=14, text="Story on the cutoff day"), _frow(5, days=15, text="Story the day before the cutoff"),
                {"headline": "Story without a timestamp"}]
CASES += [
    # Cards 212 / 213: today 2026-10-15, lookback 14 -> cutoff 2026-10-01: an item dated 09-30 is rejected, 10-01 kept; items
    # without a timestamp are always rejected.
    _scenario(
        "news.window.get_company_news_14_day_cutoff_is_inclusive", ["BR-NWS-212", "BR-NWS-213"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_WINDOW_ROWS),
        select=lambda v: {"items": [i["title"] for i in v["items"]], "finnhub": _status_rows(v, "finnhub")},
        expect={"items": ["Story fresh", "Story on the cutoff day"],
                "finnhub": {"status": "OK", "rawCount": 6, "acceptedCount": 2, "returnedCount": 2, "rejectedCount": 4,
                            "rejectionCounts": {"DATE_OUTSIDE_WINDOW": 4}}}),
    # Card 213: search_company_news fixes the lookback at 14 days and AND-s it with start_date, so start_date 2026-09-01 does not
    # reach the older rows.
    _scenario(
        "news.window.search_start_date_is_truncated_by_the_14_day_lookback", ["BR-NWS-213", "BR-NWS-248", "BR-NWS-212"],
        lambda: server.search_company_news("AAPL", "story", "2026-09-01", "", ["finnhub"], 10),
        'm.yahooFinance.searchCompanyNews("AAPL", "story", "2026-09-01", "", ["finnhub"], 10)',
        env=FINNHUB_ENV, routes=_finnhub(_WINDOW_ROWS),
        select=lambda v: {"items": [i["title"] for i in v["items"]], "matchCount": v["matchCount"], "finnhub": _status_rows(v, "finnhub")},
        expect={"items": ["Story fresh", "Story on the cutoff day"], "matchCount": 2,
                "finnhub": {"status": "OK", "rawCount": 6, "acceptedCount": 2, "returnedCount": 2, "rejectedCount": 4,
                            "rejectionCounts": {"DATE_OUTSIDE_WINDOW": 4}}}),
    # Card 248: start 2026-07-01, end 2026-09-30, today 2026-10-15: the lookback leaves only 10-01 onward and end_date removes
    # all of those, so a matching August row is lost and nothing warns. (Finnhub is also asked only for the lookback range.)
    _scenario(
        "news.window.search_explicit_historical_range_returns_nothing_without_warning", ["BR-NWS-248", "BR-NWS-213"],
        lambda: server.search_company_news("MSFT", "acquisition", "2026-07-01", "2026-09-30", ["finnhub"], 10),
        'm.yahooFinance.searchCompanyNews("MSFT", "acquisition", "2026-07-01", "2026-09-30", ["finnhub"], 10)',
        env=FINNHUB_ENV,
        routes=_finnhub([_frow(1, ticker="MSFT", days=30, text="MSFT acquisition Aug"), _frow(2, ticker="MSFT", days=3, text="MSFT acquisition now")]),
        select=lambda v: {"items": [i["title"] for i in v["items"]], "matchCount": v["matchCount"], "status": v.get("status"),
                          "warnings": _warning_codes(v), "finnhub": _status_rows(v, "finnhub")},
        expect={"items": [], "matchCount": 0, "status": "NOT_FOUND", "warnings": [],
                "finnhub": {"status": "EMPTY_RESULT", "rawCount": 2, "acceptedCount": 0, "returnedCount": 0, "rejectedCount": 2,
                            "rejectionCounts": {"DATE_OUTSIDE_WINDOW": 2}}}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# SEC filing events: cap, default form set, exact form matching, event types, Form 4 rewrite
# BR-NWS-195 / 215 / 216 / 221 / 236 / 237 / 255 / 261
# ═══════════════════════════════════════════════════════════════════════════════════════════════

SEC_CIK = 1234567


def _sec_recent(forms, days, *, accepted=True, doc="d{i}.htm"):
    n = len(forms)
    return {
        "form": forms,
        "filingDate": [_days_ago(d)[:10] for d in days],
        "acceptanceDateTime": [_days_ago(d, hour=14)[:-1] + ".000Z" if accepted else "" for d in days],
        "accessionNumber": [f"0001234567-26-{i + 1:06d}" for i in range(n)],
        "primaryDocument": [doc.format(i=i) for i in range(n)],
    }


def _sec_scenario(name, ids, py_call, ts_call, recent, *, select, routes=None, **kw) -> Case:
    return _scenario(name, ids, py_call, ts_call, routes={**_sec_routes(SEC_CIK, "ACME", "Acme Corp", recent), **(routes or {})},
                     submissions=_py_sec(SEC_CIK, "Acme Corp", recent), select=select, **kw)


def _sec_view(v):
    return {"items": [[i["filingType"], i["eventType"], _norm_ts(i["publishedAt"]), i["confidence"], i["accessionNumber"][-6:]] for i in v["items"]],
            "warnings": _warning_rows(v), "status": v.get("status"), "sourcesUsed": v["meta"]["sourcesUsed"]}


_ALL_FORMS = ["8-K", "8-K/A", "10-Q", "DEF 14A", "424B5", "4", "DEF14A", "S-3", "10-K", "PRE14A", "S-1"]
_ALL_FORM_DAYS = [1 + i for i in range(len(_ALL_FORMS))]
_EXPLICIT_TYPES = ["DEF 14A", "424B5", "PRE14A", "S-1", "S-3", "10-K/A", "424B"]
_EXPLICIT_FORMS = _ALL_FORMS + ["10-K/A", "424B"]

CASES += [
    # Card 195: 25 8-Ks in filing order; four stale ones (outside the 90-day window) sit between and are skipped without using
    # the cap; the first 10 in-window filings are returned.
    _sec_scenario(
        "news.sec.recent_events_stop_at_the_cap_in_filing_order_and_skip_stale_filings", ["BR-NWS-195"],
        lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 10),
        'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 10)',
        _sec_recent(["8-K"] * 25, [1, 2, 3, 200, 201, 202, 203] + [5 + i for i in range(18)]),
        select=lambda v: [i["accessionNumber"][-6:] for i in v["items"]],
        expect=["000001", "000002", "000003", "000008", "000009", "000010", "000011", "000012", "000013", "000014"]),
    # Cards 215 / 216 / 236: the default news form set is 8-K, 8-K/A (2.25.4, F43), 10-Q, 10-K, S-3, DEF14A, matched exactly
    # (case-insensitively) on the whole form string. 'DEF 14A' (EDGAR's spelling), 424B5, 4, PRE14A and S-1 are dropped; a form
    # literally spelled 'DEF14A' is kept and mapped to regulatory. An 8-K/A types as the 8-K it amends.
    _sec_scenario(
        "news.sec.default_news_forms_match_exactly_so_def_14a_is_dropped", ["BR-NWS-215", "BR-NWS-216", "BR-NWS-236", "BR-NWS-261"],
        lambda: server.get_company_news("ACME", 20, 30, ["sec"]),
        'm.yahooFinance.getCompanyNews("ACME", 20, 30, ["sec"])',
        _sec_recent(_ALL_FORMS, _ALL_FORM_DAYS), select=_sec_view,
        expect={"items": [["8-K", "regulatory", "2026-10-14T14:00:00Z", "HIGH", "000001"],
                          ["8-K/A", "regulatory", "2026-10-13T14:00:00Z", "HIGH", "000002"],
                          ["10-Q", "earnings", "2026-10-12T14:00:00Z", "HIGH", "000003"],
                          ["DEF14A", "regulatory", "2026-10-08T14:00:00Z", "HIGH", "000007"],
                          ["S-3", "financing", "2026-10-07T14:00:00Z", "HIGH", "000008"],
                          ["10-K", "earnings", "2026-10-06T14:00:00Z", "HIGH", "000009"]],
                "warnings": [], "status": None, "sourcesUsed": ["sec"]}),
    # Cards 215 / 216: when the caller names the forms, 'DEF 14A' and 424B5 are collected but typed 'other'; 424B and S-1 / S-3
    # are financing; PRE14A regulatory; an amendment such as 10-K/A is 'other'.
    _sec_scenario(
        "news.sec.explicit_form_types_map_to_event_types", ["BR-NWS-215", "BR-NWS-216", "BR-NWS-236"],
        lambda: server.get_sec_recent_events("ACME", _EXPLICIT_TYPES, 90, 20),
        f"m.yahooFinance.getSecRecentEvents(\"ACME\", {json.dumps(_EXPLICIT_TYPES)}, 90, 20)",
        _sec_recent(_EXPLICIT_FORMS, [1 + i for i in range(len(_EXPLICIT_FORMS))]), select=_sec_view,
        expect={"items": [["DEF 14A", "other", "2026-10-11T14:00:00Z", "HIGH", "000004"],
                          ["424B5", "other", "2026-10-10T14:00:00Z", "HIGH", "000005"],
                          ["S-3", "financing", "2026-10-07T14:00:00Z", "HIGH", "000008"],
                          ["PRE14A", "regulatory", "2026-10-05T14:00:00Z", "HIGH", "000010"],
                          ["S-1", "financing", "2026-10-04T14:00:00Z", "HIGH", "000011"],
                          # An amendment types as the form it amends (2.25.4, F43).
                          ["10-K/A", "earnings", "2026-10-03T14:00:00Z", "HIGH", "000012"],
                          ["424B", "financing", "2026-10-02T14:00:00Z", "HIGH", "000013"]],
                "warnings": [], "status": None, "sourcesUsed": ["sec"]}),
    # Card 261: no acceptance time -> publishedAt is the filing date at 00:00:00Z with PUBLISHED_AT_ESTIMATED, confidence MEDIUM
    # (HIGH needs accession, acceptance time and URL).
    _sec_scenario(
        "news.sec.missing_acceptance_time_estimates_published_at_with_medium_confidence", ["BR-NWS-261", "BR-NWS-195"],
        lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 20),
        'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 20)',
        _sec_recent(["8-K"], [14], accepted=False), select=_sec_view,
        expect={"items": [["8-K", "regulatory", "2026-10-01T00:00:00Z", "MEDIUM", "000001"]],
                "warnings": [["PUBLISHED_AT_ESTIMATED", "acceptedAt unavailable for 8-K; filingDate used."]], "status": None,
                "sourcesUsed": ["sec"]}),
    # Card 195 edge: no CIK / no submissions -> SEC reported unavailable and not used.
    _scenario(
        "news.sec.unknown_ticker_reports_source_unavailable", ["BR-NWS-195"],
        lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 20),
        'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 20)',
        submissions=(None, None), routes={SEC_TICKERS_URL: {"json": {}}}, select=_sec_view,
        expect={"items": [], "warnings": [["SOURCE_UNAVAILABLE", "SEC submissions source unavailable."]],
                "status": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": []}),
    # Card 237: blank filing types are INPUT_VALIDATION_ERROR and an empty list means the default three forms, in both runtimes (the
    # Worker since 2.19.0, B-G8; until then a blank type matched nothing and [] accepted every form).
    _multi_scenario(
        "news.sec.recent_events_blank_filing_types_are_rejected_and_an_empty_list_uses_the_defaults", ["BR-NWS-237", "BR-NWS-216"],
        [("blank", lambda: server.get_sec_recent_events("ACME", ["  "], 90, 20), 'm.yahooFinance.getSecRecentEvents("ACME", ["  "], 90, 20)'),
         ("empty_list", lambda: server.get_sec_recent_events("ACME", [], 90, 20), 'm.yahooFinance.getSecRecentEvents("ACME", [], 90, 20)')],
        routes=_sec_routes(SEC_CIK, "ACME", "Acme Corp", _sec_recent(["8-K", "DEF 14A", "10-K", "424B5"], [1, 2, 3, 4])),
        submissions=_py_sec(SEC_CIK, "Acme Corp", _sec_recent(["8-K", "DEF 14A", "10-K", "424B5"], [1, 2, 3, 4])),
        select=lambda v: ({"error": v["error"]["code"], "message": v["error"]["message"]} if "error" in v and "items" not in v
                          else {"forms": [i["filingType"] for i in v["items"]], "status": v.get("status")}),
        expect={"blank": {"error": "INPUT_VALIDATION_ERROR", "message": "filing_types must not be empty"},
                "empty_list": {"forms": ["8-K", "10-K"], "status": None}}),
    # The SEC-only event payload carries sourceCoverage / coverage / sourceStatus in both runtimes (Python since 2.19.0, B-G8 / call #15).
    _sec_scenario(
        "news.sec.recent_events_payload_has_coverage_fields", ["BR-NWS-195", "BR-NWS-261"],
        lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 20),
        'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 20)',
        _sec_recent(["8-K"], [1]), select=lambda v: sorted(v.keys()),
        expect=["coverage", "items", "meta", "sourceCoverage", "sourceStatus", "ticker", "warnings"]),
]

# --- Form 4 date normalisation and title / summary rewrite (BR-NWS-221, BR-NWS-255) -----------------------------------
_FORM4_TEMPLATE = """<?xml version="1.0"?><ownershipDocument><reportingOwner><reportingOwnerId><rptOwnerName>{owner}</rptOwnerName></reportingOwnerId>
<reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CFO</officerTitle></reportingOwnerRelationship></reportingOwner>
<nonDerivativeTable><nonDerivativeTransaction><transactionDate><value>{date}</value></transactionDate>
<transactionCoding><transactionCode>S</transactionCode></transactionCoding>
<transactionAmounts><transactionShares><value>{shares}</value></transactionShares>{price}</transactionAmounts>
<ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature></nonDerivativeTransaction></nonDerivativeTable></ownershipDocument>"""


def _form4_scenario(name, ids, docs, *, select, **kw) -> Case:
    """docs: list of dicts (owner, date, shares, price) -> one Form 4 filing each, newest first."""
    n = len(docs)
    recent = {"form": ["4"] * n, "filingDate": [_days_ago(1 + i)[:10] for i in range(n)],
              "acceptanceDateTime": [_days_ago(1 + i, hour=14) for i in range(n)],
              "accessionNumber": [f"0001234567-26-{i + 1:06d}" for i in range(n)], "primaryDocument": [f"f4_{i}.xml" for i in range(n)]}
    routes = {}
    for i, doc in enumerate(docs):
        price = f"<transactionPricePerShare><value>{doc['price']}</value></transactionPricePerShare>" if doc.get("price") else ""
        routes[f"https://www.sec.gov/Archives/edgar/data/{SEC_CIK}/{recent['accessionNumber'][i].replace('-', '')}/f4_{i}.xml"] = {
            "body": _FORM4_TEMPLATE.format(owner=doc.get("owner", "Doe Jane"), date=doc.get("date", "2024-03-05"), shares=doc.get("shares", "1500"), price=price)}
    return _sec_scenario(name, ids, lambda: server.get_sec_recent_events("ACME", ["4"], 90, 20),
                         'm.yahooFinance.getSecRecentEvents("ACME", ["4"], 90, 20)', recent, routes=routes, select=select, **kw)


def _form4_view(v):
    return [[i["title"], i["summary"], i["evidenceText"], (i.get("insiderTransaction") or {}).get("transactionDate"),
             (i.get("insiderTransaction") or {}).get("value")] for i in v["items"]]


CASES += [
    # Cards 221 / 255: dates '2024-03-05' and '3/5/2024' both read 2024-03-05, 'March 5' passes through, month and day are
    # not range-checked, an empty cell is null. Title 'Form 4: Sale by Jane Doe', summary 'Sale by ...: 1,500 shares, value $18,518.'
    # A missing owner reads 'reporting owner'; missing shares read 'shares unavailable'; no price means no value.
    _form4_scenario(
        "news.sec.form4_dates_and_title_summary_rewrite",
        ["BR-NWS-221", "BR-NWS-255"],
        [{"date": "2024-03-05", "price": "12.345"}, {"owner": "", "date": "3/5/2024", "price": "12.345"},
         {"date": "March 5", "shares": "", "price": "12.345"}, {"date": "13/40/2024"}, {"date": "", "price": "12.345"}],
        select=_form4_view,
        expect=[["Form 4: Sale by Doe Jane", "Sale by Doe Jane: 1,500 shares, value $18,518.", "SEC Form 4 transaction code S on 2024-03-05.",
                 "2024-03-05", 18517.5],
                ["Form 4: Sale by reporting owner", "Sale by reporting owner: 1,500 shares, value $18,518.",
                 "SEC Form 4 transaction code S on 2024-03-05.", "2024-03-05", 18517.5],
                ["Form 4: Sale by Doe Jane", "Sale by Doe Jane: shares unavailable.", "SEC Form 4 transaction code S on March 5.", "March 5", None],
                ["Form 4: Sale by Doe Jane", "Sale by Doe Jane: 1,500 shares.", "SEC Form 4 transaction code S on 2024-13-40.", "2024-13-40", None],
                ["Form 4: Sale by Doe Jane", "Sale by Doe Jane: 1,500 shares, value $18,518.", "SEC Form 4 transaction code S on 2026-10-10.",
                 None, 18517.5]]),
    # Until 2.17.0 Python formatted the dollar value and share count with f"{x:,.0f}" (round half to even), the Worker with
    # Math.round (half up): 18,516.5 printed $18,516 and $18,517; 0.5 printed $0 and $1; 2.5 shares printed 2 and 3.
    # R-news Batch A (B-G9): both runtimes print with the half-away-from-zero helper.
    _form4_scenario(
        "news.sec.form4_summary_rounds_halves_away_from_zero",
        ["BR-NWS-255"],
        [{"shares": "1000", "price": "18.5165"}, {"shares": "1", "price": "0.5"}, {"shares": "2.5", "price": "10"}],
        select=lambda v: [i["summary"] for i in v["items"]], expect=["Sale by Doe Jane: 1,000 shares, value $18,517.", "Sale by Doe Jane: 1 shares, value $1.",
                   "Sale by Doe Jane: 3 shares, value $25."]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Marketaux wire fallback: row acceptance, exchange check, HTTP outcomes (BR-NWS-229, BR-NWS-230)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

MARKETAUX_ENV = {"MARKETAUX_API_TOKEN": FAKE_MARKETAUX_TOKEN}
APPLE_INFO = {"shortName": "Apple Inc.", "longName": "Apple Inc.", "exchange": "NMS"}


def _mrow(i, symbol="AAPL", exchange="NASDAQ", url=None, title=None, days=1, **kw):
    """A Marketaux article tagged with one entity."""
    row = {"uuid": f"uuid-{i}", "title": title if title is not None else f"AAPL wire headline {i}",
           "url": url or f"https://www.prnewswire.com/news/{i}", "source": "prnewswire.com", "snippet": f"snippet {i}",
           "published_at": _days_ago(days)[:-1] + ".000000Z",
           "entities": [{"symbol": symbol, "name": "Apple Inc.", "exchange": exchange, "match_score": 12.5}]}
    row.update(kw)
    return row


def _marketaux_view(v):
    mx = v["sourceStatus"]["marketaux"]
    return {"items": _item_rows(v, ("title", "matchBasis", "confidence", "tickerRelevance", "issuer")),
            "marketaux": {k: mx.get(k) for k in ("status", "acceptedCount", "returnedCount", "rejectedCount", "rejectionCounts", "fallbackReason",
                                                 "attempted", "allowedAction")},
            "status": v.get("status"), "warnings": _warning_rows(v)}


def _marketaux_case(name, ids, payload, *, status=200, body=None, env=MARKETAUX_ENV, **kw) -> Case:
    """Yahoo returns nothing (so the Marketaux fallback runs, with the Yahoo profile's exchange NMS) and Marketaux answers `payload`."""
    routes = {MARKETAUX_PREFIX: {"json": payload} if body is None else {"status": status, "body": body}}
    return _scenario(name, ids, lambda: server.get_company_news("AAPL", 10, 14, ["yahoo_finance_news", "marketaux"]),
                     'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["yahoo_finance_news", "marketaux"])',
                     news=[], info=APPLE_INFO, env=env, routes=routes, select=_marketaux_view, **kw)


_MX_OK = {"items": [["AAPL wire headline 1", "PROVIDER_ENTITY_SYMBOL", "LOW", "HIGH", "Apple Inc."]],
          "marketaux": {"status": "OK", "acceptedCount": 1, "returnedCount": 1, "rejectedCount": 0, "rejectionCounts": {},
                        "fallbackReason": "HIGHER_PRIORITY_EMPTY", "attempted": True, "allowedAction": "check_official_releases"},
          "status": None, "warnings": []}


def _mx_failed(status, action, message, attempted=True):
    return {"items": [], "marketaux": {"status": status, "acceptedCount": 0, "returnedCount": 0, "rejectedCount": 0, "rejectionCounts": {},
                                       "fallbackReason": "HIGHER_PRIORITY_EMPTY", "attempted": attempted, "allowedAction": action},
            "status": "SOURCE_LIMITED_NOT_FOUND", "warnings": [["SOURCE_UNAVAILABLE", message]]}


CASES += [
    # Card 229: an article on a wire domain tagged AAPL / NASDAQ for a NASDAQ (NMS) company: provider entity symbol match,
    # confidence LOW, tickerRelevance HIGH. The fallback runs because Yahoo returned nothing.
    _marketaux_case("news.marketaux.entity_on_matching_exchange_is_accepted_with_low_confidence", ["BR-NWS-229", "BR-NWS-230"],
                    {"data": [_mrow(1)]}, expect=_MX_OK),
    # Card 229 / 230: the same article with an NYSE entity is ENTITY_EXCHANGE_MISMATCH; a blank exchange skips the check.
    _marketaux_case("news.marketaux.entity_on_other_exchange_is_rejected", ["BR-NWS-229", "BR-NWS-230"],
                    {"data": [_mrow(1, exchange="NYSE")]},
                    expect={"items": [], "marketaux": {"status": "EMPTY_RESULT", "acceptedCount": 0, "returnedCount": 0, "rejectedCount": 1,
                                                       "rejectionCounts": {"ENTITY_EXCHANGE_MISMATCH": 1}, "fallbackReason": "HIGHER_PRIORITY_EMPTY",
                                                       "attempted": True, "allowedAction": "none"},
                            "status": "NOT_FOUND", "warnings": []}),
    _marketaux_case("news.marketaux.entity_with_blank_exchange_skips_the_exchange_check", ["BR-NWS-229", "BR-NWS-230"],
                    {"data": [_mrow(1, exchange="")]}, expect=_MX_OK),
    # Card 230: every rejection reason once; one request returns at most 3 rows. Both runtimes keep reading after the third
    # accepted row (the Worker since 2.20.0, R-news Batch C decision 12; it stopped at the limit before), so acceptedCount is 4
    # and the MSFT row after the limit is still counted as ENTITY_SYMBOL_MISMATCH.
    _marketaux_case(
        "news.marketaux.row_rejection_reasons_and_accepted_count_cover_rows_past_the_3_row_limit", ["BR-NWS-229", "BR-NWS-230"],
        {"data": ["junk", _mrow(2, title=""), _mrow(3, url="http://www.prnewswire.com/x"), _mrow(4, url="https://evil.example.com/x"),
                  _mrow(5, symbol="MSFT"), _mrow(6, exchange="NYSE"), _mrow(7, days=40), _mrow(8), _mrow(9), _mrow(10), _mrow(11),
                  _mrow(12, symbol="MSFT")]},
        expect={"items": [[f"AAPL wire headline {i}", "PROVIDER_ENTITY_SYMBOL", "LOW", "HIGH", "Apple Inc."] for i in (8, 9, 10)],
                "marketaux": {"status": "OK", "acceptedCount": 4, "returnedCount": 3, "rejectedCount": 8,
                              "rejectionCounts": {"INVALID_ROW": 1, "MISSING_HEADLINE": 1, "INVALID_URL": 1, "DOMAIN_NOT_ALLOWED": 1,
                                                  "ENTITY_SYMBOL_MISMATCH": 2, "ENTITY_EXCHANGE_MISMATCH": 1, "DATE_OUTSIDE_WINDOW": 1},
                              "fallbackReason": "HIGHER_PRIORITY_EMPTY", "attempted": True, "allowedAction": "check_official_releases"},
                "status": None, "warnings": []}),
    _marketaux_case("news.marketaux.missing_token_is_unconfigured_and_not_attempted", ["BR-NWS-230"], {"data": [_mrow(1)]}, env={},
                    expect=_mx_failed("UNCONFIGURED", "configure_marketaux_api_token",
                                      "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it.", attempted=False)),
    _marketaux_case("news.marketaux.non_list_data_is_provider_changed", ["BR-NWS-230"], {"x": 1},
                    expect=_mx_failed("PROVIDER_CHANGED", "inspect_provider_contract", "Marketaux provider changed: expected a data array.")),
    # Card 230: 401/403 -> AUTH_ERROR, 402/429 -> RATE_LIMITED, others PROVIDER_ERROR. The warning text is one template in both
    # runtimes since 2.20.0 (decision 12): 'returned HTTP <n> (<STATUS>).' (the Worker said 'unavailable (<STATUS>).').
    _marketaux_case("news.marketaux.http_401_is_auth_error_with_the_http_status_message", ["BR-NWS-230"], None, status=401, body="nope",
                    expect=_mx_failed("AUTH_ERROR", "verify_marketaux_api_token", "Marketaux wire fallback returned HTTP 401 (AUTH_ERROR).")),
    _marketaux_case("news.marketaux.http_402_is_rate_limited_with_the_http_status_message", ["BR-NWS-230"], None, status=402, body="quota",
                    expect=_mx_failed("RATE_LIMITED", "retry_after_quota_reset", "Marketaux wire fallback returned HTTP 402 (RATE_LIMITED).")),
    _marketaux_case("news.marketaux.http_500_is_provider_error_with_the_http_status_message", ["BR-NWS-230"], None, status=500, body="boom",
                    expect=_mx_failed("PROVIDER_ERROR", "retry_or_use_official_sources", "Marketaux wire fallback returned HTTP 500 (PROVIDER_ERROR).")),
    # R-news D (2.22.0, F13a): a Marketaux 404 is PROVIDER_ERROR in both runtimes; a provider 404 is not evidence of absence (the
    # Worker reported NOT_FOUND through the shared providerHttpStatus, which other providers still use).
    _marketaux_case("news.marketaux.http_404_is_provider_error_with_the_http_status_message", ["BR-NWS-230"], None, status=404, body="missing",
                    expect=_mx_failed("PROVIDER_ERROR", "retry_or_use_official_sources", "Marketaux wire fallback returned HTTP 404 (PROVIDER_ERROR).")),
    # R-news D (2.22.0, F13b): a non-object JSON body is PROVIDER_CHANGED in both runtimes (the Worker said PROVIDER_ERROR), with
    # Python's data-array message for the same shape failure (RNEWS_D_SPEC planner decision 1).
    _marketaux_case("news.marketaux.non_object_body_is_provider_changed", ["BR-NWS-230"], [1, 2],
                    # R-news D F13b (2.22.0): PROVIDER_CHANGED in both runtimes, with the data-array message (planner decision 1).
                    expect=_mx_failed("PROVIDER_CHANGED", "inspect_provider_contract", "Marketaux provider changed: expected a data array.")),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Coverage, source reporting, warning de-duplication, source list normalisation, verification
# BR-NWS-196 / 197 / 238 / 242 / 269 / 270
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_VOD_INFO = {"shortName": "Vodafone Group", "longName": "Vodafone Group Plc", "exchange": "LSE"}
_VOD_FEED = [_yn("VOD.L rallies on results", when=_days_ago(1))]


def _coverage_report(v):
    return {"sourceCoverage": v["sourceCoverage"], "state": v["coverage"]["state"], "action": v["coverage"]["recommendedNextAction"],
            "sourceStatus": {k: s.get("status") for k, s in v["sourceStatus"].items()}, "status": v.get("status"),
            "skipped": [[s["source"], s["status"], s["reasonCode"]] for s in v["coverage"]["skippedSources"]],
            "failed": [[s["source"], s["status"]] for s in v["coverage"]["failedSources"]]}


async def _with_unreadable_finnhub_policy(call):
    """Run the Python tool coroutine with the news-source policy file unreadable (the Worker's copy is compiled in)."""
    with patch.object(news_tools, "_NEWS_SOURCE_CAPABILITIES_PATH", Path("/nonexistent/news-source-capabilities.json")):
        news_tools._finnhub_policy.cache_clear()
        try:
            return await call()
        finally:
            news_tools._finnhub_policy.cache_clear()


CASES += [
    # R-news Batch C, decision 14: an unreadable policy file fails closed on the news path too. Finnhub is NOT_ELIGIBLE with the
    # bundled policy's reasonCode and is never called, as in the Worker (whose policy is compiled in).
    _scenario(
        "news.coverage.unreadable_finnhub_policy_fails_closed_as_not_eligible", ["BR-NWS-180", "BR-NWS-196"],
        lambda: _with_unreadable_finnhub_policy(lambda: server.get_company_news("VOD.L", 10, 14, ["yahoo_finance_news", "finnhub"])),
        'm.yahooFinance.getCompanyNews("VOD.L", 10, 14, ["yahoo_finance_news", "finnhub"])',
        news=_VOD_FEED, info=_VOD_INFO, env=FINNHUB_ENV, select=_coverage_report,
        expect={"sourceCoverage": "PARTIAL", "state": "PARTIAL", "action": "CHECK_OFFICIAL_RELEASES",
                "sourceStatus": {"yahoo_finance_news": "OK", "finnhub": "NOT_ELIGIBLE"}, "status": "PARTIAL",
                "skipped": [["finnhub", "NOT_ELIGIBLE", "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT"]], "failed": []}),
    # Card 196: Yahoo OK + Finnhub NOT_ELIGIBLE -> PARTIAL (NOT_ELIGIBLE limits coverage); the next action is CHECK_OFFICIAL_RELEASES.
    _scenario(
        "news.coverage.not_eligible_source_makes_coverage_partial", ["BR-NWS-196", "BR-NWS-192"],
        lambda: server.get_company_news("VOD.L", 10, 14, ["yahoo_finance_news", "finnhub"]),
        'm.yahooFinance.getCompanyNews("VOD.L", 10, 14, ["yahoo_finance_news", "finnhub"])',
        news=_VOD_FEED, info=_VOD_INFO, env=FINNHUB_ENV, select=_coverage_report,
        expect={"sourceCoverage": "PARTIAL", "state": "PARTIAL", "action": "CHECK_OFFICIAL_RELEASES",
                "sourceStatus": {"yahoo_finance_news": "OK", "finnhub": "NOT_ELIGIBLE"}, "status": "PARTIAL",
                "skipped": [["finnhub", "NOT_ELIGIBLE", "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT"]], "failed": []}),
    # Card 196 edge: NOT_NEEDED (Marketaux skipped because Yahoo found items) keeps coverage FULL.
    _scenario(
        "news.coverage.not_needed_marketaux_keeps_coverage_full", ["BR-NWS-196", "BR-NWS-192"],
        lambda: server.get_company_news("VOD.L", 10, 14, ["yahoo_finance_news", "marketaux"]),
        'm.yahooFinance.getCompanyNews("VOD.L", 10, 14, ["yahoo_finance_news", "marketaux"])',
        news=_VOD_FEED, info=_VOD_INFO, env=MARKETAUX_ENV, select=_coverage_report,
        expect={"sourceCoverage": "FULL", "state": "FULL", "action": "USE_RETURNED_CONTEXT",
                "sourceStatus": {"yahoo_finance_news": "OK", "marketaux": "NOT_NEEDED"}, "status": None,
                "skipped": [["marketaux", "NOT_NEEDED", "HIGHER_PRIORITY_EVIDENCE_FOUND"]], "failed": []}),
    # Card 196 edge: EMPTY_RESULT keeps coverage FULL (and the collection status is NOT_FOUND).
    _scenario(
        "news.coverage.empty_result_keeps_coverage_full", ["BR-NWS-196", "BR-NWS-214"],
        lambda: server.get_company_news("VOD.L", 10, 14, ["yahoo_finance_news"]),
        'm.yahooFinance.getCompanyNews("VOD.L", 10, 14, ["yahoo_finance_news"])',
        news=[], info=_VOD_INFO, select=_coverage_report,
        expect={"sourceCoverage": "FULL", "state": "FULL", "action": "USE_RETURNED_CONTEXT",
                "sourceStatus": {"yahoo_finance_news": "EMPTY_RESULT"}, "status": "NOT_FOUND", "skipped": [], "failed": []}),
    # Card 269: sources omitted -> requestedSources null, defaultSourcesApplied true; attemptedSources lists only the sources that
    # were tried (Finnhub NOT_ELIGIBLE and an unconfigured Marketaux are left out).
    _scenario(
        "news.sources.omitted_sources_report_defaults_and_only_attempted_sources", ["BR-NWS-269", "BR-NWS-196"],
        lambda: server.get_company_news("VOD.L", 10, 14),
        'm.yahooFinance.getCompanyNews("VOD.L", 10, 14)',
        news=_VOD_FEED, info=_VOD_INFO,
        select=lambda v: {"requested": v["requestedSources"], "defaultApplied": v["defaultSourcesApplied"], "attempted": v["attemptedSources"],
                          "sourceStatus": {k: s.get("status") for k, s in v["sourceStatus"].items()}},
        # 2.16.0 (RNEWS2 §4.1): the defaults include sec; VOD.L has no CIK, so sec is attempted and reports PROVIDER_ERROR.
        expect={"requested": None, "defaultApplied": True,
                   "attempted": [{"sourceType": "sec", "status": "PROVIDER_ERROR"}, {"sourceType": "yahoo_finance_news", "status": "OK"},
                                 {"sourceType": "yahoo_finance_press_releases", "status": "EMPTY_RESULT"}],
                   "sourceStatus": {"sec": "PROVIDER_ERROR", "yahoo_finance_news": "OK", "yahoo_finance_press_releases": "EMPTY_RESULT",
                                    "finnhub": "NOT_ELIGIBLE", "marketaux": "UNCONFIGURED"}}),
    # Card 238: names are trimmed and lower-cased, duplicates dropped, unsupported names warned once. ['bloomberg'] alone selects
    # nothing (the defaults are NOT applied: an explicit list is used as given), so the result is source-limited.
    _scenario(
        "news.sources.names_are_normalised_and_unsupported_names_warned_once", ["BR-NWS-238", "BR-NWS-270"],
        lambda: server.get_company_news("AAPL", 10, 14, ["Finnhub", "bloomberg", "finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["Finnhub", "bloomberg", "finnhub"])',
        select=lambda v: {"requested": v["requestedSources"], "sourceStatus": {k: s.get("status") for k, s in v["sourceStatus"].items()},
                          "warnings": _warning_rows(v), "status": v.get("status")},
        expect={"requested": ["Finnhub", "bloomberg", "finnhub"], "sourceStatus": {"finnhub": "UNCONFIGURED"},
                "warnings": [["SOURCE_UNSUPPORTED", "Source 'bloomberg' is not supported."],
                             ["SOURCE_UNAVAILABLE", "Finnhub company-news source is not configured; skipped."]],
                "status": "SOURCE_LIMITED_NOT_FOUND"}),
    _scenario(
        "news.sources.only_unsupported_names_attempt_nothing", ["BR-NWS-238"],
        lambda: server.get_company_news("AAPL", 10, 14, ["bloomberg"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["bloomberg"])',
        select=lambda v: {"sourceStatus": v["sourceStatus"], "warnings": _warning_rows(v), "status": v.get("status"), "attempted": v["attemptedSources"]},
        expect={"sourceStatus": {}, "warnings": [["SOURCE_UNSUPPORTED", "Source 'bloomberg' is not supported."]],
                "status": "SOURCE_LIMITED_NOT_FOUND", "attempted": []}),
    # Card 270: warnings are de-duplicated on code|message: the repeated SOURCE_UNSUPPORTED name and three identical weak-dedupe-key
    # warnings (rows whose headline is only a stop word and which have no date) are kept once; a different message is kept.
    _scenario(
        "news.warnings.identical_code_and_message_pairs_are_kept_once", ["BR-NWS-270", "BR-NWS-238"],
        lambda: server.get_company_news("AAPL", 10, 14, ["bogus", "bogus", "other", "finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["bogus", "bogus", "other", "finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([{"headline": t, "datetime": None} for t in ("Inc", "Corp", "Company")]),
        select=lambda v: {"warnings": _warning_rows(v), "finnhub": _status_rows(v, "finnhub", keys=("status", "rawCount", "acceptedCount", "rejectedCount"))},
        expect={"warnings": [["SOURCE_UNSUPPORTED", "Source 'bogus' is not supported."], ["SOURCE_UNSUPPORTED", "Source 'other' is not supported."],
                             ["DEDUPE_WEAK_KEY", "Weak dedupe key for at least one item."]],
                "finnhub": {"status": "EMPTY_RESULT", "rawCount": 3, "acceptedCount": 0, "rejectedCount": 3}}),
]

# --- verification (BR-NWS-197, BR-NWS-242) -------------------------------------------------------------------------------
_VERIFY_ACTION = ("Inspect sourceStatus and retry with a narrower sources list that excludes failed providers; "
                  "do not treat this result as confirmed absence.")
_VERIFY_LIMITED = ["SOURCE_LIMITED_NOT_FOUND", "One or more selected sources were unavailable; this is not confirmed evidence that the event did not occur."]


def _verify_view(v):
    return {"status": v["status"], "failureMode": v["failureMode"], "retryable": v["retryable"], "action": v["recommendedAction"],
            "sourceStatus": {k: s.get("status") for k, s in v["sourceStatus"].items()}, "warnings": _warning_rows(v),
            "next": v["coverage"]["recommendedNextAction"], "best": len(v["bestEvidence"])}


def _verify_case(name, ids, sources, *, ticker="ACME", env=None, routes=None, query="acquisition", **kw) -> Case:
    return _scenario(name, ids, lambda: server.verify_company_event(ticker, query, "", "", sources),
                     f'm.yahooFinance.verifyCompanyEvent({json.dumps(ticker)}, {json.dumps(query)}, "", "", {json.dumps(sources)})',
                     env=env, routes=routes or {}, select=_verify_view, **kw)


CASES += [
    # Card 197: NOT_FOUND + Finnhub RATE_LIMITED + Marketaux UNCONFIGURED -> SOURCE_LIMITED_NOT_FOUND, failureMode RATE_LIMITED
    # (it outranks SOURCE_UNCONFIGURED), retryable true, with the 'exclude failed providers' recommended action.
    _verify_case(
        "news.verify.rate_limited_provider_gives_retryable_source_limited_not_found", ["BR-NWS-197", "BR-NWS-192"], ["finnhub", "marketaux"],
        env=FINNHUB_ENV, routes=_finnhub(None, 429, "slow down"),
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "RATE_LIMITED", "retryable": True, "action": _VERIFY_ACTION,
                "sourceStatus": {"finnhub": "RATE_LIMITED", "marketaux": "UNCONFIGURED"},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub rate limited: HTTP 429"],
                             ["SOURCE_UNAVAILABLE", "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it."], _VERIFY_LIMITED],
                "next": "RETRY_RETRYABLE_SOURCES", "best": 0}),
    # Card 197 precedence: AUTH_ERROR maps to SOURCE_UNCONFIGURED since 2.19.0 (call #10), as the unconfigured Marketaux does, so the mode
    # is SOURCE_UNCONFIGURED either way. The Finnhub auth-error text is one template in both runtimes since 2.20.0 (decision 11).
    _verify_case(
        "news.verify.auth_error_with_unconfigured_marketaux_is_source_unconfigured", ["BR-NWS-197"], ["finnhub", "marketaux"], env=FINNHUB_ENV,
        routes=_finnhub(None, 401, "bad token"),
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_UNCONFIGURED", "retryable": False, "action": _VERIFY_ACTION,
                "sourceStatus": {"finnhub": "AUTH_ERROR", "marketaux": "UNCONFIGURED"},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub auth error: HTTP 401"],
                             ["SOURCE_UNAVAILABLE", "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it."], _VERIFY_LIMITED],
                "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    _verify_case(
        "news.verify.unconfigured_finnhub_is_source_unconfigured", ["BR-NWS-197"], ["finnhub"],
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_UNCONFIGURED", "retryable": False, "action": _VERIFY_ACTION,
                "sourceStatus": {"finnhub": "UNCONFIGURED"},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub company-news source is not configured; skipped."], _VERIFY_LIMITED],
                "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    _verify_case(
        "news.verify.ineligible_ticker_is_source_not_eligible", ["BR-NWS-197", "BR-NWS-196"], ["finnhub"], ticker="VOD.L", env=FINNHUB_ENV,
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_NOT_ELIGIBLE", "retryable": False, "action": _VERIFY_ACTION,
                "sourceStatus": {"finnhub": "NOT_ELIGIBLE"},
                "warnings": [["SOURCE_NOT_ELIGIBLE", "Finnhub company-news is intentionally skipped for VOD.L under the deployed market capability policy."],
                             _VERIFY_LIMITED],
                "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    _verify_case(
        "news.verify.changed_payload_is_provider_changed", ["BR-NWS-197"], ["finnhub"], env=FINNHUB_ENV, routes=_finnhub({"x": 1}),
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "PROVIDER_CHANGED", "retryable": False, "action": _VERIFY_ACTION,
                "sourceStatus": {"finnhub": "PROVIDER_CHANGED"},
                "warnings": [["SOURCE_UNAVAILABLE", "Finnhub provider changed: unexpected response format"], _VERIFY_LIMITED],
                "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    # Card 197 edge: a company_ir verification with no approved registry entry. company_ir is the approved IR registry only in both
    # runtimes (the Worker since 2.10.0, Python since 2.19.0), so it reads NOT_REGISTERED, which maps to the existing failureMode
    # SOURCE_UNCONFIGURED when company_ir is requested explicitly, as here (call #14, planner decision 7), and the result is
    # SOURCE_LIMITED_NOT_FOUND. Python no longer reads the Yahoo profile for company_ir, so the failed profile lookup is not involved.
    _verify_case(
        "news.verify.unregistered_company_ir_is_source_unconfigured", ["BR-NWS-197"], ["company_ir"], news=[], info=RuntimeError("profile unavailable"),
        identity_fails=True,
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_UNCONFIGURED", "retryable": False, "action": _VERIFY_ACTION,
                "sourceStatus": {"company_ir": "NOT_REGISTERED"}, "warnings": [_VERIFY_LIMITED],
                "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    # Planner decision 7 (2.19.0): the call #14 upgrade needs company_ir in the caller's own sources. With default sources the
    # NOT_REGISTERED company_ir row stays in sourceStatus / coverage and a clean miss is still NOT_FOUND (every other default source
    # answers: SEC empty, Yahoo no match, Finnhub empty, Marketaux empty).
    _scenario(
        "news.verify.default_sources_with_unregistered_company_ir_can_still_be_not_found", ["BR-NWS-197", "BR-NWS-106"],
        lambda: server.verify_company_event("ACME", "acquisition", "", "", None),
        'm.yahooFinance.verifyCompanyEvent("ACME", "acquisition", "", "", null)',
        news=[_yn("Acme opens a plant", when=_days_ago(2))], info={"shortName": "Acme", "longName": "Acme Corp", "exchange": "NMS"},
        env={**FINNHUB_ENV, **MARKETAUX_ENV}, submissions=_py_sec(SEC_CIK, "Acme Corp", _sec_recent([], [])),
        routes={**_sec_routes(SEC_CIK, "ACME", "Acme Corp", _sec_recent([], [])), **_finnhub([]), MARKETAUX_PREFIX: {"json": {"data": []}}},
        select=lambda v: {"status": v["status"], "failureMode": v["failureMode"], "companyIr": v["sourceStatus"].get("company_ir", {}).get("status"),
                          "failed": sorted(f["source"] for f in v["coverage"]["failedSources"])},
        expect={"status": "NOT_FOUND", "failureMode": None, "companyIr": "NOT_REGISTERED", "failed": ["company_ir", "company_ir_page"]}),
    # Decision 7, explicit side: sources=['company_ir'] with no approved entry upgrades NOT_FOUND to SOURCE_LIMITED_NOT_FOUND /
    # SOURCE_UNCONFIGURED in both runtimes.
    _scenario(
        "news.verify.explicit_unregistered_company_ir_is_source_limited", ["BR-NWS-197", "BR-NWS-106"],
        lambda: server.verify_company_event("ACME", "acquisition", "", "", ["company_ir"]),
        'm.yahooFinance.verifyCompanyEvent("ACME", "acquisition", "", "", ["company_ir"])',
        news=[_yn("Acme opens a plant", when=_days_ago(2))], info={"shortName": "Acme", "longName": "Acme Corp", "exchange": "NMS"},
        select=lambda v: {"status": v["status"], "failureMode": v["failureMode"], "companyIr": v["sourceStatus"].get("company_ir", {}).get("status")},
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_UNCONFIGURED", "companyIr": "NOT_REGISTERED"}),
    # Card 242: until 2.17.0 every collected item carried tickers=[ticker], so the relevance score was HIGH at its first test and
    # the low-relevance downgrade could never fire. Since 2.17.0 (B-G12c) relevance reads the evidence text and issuer only; this
    # release headline names the company ('Zeta Platforms', from Yahoo in the Worker and from SEC submissions in Python), so it is
    # still HIGH and CONFIRMED. The downgrade itself is the GlobeNewswire case news.verify.low_relevance_downgrade_fires_without_ticker_or_name.
    _scenario(
        "news.verify.low_relevance_downgrade_never_fires", ["BR-NWS-242"],
        lambda: server.verify_company_event("ZZTP", "acquisition", "", "", ["yahoo_finance_press_releases"]),
        'm.yahooFinance.verifyCompanyEvent("ZZTP", "acquisition", "", "", ["yahoo_finance_press_releases"])',
        news=[_yn("Zeta Platforms announces acquisition of Beta Corp", publisher="Business Wire", when=_days_ago(2))],
        info={"shortName": "Zeta Platforms", "longName": "Zeta Platforms, Inc.", "exchange": "NMS"}, submissions=_py_sec(SEC_CIK, "Zeta Platforms, Inc.", {}),
        select=lambda v: {"status": v["status"], "warnings": _warning_codes(v),
                          "best": [[e["sourceType"], e["confidence"], e["relevance"], e["entityRelevance"], e["queryMatch"]] for e in v["bestEvidence"]]},
        expect={"status": "CONFIRMED", "warnings": [],
                "best": [["yahoo_finance_press_releases", "MEDIUM", "HIGH", "HIGH",
                          {"method": "PHRASE", "matchedTerms": ["acquisition"], "requiredTermMatches": 1}]]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Input validation at the tool boundary (Worker: m.tools.callTool, Python: the server tool function)
# BR-NWS-204 / 205 / 234 / 240 / 241 / 271 / 275 / 279
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _call_tool(tool, args):
    return f"m.tools.callTool({json.dumps(tool)}, {json.dumps(args)})"


def _tool_outcome(v):
    """['error', message] for a validation failure, ['ran', ticker] for a single result, {'batch': {ticker: status}} for a batch."""
    d = _unwrap(v)
    if "error" in d and "ticker" not in d:
        return [d["error"], d["message"]]
    if "ticker" in d:
        return ["ran", d["ticker"]]
    return {"batch": {k: [x.get("ticker"), x.get("status")] for k, x in d.items()}}


_INVALID_TICKER_MSG = "Invalid ticker symbol: '{}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="
_NO_NEWS_STATUS = "SOURCE_LIMITED_NOT_FOUND"
_SIX_TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOG"]
_VALIDATION_ROUTES = {SEC_TICKERS_URL: {"json": {}}}


async def _gated_tool(name, args):
    """The registered Python tool, i.e. through the tool-call gate (yfmcp.app wrapper); returns the envelope text."""
    return await registered_tool_fn(name)(**args)


def _alias_py():
    """get_yahoo_finance_news is not a Python tool any more (R-news Batch C, B-G19): no server function, no registration. The
    Python SDK answers an unregistered name with its own unknown-tool error, reduced here to the Worker's dispatcher outcome."""
    assert not hasattr(server, "get_yahoo_finance_news")
    try:
        registered_tool_fn("get_yahoo_finance_news")
    except RuntimeError as exc:
        assert "is not registered" in str(exc)
        return {"error": "INPUT_VALIDATION_ERROR", "message": "Unknown tool name."}
    raise AssertionError("get_yahoo_finance_news is still registered")


CASES += [
    # Cards 205 / 234 / 279 / 241 / 275: an empty batch or an empty symbol, and a blank query, fail with the same message in both
    # runtimes; a valid batch runs per ticker; a lower-case ticker passes the format check (upper-cased in the result).
    _multi_scenario(
        "news.validation.empty_symbols_blank_queries_and_valid_batches_agree", ["BR-NWS-205", "BR-NWS-234", "BR-NWS-279", "BR-NWS-241", "BR-NWS-275"],
        [("empty_symbol", lambda: server.get_company_news(["AAPL", ""], 10, 14, []), _call_tool("get_company_news", {"ticker": ["AAPL", ""], "sources": []})),
         ("empty_list", lambda: server.get_company_news([], 10, 14, []), _call_tool("get_company_news", {"ticker": [], "sources": []})),
         ("valid_batch", lambda: server.get_company_news(["AAPL", "MSFT"], 10, 14, []), _call_tool("get_company_news", {"ticker": ["AAPL", "MSFT"], "sources": []})),
         ("lower_case_ticker", lambda: server.get_company_news("brk.b", 10, 14, []), _call_tool("get_company_news", {"ticker": "brk.b", "sources": []})),
         ("blank_search_query", lambda: server.search_company_news("AAPL", "   ", "", "", []),
          _call_tool("search_company_news", {"ticker": "AAPL", "query": "   ", "sources": []})),
         ("blank_verify_query", lambda: server.verify_company_event("AAPL", " ", "", "", []),
          _call_tool("verify_company_event", {"ticker": "AAPL", "event_query": " ", "sources": []}))],
        news=[], info={}, routes=_VALIDATION_ROUTES, select=_tool_outcome,
        expect={"empty_symbol": ["INPUT_VALIDATION_ERROR", "ticker is required and must not contain empty symbols."],
                "empty_list": ["INPUT_VALIDATION_ERROR", "ticker is required and must not contain empty symbols."],
                "valid_batch": {"batch": {"AAPL": ["AAPL", _NO_NEWS_STATUS], "MSFT": ["MSFT", _NO_NEWS_STATUS]}},
                "lower_case_ticker": ["ran", "BRK.B"],
                "blank_search_query": ["INPUT_VALIDATION_ERROR", "query is required"],
                "blank_verify_query": ["INPUT_VALIDATION_ERROR", "event_query is required"]}),
    # Cards 240 / 241 (Q3b, 2.12.0): the ticker format (after trim and upper-case; 1-20 characters of A-Z 0-9 . - ^ =) is
    # checked in both runtimes for all six news and event tools, per symbol in the batch path, echoing the raw value.
    _multi_scenario(
        "news.validation.ticker_format_is_checked_in_both_runtimes", ["BR-NWS-240", "BR-NWS-241"],
        [("get_company_news_dollar", lambda: server.get_company_news("AAPL$", 10, 14, []), _call_tool("get_company_news", {"ticker": "AAPL$", "sources": []})),
         ("get_company_news_21_chars", lambda: server.get_company_news("A" * 21, 10, 14, []), _call_tool("get_company_news", {"ticker": "A" * 21, "sources": []})),
         ("search_company_news_space", lambda: server.search_company_news("AAPL US!", "dividend", "", "", []),
          _call_tool("search_company_news", {"ticker": "AAPL US!", "query": "dividend", "sources": []})),
         ("verify_company_event_dollar", lambda: server.verify_company_event("AAPL$", "dividend", "", "", []),
          _call_tool("verify_company_event", {"ticker": "AAPL$", "event_query": "dividend", "sources": []})),
         ("get_company_press_releases_dollar", lambda: server.get_company_press_releases("AAPL$", 90, 20, []),
          _call_tool("get_company_press_releases", {"ticker": "AAPL$", "sources": []})),
         ("get_public_event_timeline_dollar", lambda: server.get_public_event_timeline("AAPL$", "", "", [], 50, False),
          _call_tool("get_public_event_timeline", {"ticker": "AAPL$", "sources": []})),
         ("get_sec_recent_events_dollar", lambda: server.get_sec_recent_events("AAPL$", ["8-K"], 90, 20),
          _call_tool("get_sec_recent_events", {"ticker": "AAPL$", "filing_types": ["8-K"]}))],
        news=[], info={}, routes=_VALIDATION_ROUTES, select=_tool_outcome,
        expect={"get_company_news_dollar": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("AAPL$")],
                   "get_company_news_21_chars": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("A" * 21)],
                   "search_company_news_space": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("AAPL US!")],
                   "verify_company_event_dollar": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("AAPL$")],
                   "get_company_press_releases_dollar": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("AAPL$")],
                   "get_public_event_timeline_dollar": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("AAPL$")],
                "get_sec_recent_events_dollar": ["INPUT_VALIDATION_ERROR", _INVALID_TICKER_MSG.format("AAPL$")]}),
    # Card 204 (Q3c, 2.12.0): six tickers is over get_company_news's cap of 5; the tool-call gate rejects it with
    # BATCH_LIMIT_EXCEEDED in both runtimes (Python through the registered, gated tool wrapper), before any fetch.
    _multi_scenario(
        "news.validation.batch_of_six_tickers_is_rejected_over_the_cap_in_both_runtimes", ["BR-NWS-204", "BR-NWS-275"],
        [("six", lambda: _gated_tool("get_company_news", {"ticker": _SIX_TICKERS, "sources": []}), _call_tool("get_company_news", {"ticker": _SIX_TICKERS, "sources": []})),
         ("five", lambda: _gated_tool("get_company_news", {"ticker": _SIX_TICKERS[:5], "sources": []}), _call_tool("get_company_news", {"ticker": _SIX_TICKERS[:5], "sources": []}))],
        news=[], info={}, select=_tool_outcome,
        expect={"six": ["BATCH_LIMIT_EXCEEDED", "Too many tickers: 6. Maximum is 5 per call for get_company_news."],
                "five": {"batch": {t: [t, _NO_NEWS_STATUS] for t in _SIX_TICKERS[:5]}}}),
    # Card 271 / B-G19: get_yahoo_finance_news (the old Python alias) is gone; both runtimes treat it as an unknown tool.
    Case(name="news.validation.get_yahoo_finance_news_is_an_unknown_tool_in_both_runtimes", ids=["BR-NWS-271"],
         py=lambda: _alias_py(),
         ts={"js": _worker_js(f"return JSON.parse(await {_call_tool('get_yahoo_finance_news', {'ticker': 'AAPL'})});"), "isolate": True},
         routes=_yahoo_routes([], {}),
         select=_unwrap,
         expect={"error": "INPUT_VALIDATION_ERROR", "message": "Unknown tool name."}),
]



# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Provider failure -> recommended action, entitlement classification, remembered denials (ownership tool)
# BR-NWS-191 / 193 / 233
# The expanded-ownership tool shares the provider client (fetch_finnhub_json / fetch_alpha_vantage_json) with the news providers.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

FINNHUB_OWNERSHIP = "https://finnhub.io/api/v1/stock/ownership?symbol="
ALPHA_QUERY = "https://www.alphavantage.co/query?*"
ALPHA_ENV = {"ALPHA_VANTAGE_API_KEY": FAKE_ALPHA_KEY}


def _ownership_view(v):
    d = v.get("data") if isinstance(v.get("data"), dict) else v
    out = []
    for a in d.get("providerAttempts", []):
        remembered = re.search(r"remembered until (\S+?);", str(a.get("message") or ""))
        out.append([a.get("provider"), a.get("status"), a.get("attempted"), a.get("cacheStatus"), a.get("httpStatus"),
                    _norm_ts(remembered.group(1)) if remembered else None])
    return {"status": d.get("status"), "action": d.get("recommendedNextAction"), "alphaFallbackAllowed": d.get("alphaFallbackAllowed"), "attempts": out}


def _ownership_calls_case(name, ids, tickers, routes, env, *, allow=False, hours=None, **kw) -> Case:
    """Sequential ownership calls (Worker clock set to `hours[i]` o'clock, 2026-10-15 UTC, before call i)."""
    def py():
        out = []
        with _py_news_env(env=env):
            for ticker in tickers:
                out.append(json.loads(asyncio.run(provider_gaps.get_expanded_institutional_ownership(ticker, allow, 50))))
        return out

    hours = hours or [12] * len(tickers)
    body = "const out = [];\n" + "\n".join(
        f"__now = Date.UTC(2026, 9, 15, {h}, 0, 0);\n"
        f"out.push(JSON.parse(await m.yahooFinance.getExpandedInstitutionalOwnership({json.dumps(t)}, {json.dumps(allow)}, 50)));"
        for t, h in zip(tickers, hours)) + "\nreturn out;"
    return Case(name=name, ids=ids, py=py, ts={"js": _worker_js(body, env), "isolate": True}, routes=routes,
                select=lambda v: [_ownership_view(x) for x in v], **kw)


_PREMIUM_BODY = json.dumps({"error": "You don't have access to this resource."})

CASES += [
    # Card 191 / 233: HTTP 403 whose body says the caller has no access to the resource is ENTITLEMENT_REQUIRED; with the scarce
    # fallback off the recommended action is USE_ALPHA_VANTAGE_EXPLICITLY.
    _ownership_calls_case(
        "news.provider.entitlement_required_recommends_alpha_when_fallback_is_off", ["BR-NWS-191", "BR-NWS-233"], ["AAA1"],
        {FINNHUB_OWNERSHIP + "*": {"status": 403, "body": _PREMIUM_BODY}}, FINNHUB_ENV,
        expect=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "ENTITLEMENT_REQUIRED", True, "MISS", 403, None]]}]),
    # Card 191: with the fallback on, Alpha's own status drives the action; ENTITLEMENT_REQUIRED there is CHECK_PROVIDER_ENTITLEMENT.
    _ownership_calls_case(
        "news.provider.alpha_entitlement_with_fallback_on_recommends_checking_the_plan", ["BR-NWS-191"], ["AAA1"],
        {FINNHUB_OWNERSHIP + "*": {"status": 403, "body": _PREMIUM_BODY},
         ALPHA_QUERY: {"json": {"Information": "This is a premium endpoint; subscribe"}}}, {**FINNHUB_ENV, **ALPHA_ENV}, allow=True,
        expect=[{"status": "ENTITLEMENT_REQUIRED", "action": "CHECK_PROVIDER_ENTITLEMENT", "alphaFallbackAllowed": True,
                 "attempts": [["finnhub", "ENTITLEMENT_REQUIRED", True, "MISS", 403, None],
                              ["alpha_vantage", "ENTITLEMENT_REQUIRED", True, "MISS", None, None]]}]),
    # Card 191 parameter table: one status per ticker (AUTH_ERROR last, since the Worker remembers it for the endpoint).
    _ownership_calls_case(
        "news.provider.failure_status_to_recommended_action_table", ["BR-NWS-191", "BR-NWS-233"], ["RATE1", "NF1", "SRV1", "EMPTY1", "AUTH1"],
        {FINNHUB_OWNERSHIP + "RATE1*": {"status": 429, "body": json.dumps({"error": "slow down"})},
         FINNHUB_OWNERSHIP + "NF1*": {"status": 404, "body": json.dumps({"error": "nope"})},
         FINNHUB_OWNERSHIP + "SRV1*": {"status": 500, "body": json.dumps({"error": "boom"})},
         FINNHUB_OWNERSHIP + "EMPTY1*": {"json": {"data": [], "symbol": "EMPTY1"}},
         FINNHUB_OWNERSHIP + "AUTH1*": {"status": 403, "body": json.dumps({"error": "forbidden"})}}, FINNHUB_ENV,
        expect=[{"status": "RATE_LIMIT", "action": "RETRY_AFTER_PROVIDER_RESET", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "RATE_LIMIT", True, "MISS", 429, None]]},
                {"status": "NOT_FOUND", "action": "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "NOT_FOUND", True, "MISS", 404, None]]},
                {"status": "PROVIDER_ERROR", "action": "RETRY_OR_USE_PRIMARY_PROVIDER", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "PROVIDER_ERROR", True, "MISS", 500, None]]},
                {"status": "NOT_FOUND", "action": "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "OK", True, "MISS", None, None]]},
                {"status": "AUTH_ERROR", "action": "CHECK_PROVIDER_CREDENTIALS", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "AUTH_ERROR", True, "MISS", 403, None]]}]),
    _ownership_calls_case(
        "news.provider.missing_key_is_unconfigured_and_not_attempted", ["BR-NWS-191"], ["AAA1"], {}, {},
        expect=[{"status": "SOURCE_UNCONFIGURED", "action": "CONFIGURE_PROVIDER", "alphaFallbackAllowed": False,
                 "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, "MISS", None, None]]}]),
    # Card 233: a body that only says 'current plan' is ENTITLEMENT_REQUIRED in both runtimes (R-news Batch C, B-G10).
    _ownership_calls_case(
        "news.provider.current_plan_body_is_entitlement_required", ["BR-NWS-233", "BR-NWS-191"], ["AAA1"],
        {FINNHUB_OWNERSHIP + "*": {"status": 403, "body": json.dumps({"error": "Your current plan does not include this."})}}, FINNHUB_ENV,
        expect=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "alphaFallbackAllowed": False,
                    "attempts": [["finnhub", "ENTITLEMENT_REQUIRED", True, "MISS", 403, None]]}]),
    # Card 193: the Worker remembers an entitlement denial per endpoint for 6 hours: the second call (a different ticker, two
    # hours later) is not sent; it reports attempted false, cacheStatus HIT_PROCESS and 'remembered until 16:00Z'. Python
    # keeps no denial memory and tries Finnhub again.
    _ownership_calls_case(
        "news.provider.entitlement_denial_is_remembered_for_six_hours_in_the_worker_only", ["BR-NWS-193", "BR-NWS-233"], ["AAA1", "AAA2"],
        {FINNHUB_OWNERSHIP + "*": {"status": 403, "body": _PREMIUM_BODY}}, FINNHUB_ENV, hours=[10, 12], mode="pinned",
        decision="FINDING: Worker remembers a Finnhub entitlement denial (HIT_PROCESS, attempted false, deniedUntil +6h); Python retries the provider on every call",
        expect_py=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "alphaFallbackAllowed": False,
                    "attempts": [["finnhub", "ENTITLEMENT_REQUIRED", True, "MISS", 403, None]]}] * 2,
        expect_ts=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "alphaFallbackAllowed": False,
                    "attempts": [["finnhub", "ENTITLEMENT_REQUIRED", True, "MISS", 403, None]]},
                   {"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "alphaFallbackAllowed": False,
                    "attempts": [["finnhub", "ENTITLEMENT_REQUIRED", False, "HIT_PROCESS", 403, "2026-10-15T16:00:00Z"]]}]),
    # Card 193: a 'per day' rate-limit message from Alpha (sent with HTTP 200) is remembered until the next UTC midnight.
    _ownership_calls_case(
        "news.provider.alpha_daily_limit_is_remembered_until_utc_midnight_in_the_worker_only", ["BR-NWS-193"], ["AAA1", "AAA2"],
        {ALPHA_QUERY: {"json": {"Note": "Our standard API rate limit is 25 requests per day."}}}, ALPHA_ENV, allow=True, hours=[10, 12], mode="pinned",
        decision="FINDING: Worker remembers Alpha's daily rate limit until 00:00Z (HIT_PROCESS, attempted false); Python retries Alpha on every call",
        expect_py=[{"status": "RATE_LIMIT", "action": "RETRY_AFTER_PROVIDER_RESET", "alphaFallbackAllowed": True,
                    "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, "MISS", None, None],
                                 ["alpha_vantage", "RATE_LIMIT", True, "MISS", None, None]]}] * 2,
        expect_ts=[{"status": "RATE_LIMIT", "action": "RETRY_AFTER_PROVIDER_RESET", "alphaFallbackAllowed": True,
                    "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, "MISS", None, None],
                                 ["alpha_vantage", "RATE_LIMIT", True, "MISS", None, None]]},
                   {"status": "RATE_LIMIT", "action": "RETRY_AFTER_PROVIDER_RESET", "alphaFallbackAllowed": True,
                    "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, "MISS", None, None],
                                 ["alpha_vantage", "RATE_LIMIT", False, "HIT_PROCESS", None, "2026-10-16T00:00:00Z"]]}]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Analyst valuation evidence from news: broker attribution, subject attribution, methods, de-duplication, tool inputs
# BR-NWS-200 / 201 / 206 / 226 / 232 / 239 / 259 / 274
# Python yfmcp.capital_structure.analyst_valuation_methods; Worker capital-structure.ts analystValuationMethods.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _news_item(title, summary="", url=None, **kw):
    return {"title": title, "summary": summary, "url": url, "publishedAt": "2026-10-10T10:00:00Z", "originalSource": "Wire", **kw}


def _avm_case(name, ids, ticker, items, changes, names, *, view, **kw) -> Case:
    return Case(
        name=name, ids=ids,
        py=lambda: cs.analyst_valuation_methods(ticker, copy.deepcopy(items), copy.deepcopy(changes), copy.deepcopy(names)),
        ts={"js": "return m.capitalStructure.analystValuationMethods(args[0], args[1], args[2], args[3]);",
            "args": [ticker, items, changes, names]},
        select=view, **kw)


def _avm_view(d):
    return {"attribution": d["attribution"], "status": d["status"], "scanned": d["itemsScanned"], "methodCounts": d["methodCounts"],
            "evidence": [[e["firm"], e["subjectMatch"], e["priceTarget"], [m["method"] for m in e["methods"]], e["sentence"]] for e in d["evidence"]],
            "methodNotDisclosed": [[m["firm"], m["source"]] for m in d["methodNotDisclosed"]]}


_NO_CHECK = {"checked": False, "rejectedForOtherCompany": 0, "subjectAliases": []}

CASES += [
    # Card 206: rating-change firms are tried first; the earliest firm in the text wins ('Cantor Fitzgerald', not 'Benchmark');
    # firm names are case-sensitive, so a lower-case 'benchmark' names no firm and yields no evidence.
    _avm_case(
        "news.valuation.firm_is_the_earliest_match_and_matching_is_case_sensitive", ["BR-NWS-206"], "RKLB",
        [_news_item("Cantor Fitzgerald and Benchmark lift price target to $60 from $50", url="https://e.com/1"),
         _news_item("Analyst benchmark raises price target to $70", url="https://e.com/2")],
        [{"firm": "Cantor Fitzgerald", "ptTo": 60, "ptFrom": 50, "date": "2026-10-10"}], None, view=_avm_view,
        expect={"attribution": _NO_CHECK, "status": "METHOD_NOT_DISCLOSED", "scanned": 2, "methodCounts": {},
                "evidence": [["Cantor Fitzgerald", "NOT_CHECKED", {"target": 60, "prior": 50, "currency": "USD"}, [],
                              "Cantor Fitzgerald and Benchmark lift price target to $60 from $50"]],
                "methodNotDisclosed": [["Cantor Fitzgerald", "news"], ["Cantor Fitzgerald", "rating_changes"]]}),
    # Cards 232 / 239: the sentence names the subject (Rocket Lab) but the clause holding the target names another company, so
    # the item is conflicted and the target is rejected (rejectedForOtherCompany +1).
    _avm_case(
        "news.valuation.target_in_another_companys_clause_is_rejected", ["BR-NWS-232", "BR-NWS-239"], "RKLB",
        [_news_item("Rocket Lab climbs; AST SpaceMobile rises as Cantor Fitzgerald reiterates $60 price target", url="https://e.com/3")],
        [], ["Rocket Lab"], view=_avm_view,
        expect={"attribution": {"checked": True, "rejectedForOtherCompany": 1, "subjectAliases": ["rklb", "rocket lab"]},
                "status": "NOT_FOUND", "scanned": 1, "methodCounts": {}, "evidence": [], "methodNotDisclosed": []}),
    _avm_case(
        "news.valuation.target_clause_names_the_other_company_when_the_subject_is_asts", ["BR-NWS-239"], "ASTS",
        [_news_item("Rocket Lab climbs as Cantor Fitzgerald reiterates $122 price target; AST SpaceMobile rises", url="https://e.com/4")],
        [], ["AST SpaceMobile, Inc."], view=_avm_view,
        expect={"attribution": {"checked": True, "rejectedForOtherCompany": 1, "subjectAliases": ["ast spacemobile", "ast spacemobile inc", "asts"]},
                "status": "NOT_FOUND", "scanned": 1, "methodCounts": {}, "evidence": [], "methodNotDisclosed": []}),
    # Cards 232 / 239: CLAUSE vs ITEM vs rejected. A sentence that names the subject outside the target clause is rejected;
    # a sentence naming nobody is accepted as ITEM when the item names the subject elsewhere and is not conflicted.
    _avm_case(
        "news.valuation.subject_match_clause_item_or_rejected", ["BR-NWS-232", "BR-NWS-239"], "RKLB",
        [_news_item("Rocket Lab climbs; Cantor Fitzgerald reiterates $60 price target", url="https://e.com/5"),
         _news_item("Rocket Lab USA, Inc. report", "Cantor Fitzgerald lifts price target to $30. Other sentence DCF with 8% WACC.", url="https://e.com/6"),
         _news_item("Cantor Fitzgerald sets price target at $40", url="https://e.com/7")],
        [], ["Rocket Lab USA, Inc.", "Rocket Lab"], view=_avm_view,
        expect={"attribution": {"checked": True, "rejectedForOtherCompany": 2, "subjectAliases": ["rklb", "rocket lab", "rocket lab usa", "rocket lab usa inc"]},
                "status": "FOUND", "scanned": 3, "methodCounts": {"DCF": 1},
                "evidence": [["Cantor Fitzgerald", "ITEM", {"target": 30, "prior": None, "currency": "USD"}, [], "Cantor Fitzgerald lifts price target to $30."],
                             ["Cantor Fitzgerald", "ITEM", None, ["DCF"], "Other sentence DCF with 8% WACC."]],
                "methodNotDisclosed": []}),
    # Card 232 edge: the short name 'Rocket Lab' is an alias only when it is given as a name; 'Rocket Lab USA, Inc.' alone keeps
    # 'rocket lab usa' (USA is not a legal suffix).
    _avm_case(
        "news.valuation.aliases_drop_legal_suffixes_and_ticker_base_only", ["BR-NWS-232", "BR-NWS-226"], "RKLB",
        [], [], ["Rocket Lab USA, Inc."], view=lambda d: d["attribution"],
        expect={"checked": True, "rejectedForOtherCompany": 0, "subjectAliases": ["rklb", "rocket lab usa", "rocket lab usa inc"]}),
    _avm_case(
        "news.valuation.one_and_two_letter_tickers_are_not_aliases", ["BR-NWS-232"], "GM",
        [], [], ["General Motors Company"], view=lambda d: d["attribution"],
        expect={"checked": True, "rejectedForOtherCompany": 0, "subjectAliases": ["general motors", "general motors company"]}),
    # Card 259: qualitative method keywords.
    _avm_case(
        "news.valuation.qualitative_method_keywords", ["BR-NWS-259"], "XYZ",
        [_news_item("Valuation is a SOTP with peer multiple cross-check", url="https://e.com/8"),
         _news_item("rNPV and scenario-weighted valuation, comps and comparable companies", url="https://e.com/9")],
        [], None, view=lambda d: [[e["sentence"], [m["method"] for m in e["methods"]]] for e in d["evidence"]],
        expect=[["Valuation is a SOTP with peer multiple cross-check", ["sum_of_the_parts", "peer_comparison"]],
                ["rNPV and scenario-weighted valuation, comps and comparable companies",
                 ["risk_adjusted_npv", "probability_weighted_scenarios", "peer_comparison"]]]),
    # Card 274: an item with the same URL as an earlier one is skipped; when the summary does not start with the title the text
    # is 'title. summary', otherwise just the summary; an item with no URL is keyed by its title.
    _avm_case(
        "news.valuation.duplicate_urls_are_skipped_and_text_assembly", ["BR-NWS-274"], "XYZ",
        [_news_item("Cantor Fitzgerald sets price target at $40", url="https://e.com/7"),
         _news_item("Cantor Fitzgerald sets price target at $40", url="https://e.com/7"),
         _news_item("Title text here", "Title text here and Morgan Stanley sets price target to $50"),
         _news_item("No url one", "Cantor Fitzgerald sets price target to $20")],
        [], None, view=_avm_view,
        expect={"attribution": _NO_CHECK, "status": "METHOD_NOT_DISCLOSED", "scanned": 3, "methodCounts": {},
                "evidence": [["Cantor Fitzgerald", "NOT_CHECKED", {"target": 40, "prior": None, "currency": "USD"}, [], "Cantor Fitzgerald sets price target at $40"],
                             ["Morgan Stanley", "NOT_CHECKED", {"target": 50, "prior": None, "currency": "USD"}, [],
                              "Title text here and Morgan Stanley sets price target to $50"],
                             ["Cantor Fitzgerald", "NOT_CHECKED", {"target": 20, "prior": None, "currency": "USD"}, [], "Cantor Fitzgerald sets price target to $20"]],
                "methodNotDisclosed": [["Cantor Fitzgerald", "news"], ["Morgan Stanley", "news"]]}),
]

# --- the tool: window clamp, issuer names, warnings (BR-NWS-200, 201, 226) ---------------------------------------------------
_RKLB_INFO = {"shortName": "Rocket Lab", "longName": "Rocket Lab USA, Inc.", "exchange": "NMS"}
_UPGRADE_HISTORY_ROUTE = {"https://query1.finance.yahoo.com/v10/finance/quoteSummary/RKLB?modules=upgradeDowngradeHistory*": {
    "json": {"quoteSummary": {"result": [{"upgradeDowngradeHistory": {"history": []}}], "error": None}}}}


def _valuation_tool_case(name, ids, days, *, ticker="RKLB", news=None, info=None, identity_fails=False, env=None, routes=None, **kw) -> Case:
    """extract_analyst_valuation_methods with no rating changes; also records the Finnhub URLs requested."""
    def py():
        urls = []
        with _py_news_env(news=news, info=info, env=env), \
                patch.object(server, "get_analyst_upgrade_radar", AsyncMock(return_value=json.dumps({"changes": []}))):
            real = url_policy.urlopen

            def record(req, *a, **k):
                urls.append(req if isinstance(req, str) else req.full_url)
                return real(req, *a, **k)

            with patch.object(url_policy, "urlopen", record):
                out = json.loads(asyncio.run(server.extract_analyst_valuation_methods(ticker, days)))
        out["_finnhub"] = [u for u in urls if "finnhub" in u]
        return out

    js = _worker_js(f"""
const urls = []; const realFetch = globalThis.fetch;
globalThis.fetch = async (i, n) => {{ urls.push(typeof i === "string" ? i : i.url); return realFetch(i, n); }};
try {{
  const out = JSON.parse(await m.yahooFinance.extractAnalystValuationMethods({json.dumps(ticker)}, {days}));
  out._finnhub = urls.filter(u => u.includes("finnhub"));
  return out;
}} finally {{ globalThis.fetch = realFetch; }}""", env)
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True},
                routes={**_yahoo_routes(news, info, None, identity_fails), **_UPGRADE_HISTORY_ROUTE, **(routes or {})}, **kw)


def _valuation_tool_view(d):
    return {"windowDays": d["windowDays"], "warnings": [w["code"] for w in d["warnings"]], "status": d["status"],
            "attribution": d["attribution"], "finnhub": d["_finnhub"],
            "evidence": [[e["firm"], e["subjectMatch"], e["priceTarget"] and e["priceTarget"]["target"], e["title"]] for e in d["evidence"]]}


_RKLB_NEWS = [_yn("Rocket Lab price target raised to $30 by Cantor Fitzgerald", when=_days_ago(2), link="https://example.com/rklb1"),
              _yn("Rocket Lab USA, Inc. gets DCF price target of $25 from Morgan Stanley", when=_days_ago(3), link="https://example.com/rklb2")]
_FINNHUB_365 = ["https://finnhub.io/api/v1/company-news?symbol=RKLB&from=2025-10-15&to=2026-10-15"]

CASES += [
    # Cards 200 / 201: days_back above 365 is clamped to 365 (windowDays 365) and the news request spans 365 days.
    _valuation_tool_case(
        "news.valuation_tool.days_back_is_clamped_to_365_and_news_spans_the_window", ["BR-NWS-200", "BR-NWS-201"], 900,
        news=[], info=_RKLB_INFO, env=FINNHUB_ENV, routes=_finnhub([]),
        select=lambda d: {k: _valuation_tool_view(d)[k] for k in ("windowDays", "warnings", "status", "finnhub")},
        expect={"windowDays": 365, "warnings": [], "status": "NOT_FOUND", "finnhub": _FINNHUB_365}),
    # Card 200: no issuer name -> attribution unchecked and ATTRIBUTION_UNCHECKED.
    _valuation_tool_case(
        "news.valuation_tool.unresolved_issuer_name_leaves_attribution_unchecked", ["BR-NWS-200", "BR-NWS-201"], 500,
        news=[], info={}, identity_fails=True, env=FINNHUB_ENV, routes=_finnhub([]), select=_valuation_tool_view,
        expect={"windowDays": 365, "warnings": ["ATTRIBUTION_UNCHECKED"], "status": "NOT_FOUND", "attribution": _NO_CHECK,
                "finnhub": _FINNHUB_365, "evidence": []}),
    # Card 226 / 200: longName 'Rocket Lab USA, Inc.', shortName 'Rocket Lab'. Both runtimes build aliases from long, short and company
    # names (Python since 2.19.0, B-G11 / call #16; it used longName only), so 'Rocket Lab target raised ... by Cantor' is accepted (CLAUSE).
    _valuation_tool_case(
        "news.valuation_tool.long_short_and_company_name_aliases", ["BR-NWS-226", "BR-NWS-200", "BR-NWS-201"], 30,
        news=_RKLB_NEWS, info=_RKLB_INFO, select=_valuation_tool_view,
        expect={"windowDays": 30, "warnings": [], "status": "METHOD_NOT_DISCLOSED",
                "attribution": {"checked": True, "rejectedForOtherCompany": 1,
                                "subjectAliases": ["rklb", "rocket lab", "rocket lab usa", "rocket lab usa inc"]},
                "finnhub": [], "evidence": [["Cantor Fitzgerald", "CLAUSE", 30, "Rocket Lab price target raised to $30 by Cantor Fitzgerald"]]}),
    # Card 201 / 226: an invalid ticker makes get_company_news return an error payload, which raises NEWS_UNAVAILABLE in both runtimes
    # (B-G11, 2.19.0). The Worker's internal news call validates the ticker format since 2.20.0 (A-G12, decision 15); until then it
    # ran the collection and raised nothing.
    _valuation_tool_case(
        "news.valuation_tool.news_error_payload_raises_news_unavailable", ["BR-NWS-201", "BR-NWS-226", "BR-NWS-240"], 30,
        ticker="BAD TKR!", news=[], info={},
        select=lambda d: {"warnings": [[w["code"], w["message"], w["severity"]] for w in d["warnings"]], "status": d["status"], "windowDays": d["windowDays"]},
        expect={"warnings": [["ATTRIBUTION_UNCHECKED", "The company name could not be resolved, so targets were not checked against the subject.", "warning"],
                             ["NEWS_UNAVAILABLE", "News could not be read.", "warning"]],
                "status": "NOT_FOUND", "windowDays": 30}),
]


# F8 (R-news Batch C, decision 16): a rating-changes (analyst radar) call that throws is disclosed with RATING_CHANGES_UNAVAILABLE in
# both runtimes, same shape and message (Python behaviour; the Worker added it in 2.20.0). The Worker's radar catches its own
# errors, so the Worker side forces the rejection by making JSON.stringify throw for the radar's result object and for its error
# payload (restored before returning); Python's radar call is patched to raise.
def _radar_failure_case(name, ids, **kw) -> Case:
    def py():
        with _py_news_env(news=_RKLB_NEWS, info=_RKLB_INFO), \
                patch.object(server, "get_analyst_upgrade_radar", AsyncMock(side_effect=RuntimeError("radar boom"))):
            return json.loads(asyncio.run(server.extract_analyst_valuation_methods("RKLB", 30)))

    js = _worker_js("""
const realStringify = JSON.stringify;
JSON.stringify = function (value, ...rest) {
  if (value && typeof value === "object" && (("netSentiment" in value && "upgrades" in value) || value.message === "radar boom")) throw new Error("radar boom");
  return realStringify.call(JSON, value, ...rest);
};
let raw;
try { raw = await m.yahooFinance.extractAnalystValuationMethods("RKLB", 30); } finally { JSON.stringify = realStringify; }
return JSON.parse(raw);""")
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True},
                routes={**_yahoo_routes(_RKLB_NEWS, _RKLB_INFO, None, False), **_UPGRADE_HISTORY_ROUTE}, **kw)


CASES += [
    _radar_failure_case(
        "news.valuation_tool.failed_rating_changes_call_raises_rating_changes_unavailable", ["BR-NWS-200", "BR-NWS-201"],
        select=lambda d: {"warnings": [[w["code"], w["message"], w["severity"]] for w in d["warnings"]], "status": d["status"],
                          "ratingChangesScanned": d["ratingChangesScanned"]},
        expect={"warnings": [["RATING_CHANGES_UNAVAILABLE", "Rating changes could not be read.", "warning"]],
                "status": "METHOD_NOT_DISCLOSED", "ratingChangesScanned": 0}),
]


# Card 226 / call #16 (2.19.0): a name equal to the ticker is not issuer identity in either runtime. With Yahoo shortName 'RKLB' (and no
# longName) there is no attribution name, so targets are unchecked (Python used to take 'RKLB' as the issuer name).
CASES += [
    _valuation_tool_case(
        "news.valuation_tool.ticker_equal_name_is_not_an_attribution_alias", ["BR-NWS-226", "BR-NWS-200"], 30,
        news=[], info={"shortName": "RKLB"},
        select=lambda d: {"warnings": [w["code"] for w in d["warnings"]], "attribution": d["attribution"]},
        expect={"warnings": ["ATTRIBUTION_UNCHECKED"], "attribution": _NO_CHECK}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# GlobeNewswire RSS (explicit `newswire` source): stock-category relevance, XML guard, cache; duplicate merge
# BR-NWS-222 / 223 / 224 / 225 / 251 / 252 / 264 / 265 / 272 / 273
# ═══════════════════════════════════════════════════════════════════════════════════════════════

GNW_FEEDS = "https://www.globenewswire.com/RssFeed/*"
_NVDA_INFO = {"shortName": "NVIDIA", "longName": "NVIDIA Corporation", "exchange": "NMS"}


def _rss(items):
    return ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/"><channel>'
            '<title>GlobeNewswire</title>' + "".join(items) + "</channel></rss>")


def _gitem(i, title, categories, *, pub="Wed, 14 Oct 2026 12:58:00 GMT", contributor="", desc="Description text"):
    cats = "".join(f'<category domain="https://www.globenewswire.com/rss/stock">{c}</category>' for c in categories)
    return (f"<item><title>{title}</title><link>https://www.globenewswire.com/news-release/2026/10/14/{i}/x.html</link>"
            f"<guid>https://www.globenewswire.com/news-release/2026/10/14/{i}/g.html</guid><description>{desc}</description>"
            + (f"<pubDate>{pub}</pubDate>" if pub else "")
            + (f"<dc:contributor>{contributor}</dc:contributor>" if contributor else "")
            + f"<dc:subject>Earnings</dc:subject><dc:language>en</dc:language><dc:identifier>id{i}</dc:identifier>{cats}<dc:keyword>kw{i}</dc:keyword></item>")


def _gnw_case(name, ids, ticker, info, items, *, select, body=None, status=200, **kw) -> Case:
    xml = body if body is not None else _rss(items)
    return _scenario(name, ids, lambda: server.get_company_news(ticker, 10, 14, ["newswire"]),
                     f'm.yahooFinance.getCompanyNews({json.dumps(ticker)}, 10, 14, ["newswire"])', news=[], info=info,
                     routes={GNW_FEEDS: {"body": xml, "headers": {"content-type": "application/rss+xml"}, "status": status}}, select=select, **kw)


def _gnw_view(v):
    return {"items": [[i["title"], i["confidence"], i["tickerRelevance"], i["stockCategories"], _norm_ts(i["publishedAt"]), i["globenewswireId"]]
                      for i in v["items"]],
            "newswire": _status_rows(v, "newswire", keys=("status", "rawCount", "filteredCount")), "warnings": _warning_rows(v),
            "sourcesUsed": v["meta"]["sourcesUsed"], "status": v.get("status")}


def _gnw_row(title, categories, ident):
    return [title, "HIGH", "HIGH", categories, "2026-10-14T12:58:00Z", ident]


def _gnw_failed(messages):
    return {"items": [], "newswire": {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}, "warnings": messages, "sourcesUsed": [],
            "status": "SOURCE_LIMITED_NOT_FOUND"}


_GNW_FEED_NAMES = ["public_companies", "press_releases", "earnings", "stock_market_news", "technology", "semiconductors", "telecommunications",
                   "mobile_telecommunications", "telecommunications_equipment", "electronic_equipment"]

CASES += [
    # Card 222 / 273: only items whose stock category names the ticker are kept (a headline that merely mentions NVIDIA is ignored); the
    # category may be one of several in one tag, or a bare symbol; items without a pubDate or older than the window are dropped
    # silently, with no PUBLISHED_AT_UNAVAILABLE warning.
    _gnw_case(
        "news.gnw.only_stock_category_tags_naming_the_ticker_are_kept", ["BR-NWS-222", "BR-NWS-225", "BR-NWS-273"], "NVDA", _NVDA_INFO,
        [_gitem(1, "NVIDIA reports results", ["Nasdaq:NVDA"], contributor="NVIDIA Corp"), _gitem(2, "Partner mentions NVIDIA tech", ["Nasdaq:AMD"]),
         _gitem(5, "Multi", ["Nasdaq:AMD, Nasdaq:NVDA"]), _gitem(6, "Bare symbol", ["NVDA"]),
         _gitem(7, "Undated", ["Nasdaq:NVDA"], pub=""), _gitem(8, "Old", ["Nasdaq:NVDA"], pub="Mon, 01 Jun 2026 12:00:00 GMT")],
        select=_gnw_view,
        expect={"items": [_gnw_row("NVIDIA reports results", ["Nasdaq:NVDA"], "id1"), _gnw_row("Multi", ["Nasdaq:AMD", "Nasdaq:NVDA"], "id5"),
                          _gnw_row("Bare symbol", ["NVDA"], "id6")],
                "newswire": {"status": "OK", "rawCount": 3, "filteredCount": 3}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    # Cards 224 / 225: a stock-category prefix naming another exchange ('NYSE:NVDA', 'TSX:NVDA') for a NASDAQ company is rejected by
    # the Worker (exchange compatibility) and accepted by Python (symbol only).
    _gnw_case(
        "news.gnw.other_exchange_prefix_is_rejected_in_both", ["BR-NWS-224", "BR-NWS-225", "BR-NWS-222"], "NVDA", _NVDA_INFO,
        [_gitem(1, "NASDAQ tagged", ["Nasdaq:NVDA"]), _gitem(3, "TSX tagged", ["TSX:NVDA"]), _gitem(4, "NYSE tagged", ["NYSE:NVDA"])],
        select=_gnw_view, expect={"items": [_gnw_row("NASDAQ tagged", ["Nasdaq:NVDA"], "id1")],
                   "newswire": {"status": "OK", "rawCount": 1, "filteredCount": 1}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    # Card 223: an NYSE American company whose Yahoo exchange reads 'NYSE American'. Until 2.17.0 it normalised to NYSE, so the Worker
    # rejected its own 'NYSE American:UUUU' tag (and any other). Since 2.17.0 (B-G4) it normalises to NYSE American, so the Worker keeps
    # the 'NYSE American:' and 'AMEX:' tags and drops 'NASDAQ:'; Python has the same check since 2.17.0.
    _gnw_case(
        "news.gnw.nyse_american_company_keeps_its_own_releases", ["BR-NWS-223", "BR-NWS-224"], "UUUU",
        {"shortName": "Energy Fuels", "longName": "Energy Fuels Inc.", "exchange": "NYSE American"},
        [_gitem(1, "Energy Fuels news", ["NYSE American:UUUU"]), _gitem(2, "Energy Fuels nasdaq", ["NASDAQ:UUUU"]), _gitem(3, "Energy Fuels amex", ["AMEX:UUUU"])],
        select=_gnw_view, expect={"items": [_gnw_row("Energy Fuels news", ["NYSE American:UUUU"], "id1"), _gnw_row("Energy Fuels amex", ["AMEX:UUUU"], "id3")],
                   "newswire": {"status": "OK", "rawCount": 2, "filteredCount": 2}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    # Card 223 edge: an exchange name 'ASE' maps to NYSEAMERICAN in both runtimes, which then accept 'NYSE American:' and 'AMEX:' tags.
    _gnw_case(
        "news.gnw.ase_exchange_name_maps_to_nyse_american_in_both", ["BR-NWS-223"], "UUUU",
        {"shortName": "Energy Fuels", "longName": "Energy Fuels Inc.", "exchange": "ASE"},
        [_gitem(1, "Energy Fuels news", ["NYSE American:UUUU"]), _gitem(2, "Energy Fuels nasdaq", ["NASDAQ:UUUU"]), _gitem(3, "Energy Fuels amex", ["AMEX:UUUU"])],
        select=_gnw_view, expect={"items": [_gnw_row("Energy Fuels news", ["NYSE American:UUUU"], "id1"), _gnw_row("Energy Fuels amex", ["AMEX:UUUU"], "id3")],
                   "newswire": {"status": "OK", "rawCount": 2, "filteredCount": 2}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    # Card 272: a feed larger than 2 MiB is rejected in both runtimes (same text).
    _gnw_case(
        "news.gnw.feed_over_2_mib_is_rejected", ["BR-NWS-272"], "NVDA", _NVDA_INFO, None, body="x" * (2 * 1024 * 1024 + 5), select=_gnw_view,
        expect=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' unavailable: GlobeNewswire RSS response exceeded size limit"]
                            for n in _GNW_FEED_NAMES])),
    # Card 272: a DOCTYPE and an ENTITY declaration are both blocked in both runtimes, with one text (the Worker since 2.17.0;
    # before, it blocked only <!ENTITY, as 'unsupported XML entity declaration').
    _gnw_case(
        "news.gnw.doctype_feed_is_rejected_in_both", ["BR-NWS-272"], "NVDA", _NVDA_INFO, None,
        body=_rss([_gitem(1, "NVIDIA reports results", ["Nasdaq:NVDA"])]).replace("<rss", "<!DOCTYPE rss [<!ELEMENT rss ANY>]><rss", 1),
        select=_gnw_view,
        expect=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' parse error: unsupported XML declaration in GlobeNewswire RSS"]
                            for n in _GNW_FEED_NAMES])),
    _gnw_case(
        "news.gnw.entity_feed_is_rejected_with_one_text", ["BR-NWS-272"], "NVDA", _NVDA_INFO, None,
        body=_rss([_gitem(1, "NVIDIA reports results", ["Nasdaq:NVDA"])]).replace("<rss", '<!DOCTYPE rss [<!ENTITY x "y">]><rss', 1),
        select=_gnw_view,
        expect=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' parse error: unsupported XML declaration in GlobeNewswire RSS"]
                            for n in _GNW_FEED_NAMES])),
]

# Card 242, 2.17.0 (B-G12c): verify relevance reads the evidence text and issuer, not the tickers list every collector injects. A
# GlobeNewswire release tagged Nasdaq:ZZTP whose text names neither the ticker nor the company (and has no contributor) is official
# evidence, so the event is CONFIRMED before the relevance check, which then downgrades it to PARTIAL with LOW_RELEVANCE_EVIDENCE.
# Python no longer scores the injected tickers list (it did, and kept CONFIRMED).
CASES += [
    _scenario(
        "news.verify.low_relevance_downgrade_fires_without_ticker_or_name", ["BR-NWS-242"],
        lambda: server.verify_company_event("ZZTP", "acquisition", "", "", ["newswire"]),
        'm.yahooFinance.verifyCompanyEvent("ZZTP", "acquisition", "", "", ["newswire"])',
        news=[], info={"shortName": "Zeta Platforms", "longName": "Zeta Platforms, Inc.", "exchange": "NMS"},
        submissions=_py_sec(SEC_CIK, "Zeta Platforms, Inc.", {}),
        routes={GNW_FEEDS: {"body": _rss([_gitem(1, "Beta Corp to be bought in all-stock acquisition", ["Nasdaq:ZZTP"], desc="Beta Corp holders approve.")]),
                            "headers": {"content-type": "application/rss+xml"}}},
        select=lambda v: {"status": v["status"], "warnings": _warning_codes(v), "relevance": [e["relevance"] for e in v["bestEvidence"]]},
        expect={"status": "PARTIAL", "warnings": ["LOW_RELEVANCE_EVIDENCE"], "relevance": ["LOW"]}),
]

# Planner decision 6 (R-news Batch B, 2.19.0): verify relevance reads one issuer name set in both runtimes: Yahoo longName and
# shortName plus the SEC submissions name, without any name equal to the ticker. The Worker read only Yahoo shortName (else
# longName) before, and Python only the SEC submissions name.
_ZETA_RELEASE = [_yn("Zeta announces acquisition of Beta Corp", publisher="Business Wire", when=_days_ago(2))]


def _relevance_case(name, *, info, sec_name, news=None, wire_title=None, **kw) -> Case:
    """Yahoo press releases (titles must name the Yahoo issuer), or with wire_title a GlobeNewswire release tagged Nasdaq:ZZTP."""
    source = "newswire" if wire_title else "yahoo_finance_press_releases"
    routes = _sec_routes(SEC_CIK, "ZZTP", sec_name, {})
    if wire_title:
        routes[GNW_FEEDS] = {"body": _rss([_gitem(1, wire_title, ["Nasdaq:ZZTP"], desc="Beta Corp holders approve.")]),
                             "headers": {"content-type": "application/rss+xml"}}
    return _scenario(
        name, ["BR-NWS-242"],
        lambda: server.verify_company_event("ZZTP", "acquisition", "", "", [source]),
        f'm.yahooFinance.verifyCompanyEvent("ZZTP", "acquisition", "", "", {json.dumps([source])})',
        news=news or [], info=info, submissions=_py_sec(SEC_CIK, sec_name, {}), routes=routes,
        select=lambda v: {"status": v["status"], "warnings": _warning_codes(v), "relevance": [e["relevance"] for e in v["bestEvidence"]]}, **kw)


CASES += [
    # The SEC submissions name alone makes the release relevant in both runtimes (the Worker reads it since 2.19.0).
    _relevance_case("news.verify.relevance_reads_the_sec_submissions_name", wire_title="Zeta announces acquisition of Beta Corp",
                    info={"exchange": "NMS"}, sec_name="Zeta Inc",
                    expect={"status": "CONFIRMED", "warnings": [], "relevance": ["HIGH"]}),
    # A Yahoo shortName ('Zeta') names the issuer while the SEC name does not: HIGH in both runtimes (Python read the SEC name only).
    _relevance_case("news.verify.relevance_reads_the_yahoo_short_name", news=_ZETA_RELEASE,
                    info={"shortName": "Zeta", "longName": "Zeta Platforms, Inc."}, sec_name="ZT Holdings Corp",
                    expect={"status": "CONFIRMED", "warnings": [], "relevance": ["HIGH"]}),
    # A name equal to the ticker is not issuer identity: 'ZZTP' inside 'ZZTPX' is not a relevance match in either runtime (Python's SEC
    # name 'ZZTP' used to match it as a substring).
    _relevance_case("news.verify.relevance_ignores_a_ticker_equal_name", wire_title="ZZTPX Group announces acquisition of Beta Corp",
                    info={"shortName": "ZZTP", "exchange": "NMS"}, sec_name="ZZTP",
                    expect={"status": "PARTIAL", "warnings": ["LOW_RELEVANCE_EVIDENCE"], "relevance": ["LOW"]}),
]


def _gnw_cache_py():
    """Fetch counts for the same ticker at +0, +14 and +16 minutes (Python: the cache entries are aged by rewinding their stored time)."""
    urls = []
    counts = []
    with _py_news_env(news=[], info=_NVDA_INFO):
        real = url_policy.urlopen

        def record(req, *a, **k):
            urls.append(req if isinstance(req, str) else req.full_url)
            return real(req, *a, **k)

        with patch.object(url_policy, "urlopen", record):
            original = {}
            for minutes in (0, 14, 16):
                store = pycache._tool_cache._store
                if minutes == 14:
                    original = {k: v for k, v in store.items() if k.startswith("gnw_rss:")}
                if minutes:
                    for key, (stored_at, value, ttl) in original.items():
                        store[key] = (stored_at - minutes * 60, value, ttl)
                before = sum("globenewswire" in u for u in urls)
                asyncio.run(server.get_company_news("NVDA", 10, 14, ["newswire"]))
                counts.append(sum("globenewswire" in u for u in urls) - before)
    return counts


_GNW_CACHE_JS = """
let n = 0; const realFetch = globalThis.fetch;
globalThis.fetch = async (i, o) => { const u = typeof i === "string" ? i : i.url; if (u.includes("globenewswire.com/RssFeed")) n++; return realFetch(i, o); };
const counts = [];
try {
  for (const minutes of [0, 14, 16]) {
    __now = Date.UTC(2026, 9, 15, 12, 0, 0) + minutes * 60000;
    const before = n;
    await m.yahooFinance.getCompanyNews("NVDA", 10, 14, ["newswire"]);
    counts.push(n - before);
  }
} finally { globalThis.fetch = realFetch; }
return counts;"""

CASES += [
    # Cards 264 / 265: a feed fetched at 12:00 is reused at +14 minutes (no request) and fetched again at +16 minutes (TTL 15 minutes);
    # ten feeds are fetched on the first and third call.
    Case(name="news.gnw.feed_cache_is_reused_for_14_minutes_and_refetched_at_16", ids=["BR-NWS-264", "BR-NWS-265"],
         py=_gnw_cache_py, ts={"js": _worker_js(_GNW_CACHE_JS), "isolate": True},
         routes={**_yahoo_routes([], _NVDA_INFO), GNW_FEEDS: {"body": _rss([_gitem(1, "NVIDIA reports results", ["Nasdaq:NVDA"])])}},
         expect=[10, 0, 10]),
]

# --- duplicate merge and group ids (BR-NWS-251, BR-NWS-252) ---------------------------------------------------------------
def _merge_view(v):
    return {"items": [[i["title"], i["source"], i["sourceType"], _norm_ts(i["publishedAt"]),
                       [[r["source"], r["sourceType"], _norm_ts(r["publishedAt"]), r["url"]] for r in i.get("sourceRefs", [])]] for i in v["items"]],
            "warnings": [[w["code"], w.get("statusImpact")] for w in v["warnings"]], "status": v.get("status")}


def _fin_dup_row(hour, text, url):
    return {"headline": text, "summary": "s", "source": "W", "url": url, "datetime": _epoch(_days_ago(1, hour=hour))}


CASES += [
    # Card 252: the same story from Yahoo (13:00Z) and GlobeNewswire (12:58Z) is one duplicate group: the newswire item (priority 3)
    # is kept, the Yahoo item (priority 5) is appended to its sourceRefs, and TIMESTAMP_VARIANCE is emitted with statusImpact false
    # (so the status stays unset).
    _scenario(
        "news.dedupe.higher_priority_source_is_kept_and_timestamp_variance_does_not_affect_status", ["BR-NWS-252"],
        lambda: server.get_company_news("NVDA", 10, 14, ["yahoo_finance_news", "newswire"]),
        'm.yahooFinance.getCompanyNews("NVDA", 10, 14, ["yahoo_finance_news", "newswire"])',
        news=[_yn("NVIDIA reports record results", publisher="Reuters", link="https://finance.yahoo.com/news/nv", when="2026-10-14T13:00:00Z")],
        info=_NVDA_INFO, routes={GNW_FEEDS: {"body": _rss([_gitem(1, "NVIDIA reports record results", ["Nasdaq:NVDA"])])}}, select=_merge_view,
        expect={"items": [["NVIDIA reports record results", "newswire", "newswire", "2026-10-14T12:58:00Z",
                           [["yahoo_finance_news", "yahoo_finance_news", "2026-10-14T13:00:00Z", "https://finance.yahoo.com/news/nv"]]]],
                "warnings": [["TIMESTAMP_VARIANCE", False]], "status": None}),
    # Card 252 edge: equal priority -> the lexically later publishedAt wins.
    _scenario(
        "news.dedupe.equal_priority_keeps_the_later_timestamp", ["BR-NWS-252"],
        lambda: server.get_company_news("NVDA", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("NVDA", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([_fin_dup_row(8, "NVDA Acme sets deal", "https://a.com/1"), _fin_dup_row(10, "NVDA Acme sets deal", "https://b.com/2")]),
        select=_merge_view,
        expect={"items": [["NVDA Acme sets deal", "finnhub", "company_news", "2026-10-14T10:00:00Z",
                           [["finnhub", "company_news", "2026-10-14T08:00:00Z", "https://a.com/1"]]]],
                "warnings": [["TIMESTAMP_VARIANCE", False]], "status": None}),
    # Card 251: both runtimes decode HTML entities before building the title stem (the Worker since 2.17.0), so
    # 'Acme &amp; Beta sign deal' and 'Acme & Beta sign deal' are one item.
    _scenario(
        "news.dedupe.html_entities_are_decoded_in_both", ["BR-NWS-251"],
        lambda: server.get_company_news("NVDA", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("NVDA", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([_fin_dup_row(8, "NVDA Acme &amp; Beta sign deal", "https://a.com/1"), _fin_dup_row(10, "NVDA Acme & Beta sign deal", "https://b.com/2")]),
        select=lambda v: [i["title"] for i in v["items"]], expect=["NVDA Acme & Beta sign deal"]),
    # The group id itself: Python is the first 16 hex characters of a SHA-256, the Worker a 32-bit rolling hash (8 hex characters),
    # for the same title, day and entity. (Related to BR-NWS-251: the key is built from the same stem.)
    _scenario(
        "news.dedupe.duplicate_group_id_algorithms_differ", ["BR-NWS-251", "BR-NWS-252"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([_frow(1, text="AAPL big news", days=1), _frow(2, text="Sector moves", days=1), _frow(3, text="Sector moves again", days=1)]),
        select=lambda v: sorted(i["duplicateGroupId"] for i in v["items"]), mode="pinned",
        decision="FINDING: duplicateGroupId is a 16-hex SHA-256 prefix in Python and an 8-hex 32-bit hash in the Worker for the same item",
        expect_py=["129406048579cefe", "63592e348f7895dc", "9fba966b8551c339"], expect_ts=["bd521049", "c5902dde", "c8bfaf1e"]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Approved company IR page (registry-backed `company_ir_page`): extraction rules, content type, size, candidates
# BR-NWS-202 / 203 / 208 / 249 / 263
# The shipped registry holds only candidates, so an approved entry is injected: Python reads a temporary registry file through
# `server._COMPANY_IR_PAGE_REGISTRY_PATH`; the Worker's registry is a bundled JSON constant, so the case body loads a bundle built
# with the IQE.L entry upgraded in the registry JSON. (Until 2.10.0 the body wrapped Array.prototype.find to upgrade the entry in
# place; the 2.10.0 fetch allow-list reads approved registry hosts once at module load, so the entry must be in the bundled JSON.)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

IR_URL = "https://www.iqep.com/investors/regulatory-news/"
IR_ENTRY = {"ticker": "IQE.L", "issuerName": "IQE plc", "status": "approved", "adapter": "html_article", "canonicalUrl": IR_URL,
            "allowedHosts": ["www.iqep.com"], "allowedPathPrefixes": ["/investors/"], "candidateReason": None,
            "discoveredAt": "2026-09-25T12:00:00Z", "reviewedBy": "reviewer", "reviewedAt": "2026-10-01T00:00:00Z", "revalidateAfter": None}
_IR_WORKER_FIELDS = ("status", "adapter", "canonicalUrl", "allowedHosts", "allowedPathPrefixes", "revalidateAfter", "issuerName")
_SHIPPED_IR_REGISTRY = json.loads((Path(__file__).resolve().parents[2] / "worker" / "src" / "company-ir-page-registry.json").read_text(encoding="utf-8"))


def _worker_ir_registry(worker_entry: dict) -> dict:
    """The shipped registry with the IQE.L entry upgraded field by field (a None value removes the field)."""
    registry = copy.deepcopy(_SHIPPED_IR_REGISTRY)
    for source in registry["sources"]:
        if source.get("ticker") == "IQE.L":
            for k, v in worker_entry.items():
                if v is None:
                    source.pop(k, None)
                else:
                    source[k] = v
    return registry


def _ir_page_case(name, ids, body, *, entry=None, content_type="text/html; charset=utf-8", max_results=100, lookback=365, select,
                  verify_query=None, **kw) -> Case:
    """get_company_news (or, with verify_query, verify_company_event) for IQE.L with one approved IR-page entry."""
    entry = entry or IR_ENTRY
    py_tool = (lambda: server.verify_company_event("IQE.L", verify_query, "", "", ["company_ir_page"])) if verify_query else \
        (lambda: server.get_company_news("IQE.L", max_results, lookback, ["company_ir_page"]))
    ts_tool = (f'mod.yahooFinance.verifyCompanyEvent("IQE.L", {json.dumps(verify_query)}, "", "", ["company_ir_page"])' if verify_query
               else f'mod.yahooFinance.getCompanyNews("IQE.L", {max_results}, {lookback}, ["company_ir_page"])')

    def py():
        with tempfile.TemporaryDirectory(prefix="ir-registry-") as tmp:
            path = Path(tmp) / "registry.json"
            path.write_text(json.dumps({"schemaVersion": "2026-07-10", "registryVersion": "2026-09-25.1200", "sources": [entry]}), encoding="utf-8")
            with _py_news_env(news=[], info={}), patch.object(ir_page_provider, "_COMPANY_IR_PAGE_REGISTRY_PATH", path), \
                    url_policy.registry_override(news_tools._load_company_ir_page_registry()):
                return json.loads(asyncio.run(py_tool()))

    worker_entry = {k: entry.get(k) for k in _IR_WORKER_FIELDS}
    fx = {"registry": _worker_ir_registry(worker_entry), "src": str(WORKER / "src"),
          "esbuild": (WORKER / "node_modules" / "esbuild" / "lib" / "main.js").as_uri()}
    js = _REGISTRY_BUNDLE_JS + _worker_js(f"""
const mod = await registryBundle(fx.registry, fx.src, fx.esbuild);
mod.response.setWorkerEnv({{}});
return JSON.parse(await {ts_tool});""")
    headers = {"content-type": content_type} if content_type else {}
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True, "args": [worker_entry], "fx": fx},
                routes={**_yahoo_routes([], {}), IR_URL: {"body": body, "headers": headers}}, select=select, **kw)


def _ir_article(i, title, *, href=None, date=None, extra=""):
    link = href or f"/investors/regulatory-news/item-{i}"
    stamp = f'<time datetime="{date}">{date}</time>' if date else ""
    return f'<article><h2>{title}</h2><a href="{link}">read</a>{stamp}<p>Summary text {i} {extra}</p></article>'


def _ir_day(n):
    return _days_ago(n)[:10]


def _ir_view(v):
    page = v["sourceStatus"]["company_ir_page"]
    return {"count": len(v["items"]), "status": page["status"], "warnings": _warning_rows(v), "collection": v.get("status"),
            "sourcesUsed": v["meta"]["sourcesUsed"]}


# 'Q3' (title under 6 characters) and an off-allow-list link are within the first 50 blocks; 10 more articles follow the 50th.
_IR_BLOCKS = ([_ir_article(0, "Q3", date=_ir_day(1)), _ir_article(1, "Offsite headline here", href="https://othersite.com/x", date=_ir_day(1)),
               _ir_article(2, "Article headline without a date")]
              + [_ir_article(i, f"Article headline number {i}", date=_ir_day(1 + i % 5)) for i in range(3, 60)])
_IR_LI_PAGE = "<ul>" + "".join(
    f'<li><a href="/investors/regulatory-news/l{i}">{t}</a> <time datetime="{_ir_day(2)}">x</time></li>'
    for i, t in enumerate(["Company announces new CFO appointment", "Contact the investor team", "Q3 results published today"])) + "</ul>"
_IR_RSS = ('<rss version="2.0"><channel><title>t</title><item><title>RSS headline one here</title>'
           '<link>https://www.iqep.com/investors/regulatory-news/r1</link><pubDate>Wed, 14 Oct 2026 12:00:00 GMT</pubDate>'
           '<description>desc</description></item></channel></rss>')

# R-news D (2.22.0, F10): a structured entry answered with an RSS or Atom body is parsed as a feed, the Worker's dispatch (feed
# first, then structured JSON, then HTML). Both runtimes get these same bytes. Each feed has one off-allow-list item (dropped);
# the Atom feed also exercises rel selection (an enclosure link before the alternate one), a relative href, <updated> when
# <published> is missing, and <content> when <summary> is missing.
_IR_STRUCTURED_RSS = (
    '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>IQE regulatory news</title>'
    '<item><title>IQE announces Q3 results &amp; outlook</title>'
    '<link>https://www.iqep.com/investors/regulatory-news/rss-1</link><pubDate>Tue, 13 Oct 2026 07:00:00 GMT</pubDate>'
    '<description><![CDATA[<p>Third-quarter <b>revenue</b> rose.</p>]]></description></item>'
    '<item><title>IQE board appointment notice</title><guid>https://www.iqep.com/investors/regulatory-news/rss-2</guid>'
    '<pubDate>Mon, 12 Oct 2026 07:00:00 GMT</pubDate><description>Board change.</description></item>'
    '<item><title>Offsite syndicated copy</title><link>https://othersite.com/investors/x</link>'
    '<pubDate>Mon, 12 Oct 2026 08:00:00 GMT</pubDate></item>'
    '</channel></rss>')
_IR_STRUCTURED_ATOM = (
    '<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://www.w3.org/2005/Atom"><title>IQE news</title>'
    '<entry><title>IQE signs supply agreement</title>'
    '<link rel="enclosure" href="https://www.iqep.com/investors/files/a1.pdf"/>'
    '<link rel="alternate" href="https://www.iqep.com/investors/regulatory-news/atom-1"/>'
    '<published>2026-10-11T09:30:00Z</published><summary>Multi-year agreement.</summary></entry>'
    '<entry><title>IQE trading update published</title><link href="/investors/regulatory-news/atom-2"/>'
    '<updated>2026-10-10T09:30:00Z</updated><content type="html">&lt;p&gt;Trading in line.&lt;/p&gt;</content></entry>'
    '<entry><title>Offsite atom copy</title><link href="https://othersite.com/investors/y"/>'
    '<published>2026-10-10T10:00:00Z</published></entry>'
    '</feed>')
_IR_STRUCTURED_ENTRY = {**IR_ENTRY, "adapter": "structured"}


def _ir_feed_view(v):
    return {**_ir_view(v),
            "items": [[i["title"], i["url"], re.sub(r"\.000Z$", "Z", i["publishedAt"] or ""), i["summary"], i["eventType"]]
                      for i in v["items"]]}


CASES += [
    _ir_page_case(
        "news.ir_page.structured_entry_parses_an_rss_body", ["BR-NWS-202"], _IR_STRUCTURED_RSS,
        entry=_IR_STRUCTURED_ENTRY, content_type="application/rss+xml; charset=utf-8", select=_ir_feed_view,
        expect={"count": 2, "status": "APPROVED_SOURCE_OK", "warnings": [], "collection": None, "sourcesUsed": ["company_ir_page"],
                   "items": [["IQE announces Q3 results & outlook", "https://www.iqep.com/investors/regulatory-news/rss-1", "2026-10-13T07:00:00Z",
                              "Third-quarter revenue rose.", "guidance"],
                             ["IQE board appointment notice", "https://www.iqep.com/investors/regulatory-news/rss-2", "2026-10-12T07:00:00Z",
                              "Board change.", "other"]]}),
    # R-news D: entities are decoded once in both runtimes, so a double-escaped `&amp;lt;b&amp;gt;` stays the text `&lt;b&gt;`
    # and never turns into markup.
    _ir_page_case(
        "news.ir_page.structured_feed_double_escaped_entities_decode_once", ["BR-NWS-202"],
        _IR_STRUCTURED_RSS.replace("IQE announces Q3 results &amp; outlook", "IQE announces Q3 &amp;lt;b&amp;gt; results"),
        entry=_IR_STRUCTURED_ENTRY, content_type="application/rss+xml; charset=utf-8",
        select=lambda v: [i["title"] for i in v["items"]][:1],
        expect=["IQE announces Q3 &lt;b&gt; results"]),
    _ir_page_case(
        "news.ir_page.structured_entry_parses_an_atom_body", ["BR-NWS-202"], _IR_STRUCTURED_ATOM,
        entry=_IR_STRUCTURED_ENTRY, content_type="application/atom+xml", select=_ir_feed_view,
        expect={"count": 2, "status": "APPROVED_SOURCE_OK", "warnings": [], "collection": None, "sourcesUsed": ["company_ir_page"],
                   "items": [["IQE signs supply agreement", "https://www.iqep.com/investors/regulatory-news/atom-1", "2026-10-11T09:30:00Z",
                              "Multi-year agreement.", "contract"],
                             ["IQE trading update published", "https://www.iqep.com/investors/regulatory-news/atom-2", "2026-10-10T09:30:00Z",
                              "Trading in line.", "other"]]}),
    # The 2.17.0 DOCTYPE / ENTITY refusal applies to these feeds: validation fails instead of parsing.
    _ir_page_case(
        "news.ir_page.structured_feed_with_a_doctype_fails_validation", ["BR-NWS-202"],
        _IR_STRUCTURED_RSS.replace('<rss version="2.0">', '<!DOCTYPE rss [<!ENTITY x "y">]><rss version="2.0">'),
        entry=_IR_STRUCTURED_ENTRY, content_type="application/rss+xml", select=_ir_feed_view,
        expect={"count": 0, "status": "SOURCE_VALIDATION_FAILED", "collection": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": [], "items": [],
                   "warnings": [["COMPANY_IR_PAGE_VALIDATION_FAILED",
                                 "Approved company IR page validation failed: unsupported XML declaration in company IR RSS/Atom"]]}),
]

CASES += [
    # Card 202: only the first 50 <article> blocks are read; the 'Q3' block (title under 6 characters), the off-allow-list link and the
    # undated block are dropped (undated items never pass the window); the rest are returned: 50 blocks - 3 = 47 items.
    _ir_page_case(
        "news.ir_page.html_block_cap_short_titles_off_list_links_and_undated_items", ["BR-NWS-202", "BR-NWS-249"], "<html>" + "".join(_IR_BLOCKS) + "</html>",
        select=lambda v: {**_ir_view(v), "titles_first_last": [v["items"][0]["title"], v["items"][-1]["title"]],
                          "item": {k: v["items"][0][k] for k in ("confidence", "tickerRelevance", "approvalStatus", "issuer", "originalSource", "sourceType",
                                                                 "url", "decisionUse", "eventType")}},
        expect={"count": 47, "status": "APPROVED_SOURCE_OK", "warnings": [], "collection": None, "sourcesUsed": ["company_ir_page"],
                "titles_first_last": ["Article headline number 5", "Article headline number 49"],
                "item": {"confidence": "HIGH", "tickerRelevance": "HIGH", "approvalStatus": "approved", "issuer": "IQE plc", "originalSource": "IQE plc",
                         "sourceType": "company_ir_page", "url": "https://www.iqep.com/investors/regulatory-news/item-5",
                         "decisionUse": "USE_OFFICIAL_EVIDENCE", "eventType": "other"}}),
    # Card 202: with no <article> blocks, <li> blocks are used when their text has a release keyword.
    _ir_page_case(
        "news.ir_page.li_fallback_requires_a_release_keyword", ["BR-NWS-202"], _IR_LI_PAGE,
        select=lambda v: {**_ir_view(v), "titles": sorted(i["title"] for i in v["items"])},
        expect={"count": 2, "status": "APPROVED_SOURCE_OK", "warnings": [], "collection": None, "sourcesUsed": ["company_ir_page"],
                "titles": ["Company announces new CFO appointment", "Q3 results published today"]}),
    # Card 249: item typing and summary lengths; the issuer is the registry issuerName.
    _ir_page_case(
        "news.ir_page.item_event_type_summary_and_evidence_lengths", ["BR-NWS-249", "BR-NWS-256"],
        "<html>" + _ir_article(1, "Acme announces Q3 earnings", date=_ir_day(1), extra="x" * 400) + "</html>",
        select=lambda v: [[i["eventType"], i["confidence"], i["tickerRelevance"], i["approvalStatus"], i["issuer"], len(i["summary"]), len(i["evidenceText"])]
                          for i in v["items"]],
        expect=[["earnings", "HIGH", "HIGH", "approved", "IQE plc", 240, 180]]),
    # Card 249: with no issuerName in the entry the issuer is null in both runtimes (Python used the upper-cased ticker).
    _ir_page_case(
        "news.ir_page.issuer_without_a_registry_name_is_null", ["BR-NWS-249"],
        "<html>" + _ir_article(1, "Acme announces Q3 earnings", date=_ir_day(1)) + "</html>", entry={k: v for k, v in IR_ENTRY.items() if k != "issuerName"},
        select=lambda v: [[i.get("issuer"), i.get("originalSource")] for i in v["items"]],
        expect=[[None, None]]),
    # Card 202: a structured JSON item's summary reads content_text in both runtimes (Python did not, so its summary fell back to the title).
    _ir_page_case(
        "news.ir_page.structured_json_summary_reads_content_text", ["BR-NWS-202"],
        json.dumps({"items": [{"title": "Json headline one", "content_text": "summary via content_text", "url": "/investors/regulatory-news/j1", "date": _ir_day(2)}]}),
        entry={**IR_ENTRY, "adapter": "structured"}, content_type="application/json",
        select=lambda v: [[i["title"], i["summary"]] for i in v["items"]],
        expect=[["Json headline one", "summary via content_text"]]),
    # F9 (R-news Batch C): a structured entry answered with a content type outside json / xml / rss / atom / text/plain fails
    # validation in both runtimes, with the same message.
    _ir_page_case(
        "news.ir_page.structured_adapter_with_html_content_type_fails_validation", ["BR-NWS-203"],
        json.dumps({"items": [{"title": "Json headline one", "url": "/investors/regulatory-news/j1", "date": _ir_day(2)}]}),
        entry={**IR_ENTRY, "adapter": "structured"}, content_type="text/html; charset=utf-8", select=_ir_view,
        expect={"count": 0, "status": "SOURCE_VALIDATION_FAILED",
                "warnings": [["COMPANY_IR_PAGE_VALIDATION_FAILED", "Approved company IR page validation failed: unexpected structured content type: text/html; charset=utf-8"]],
                "collection": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": []}),
    # F11 (R-news Batch C): the IR-page row for a ticker with no registry entry carries registryVersion and approvalStatus
    # "not_registered" in both runtimes. F11 rest (R-news D, 2.22.0): the full key set is compared; both rows carry canonicalUrl
    # (null) and registrySchemaVersion (the registry file's schemaVersion). The Worker side landed first.
    _scenario(
        "news.ir_page.unregistered_ticker_row_carries_registry_version_and_not_registered", ["BR-NWS-249"],
        lambda: server.get_company_news("AAPL", 10, 14, ["company_ir_page"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["company_ir_page"])',
        select=lambda v: {**v["sourceStatus"]["company_ir_page"],
                          "registryVersion": bool(v["sourceStatus"]["company_ir_page"].get("registryVersion"))},
        expect={"status": "NOT_REGISTERED", "rawCount": 0, "filteredCount": 0, "registryVersion": True,
                   "registrySchemaVersion": _SHIPPED_IR_REGISTRY["schemaVersion"], "canonicalUrl": None,
                   "approvalStatus": "not_registered", "allowedAction": "registry_review_required"}),
    # Card 202 edge, call #17 (2.19.0): a feed-shaped body served to an html_article entry fails validation in both runtimes (the Worker
    # parsed it as RSS whatever the adapter, and Python as HTML, finding nothing, until then).
    _ir_page_case(
        "news.ir_page.feed_shaped_body_on_an_html_article_entry_fails_validation", ["BR-NWS-202"], _IR_RSS,
        select=lambda v: {"titles": [i["title"] for i in v["items"]], "status": v["sourceStatus"]["company_ir_page"]["status"],
                          "warnings": _warning_rows(v), "collection": v.get("status")},
        expect={"titles": [], "status": "SOURCE_VALIDATION_FAILED",
                "warnings": [["COMPANY_IR_PAGE_VALIDATION_FAILED", "Approved company IR page validation failed: unexpected feed body for html_article adapter"]],
                "collection": "SOURCE_LIMITED_NOT_FOUND"}),
    # Card 203: an html_article entry answered with application/json fails validation in both runtimes, with the same message (Python
    # parsed the body as JSON, found no records and reported an empty approved source until 2.19.0, B-G17).
    _ir_page_case(
        "news.ir_page.html_adapter_with_json_content_type_fails_validation", ["BR-NWS-203"], "<html>" + _ir_article(1, "Article headline one", date=_ir_day(1)) + "</html>",
        content_type="application/json", select=_ir_view,
        expect={"count": 0, "status": "SOURCE_VALIDATION_FAILED",
                "warnings": [["COMPANY_IR_PAGE_VALIDATION_FAILED", "Approved company IR page validation failed: unexpected HTML content type: application/json"]],
                "collection": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": []}),
    # Planner decision 6 (R-news Batch B, 2.19.0): an approved company_ir_page item counts as an official, HIGH-relevance source in
    # verify in both runtimes, as sec_filing / company_ir do. The item names neither the ticker nor an issuer name, yet it stays HIGH
    # and CONFIRMED (Python scored it LOW and downgraded it to PARTIAL until 2.19.0).
    _ir_page_case(
        "news.ir_page.verify_counts_an_approved_ir_page_item_as_high_relevance", ["BR-NWS-242", "BR-NWS-249"],
        "<html>" + _ir_article(1, "Board approves acquisition of Beta Corp", date=_ir_day(1)) + "</html>", verify_query="acquisition",
        select=lambda v: {"status": v["status"], "warnings": _warning_codes(v),
                          "best": [[e["sourceType"], e["confidence"], e["relevance"]] for e in v["bestEvidence"]]},
        expect={"status": "CONFIRMED", "warnings": [], "best": [["company_ir_page", "HIGH", "HIGH"]]}),
    # Card 208: exactly 768,000 bytes (750 KiB) is accepted; 768,001 is rejected with COMPANY_IR_PAGE_VALIDATION_FAILED and the source is
    # SOURCE_VALIDATION_FAILED. (A redirect to an off-list host cannot be driven here: both stubs answer with the requested URL.)
    _ir_page_case(
        "news.ir_page.page_of_exactly_768000_bytes_is_accepted", ["BR-NWS-208"],
        (lambda b: b + " " * (768000 - len(b.encode())))(_ir_article(1, "Article headline one", date=_ir_day(1))), select=_ir_view,
        expect={"count": 1, "status": "APPROVED_SOURCE_OK", "warnings": [], "collection": None, "sourcesUsed": ["company_ir_page"]}),
    _ir_page_case(
        "news.ir_page.page_of_768001_bytes_is_rejected", ["BR-NWS-208"],
        (lambda b: b + " " * (768001 - len(b.encode())))(_ir_article(1, "Article headline one", date=_ir_day(1))), select=_ir_view,
        expect={"count": 0, "status": "SOURCE_VALIDATION_FAILED",
                "warnings": [["COMPANY_IR_PAGE_VALIDATION_FAILED", "Approved company IR page validation failed: response exceeded size limit"]],
                "collection": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# `company_ir` is the approved company IR registry only (BR-NWS-207 / 209 / 210 / 211 / 228 / 258)
# Since 2.10.0 the Worker's company_ir reads only approved company IR registry entries (the official-site discovery these cards
# described was removed with the fetch allow-list), and since 2.19.0 (R-news Batch B) so does Python's, which used to serve it from the
# Yahoo feed. The scenario is NOT_REGISTERED in both runtimes and probes no host.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _company_ir_case(name, ids, website, info, *, ticker="ACME", routes=None, hosts=False, **kw) -> Case:
    def py():
        urls = []
        with _py_news_env(news=[], info=info):
            real = url_policy.urlopen

            def record(req, *a, **k):
                urls.append(req if isinstance(req, str) else req.full_url)
                return real(req, *a, **k)

            with patch.object(url_policy, "urlopen", record):
                out = json.loads(asyncio.run(server.get_company_news(ticker, 10, 14, ["company_ir"])))
        out["_urls"] = [u for u in urls if "yahoo.com" not in u and "sec.gov" not in u]
        return out

    js = _worker_js(f"""
const urls = []; const realFetch = globalThis.fetch;
globalThis.fetch = async (i, o) => {{ urls.push(typeof i === "string" ? i : i.url); return realFetch(i, o); }};
try {{
  const out = JSON.parse(await m.yahooFinance.getCompanyNews({json.dumps(ticker)}, 10, 14, ["company_ir"]));
  out._urls = urls.filter(u => !u.includes("yahoo.com") && !u.includes("sec.gov"));
  return out;
}} finally {{ globalThis.fetch = realFetch; }}""")

    def view(v):
        ci = v["sourceStatus"]["company_ir"]
        seen = []
        for url in v.get("_urls") or []:
            host = url.split("/")[2]
            if host not in seen:
                seen.append(host)
        return {"status": ci.get("status"), "website": ci.get("companyWebsite"), "identityConfidence": ci.get("identityConfidence"),
                "warnings": _warning_rows(v), "collection": v.get("status"),
                "items": [[i["title"], i["confidence"], i["tickerRelevance"]] for i in v["items"]],
                **({"probedHosts": seen} if hosts else {})}

    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True}, routes={**_yahoo_routes([], info, website), **(routes or {})},
                select=view, **kw)


_NOT_REGISTERED = {"status": "NOT_REGISTERED", "website": None, "identityConfidence": None, "warnings": [], "collection": "PROVIDER_ERROR",
                      "items": []}

def _rss_feed(title, items):
    body = "".join(f"<item><title>{t}</title>" + (f"<link>{link}</link>" if link else "")
                   + f"<pubDate>Wed, 14 Oct 2026 12:00:00 GMT</pubDate><description>{t} details</description></item>" for t, link in items)
    return f'<?xml version="1.0"?><rss version="2.0"><channel><title>{title}</title>{body}</channel></rss>'


_ACMEINC = {"shortName": "Acmeinc", "longName": "Acmeinc Holdings Inc", "exchange": "NMS"}
_FEED_ROUTES = {
    "https://www.acmeinc.com/": {"body": '<html><head><link rel="alternate" type="application/rss+xml" href="https://news.acmeinc.com/feed.xml"></head><body>'
                                         '<a href="https://feeds.example-wire.com/digest.xml">RSS digest</a> <a href="https://feeds.example-wire.com/acme.xml">feed</a></body></html>'},
    "https://news.acmeinc.com/feed.xml": {"body": _rss_feed("Acmeinc News", [("Acmeinc reports third quarter results", "https://news.acmeinc.com/q3"),
                                                                              ("Acmeinc undated link", None)])},
    "https://feeds.example-wire.com/digest.xml": {"body": _rss_feed("Industry Digest", [("Other firm results", "https://feeds.example-wire.com/o1")])},
    "https://feeds.example-wire.com/acme.xml": {"body": _rss_feed("Acmeinc Newsroom wire", [("Acmeinc signs contract", "https://feeds.example-wire.com/a1"),
                                                                                          ("Acmeinc quiet item without link", None)])},
}

# 2.19.0 (R-news Batch B, A-G10 / B-G2): the official-site RSS discovery cases (cards 207 / 209 / 210 / 211 / 228 / 258: website
# normalisation, identity grade, same-domain and name-token feed acceptance, the entity-declaration parse error) described behaviour
# neither runtime keeps once Python's company_ir is registry-only, so they were merged into this one case: a company with a website
# that links same-domain and external feeds is NOT_REGISTERED and no company host is probed.
CASES += [
    _company_ir_case(
        "news.company_ir.registry_only_source_probes_no_company_host",
        ["BR-NWS-207", "BR-NWS-209", "BR-NWS-210", "BR-NWS-211", "BR-NWS-228", "BR-NWS-258"], "https://www.acmeinc.com", _ACMEINC,
        routes=_FEED_ROUTES, hosts=True,
        expect={**_NOT_REGISTERED, "probedHosts": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# get_company_press_releases: release-type allow-list and payload differences (BR-NWS-231, BR-NWS-267)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_ACME_CORP = {"shortName": "Acme", "longName": "Acme Corp", "exchange": "NMS"}
_PR_SEC = _sec_recent(["8-K", "10-Q"], [3, 5])
_PR_NEWS = [_yn("Acme beats estimates", when=_days_ago(1))]


def _press_view(d):
    return {"items": [[i["title"], i["sourceType"], i.get("filingType")] for i in d["items"]], "status": d.get("status"),
            "coverageStatus": d["coverageStatus"], "decisionGrade": d["decisionGrade"], "failureMode": d["failureMode"],
            "sourceStatus": {k: s.get("status") for k, s in d["sourceStatus"].items()}, "sourcesUsed": d["meta"]["sourcesUsed"],
            "warnings": _warning_rows(d), "next": d["coverage"]["recommendedNextAction"]}


CASES += [
    # Card 231: sec and company_ir_page run as primary sources with SEC types fixed to ['8-K'] (the 10-Q is not collected); the
    # yahoo_finance_news item is collected but is not a release type, so it is dropped from the items. The 8-K has no EX-99.1 here.
    _sec_scenario(
        "news.press_releases.only_release_type_items_and_8k_filings_are_returned", ["BR-NWS-231"],
        lambda: server.get_company_press_releases("ACME", 90, 20, ["sec", "yahoo_finance_news", "company_ir_page"]),
        'm.yahooFinance.getCompanyPressReleases("ACME", 90, 20, ["sec", "yahoo_finance_news", "company_ir_page"])',
        _PR_SEC, news=_PR_NEWS, info=_ACME_CORP, select=_press_view,
        expect={"items": [["8-K filed", "sec_filing", "8-K"]], "status": "SEC_8K_FOUND_EX99_NOT_FOUND", "coverageStatus": "SEC_8K_FOUND_EX99_NOT_FOUND",
                "decisionGrade": False, "failureMode": "SEC_8K_FOUND_EX99_NOT_FOUND",
                "sourceStatus": {"sec": "OK", "yahoo_finance_news": "OK", "company_ir_page": "NOT_REGISTERED"},
                "sourcesUsed": ["sec", "yahoo_finance_news"],
                "warnings": [["SEC_8K_FOUND_EX99_NOT_FOUND", "SEC 8-K filing(s) were found, but no EX-99.1 press-release exhibit was resolved."]],
                "next": "CHECK_OFFICIAL_RELEASES"}),
    # Card 231 edge: sources=['yahoo_finance_news'] alone returns no items and NO_OFFICIAL_RELEASE_SOURCE.
    _scenario(
        "news.press_releases.news_only_selection_has_no_official_release_source", ["BR-NWS-231"],
        lambda: server.get_company_press_releases("ACME", 90, 20, ["yahoo_finance_news"]),
        'm.yahooFinance.getCompanyPressReleases("ACME", 90, 20, ["yahoo_finance_news"])',
        news=_PR_NEWS, info=_ACME_CORP, select=_press_view,
        expect={"items": [], "status": "NOT_FOUND", "coverageStatus": "NO_OFFICIAL_RELEASE_SOURCE", "decisionGrade": False,
                "failureMode": "NO_OFFICIAL_RELEASE_SOURCE", "sourceStatus": {"yahoo_finance_news": "OK"}, "sourcesUsed": ["yahoo_finance_news"],
                "warnings": [["NO_OFFICIAL_RELEASE_SOURCE", "No company-originated or official release source found in requested window."]],
                "next": "USE_RETURNED_CONTEXT"}),
    # Card 267: since 3.0.0 neither runtime emits doctrineUse (the Worker's deprecated field was removed; Python never had it). Since 2.19.0
    # (B-G18 / call #18) secEvidence[] carries cikInt in Python too, and decisionGradeBasis uses Python's wording in both runtimes (the
    # Worker said 'SEC EX-99 press-release exhibit').
    _sec_scenario(
        "news.press_releases.payload_has_no_doctrine_use_in_either_runtime", ["BR-NWS-267"],
        lambda: server.get_company_press_releases("ACME", 90, 20, ["sec"]),
        'm.yahooFinance.getCompanyPressReleases("ACME", 90, 20, ["sec"])',
        _PR_SEC, news=[], info=_ACME_CORP,
        select=lambda d: {"hasDoctrineUse": "doctrineUse" in d, "reliabilityClass": d.get("reliabilityClass"),
                          "basis": d["decisionGradeBasis"], "capabilityStatus": d["capabilityStatus"],
                          "secEvidenceKeys": sorted(d["secEvidence"][0].keys()), "cikInt": d["secEvidence"][0]["cikInt"],
                          "failureMode": d["failureMode"]},
        expect={"hasDoctrineUse": False, "reliabilityClass": "RELIABLE",
                "basis": "No resolved SEC EX-99 or approved IR-page evidence in this call; use for verification/context only.",
                "capabilityStatus": "ACTIVE", "secEvidenceKeys": ["acceptedAt", "accessionNumber", "cikInt", "documentUrl", "filingDate", "filingType"],
                "cikInt": "1234567", "failureMode": "SEC_8K_FOUND_EX99_NOT_FOUND"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Declared output schema vs runtime statuses (BR-NWS-214)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_NEWS_TOOL_NAMES = ["get_company_news", "search_company_news", "get_public_event_timeline", "verify_company_event",
                    "get_company_press_releases", "get_sec_recent_events"]


def _declared_status_py():
    out = {}
    for tool in _NEWS_TOOL_NAMES:
        schema = server._TOOL_OUTPUT_SCHEMAS.get(tool) or {}
        out[tool] = (schema.get("properties") or {}).get("status", {}).get("enum")
    with _py_news_env(news=[], info=_VOD_INFO):
        payload = json.loads(asyncio.run(server.get_company_news("VOD.L", 10, 14, ["yahoo_finance_news"])))
    return {"declared": out, "runtimeStatus": payload.get("status")}


CASES += [
    # Card 214: every source succeeds and returns nothing -> status NOT_FOUND. Since 2.14.0 (R4B1_SPEC section 6) Python no longer
    # declares the stale [OK, PARTIAL] enum; neither runtime declares one.
    Case(name="news.schema.python_declares_a_status_enum_that_runtime_statuses_leave", ids=["BR-NWS-214"],
         py=_declared_status_py,
         ts={"js": _worker_js(f"""
const out = {{}};
for (const t of {json.dumps(_NEWS_TOOL_NAMES)}) {{
  const tool = m.tools.TOOLS.find(x => x.name === t);
  const props = (tool && tool.outputSchema && tool.outputSchema.properties) || {{}};
  out[t] = props.status && props.status.enum ? props.status.enum : null;
}}
const payload = JSON.parse(await m.yahooFinance.getCompanyNews("VOD.L", 10, 14, ["yahoo_finance_news"]));
return {{declared: out, runtimeStatus: payload.status ?? null}};"""), "isolate": True},
         routes=_yahoo_routes([], _VOD_INFO),
         expect={"declared": {t: None for t in _NEWS_TOOL_NAMES}, "runtimeStatus": "NOT_FOUND"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Classification, labels, text limits, ordering, timeline, timestamps (P2 rules)
# BR-NWS-253 / 254 / 256 / 257 / 260 / 262 / 263 / 266 / 268 / 275 / 276 / 278
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _finnhub_rows(titles, *, summary="", url_prefix="https://e.com/"):
    return [{"headline": t, "summary": summary, "source": "W", "url": f"{url_prefix}{i}", "datetime": _epoch(_days_ago(1, hour=i % 24))}
            for i, t in enumerate(titles)]


def _event_types(v):
    return sorted([i["title"], i["eventType"]] for i in v["items"])


_EVENT_TITLES_SHARED = [
    "Acme raises outlook after quarterly beat", "Acme eps tops estimates", "Acme steps up hiring", "Acme guidance cut", "Acme signs supply contract",
    "Acme dealer network grows", "Acme prices debt offering", "Acme issues notes", "Acme will denote changes", "Acme launches new product",
    "Acme introduces chip", "Analyst upgrades Acme", "Acme price target lifted", "Fed rates decision and CPI", "Acme corporates surge",
    "Acme wins court ruling", "Acme settlement reached", "Acme director buys shares", "Acme files form 4", "Acme sector rotation",
    "Acme 8-K filing posted", "Acme regulatory approval", "Acme 10-Q posted", "Acme quiet day", "Acme forecast revised",
]
_EVENT_TYPES_SHARED = {
    "Acme raises outlook after quarterly beat": "earnings", "Acme eps tops estimates": "earnings", "Acme steps up hiring": "earnings",
    "Acme guidance cut": "guidance", "Acme signs supply contract": "contract", "Acme dealer network grows": "contract",
    "Acme prices debt offering": "financing", "Acme issues notes": "financing", "Acme will denote changes": "financing",
    "Acme launches new product": "product", "Acme introduces chip": "product", "Analyst upgrades Acme": "analyst",
    "Acme price target lifted": "analyst", "Fed rates decision and CPI": "macro", "Acme corporates surge": "macro",
    "Acme wins court ruling": "litigation", "Acme settlement reached": "litigation", "Acme director buys shares": "insider",
    "Acme files form 4": "insider", "Acme sector rotation": "regulatory", "Acme 8-K filing posted": "regulatory",
    "Acme regulatory approval": "regulatory", "Acme 10-Q posted": "earnings", "Acme quiet day": "other", "Acme forecast revised": "guidance",
}
_EVENT_TYPES_WHOLE_WORD = {
    **_EVENT_TYPES_SHARED,
    "Acme steps up hiring": "other", "Acme dealer network grows": "other", "Acme will denote changes": "other",
    "Acme corporates surge": "other", "Acme sector rotation": "other",
}

CASES += [
    # Card 253: keyword classification (first matching group wins: 'quarterly' beats 'raises'/'outlook'). Substring matching (Python
    # today) causes false hits: 'steps' -> eps -> earnings, 'dealer' -> deal -> contract, 'denote' -> note -> financing,
    # 'corporates' -> rates -> macro, 'sector' -> sec -> regulatory. Whole-word matching (the Worker since 2.17.0, A-G1 / B-G13 /
    # call #6) types those 'other'. An s/es/d/ed/ing suffix on a keyword still matches (planner decision 1), so 'introduces' and
    # 'notes' keep their types.
    _scenario(
        "news.classification.keyword_groups_match_whole_words", ["BR-NWS-253"],
        lambda: server.get_company_news("ACME", 100, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("ACME", 100, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(_EVENT_TITLES_SHARED)), select=_event_types, expect=sorted([t, e] for t, e in _EVENT_TYPES_WHOLE_WORD.items())),
    # Card 253: one keyword table, Python's superset (2.17.0): 'announces new', 'annual report' and 'credit facility' type in both
    # runtimes ('secures' no longer contains the whole word 'sec').
    _scenario(
        "news.classification.python_superset_keywords_in_both", ["BR-NWS-253"],
        lambda: server.get_company_news("ACME", 100, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("ACME", 100, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(["Acme announces new unit", "Acme posts annual report", "Acme secures credit facility"])),
        select=_event_types,
        expect=[["Acme announces new unit", "product"], ["Acme posts annual report", "earnings"], ["Acme secures credit facility", "financing"]]),
]

_LABEL_ROWS = [
    {"headline": "AAPL news one", "summary": "s", "source": "W", "url": "https://finnhub.io/api/news?id=1", "datetime": _epoch(_days_ago(1, hour=1))},
    {"headline": "Sector headline", "summary": "s", "source": "W", "url": "https://finnhub.io/api/news?id=2", "datetime": _epoch(_days_ago(1, hour=2))},
    {"headline": "Sector two", "summary": "s", "source": "W", "url": "https://www.reuters.com/z", "datetime": _epoch(_days_ago(1, hour=3))},
    {"headline": "Sector three", "summary": "s", "source": "W", "url": "https://finance.yahoo.com/y", "datetime": _epoch(_days_ago(1, hour=4))},
    {"headline": "Sector four", "summary": "s", "source": "W", "url": "not a url", "datetime": _epoch(_days_ago(1, hour=5))},
    {"headline": "Sector five", "summary": "s", "source": "W", "url": "", "datetime": _epoch(_days_ago(1, hour=6))},
]


def _labels(v):
    return sorted([i["title"], i["evidenceClass"], i["tickerMatch"], i["urlProvenance"], i["decisionUse"]] for i in v["items"])


_LABELS_SHARED = [  # before 2.17.0 (substring keywords) the labels were typed the 'Sector ...' rows regulatory
    ["AAPL news one", "CONTEXTUAL_NEWS", "EXPLICIT", "PROVIDER", "CONTEXT_ONLY"],
    ["Sector five", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "UNKNOWN", "CHECK_OFFICIAL_RELEASES"],
    ["Sector headline", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PROVIDER", "CHECK_OFFICIAL_RELEASES"],
    ["Sector three", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PROVIDER", "CHECK_OFFICIAL_RELEASES"],
    ["Sector two", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PUBLISHER", "CHECK_OFFICIAL_RELEASES"],
]
# 2.17.0 whole-word keywords: the 'Sector ...' rows are 'other', so CONTEXT_ONLY.
_LABELS_WHOLE_WORD = [row[:4] + ["CONTEXT_ONLY"] for row in _LABELS_SHARED]

CASES += [
    # Card 254: a Finnhub item is CONTEXTUAL_NEWS; its ticker match is EXPLICIT when the text holds the ticker, else SOURCE_SCOPED; a
    # finnhub.io or finance.yahoo.com host is PROVIDER, another host PUBLISHER, no URL UNKNOWN. A URL that does not parse is UNKNOWN
    # (both runtimes since 2.17.0, B-G14). 'Sector ...' headlines contain 'sec': with substring keywords (before 2.17.0)
    # they were typed regulatory and carried CHECK_OFFICIAL_RELEASES; with whole-word keywords (both runtimes) they are 'other'
    # and CONTEXT_ONLY (BR-NWS-253).
    _scenario(
        "news.labels.evidence_class_ticker_match_and_url_provenance", ["BR-NWS-254"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([r for r in _LABEL_ROWS if r["headline"] != "Sector four"]), select=_labels, expect=_LABELS_WHOLE_WORD),
    _scenario(
        "news.labels.unparseable_url_provenance_is_unknown", ["BR-NWS-254"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([r for r in _LABEL_ROWS if r["headline"] == "Sector four"]), select=_labels, expect=[["Sector four", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "UNKNOWN", "CONTEXT_ONLY"]]),
]

_LONG_TEXT = ("Sentence number one  with   extra   spaces. " * 20).strip()
CASES += [
    # Card 256: summary keeps the first 240 characters and evidenceText the first 180 of the whitespace-collapsed text, cut hard; a
    # blank summary falls back to the title.
    _scenario(
        "news.text.summary_240_evidence_180_after_whitespace_collapse", ["BR-NWS-256"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": "AAPL long summary", "summary": _LONG_TEXT, "source": "W", "url": "https://e.com/1", "datetime": _epoch(_days_ago(1, hour=1))},
                         {"headline": "AAPL whitespace only", "summary": "   \n\t ", "source": "W", "url": "https://e.com/2", "datetime": _epoch(_days_ago(1, hour=2))}]),
        select=lambda v: sorted([i["title"], len(i["summary"] or ""), len(i["evidenceText"] or ""), (i["summary"] or "")[:60], (i["evidenceText"] or "")[-20:]]
                                for i in v["items"]),
        expect=[["AAPL long summary", 240, 180, "Sentence number one with extra spaces. Sentence number one w", "ence number one with"],
                ["AAPL whitespace only", 20, 20, "AAPL whitespace only", "AAPL whitespace only"]]),
    # Card 256 edge: both runtimes cut at code points (the Worker since 2.17.0; before, at UTF-16 units, leaving a lone surrogate), so
    # an emoji straddling the cut is kept whole.
    _scenario(
        "news.text.cut_position_counts_code_points_in_both", ["BR-NWS-256"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": "AAPL astral", "summary": "x" * 239 + "\U0001F600" + "tail", "source": "W", "url": "https://e.com/1",
                          "datetime": _epoch(_days_ago(1, hour=1))}]),
        select=lambda v: [[len(i["summary"]), ascii(i["summary"][-3:]), len(i["evidenceText"])] for i in v["items"]],
        expect=[[240, "'xx\\U0001f600'", 180]]),
]

CASES += [
    # Card 257: with the legacy yahoo_finance source the news and press-release passes read the same 20 stories. Until 2.17.0 both
    # runtimes summed the passes (rawCount and retrievedCount 40, and the press-release pass's 20 NOT_ISSUER_WIRE_RELEASE rejections of
    # stories the news pass accepted). R-news Batch A (B-G12b): the shared feed is counted once (both runtimes).
    _scenario(
        "news.yahoo.legacy_aggregate_counts_the_feed_once", ["BR-NWS-257"],
        lambda: server.get_company_news("AMD", 100, 14, ["yahoo_finance"]), 'm.yahooFinance.getCompanyNews("AMD", 100, 14, ["yahoo_finance"])',
        news=[_yn(f"AMD story {i}", when=_days_ago(1 + i % 10)) for i in range(20)], info=AMD_INFO,
        select=lambda v: {"returned": len(v["items"]), **{k: v["sourceStatus"]["yahoo_finance"][k] for k in
                                                         ("status", "rawCount", "retrievedCount", "filteredCount", "acceptedCount", "returnedCount",
                                                          "rejectedCount", "rejectionCounts", "identityStatus", "attempted")}},
        expect={"returned": 20, "status": "OK", "rawCount": 20, "retrievedCount": 20, "filteredCount": 20, "acceptedCount": 20, "returnedCount": 20,
                   "rejectedCount": 0, "rejectionCounts": {}, "identityStatus": "RESOLVED", "attempted": True}),
]

_RANK_ROWS = [{"headline": f"AAPL item {i}" if i in (3, 12, 25, 33, 38) else f"Sector item {i}", "summary": "s", "source": "W", "url": f"https://e.com/{i}",
               "datetime": _epoch(_days_ago(1 + (i % 7), hour=(i * 5) % 24))} for i in range(40)]

CASES += [
    # Cards 260 / 266: max_results 10 -> a pool of the first 30 accepted rows in provider order (rows 33 and 38 are outside it); the 10
    # returned are the HIGH-relevance items first (newest first), then the LOW ones (newest first).
    _scenario(
        "news.ranking.pool_of_three_times_max_results_then_high_relevance_first", ["BR-NWS-260", "BR-NWS-266", "BR-NWS-275"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_RANK_ROWS),
        select=lambda v: [[i["title"], i["tickerRelevance"], _norm_ts(i["publishedAt"])] for i in v["items"]],
        expect=[["AAPL item 3", "HIGH", "2026-10-11T15:00:00Z"], ["AAPL item 25", "HIGH", "2026-10-10T05:00:00Z"], ["AAPL item 12", "HIGH", "2026-10-09T12:00:00Z"],
                ["Sector item 14", "LOW", "2026-10-14T22:00:00Z"], ["Sector item 28", "LOW", "2026-10-14T20:00:00Z"], ["Sector item 7", "LOW", "2026-10-14T11:00:00Z"],
                ["Sector item 21", "LOW", "2026-10-14T09:00:00Z"], ["Sector item 0", "LOW", "2026-10-14T00:00:00Z"], ["Sector item 8", "LOW", "2026-10-13T16:00:00Z"],
                ["Sector item 22", "LOW", "2026-10-13T14:00:00Z"]]),
    # Card 262: until 2.17.0 the raw-substring relevance test found the ticker 'A' inside 'MARKETS RALLY' in both runtimes, so a Finnhub
    # row about nothing in particular was HIGH. R-news Batch A (B-G12a): a single-letter ticker matches only as a whole token (both
    # runtimes).
    _scenario(
        "news.ranking.single_letter_ticker_is_not_found_inside_a_word", ["BR-NWS-262"],
        lambda: server.get_company_news("A", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("A", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(["Markets rally", "Zzz yyy"], summary="")),
        select=lambda v: sorted([i["title"], i["tickerRelevance"], i["confidence"]] for i in v["items"]), expect=[["Markets rally", "LOW", "MEDIUM"], ["Zzz yyy", "LOW", "MEDIUM"]]),
    # 2.17.0 (B-G12a): a single-letter ticker that stands as its own token is still found (HIGH in both runtimes).
    _scenario(
        "news.ranking.single_letter_ticker_matches_as_a_whole_token", ["BR-NWS-262"],
        lambda: server.get_company_news("A", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("A", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(["Agilent (A) shares climb", "Zzz yyy"], summary="")),
        select=lambda v: sorted([i["title"], i["tickerRelevance"]] for i in v["items"]),
        expect=[["Agilent (A) shares climb", "HIGH"], ["Zzz yyy", "LOW"]]),
]

CASES += [
    # Cards 268 / 276: max_results 200 is clamped to 100 for collection and the timeline; events come back in ascending timestamp order;
    # an undated provider row is rejected before it can reach the timeline (it counts as DATE_OUTSIDE_WINDOW).
    _scenario(
        "news.timeline.ascending_order_cap_and_undated_rows", ["BR-NWS-268", "BR-NWS-276"],
        lambda: server.get_public_event_timeline("AAPL", "", "", ["finnhub"], 200, False),
        'm.yahooFinance.getPublicEventTimeline("AAPL", "", "", ["finnhub"], 200, false)',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": f"AAPL timeline {i}", "summary": "s", "source": "W", "url": f"https://e.com/{i}", "datetime": _epoch(_days_ago(d, hour=9))}
                         for i, d in enumerate([3, 1, 2])] + [{"headline": "AAPL undated"}]),
        select=lambda v: {"timeline": [[_norm_ts(e["timestamp"]), e["title"]] for e in v["timeline"]], "status": v.get("status"),
                          "finnhub": _status_rows(v, "finnhub", keys=("status", "rawCount", "acceptedCount", "rejectedCount", "rejectionCounts"))},
        expect={"timeline": [["2026-10-12T09:00:00Z", "AAPL timeline 0"], ["2026-10-13T09:00:00Z", "AAPL timeline 2"], ["2026-10-14T09:00:00Z", "AAPL timeline 1"]],
                "status": None, "finnhub": {"status": "OK", "rawCount": 4, "acceptedCount": 3, "rejectedCount": 1, "rejectionCounts": {"DATE_OUTSIDE_WINDOW": 1}}}),
    # max_results also bounds the collection: with 2, only the first two provider rows (days 3 and 1 ago) are collected, so the 2-days-ago
    # row never reaches the timeline.
    _scenario(
        "news.timeline.newest_first_reverses_the_order", ["BR-NWS-268"],
        lambda: server.get_public_event_timeline("AAPL", "", "", ["finnhub"], 2, True),
        'm.yahooFinance.getPublicEventTimeline("AAPL", "", "", ["finnhub"], 2, true)',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": f"AAPL timeline {i}", "summary": "s", "source": "W", "url": f"https://e.com/{i}", "datetime": _epoch(_days_ago(d, hour=9))}
                         for i, d in enumerate([3, 1, 2])]),
        select=lambda v: [[_norm_ts(e["timestamp"]), e["title"]] for e in v["timeline"]],
        expect=[["2026-10-14T09:00:00Z", "AAPL timeline 1"], ["2026-10-12T09:00:00Z", "AAPL timeline 0"]]),
]

_YAHOO_TS = _epoch(_days_ago(1, hour=9))
CASES += [
    # Card 278: timestamp format. Both runtimes write 2026-10-14T09:00:00Z (the Worker dropped '.000' for zero milliseconds in 2.17.0,
    # watermark included); compact YYYYMMDD / YYYYMMDDHHMMSS are alike in both. A Yahoo row with providerPublishTime 0 falls through to
    # content.pubDate in both (the Worker's getNews() since 2.17.0, B-G1).
    _scenario(
        "news.timestamps.yahoo_epoch_zero_falls_back_to_pub_date_and_zero_milliseconds_are_dropped", ["BR-NWS-278"],
        lambda: server.get_company_news("AMD", 100, 14, ["yahoo_finance_news"]), 'm.yahooFinance.getCompanyNews("AMD", 100, 14, ["yahoo_finance_news"])',
        news=[{"title": "AMD tick", "publisher": "R", "link": "https://e.com/a", "providerPublishTime": _YAHOO_TS},
              {"title": "AMD zero", "publisher": "R", "link": "https://e.com/b", "providerPublishTime": 0, "content": {"pubDate": "2026-10-13T00:00:00Z"}}],
        info=AMD_INFO, select=lambda v: [[i["title"], i["publishedAt"], v["meta"]["watermark"]] for i in v["items"]],
        expect=[["AMD tick", "2026-10-14T09:00:00Z", NOW_ISO], ["AMD zero", "2026-10-13T00:00:00Z", NOW_ISO]]),
    _scenario(
        "news.timestamps.provider_date_strings_parse_alike", ["BR-NWS-278"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": "AAPL rfc date", "summary": "s", "source": "W", "url": "https://e.com/1", "datetime": "Tue, 13 Oct 2026 10:00:00 GMT"},
                         {"headline": "AAPL iso date", "summary": "s", "source": "W", "url": "https://e.com/2", "datetime": "2026-10-13T10:00:00Z"},
                         {"headline": "AAPL compact", "summary": "s", "source": "W", "url": "https://e.com/3", "datetime": "20261012"},
                         {"headline": "AAPL compact14", "summary": "s", "source": "W", "url": "https://e.com/4", "datetime": "20261011101010"}]),
        select=lambda v: [[i["title"], i["publishedAt"]] for i in v["items"]], expect=[["AAPL rfc date", "2026-10-13T10:00:00Z"], ["AAPL iso date", "2026-10-13T10:00:00Z"], ["AAPL compact", "2026-10-12T00:00:00Z"],
                   ["AAPL compact14", "2026-10-11T10:10:10Z"]]),
]

# Card 263: the registry shipped today (version 2026-09-25.1200) holds only candidates: IQE.L is reported as a candidate, never fetched.
_CANDIDATE_VIEW = lambda v: {  # noqa: E731
    "status": v["sourceStatus"]["company_ir_page"]["status"], "approval": v["sourceStatus"]["company_ir_page"]["approvalStatus"],
    "allowedAction": v["sourceStatus"]["company_ir_page"]["allowedAction"], "registryVersion": v["sourceStatus"]["company_ir_page"]["registryVersion"],
    "candidates": [{k: c[k] for k in ("ticker", "issuerName", "canonicalUrl", "adapter", "decisionGrade")} for c in v["sourceStatus"]["company_ir_page"]["candidates"]],
    "failed": [[f["source"], f["status"]] for f in v["coverage"]["failedSources"]], "warnings": _warning_codes(v), "collection": v.get("status"),
    "items": len(v["items"]), "sourceCoverage": v["sourceCoverage"]}
_CANDIDATE_COMMON = {"status": "CANDIDATE_AVAILABLE", "approval": "candidate", "allowedAction": "review_and_promote", "registryVersion": "2026-09-25.1200",
                     "candidates": [{"ticker": "IQE.L", "issuerName": "IQE plc", "canonicalUrl": "https://www.iqep.com/investors/regulatory-news/",
                                     "adapter": "html_article", "decisionGrade": False}],
                     "failed": [["company_ir_page", "CANDIDATE_AVAILABLE"]], "items": 0, "sourceCoverage": "PARTIAL"}
CASES += [
    _scenario(
        "news.ir_page.shipped_registry_entries_are_candidates_only", ["BR-NWS-263"],
        lambda: server.get_company_news("IQE.L", 10, 14, ["company_ir_page"]), 'm.yahooFinance.getCompanyNews("IQE.L", 10, 14, ["company_ir_page"])',
        news=[], info={}, select=_CANDIDATE_VIEW,
        expect={**_CANDIDATE_COMMON, "warnings": ["COMPANY_IR_PAGE_CANDIDATE_AVAILABLE"], "collection": "SOURCE_LIMITED_NOT_FOUND"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Python-only helpers with no reachable Worker counterpart (BR-NWS-235, BR-NWS-250)
# The Worker side is a placeholder returning null: the SEC URL checks are inline / unexported there and unreachable through a tool
# (every SEC URL is built from the CIK and accession), and the audit script is a Python-only CI tool. The cases pin the Python value
# so the move of the Python news code cannot change it.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _audit_overlap_py():
    import importlib.util
    spec = importlib.util.spec_from_file_location("audit_arsenal_news", Path(__file__).resolve().parents[1] / "audit_arsenal_news.py")
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)

    def response(titles, source, status):
        return {"ok": True, "data": {"status": None, "items": [{"title": t, "evidenceClass": "CONTEXTUAL_NEWS"} for t in titles],
                                     "sourceStatus": {source: {"status": status}}, "coverage": {"state": "FULL"}}}

    both = audit.build_ticker_result("AAA", {"yahoo_finance_news": response(["A", "B", "C"], "yahoo_finance_news", "OK"),
                                             "finnhub": response(["b", "D"], "finnhub", "OK")})
    none = audit.build_ticker_result("BBB", {"yahoo_finance_news": {"ok": False}, "finnhub": response([], "finnhub", "EMPTY_RESULT")})
    return {"both": both["comparison"], "none": none["comparison"],
            "states": [none["providers"]["yahoo_finance_news"]["callState"], none["providers"]["finnhub"]["callState"]]}


CASES += [
    Case(name="news.python_only.safe_sec_url_and_xbrl_document_detection", ids=["BR-NWS-235"],
         py=lambda: {
             "safe": [server._safe_sec_url(u) for u in ("http://www.sec.gov/Archives/x.htm", "https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1",
                                                       "  https://www.sec.gov/Archives/xbrl/a.htm  ", None, "")],
             "xbrl": [server._is_likely_xbrl_document_url(u) for u in ("https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1",
                                                                      "https://www.sec.gov/Archives/xbrl/a.htm", "https://www.sec.gov/Archives/edgar/data/1/a.htm")]},
         ts={"js": "return null;"}, mode="pinned",
         decision="FINDING: Python-only helpers (the Worker checks the SEC Archives prefix inline and cannot be driven to an unsafe URL); the Worker side is a placeholder",
         expect_py={"safe": [None, "https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1", "https://www.sec.gov/Archives/xbrl/a.htm", None, None],
                    "xbrl": [True, True, False]},
         expect_ts=None),
    Case(name="news.python_only.audit_title_overlap_ratio_and_call_states", ids=["BR-NWS-250"],
         py=_audit_overlap_py, ts={"js": "return null;"}, mode="pinned",
         decision="FINDING: scripts/audit_arsenal_news.py is a Python-only CI audit; the Worker side is a placeholder",
         expect_py={"both": {"sharedTitleCount": 1, "combinedUniqueTitleCount": 4, "titleOverlapRatio": 0.25},
                    "none": {"sharedTitleCount": 0, "combinedUniqueTitleCount": 0, "titleOverlapRatio": None},
                    "states": ["CALL_FAILED", "COMPLETED"]},
         expect_ts=None),
]




# ═══════════════════════════════════════════════════════════════════════════════════════════════
# News source registry and the default sources (2.16.0, RNEWS2 §1 / §4.1 / §6)
# worker/src/news-source-registry.json holds defaultSources; both runtimes read it.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_NEWS_SOURCE_REGISTRY = json.loads((WORKER / "src" / "news-source-registry.json").read_text(encoding="utf-8"))
_DEFAULTS_2160 = ["yahoo_finance_news", "yahoo_finance_press_releases", "sec", "finnhub", "marketaux"]


def _default_sec_view(v):
    return {"requested": v["requestedSources"], "defaultApplied": v["defaultSourcesApplied"],
            "attempted": [[a["sourceType"], a["status"]] for a in v["attemptedSources"]],
            "sourcesUsed": v["meta"]["sourcesUsed"],
            "sourceStatus": {k: s.get("status") for k, s in v["sourceStatus"].items()},
            "sec": _status_rows(v, "sec", ("status", "rawCount", "filteredCount")),
            "items": [[i.get("source"), i.get("sourceType"), i.get("filingType")] for i in v["items"]],
            "status": v.get("status"), "coverage": v["coverage"]["state"]}


CASES += [
    # §1: the default news sources are the registry's defaultSources, with sec right after yahoo_finance_press_releases.
    Case(name="news.registry.default_news_sources_come_from_the_registry", ids=["RNEWS2-1", "BR-NWS-269"],
         py=lambda: list(news_tools._DEFAULT_NEWS_SOURCES),
         ts={"js": "return [...m.yahooFinance.DEFAULT_NEWS_SOURCES];"},
         expect=_DEFAULTS_2160),
    # §1: the registry file itself carries the planner's default order (data shared by both runtimes, so equal today).
    Case(name="news.registry.default_sources_order_in_the_registry_file", ids=["RNEWS2-1"],
         py=lambda: _NEWS_SOURCE_REGISTRY["defaultSources"],
         ts={"js": "return [...m.yahooFinance.DEFAULT_NEWS_SOURCES];"},
         expect=_DEFAULTS_2160),
    # §4.1 / §6: a default get_company_news call includes sec as an explicit sec would: SEC items, sourceStatus.sec,
    # sourcesUsed and attemptedSources.
    _sec_scenario(
        "news.sources.default_call_includes_sec_status_and_items", ["RNEWS2-4.1", "BR-NWS-269"],
        lambda: server.get_company_news("ACME", 10, 14),
        'm.yahooFinance.getCompanyNews("ACME", 10, 14)',
        _sec_recent(["8-K"], [2]), info={"shortName": "Acme Corp", "longName": "Acme Corp"},
        news=[_yn("ACME shares rise", when=_days_ago(1))], select=_default_sec_view,
        expect={"requested": None, "defaultApplied": True,
                   "attempted": [["sec", "OK"], ["yahoo_finance_news", "OK"], ["yahoo_finance_press_releases", "EMPTY_RESULT"]],
                   "sourcesUsed": ["sec", "yahoo_finance_news", "yahoo_finance_press_releases"],
                   "sourceStatus": {"sec": "OK", "yahoo_finance_news": "OK", "yahoo_finance_press_releases": "EMPTY_RESULT",
                                    "finnhub": "UNCONFIGURED", "marketaux": "UNCONFIGURED"},
                   "sec": {"status": "OK", "rawCount": 1, "filteredCount": 1},
                   "items": [["yahoo_finance_news", "yahoo_finance_news", None], ["SEC", "sec_filing", "8-K"]],
                   "status": "PARTIAL", "coverage": "PARTIAL"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Operator off switch NEWS_SOURCES_DISABLED (2.16.0, RNEWS2 §2 / §8 / §6)
# Each case returns the tool result plus a fetch spy: the non-Yahoo hosts fetched (Worker fetch, Python urllib) and
# whether the Yahoo feed was read (Worker fetch to a Yahoo host, Python's Yahoo search helper).
# Both runtimes apply the setting (Python ported in 2.16.0), so these cases run in mode equal.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from urllib.parse import urlparse as _urlparse  # noqa: E402
import urllib.request as _urllib_request  # noqa: E402

_OFF_RECENT = _sec_recent(["8-K"], [2])
_OFF_INFO = {"shortName": "Acme Corp", "longName": "Acme Corp"}
_OFF_NEWS = [_yn("ACME shares rise", when=_days_ago(1))]
_OFF_KEYS = {**FINNHUB_ENV, **MARKETAUX_ENV}
_OFF_ROUTES = {**_sec_routes(SEC_CIK, "ACME", "Acme Corp", _OFF_RECENT), **_finnhub([]), MARKETAUX_PREFIX: {"json": {"data": []}}}
# Provider hosts the spy reports (the SEC ticker map on www.sec.gov is an identity lookup the runtimes make differently).
_SPY_PROVIDER_HOSTS = {"finnhub.io", "api.marketaux.com", "data.sec.gov"}
_SPY_JS = """
const __hosts = new Set(); let __yahoo = false; const __realFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => {
  const u = new URL(typeof input === "string" ? input : input instanceof URL ? input.href : input.url);
  if (u.hostname.includes("yahoo") && u.pathname.startsWith("/v1/finance/search")) __yahoo = true; else __hosts.add(u.hostname);
  return __realFetch(input, init);
};
try {
  const __result = JSON.parse(await %s);
  return { result: __result, fetchedHosts: [...__hosts].filter(h => %s.includes(h)).sort(), yahooFetched: __yahoo };
} finally { globalThis.fetch = __realFetch; }
"""


_OFF_EXPECT = {'news.off_switch.disabled_default_finnhub_is_not_fetched_and_reported': {'py': {'attempted': ['yahoo_finance_news',
                                                                                               'yahoo_finance_press_releases',
                                                                                               'finnhub'],
                                                                                 'coverage': 'FULL',
                                                                                 'failed': [],
                                                                                 'fetchedHosts': ['finnhub.io'],
                                                                                 'marketaux': {'fallbackReason': None,
                                                                                               'status': 'NOT_NEEDED'},
                                                                                 'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                                  'marketaux': 'NOT_NEEDED',
                                                                                                  'yahoo_finance_news': 'OK',
                                                                                                  'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                                 'sourcesUsed': ['yahoo_finance_news',
                                                                                                 'yahoo_finance_press_releases',
                                                                                                 'finnhub'],
                                                                                 'status': None,
                                                                                 'warnings': [],
                                                                                 'yahooFetched': True},
                                                                          'ts': {'attempted': ['sec',
                                                                                               'yahoo_finance_news',
                                                                                               'yahoo_finance_press_releases',
                                                                                               'marketaux'],
                                                                                 'coverage': 'PARTIAL',
                                                                                 'failed': [['finnhub', 'UNCONFIGURED', False, None]],
                                                                                 'fetchedHosts': ['api.marketaux.com', 'data.sec.gov'],
                                                                                 'marketaux': {'fallbackReason': 'HIGHER_PRIORITY_COVERAGE_LIMITED',
                                                                                               'status': 'EMPTY_RESULT'},
                                                                                 'sourceStatus': {'finnhub': {'attempted': False,
                                                                                                              'filteredCount': 0,
                                                                                                              'rawCount': 0,
                                                                                                              'status': 'UNCONFIGURED'},
                                                                                                  'marketaux': 'EMPTY_RESULT',
                                                                                                  'sec': 'OK',
                                                                                                  'yahoo_finance_news': 'OK',
                                                                                                  'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                                 'sourcesUsed': ['sec',
                                                                                                 'yahoo_finance_news',
                                                                                                 'yahoo_finance_press_releases',
                                                                                                 'marketaux'],
                                                                                 'status': 'PARTIAL',
                                                                                 'warnings': [['SOURCE_UNAVAILABLE',
                                                                                               'info',
                                                                                               'finnhub',
                                                                                               'Source finnhub is disabled by the server '
                                                                                               'operator.']],
                                                                                 'yahooFetched': True}},
 'news.off_switch.empty_setting_changes_nothing': {'expect': {'attempted': ['yahoo_finance_news', 'finnhub'],
                                                              'coverage': 'FULL',
                                                              'failed': [],
                                                              'fetchedHosts': ['finnhub.io'],
                                                              'marketaux': {'fallbackReason': None, 'status': 'NOT_NEEDED'},
                                                              'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                               'marketaux': 'NOT_NEEDED',
                                                                               'yahoo_finance_news': 'OK'},
                                                              'sourcesUsed': ['yahoo_finance_news', 'finnhub'],
                                                              'status': None,
                                                              'warnings': [],
                                                              'yahooFetched': True}},
 'news.off_switch.every_default_source_disabled_fetches_nothing': {'py': {'attempted': ['yahoo_finance_news',
                                                                                        'yahoo_finance_press_releases',
                                                                                        'finnhub'],
                                                                          'coverage': 'FULL',
                                                                          'failed': [],
                                                                          'fetchedHosts': ['finnhub.io'],
                                                                          'marketaux': {'fallbackReason': None, 'status': 'NOT_NEEDED'},
                                                                          'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                           'marketaux': 'NOT_NEEDED',
                                                                                           'yahoo_finance_news': 'OK',
                                                                                           'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                          'sourcesUsed': ['yahoo_finance_news',
                                                                                          'yahoo_finance_press_releases',
                                                                                          'finnhub'],
                                                                          'status': None,
                                                                          'warnings': [],
                                                                          'yahooFetched': True},
                                                                   'ts': {'attempted': [],
                                                                          'coverage': 'PARTIAL',
                                                                          'failed': [['sec', 'UNCONFIGURED', False, None],
                                                                                     ['yahoo_finance_news', 'UNCONFIGURED', False, None],
                                                                                     ['yahoo_finance_press_releases',
                                                                                      'UNCONFIGURED',
                                                                                      False,
                                                                                      None],
                                                                                     ['finnhub', 'UNCONFIGURED', False, None],
                                                                                     ['marketaux', 'UNCONFIGURED', False, None]],
                                                                          'fetchedHosts': [],
                                                                          'marketaux': {'fallbackReason': None, 'status': 'UNCONFIGURED'},
                                                                          'sourceStatus': {'finnhub': {'attempted': False,
                                                                                                       'filteredCount': 0,
                                                                                                       'rawCount': 0,
                                                                                                       'status': 'UNCONFIGURED'},
                                                                                           'marketaux': {'attempted': False,
                                                                                                         'filteredCount': 0,
                                                                                                         'rawCount': 0,
                                                                                                         'status': 'UNCONFIGURED'},
                                                                                           'sec': {'attempted': False,
                                                                                                   'filteredCount': 0,
                                                                                                   'rawCount': 0,
                                                                                                   'status': 'UNCONFIGURED'},
                                                                                           'yahoo_finance_news': {'attempted': False,
                                                                                                                  'filteredCount': 0,
                                                                                                                  'rawCount': 0,
                                                                                                                  'status': 'UNCONFIGURED'},
                                                                                           'yahoo_finance_press_releases': {'attempted': False,
                                                                                                                            'filteredCount': 0,
                                                                                                                            'rawCount': 0,
                                                                                                                            'status': 'UNCONFIGURED'}},
                                                                          'sourcesUsed': [],
                                                                          'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                                          'warnings': [['SOURCE_UNAVAILABLE',
                                                                                        'info',
                                                                                        'yahoo_finance_news',
                                                                                        'Source yahoo_finance_news is disabled by the '
                                                                                        'server operator.'],
                                                                                       ['SOURCE_UNAVAILABLE',
                                                                                        'info',
                                                                                        'yahoo_finance_press_releases',
                                                                                        'Source yahoo_finance_press_releases is disabled '
                                                                                        'by the server operator.'],
                                                                                       ['SOURCE_UNAVAILABLE',
                                                                                        'info',
                                                                                        'sec',
                                                                                        'Source sec is disabled by the server operator.'],
                                                                                       ['SOURCE_UNAVAILABLE',
                                                                                        'info',
                                                                                        'finnhub',
                                                                                        'Source finnhub is disabled by the server '
                                                                                        'operator.'],
                                                                                       ['SOURCE_UNAVAILABLE',
                                                                                        'info',
                                                                                        'marketaux',
                                                                                        'Source marketaux is disabled by the server '
                                                                                        'operator.']],
                                                                          'yahooFetched': False}},
 'news.off_switch.every_requested_source_disabled_fetches_nothing': {'py': {'attempted': ['finnhub', 'marketaux'],
                                                                            'coverage': 'FULL',
                                                                            'failed': [],
                                                                            'fetchedHosts': ['api.marketaux.com', 'finnhub.io'],
                                                                            'marketaux': {'fallbackReason': 'HIGHER_PRIORITY_EMPTY',
                                                                                          'status': 'EMPTY_RESULT'},
                                                                            'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                             'marketaux': 'EMPTY_RESULT'},
                                                                            'sourcesUsed': ['finnhub', 'marketaux'],
                                                                            'status': 'NOT_FOUND',
                                                                            'warnings': [],
                                                                            'yahooFetched': False},
                                                                     'ts': {'attempted': [],
                                                                            'coverage': 'PARTIAL',
                                                                            'failed': [['finnhub', 'UNCONFIGURED', False, None],
                                                                                       ['marketaux', 'UNCONFIGURED', False, None]],
                                                                            'fetchedHosts': [],
                                                                            'marketaux': {'fallbackReason': None, 'status': 'UNCONFIGURED'},
                                                                            'sourceStatus': {'finnhub': {'attempted': False,
                                                                                                         'filteredCount': 0,
                                                                                                         'rawCount': 0,
                                                                                                         'status': 'UNCONFIGURED'},
                                                                                             'marketaux': {'attempted': False,
                                                                                                           'filteredCount': 0,
                                                                                                           'rawCount': 0,
                                                                                                           'status': 'UNCONFIGURED'}},
                                                                            'sourcesUsed': [],
                                                                            'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                                            'warnings': [['SOURCE_UNAVAILABLE',
                                                                                          'info',
                                                                                          'finnhub',
                                                                                          'Source finnhub is disabled by the server '
                                                                                          'operator.'],
                                                                                         ['SOURCE_UNAVAILABLE',
                                                                                          'info',
                                                                                          'marketaux',
                                                                                          'Source marketaux is disabled by the server '
                                                                                          'operator.']],
                                                                            'yahooFetched': False}},
 'news.off_switch.explicit_disabled_source_gets_the_same_report': {'py': {'attempted': ['yahoo_finance_news', 'finnhub'],
                                                                          'coverage': 'FULL',
                                                                          'failed': [],
                                                                          'fetchedHosts': ['finnhub.io'],
                                                                          'marketaux': None,
                                                                          'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                           'yahoo_finance_news': 'OK'},
                                                                          'sourcesUsed': ['yahoo_finance_news', 'finnhub'],
                                                                          'status': None,
                                                                          'warnings': [],
                                                                          'yahooFetched': True},
                                                                   'ts': {'attempted': ['yahoo_finance_news'],
                                                                          'coverage': 'PARTIAL',
                                                                          'failed': [['finnhub', 'UNCONFIGURED', False, None]],
                                                                          'fetchedHosts': [],
                                                                          'marketaux': None,
                                                                          'sourceStatus': {'finnhub': {'attempted': False,
                                                                                                       'filteredCount': 0,
                                                                                                       'rawCount': 0,
                                                                                                       'status': 'UNCONFIGURED'},
                                                                                           'yahoo_finance_news': 'OK'},
                                                                          'sourcesUsed': ['yahoo_finance_news'],
                                                                          'status': 'PARTIAL',
                                                                          'warnings': [['SOURCE_UNAVAILABLE',
                                                                                        'info',
                                                                                        'finnhub',
                                                                                        'Source finnhub is disabled by the server '
                                                                                        'operator.']],
                                                                          'yahooFetched': True}},
 'news.off_switch.marketaux_disabled_skips_the_fallback': {'py': {'attempted': ['yahoo_finance_news',
                                                                                'yahoo_finance_press_releases',
                                                                                'finnhub'],
                                                                  'coverage': 'FULL',
                                                                  'failed': [],
                                                                  'fetchedHosts': ['finnhub.io'],
                                                                  'marketaux': {'fallbackReason': None, 'status': 'NOT_NEEDED'},
                                                                  'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                   'marketaux': 'NOT_NEEDED',
                                                                                   'yahoo_finance_news': 'OK',
                                                                                   'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                  'sourcesUsed': ['yahoo_finance_news',
                                                                                  'yahoo_finance_press_releases',
                                                                                  'finnhub'],
                                                                  'status': None,
                                                                  'warnings': [],
                                                                  'yahooFetched': True},
                                                           'ts': {'attempted': ['sec',
                                                                                'yahoo_finance_news',
                                                                                'yahoo_finance_press_releases'],
                                                                  'coverage': 'PARTIAL',
                                                                  'failed': [['finnhub', 'UNCONFIGURED', False, None],
                                                                             ['marketaux', 'UNCONFIGURED', False, None]],
                                                                  'fetchedHosts': ['data.sec.gov'],
                                                                  'marketaux': {'fallbackReason': None, 'status': 'UNCONFIGURED'},
                                                                  'sourceStatus': {'finnhub': {'attempted': False,
                                                                                               'filteredCount': 0,
                                                                                               'rawCount': 0,
                                                                                               'status': 'UNCONFIGURED'},
                                                                                   'marketaux': {'attempted': False,
                                                                                                 'filteredCount': 0,
                                                                                                 'rawCount': 0,
                                                                                                 'status': 'UNCONFIGURED'},
                                                                                   'sec': 'OK',
                                                                                   'yahoo_finance_news': 'OK',
                                                                                   'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                  'sourcesUsed': ['sec',
                                                                                  'yahoo_finance_news',
                                                                                  'yahoo_finance_press_releases'],
                                                                  'status': 'PARTIAL',
                                                                  'warnings': [['SOURCE_UNAVAILABLE',
                                                                                'info',
                                                                                'finnhub',
                                                                                'Source finnhub is disabled by the server operator.'],
                                                                               ['SOURCE_UNAVAILABLE',
                                                                                'info',
                                                                                'marketaux',
                                                                                'Source marketaux is disabled by the server operator.']],
                                                                  'yahooFetched': True}},
 'news.off_switch.press_releases_report_a_disabled_source_once': {'py': {'attempted': ['sec', 'yahoo_finance_press_releases'],
                                                                         'coverage': 'FULL',
                                                                         'coverageStatus': 'SEC_8K_FOUND_EX99_NOT_FOUND',
                                                                         'failed': [],
                                                                         'fetchedHosts': ['data.sec.gov'],
                                                                         'marketaux': None,
                                                                         'sourceStatus': {'sec': 'OK',
                                                                                          'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                         'sourcesUsed': ['sec', 'yahoo_finance_press_releases'],
                                                                         'status': 'SEC_8K_FOUND_EX99_NOT_FOUND',
                                                                         'warnings': [['SEC_8K_FOUND_EX99_NOT_FOUND',
                                                                                       'warning',
                                                                                       None,
                                                                                       'SEC 8-K filing(s) were found, but no EX-99.1 '
                                                                                       'press-release exhibit was resolved.']],
                                                                         'yahooFetched': True},
                                                                  'ts': {'attempted': ['yahoo_finance_press_releases'],
                                                                         'coverage': 'PARTIAL',
                                                                         'coverageStatus': 'NO_OFFICIAL_RELEASE_SOURCE',
                                                                         'failed': [['sec', 'UNCONFIGURED', False, None]],
                                                                         'fetchedHosts': [],
                                                                         'marketaux': None,
                                                                         'sourceStatus': {'sec': {'attempted': False,
                                                                                                  'filteredCount': 0,
                                                                                                  'rawCount': 0,
                                                                                                  'status': 'UNCONFIGURED'},
                                                                                          'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                         'sourcesUsed': ['yahoo_finance_press_releases'],
                                                                         'status': 'NO_YAHOO_PRESS_RELEASE',
                                                                         'warnings': [['SOURCE_UNAVAILABLE',
                                                                                       'info',
                                                                                       'sec',
                                                                                       'Source sec is disabled by the server operator.'],
                                                                                      ['NO_OFFICIAL_RELEASE_SOURCE',
                                                                                       'warning',
                                                                                       None,
                                                                                       'No company-originated or official release source '
                                                                                       'found in requested window.']],
                                                                         'yahooFetched': True}},
 'news.off_switch.press_releases_with_every_default_disabled_are_source_limited': {'py': {'attempted': ['sec',
                                                                                                        'yahoo_finance_press_releases',
                                                                                                        'company_ir_page'],
                                                                                          'coverage': 'PARTIAL',
                                                                                          'coverageStatus': 'SEC_8K_FOUND_EX99_NOT_FOUND',
                                                                                          'failed': [['company_ir_page',
                                                                                                      'NOT_REGISTERED',
                                                                                                      True,
                                                                                                      None]],
                                                                                          'fetchedHosts': ['data.sec.gov'],
                                                                                          'marketaux': None,
                                                                                          'sourceStatus': {'company_ir_page': 'NOT_REGISTERED',
                                                                                                           'sec': 'OK',
                                                                                                           'yahoo_finance_press_releases': 'EMPTY_RESULT'},
                                                                                          'sourcesUsed': ['sec',
                                                                                                          'yahoo_finance_press_releases'],
                                                                                          'status': 'SEC_8K_FOUND_EX99_NOT_FOUND',
                                                                                          'warnings': [['SEC_8K_FOUND_EX99_NOT_FOUND',
                                                                                                        'warning',
                                                                                                        None,
                                                                                                        'SEC 8-K filing(s) were found, but '
                                                                                                        'no EX-99.1 press-release exhibit '
                                                                                                        'was resolved.']],
                                                                                          'yahooFetched': True},
                                                                                   'ts': {'attempted': [],
                                                                                          'coverage': 'PARTIAL',
                                                                                          'coverageStatus': 'NO_OFFICIAL_RELEASE_SOURCE',
                                                                                          'failed': [['sec', 'UNCONFIGURED', False, None],
                                                                                                     ['yahoo_finance_press_releases',
                                                                                                      'UNCONFIGURED',
                                                                                                      False,
                                                                                                      None],
                                                                                                     ['company_ir_page',
                                                                                                      'UNCONFIGURED',
                                                                                                      False,
                                                                                                      None]],
                                                                                          'fetchedHosts': [],
                                                                                          'marketaux': None,
                                                                                          'sourceStatus': {'company_ir_page': {'attempted': False,
                                                                                                                               'filteredCount': 0,
                                                                                                                               'rawCount': 0,
                                                                                                                               'status': 'UNCONFIGURED'},
                                                                                                           'sec': {'attempted': False,
                                                                                                                   'filteredCount': 0,
                                                                                                                   'rawCount': 0,
                                                                                                                   'status': 'UNCONFIGURED'},
                                                                                                           'yahoo_finance_press_releases': {'attempted': False,
                                                                                                                                            'filteredCount': 0,
                                                                                                                                            'rawCount': 0,
                                                                                                                                            'status': 'UNCONFIGURED'}},
                                                                                          'sourcesUsed': [],
                                                                                          'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                                                          'warnings': [['SOURCE_UNAVAILABLE',
                                                                                                        'info',
                                                                                                        'sec',
                                                                                                        'Source sec is disabled by the '
                                                                                                        'server operator.'],
                                                                                                       ['SOURCE_UNAVAILABLE',
                                                                                                        'info',
                                                                                                        'company_ir_page',
                                                                                                        'Source company_ir_page is '
                                                                                                        'disabled by the server operator.'],
                                                                                                       ['SOURCE_UNAVAILABLE',
                                                                                                        'info',
                                                                                                        'yahoo_finance_press_releases',
                                                                                                        'Source '
                                                                                                        'yahoo_finance_press_releases is '
                                                                                                        'disabled by the server operator.'],
                                                                                                       ['NO_OFFICIAL_RELEASE_SOURCE',
                                                                                                        'warning',
                                                                                                        None,
                                                                                                        'No company-originated or official '
                                                                                                        'release source found in requested '
                                                                                                        'window.']],
                                                                                          'yahooFetched': False}},
 'news.off_switch.sec_recent_events_ignore_a_disabled_sec': {'expect': {'fetchedHosts': ['data.sec.gov'],
                                                                        'items': ['8-K'],
                                                                        'sourcesUsed': ['sec'],
                                                                        'warnings': []}},
 'news.off_switch.timeline_reports_a_disabled_source': {'py': {'attempted': ['sec', 'finnhub'],
                                                               'coverage': 'FULL',
                                                               'failed': [],
                                                               'fetchedHosts': ['data.sec.gov', 'finnhub.io'],
                                                               'marketaux': None,
                                                               'sourceStatus': {'finnhub': 'EMPTY_RESULT', 'sec': 'OK'},
                                                               'sourcesUsed': ['sec', 'finnhub'],
                                                               'status': None,
                                                               'warnings': [],
                                                               'yahooFetched': False},
                                                        'ts': {'attempted': ['sec'],
                                                               'coverage': 'PARTIAL',
                                                               'failed': [['finnhub', 'UNCONFIGURED', False, None]],
                                                               'fetchedHosts': ['data.sec.gov'],
                                                               'marketaux': None,
                                                               'sourceStatus': {'finnhub': {'attempted': False,
                                                                                            'filteredCount': 0,
                                                                                            'rawCount': 0,
                                                                                            'status': 'UNCONFIGURED'},
                                                                                'sec': 'OK'},
                                                               'sourcesUsed': ['sec'],
                                                               'status': 'PARTIAL',
                                                               'warnings': [['SOURCE_UNAVAILABLE',
                                                                             'info',
                                                                             'finnhub',
                                                                             'Source finnhub is disabled by the server operator.']],
                                                               'yahooFetched': False}},
 'news.off_switch.unknown_names_are_ignored': {'expect': {'attempted': ['yahoo_finance_news', 'finnhub'],
                                                          'coverage': 'FULL',
                                                          'failed': [],
                                                          'fetchedHosts': ['finnhub.io'],
                                                          'marketaux': {'fallbackReason': None, 'status': 'NOT_NEEDED'},
                                                          'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                           'marketaux': 'NOT_NEEDED',
                                                                           'yahoo_finance_news': 'OK'},
                                                          'sourcesUsed': ['yahoo_finance_news', 'finnhub'],
                                                          'status': None,
                                                          'warnings': [],
                                                          'yahooFetched': True}},
 'news.off_switch.unset_baseline_for_the_two_cases_above': {'expect': {'attempted': ['yahoo_finance_news', 'finnhub'],
                                                                       'coverage': 'FULL',
                                                                       'failed': [],
                                                                       'fetchedHosts': ['finnhub.io'],
                                                                       'marketaux': {'fallbackReason': None, 'status': 'NOT_NEEDED'},
                                                                       'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                        'marketaux': 'NOT_NEEDED',
                                                                                        'yahoo_finance_news': 'OK'},
                                                                       'sourcesUsed': ['yahoo_finance_news', 'finnhub'],
                                                                       'status': None,
                                                                       'warnings': [],
                                                                       'yahooFetched': True}},
 'news.off_switch.verify_company_event_reports_a_disabled_source': {'py': {'attempted': ['yahoo_finance_news', 'finnhub'],
                                                                           'coverage': 'FULL',
                                                                           'failed': [],
                                                                           'failureMode': None,
                                                                           'fetchedHosts': ['finnhub.io'],
                                                                           'marketaux': None,
                                                                           'sourceStatus': {'finnhub': 'EMPTY_RESULT',
                                                                                            'yahoo_finance_news': 'OK'},
                                                                           'sourcesUsed': None,
                                                                           'status': 'PARTIAL',
                                                                           'warnings': [],
                                                                           'yahooFetched': True},
                                                                    'ts': {'attempted': ['yahoo_finance_news'],
                                                                           'coverage': 'PARTIAL',
                                                                           'failed': [['finnhub', 'UNCONFIGURED', False, None]],
                                                                           'failureMode': None,
                                                                           'fetchedHosts': [],
                                                                           'marketaux': None,
                                                                           'sourceStatus': {'finnhub': {'attempted': False,
                                                                                                        'filteredCount': 0,
                                                                                                        'rawCount': 0,
                                                                                                        'status': 'UNCONFIGURED'},
                                                                                            'yahoo_finance_news': 'OK'},
                                                                           'sourcesUsed': None,
                                                                           'status': 'PARTIAL',
                                                                           'warnings': [['SOURCE_UNAVAILABLE',
                                                                                         'info',
                                                                                         'finnhub',
                                                                                         'Source finnhub is disabled by the server '
                                                                                         'operator.']],
                                                                           'yahooFetched': True}}}


def _off_case(name, ids, py_call, ts_call, disabled, *, env=None, news=None, info=None, routes=None, submissions=None, select=None, **kw) -> Case:
    full_env = {**(env if env is not None else _OFF_KEYS)}
    if disabled is not None:
        full_env["NEWS_SOURCES_DISABLED"] = disabled
    news = _OFF_NEWS if news is None else news
    info = _OFF_INFO if info is None else info
    submissions = _py_sec(SEC_CIK, "Acme Corp", _OFF_RECENT) if submissions is None else submissions

    def py():
        hosts: set = set()
        yahoo = [False]
        with _py_news_env(news=news, info=info, env=full_env, submissions=submissions):
            orig_urlopen, orig_open = _urllib_request.urlopen, _urllib_request.OpenerDirector.open
            orig_search = yahoo_provider._yahoo_search_news

            def _host(req):
                return _urlparse(req if isinstance(req, str) else req.full_url).hostname

            def spy_urlopen(req, *a, **k):
                hosts.add(_host(req))
                return orig_urlopen(req, *a, **k)

            def spy_open(self, req, *a, **k):
                hosts.add(_host(req))
                return orig_open(self, req, *a, **k)

            def spy_search(ticker):
                yahoo[0] = True
                return orig_search(ticker)

            async def spy_submissions(*_a, **_k):
                hosts.add("data.sec.gov")  # Python serves SEC submissions from a stub; the Worker fetches data.sec.gov
                return submissions

            with patch.object(_urllib_request, "urlopen", spy_urlopen), \
                    patch.object(_urllib_request.OpenerDirector, "open", spy_open), \
                    patch.object(yahoo_provider, "_yahoo_search_news", spy_search), \
                    patch.object(news_tools, "_get_submissions_for_ticker", spy_submissions), \
                    patch.object(sec_provider, "_get_submissions_for_ticker", spy_submissions):
                result = json.loads(asyncio.run(py_call()))
        return {"result": result, "fetchedHosts": sorted(h for h in hosts if h in _SPY_PROVIDER_HOSTS), "yahooFetched": yahoo[0]}

    all_routes = {**_yahoo_routes(news, info), **(_OFF_ROUTES if routes is None else routes)}
    js = _worker_js(_SPY_JS % (ts_call, json.dumps(sorted(_SPY_PROVIDER_HOSTS))), full_env)
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True}, routes=all_routes, select=select, **kw)


def _off_view(v):
    r = v["result"]
    return {"sourceStatus": {k: (s if s.get("status") == "UNCONFIGURED" and s.get("attempted") is False else s.get("status"))
                             for k, s in r["sourceStatus"].items()},
            "warnings": [[w["code"], w.get("severity"), w.get("source"), w.get("message")] for w in r["warnings"]],
            "coverage": r["coverage"]["state"], "failed": [[f["source"], f["status"], f["attempted"], f["reasonCode"]] for f in r["coverage"]["failedSources"]],
            "status": r.get("status"), "sourcesUsed": (r.get("meta") or {}).get("sourcesUsed", r.get("sourcesUsed")),
            "attempted": [a["sourceType"] for a in r.get("attemptedSources", [])],
            "marketaux": {k: (r["sourceStatus"].get("marketaux") or {}).get(k) for k in ("status", "fallbackReason")} if "marketaux" in r["sourceStatus"] else None,
            "fetchedHosts": v["fetchedHosts"], "yahooFetched": v["yahooFetched"]}


def _off_news(disabled, sources=None, **kw):
    py_sources = "" if sources is None else f", {sources!r}"
    ts_sources = "" if sources is None else f", {json.dumps(sources)}"
    py_call = (lambda: server.get_company_news("ACME", 10, 14)) if sources is None else (lambda: server.get_company_news("ACME", 10, 14, sources))
    return py_call, f'm.yahooFinance.getCompanyNews("ACME", 10, 14{ts_sources})'


CASES += [
    # §6: one disabled default source (finnhub). No Finnhub fetch; the row is UNCONFIGURED/attempted false; one info warning;
    # failedSources gains it and coverage reads PARTIAL. A disabled finnhub counts as limited, so the Marketaux fallback runs
    # (HIGHER_PRIORITY_COVERAGE_LIMITED).
    _off_case("news.off_switch.disabled_default_finnhub_is_not_fetched_and_reported", ["RNEWS2-2", "RNEWS2-8.3"],
              *_off_news("finnhub"), "finnhub", select=_off_view, expect=_OFF_EXPECT["news.off_switch.disabled_default_finnhub_is_not_fetched_and_reported"]["ts"]),
    # §6: an explicitly requested disabled source gets the same reporting.
    _off_case("news.off_switch.explicit_disabled_source_gets_the_same_report", ["RNEWS2-2"],
              *_off_news("finnhub", ["yahoo_finance_news", "finnhub"]), "finnhub", select=_off_view, expect=_OFF_EXPECT["news.off_switch.explicit_disabled_source_gets_the_same_report"]["ts"]),
    # §2 / §8.3: finnhub and marketaux both disabled: no provider fetch, the fallback is skipped and reported as disabled.
    # Names are trimmed and lower-cased.
    _off_case("news.off_switch.marketaux_disabled_skips_the_fallback", ["RNEWS2-2", "RNEWS2-8.3"],
              *_off_news(None), " Finnhub , MARKETAUX ", select=_off_view, expect=_OFF_EXPECT["news.off_switch.marketaux_disabled_skips_the_fallback"]["ts"]),
    # §6: every requested source disabled: no fetch at all, source-limited status, never NOT_FOUND.
    _off_case("news.off_switch.every_requested_source_disabled_fetches_nothing", ["RNEWS2-2"],
              *_off_news(None, ["finnhub", "marketaux"]), "finnhub,marketaux", select=_off_view, expect=_OFF_EXPECT["news.off_switch.every_requested_source_disabled_fetches_nothing"]["ts"]),
    _off_case("news.off_switch.every_default_source_disabled_fetches_nothing", ["RNEWS2-2"],
              *_off_news(None), "yahoo_finance_news,yahoo_finance_press_releases,sec,finnhub,marketaux", select=_off_view,
              expect=_OFF_EXPECT["news.off_switch.every_default_source_disabled_fetches_nothing"]["ts"]),
    # §6: unknown names are ignored and an empty setting changes nothing (equal today and after the port: same as unset).
    _off_case("news.off_switch.unknown_names_are_ignored", ["RNEWS2-2"],
              *_off_news(None, ["yahoo_finance_news", "finnhub", "marketaux"]), "bloomberg, ,reuters", select=_off_view,
              expect=_OFF_EXPECT["news.off_switch.unknown_names_are_ignored"]["expect"]),
    _off_case("news.off_switch.empty_setting_changes_nothing", ["RNEWS2-2"],
              *_off_news(None, ["yahoo_finance_news", "finnhub", "marketaux"]), "", select=_off_view, expect=_OFF_EXPECT["news.off_switch.empty_setting_changes_nothing"]["expect"]),
    _off_case("news.off_switch.unset_baseline_for_the_two_cases_above", ["RNEWS2-2"],
              *_off_news(None, ["yahoo_finance_news", "finnhub", "marketaux"]), None, select=_off_view, expect=_OFF_EXPECT["news.off_switch.unset_baseline_for_the_two_cases_above"]["expect"]),
    # §8.5: get_company_press_releases attaches the disabled report once (row, warning, failedSources entry).
    _off_case("news.off_switch.press_releases_report_a_disabled_source_once", ["RNEWS2-2", "RNEWS2-8.5"],
              lambda: server.get_company_press_releases("ACME", 90, 20, ["sec", "yahoo_finance_press_releases"]),
              'm.yahooFinance.getCompanyPressReleases("ACME", 90, 20, ["sec", "yahoo_finance_press_releases"])', "sec",
              select=lambda v: {**_off_view(v), "coverageStatus": v["result"].get("coverageStatus")}, expect=_OFF_EXPECT["news.off_switch.press_releases_report_a_disabled_source_once"]["ts"]),
    _off_case("news.off_switch.press_releases_with_every_default_disabled_are_source_limited", ["RNEWS2-2", "RNEWS2-8.5"],
              lambda: server.get_company_press_releases("ACME", 90, 20),
              'm.yahooFinance.getCompanyPressReleases("ACME", 90, 20)', "sec,company_ir_page,yahoo_finance_press_releases",
              select=lambda v: {**_off_view(v), "coverageStatus": v["result"].get("coverageStatus")}, expect=_OFF_EXPECT["news.off_switch.press_releases_with_every_default_disabled_are_source_limited"]["ts"]),
    # §2: the timeline and verification use the same shared selection.
    _off_case("news.off_switch.verify_company_event_reports_a_disabled_source", ["RNEWS2-2"],
              lambda: server.verify_company_event("ACME", "shares rise", "", "", ["yahoo_finance_news", "finnhub"]),
              'm.yahooFinance.verifyCompanyEvent("ACME", "shares rise", "", "", ["yahoo_finance_news", "finnhub"])', "finnhub",
              # Python's verify reads SEC submissions for identity whatever the sources, so data.sec.gov is left out here.
              select=lambda v: {**_off_view(v), "fetchedHosts": [h for h in v["fetchedHosts"] if h != "data.sec.gov"],
                                "failureMode": v["result"].get("failureMode")},
              expect=_OFF_EXPECT["news.off_switch.verify_company_event_reports_a_disabled_source"]["ts"]),
    _off_case("news.off_switch.timeline_reports_a_disabled_source", ["RNEWS2-2"],
              lambda: server.get_public_event_timeline("ACME", "", "", ["sec", "finnhub"]),
              'm.yahooFinance.getPublicEventTimeline("ACME", "", "", ["sec", "finnhub"])', "finnhub",
              select=lambda v: {k: x for k, x in _off_view(v).items()}, expect=_OFF_EXPECT["news.off_switch.timeline_reports_a_disabled_source"]["ts"]),
    # §8.4: get_sec_recent_events selects no news sources, so a disabled sec does not affect it (equal today and after).
    _off_case("news.off_switch.sec_recent_events_ignore_a_disabled_sec", ["RNEWS2-8.4"],
              lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 20),
              'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 20)', "sec",
              select=lambda v: {"sourcesUsed": v["result"]["meta"]["sourcesUsed"], "warnings": [w["code"] for w in v["result"]["warnings"]],
                                "items": [i["filingType"] for i in v["result"]["items"]], "fetchedHosts": v["fetchedHosts"]},
              expect=_OFF_EXPECT["news.off_switch.sec_recent_events_ignore_a_disabled_sec"]["expect"]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# SEC on a ticker SEC has no CIK for (2.16.0, RNEWS2 §8.7): NOT_ELIGIBLE with reasonCode null and a SOURCE_NOT_ELIGIBLE
# warning, for default-applied and explicit sec alike (PARTIAL accepted). "No CIK" means SEC's ticker index was read and
# holds no entry; a failed submissions fetch stays PROVIDER_ERROR "SEC submissions source unavailable.".
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_NO_CIK_ROUTES = {SEC_TICKERS_URL: {"json": {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}}}}
_SUBMISSIONS_DOWN_ROUTES = {SEC_TICKERS_URL: {"json": {"0": {"cik_str": SEC_CIK, "ticker": "ACME", "title": "Acme Corp"}}},
                            f"https://data.sec.gov/submissions/CIK{SEC_CIK:010d}.json": {"status": 500, "body": "{}"}}


_NO_CIK_EXPECT = {'news.sec.no_cik_default_call_reports_sec_not_eligible': {'py': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                                  'attempted': ['yahoo_finance_news', 'yahoo_finance_press_releases'],
                                                                  'coverage': 'PARTIAL',
                                                                  'failed': [['finnhub', 'UNCONFIGURED'], ['marketaux', 'UNCONFIGURED']],
                                                                  'sec': None,
                                                                  'skipped': [],
                                                                  'status': 'PARTIAL',
                                                                  'warnings': [['SOURCE_UNAVAILABLE',
                                                                                None,
                                                                                'Finnhub company-news source is not configured; skipped.',
                                                                                None,
                                                                                None],
                                                                               ['SOURCE_UNAVAILABLE',
                                                                                'marketaux',
                                                                                'Marketaux wire fallback is not configured; set '
                                                                                'MARKETAUX_API_TOKEN to enable it.',
                                                                                None,
                                                                                None]]},
                                                           'ts': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                                  'attempted': ['yahoo_finance_news', 'yahoo_finance_press_releases'],
                                                                  'coverage': 'PARTIAL',
                                                                  'failed': [['finnhub', 'UNCONFIGURED'], ['marketaux', 'UNCONFIGURED']],
                                                                  'sec': {'attempted': False,
                                                                          'filteredCount': 0,
                                                                          'rawCount': 0,
                                                                          'reasonCode': None,
                                                                          'status': 'NOT_ELIGIBLE'},
                                                                  'skipped': [['sec', 'NOT_ELIGIBLE', False, None]],
                                                                  'status': 'PARTIAL',
                                                                  'warnings': [['SOURCE_NOT_ELIGIBLE',
                                                                                'sec',
                                                                                'SEC has no filings for ACME (no SEC CIK).',
                                                                                False,
                                                                                None],
                                                                               ['SOURCE_UNAVAILABLE',
                                                                                None,
                                                                                'Finnhub company-news source is not configured; skipped.',
                                                                                None,
                                                                                None],
                                                                               ['SOURCE_UNAVAILABLE',
                                                                                'marketaux',
                                                                                'Marketaux wire fallback is not configured; set '
                                                                                'MARKETAUX_API_TOKEN to enable it.',
                                                                                None,
                                                                                None]]}},
 'news.sec.no_cik_explicit_sec_reports_not_eligible': {'py': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                              'attempted': ['sec'],
                                                              'coverage': 'PARTIAL',
                                                              'failed': [['sec', 'PROVIDER_ERROR']],
                                                              'sec': {'filteredCount': 0, 'rawCount': 0, 'status': 'PROVIDER_ERROR'},
                                                              'skipped': [],
                                                              'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                              'warnings': [['SOURCE_UNAVAILABLE',
                                                                            None,
                                                                            'SEC submissions source unavailable.',
                                                                            None,
                                                                            None]]},
                                                       'ts': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                              'attempted': [],
                                                              'coverage': 'PARTIAL',
                                                              'failed': [],
                                                              'sec': {'attempted': False,
                                                                      'filteredCount': 0,
                                                                      'rawCount': 0,
                                                                      'reasonCode': None,
                                                                      'status': 'NOT_ELIGIBLE'},
                                                              'skipped': [['sec', 'NOT_ELIGIBLE', False, None]],
                                                              'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                              'warnings': [['SOURCE_NOT_ELIGIBLE',
                                                                            'sec',
                                                                            'SEC has no filings for ACME (no SEC CIK).',
                                                                            False,
                                                                            None]]}},
 'news.sec.no_cik_verify_failure_mode_is_source_not_eligible': {'py': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                                       'attempted': ['sec'],
                                                                       'coverage': 'PARTIAL',
                                                                       'failed': [['sec', 'PROVIDER_ERROR']],
                                                                       'failureMode': 'PROVIDER_ERROR',
                                                                       'sec': {'filteredCount': 0,
                                                                               'rawCount': 0,
                                                                               'status': 'PROVIDER_ERROR'},
                                                                       'skipped': [],
                                                                       'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                                       'warnings': [['SOURCE_UNAVAILABLE',
                                                                                     None,
                                                                                     'SEC submissions source unavailable.',
                                                                                     None,
                                                                                     None]]},
                                                                'ts': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                                       'attempted': [],
                                                                       'coverage': 'PARTIAL',
                                                                       'failed': [],
                                                                       'failureMode': 'SOURCE_NOT_ELIGIBLE',
                                                                       'sec': {'attempted': False,
                                                                               'filteredCount': 0,
                                                                               'rawCount': 0,
                                                                               'reasonCode': None,
                                                                               'status': 'NOT_ELIGIBLE'},
                                                                       'skipped': [['sec', 'NOT_ELIGIBLE', False, None]],
                                                                       'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                                       'warnings': [['SOURCE_NOT_ELIGIBLE',
                                                                                     'sec',
                                                                                     'SEC has no filings for ACME (no SEC CIK).',
                                                                                     False,
                                                                                     None]]}},
 'news.sec.submissions_fetch_failure_stays_provider_error': {'expect': {'action': 'CHECK_OFFICIAL_RELEASES',
                                                                        'attempted': ['sec'],
                                                                        'coverage': 'PARTIAL',
                                                                        'failed': [['sec', 'PROVIDER_ERROR']],
                                                                        'sec': {'filteredCount': 0,
                                                                                'rawCount': 0,
                                                                                'status': 'PROVIDER_ERROR'},
                                                                        'skipped': [],
                                                                        'status': 'SOURCE_LIMITED_NOT_FOUND',
                                                                        'warnings': [['SOURCE_UNAVAILABLE',
                                                                                      None,
                                                                                      'SEC submissions source unavailable.',
                                                                                      None,
                                                                                      None]]}}}


def _sec_eligibility_view(v):
    return {"sec": v["sourceStatus"].get("sec"),
            "warnings": [[w["code"], w.get("source"), w.get("message"), w.get("attempted"), w.get("reasonCode")] for w in v["warnings"]
                         if w["code"] in ("SOURCE_NOT_ELIGIBLE", "SOURCE_UNAVAILABLE")],
            "coverage": v["coverage"]["state"], "action": v["coverage"]["recommendedNextAction"],
            "skipped": [[s["source"], s["status"], s["attempted"], s["reasonCode"]] for s in v["coverage"]["skippedSources"]],
            "failed": [[s["source"], s["status"]] for s in v["coverage"]["failedSources"]],
            "attempted": [a["sourceType"] for a in v.get("attemptedSources", [])], "status": v.get("status"),
            **({"failureMode": v.get("failureMode")} if "failureMode" in v else {})}


CASES += [
    # Default call on a no-CIK ticker.
    _scenario(
        "news.sec.no_cik_default_call_reports_sec_not_eligible", ["RNEWS2-8.7"],
        lambda: server.get_company_news("ACME", 10, 14), 'm.yahooFinance.getCompanyNews("ACME", 10, 14)',
        news=[_yn("ACME shares rise", when=_days_ago(1))], info={"shortName": "Acme Corp", "longName": "Acme Corp"},
        submissions=(None, None), routes=_NO_CIK_ROUTES, select=_sec_eligibility_view,
        expect=_NO_CIK_EXPECT["news.sec.no_cik_default_call_reports_sec_not_eligible"]["ts"]),
    # Explicit sec on a no-CIK ticker: the same report.
    _scenario(
        "news.sec.no_cik_explicit_sec_reports_not_eligible", ["RNEWS2-8.7"],
        lambda: server.get_company_news("ACME", 10, 14, ["sec"]), 'm.yahooFinance.getCompanyNews("ACME", 10, 14, ["sec"])',
        submissions=(None, None), routes=_NO_CIK_ROUTES, select=_sec_eligibility_view,
        expect=_NO_CIK_EXPECT["news.sec.no_cik_explicit_sec_reports_not_eligible"]["ts"]),
    # verify_company_event: NOT_ELIGIBLE gives failureMode SOURCE_NOT_ELIGIBLE.
    _scenario(
        "news.sec.no_cik_verify_failure_mode_is_source_not_eligible", ["RNEWS2-8.7"],
        lambda: server.verify_company_event("ACME", "acquisition", "", "", ["sec"]),
        'm.yahooFinance.verifyCompanyEvent("ACME", "acquisition", "", "", ["sec"])',
        submissions=(None, None), routes=_NO_CIK_ROUTES, select=_sec_eligibility_view,
        expect=_NO_CIK_EXPECT["news.sec.no_cik_verify_failure_mode_is_source_not_eligible"]["ts"]),
    # A ticker with a CIK whose submissions fetch fails keeps PROVIDER_ERROR and the unavailable warning.
    _scenario(
        "news.sec.submissions_fetch_failure_stays_provider_error", ["RNEWS2-8.7", "BR-NWS-195"],
        lambda: server.get_company_news("ACME", 10, 14, ["sec"]), 'm.yahooFinance.getCompanyNews("ACME", 10, 14, ["sec"])',
        submissions=(f"{SEC_CIK:010d}", None), routes=_SUBMISSIONS_DOWN_ROUTES, select=_sec_eligibility_view,
        expect=_NO_CIK_EXPECT["news.sec.submissions_fetch_failure_stays_provider_error"]["expect"]),
    # get_sec_recent_events keeps its own reporting for a no-CIK ticker (§8.4: not a news-source selection).
    _scenario(
        "news.sec.no_cik_sec_recent_events_reporting_is_unchanged", ["RNEWS2-8.4", "BR-NWS-195"],
        lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 20), 'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 20)',
        submissions=(None, None), routes=_NO_CIK_ROUTES, select=_sec_view,
        expect={"items": [], "warnings": [["SOURCE_UNAVAILABLE", "SEC submissions source unavailable."]],
                "status": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": []}),
]
