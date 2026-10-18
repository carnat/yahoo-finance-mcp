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
            stack.enter_context(patch.object(news_tools, "datetime", _DT_SHIM))
            stack.enter_context(patch.object(news_tools, "_utc_now_iso", lambda: NOW_ISO))
        stack.enter_context(patch.object(news_tools, "_yahoo_search_news", lambda ticker: copy.deepcopy(news or [])))
        stack.enter_context(patch.object(server.yf, "Ticker", lambda ticker, *a, **k: _FakeTicker(info if info is not None else {})))
        if submissions is not None:
            stack.enter_context(patch.object(news_tools, "_get_submissions_for_ticker", AsyncMock(return_value=submissions)))
        saved = dict(pycache._tool_cache._store)
        saved_html = dict(edgar._EDGAR_HTML_CACHE)
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
    # in both runtimes. yahoo_finance_news differs: Python reads each row's contentType (PRESS_RELEASE rows are relabelled
    # yahoo_finance_press_releases and dropped from a news-only selection); the Worker's getNews() drops contentType, so all
    # 7 rows stay news. company_ir differs: Python serves it from the Yahoo feed, the Worker (2.10.0) from approved company IR
    # registry entries only (none for AMD, so NOT_REGISTERED and nothing).
    _multi_scenario(
        "news.yahoo.source_selector_item_sets", ["BR-NWS-198", "BR-NWS-246"],
        [(label, (lambda s=srcs: server.get_company_news("AMD", 10, 14, s)),
          f"m.yahooFinance.getCompanyNews(\"AMD\", 10, 14, {json.dumps(srcs)})")
         for label, srcs in (("yahoo_finance_news", ["yahoo_finance_news"]), ("yahoo_finance", ["yahoo_finance"]),
                             ("company_ir", ["company_ir"]), ("yahoo_finance_press_releases", ["yahoo_finance_press_releases"]))],
        news=_SEL_FEED, info=AMD_INFO, select=_selector_view, mode="pinned",
        decision="FINDING: Python relabels contentType=PRESS_RELEASE rows and drops them from a yahoo_finance_news selection (accepted 7, returned 5); "
                 "the Worker's getNews() drops contentType so it returns all 7 as news; company_ir is a Yahoo passthrough in Python, "
                 "the approved IR registry only in the Worker (2.10.0; NOT_REGISTERED here)",
        expect_py={
            "yahoo_finance_news": {"items": _STORIES, "sourcesUsed": ["yahoo_finance_news"], "coverage": "FULL", "status": None,
                                   "news": {"acceptedCount": 7, "returnedCount": 5}, "sourceStatus": {"yahoo_finance_news": "OK"}},
            "yahoo_finance": {"items": _LEGACY_ITEMS, "sourcesUsed": ["yahoo_finance"], "coverage": "FULL", "status": None,
                              "news": {}, "sourceStatus": {"yahoo_finance": "OK"}},
            "company_ir": {"items": _LEGACY_ITEMS, "sourcesUsed": ["company_ir"], "coverage": "FULL", "status": None,
                           "news": {}, "sourceStatus": {"company_ir": "EMPTY_RESULT"}},
            "yahoo_finance_press_releases": {
                "items": [["AMD Announces Release One", "yahoo_finance_press_releases"],
                          ["Advanced Micro Devices Announces Release Two", "yahoo_finance_press_releases"]],
                "sourcesUsed": ["yahoo_finance_press_releases"], "coverage": "FULL", "status": None, "news": {},
                "sourceStatus": {"yahoo_finance_press_releases": "OK"}},
        },
        expect_ts={
            "yahoo_finance_news": {
                "items": [["AMD story 0", "yahoo_finance_news"], ["AMD Announces Release One", "yahoo_finance_news"],
                          ["AMD story 1", "yahoo_finance_news"], ["Advanced Micro Devices Announces Release Two", "yahoo_finance_news"],
                          ["AMD story 2", "yahoo_finance_news"], ["AMD story 3", "yahoo_finance_news"], ["AMD story 4", "yahoo_finance_news"]],
                "sourcesUsed": ["yahoo_finance_news"], "coverage": "FULL", "status": None,
                "news": {"acceptedCount": 7, "returnedCount": 7}, "sourceStatus": {"yahoo_finance_news": "OK"}},
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
    # Finding (Q7, for triage): a company whose exchange reads 'NYSE American' normalises to NYSE (NYSE is tested before
    # NYSE American), so an 'AMEX:' mention is rejected and 'NYSE:' accepted; 'NYSE American:' is accepted only because its
    # prefix token 'American' is unknown. Both runtimes agree.
    _collector_case(
        "news.yahoo.exchange_nyse_american_company_reads_as_nyse", ["BR-NWS-217"], "ABC",
        [_yn(t, when=_days_ago(1)) for t in ["NYSE American: ABC lifts", "AMEX: ABC lifts", "NYSE: ABC lifts2"]],
        {**_ZETA, "exchange": "NYSE American"}, view=_collector_view,
        expect=_collected([_kept("NYSE American: ABC lifts"), _kept("NYSE: ABC lifts2")], raw=3, rejections={"IDENTITY_MISMATCH": 1})),
    # Finding (Q7, for triage): 'GS' is matched as a substring, so an exchange string such as 'LSE GS' reads as NASDAQ.
    _collector_case(
        "news.yahoo.exchange_gs_substring_reads_as_nasdaq", ["BR-NWS-217"], "ABC",
        [_yn(t, when=_days_ago(1)) for t in ["Nasdaq: ABC climbs", "NYSE: ABC slips"]],
        {**_ZETA, "exchange": "LSE GS"}, view=_collector_view,
        expect=_collected([_kept("Nasdaq: ABC climbs")], raw=2, rejections={"IDENTITY_MISMATCH": 1})),
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
    # BR-NWS-280 / 244: Python rejects a VIDEO row (CONTENT_TYPE); the Worker's getNews() keeps only title, publisher, url and
    # publishedAt, so contentType never reaches the collector and the VIDEO row is accepted.
    _collector_case(
        "news.yahoo.content_type_video_filter_fires_in_python_only", ["BR-NWS-280", "BR-NWS-244"], "AMD", _VIDEO_FEED, AMD_IDENTITY,
        view=_collector_view, mode="pinned",
        decision="FINDING: contentType=VIDEO is rejected (CONTENT_TYPE) in Python; the Worker's getNews() drops contentType so the filter never fires",
        expect_py=_collected([_kept("AMD story fresh")], raw=2, rejections={"CONTENT_TYPE": 1}),
        expect_ts=_collected([_kept("AMD story fresh"), _kept("AMD video interview")], raw=2, rejections={})),
    # BR-NWS-246 card: 5 accepted items, one with contentType PRESS_RELEASE, selection yahoo_finance_news.
    _scenario(
        "news.yahoo.press_release_row_in_news_selection_is_dropped_in_python_only", ["BR-NWS-246", "BR-NWS-198"],
        lambda: server.get_company_news("AMD", 10, 14, ["yahoo_finance_news"]),
        'm.yahooFinance.getCompanyNews("AMD", 10, 14, ["yahoo_finance_news"])',
        news=[_yn(f"AMD story {i}", when=_days_ago(i + 1), contentType="STORY") for i in range(4)]
        + [_yn("AMD Announces Release", publisher="Business Wire", when=_days_ago(6), contentType="PRESS_RELEASE")],
        info=AMD_INFO, select=_selector_view, mode="pinned",
        decision="FINDING: Python labels the PRESS_RELEASE row yahoo_finance_press_releases and drops it (returned 4, accepted 5); the Worker returns all 5 as yahoo_finance_news",
        expect_py={"items": [["AMD story 0", "yahoo_finance_news"], ["AMD story 1", "yahoo_finance_news"], ["AMD story 2", "yahoo_finance_news"],
                             ["AMD story 3", "yahoo_finance_news"]],
                   "sourcesUsed": ["yahoo_finance_news"], "coverage": "FULL", "status": None,
                   "news": {"acceptedCount": 5, "returnedCount": 4}, "sourceStatus": {"yahoo_finance_news": "OK"}},
        expect_ts={"items": [["AMD story 0", "yahoo_finance_news"], ["AMD story 1", "yahoo_finance_news"], ["AMD story 2", "yahoo_finance_news"],
                             ["AMD story 3", "yahoo_finance_news"], ["AMD Announces Release", "yahoo_finance_news"]],
                   "sourcesUsed": ["yahoo_finance_news"], "coverage": "FULL", "status": None,
                   "news": {"acceptedCount": 5, "returnedCount": 5}, "sourceStatus": {"yahoo_finance_news": "OK"}}),
    # BR-NWS-256 on a Yahoo row with a 539-character summary: Python keeps summary[:240] / evidenceText[:180]; the Worker's
    # getNews() drops the summary, so both fields come from the (short) title.
    _collector_case(
        "news.yahoo.summary_text_is_available_in_python_only", ["BR-NWS-256", "BR-NWS-280"], "AMD",
        [_yn("AMD summary check", when=_days_ago(1), summary=_LONG_SUMMARY)], AMD_IDENTITY,
        view=lambda v: [[len(i["summary"]), len(i["evidenceText"])] for i in v["items"]], mode="pinned",
        decision="FINDING: Python carries the Yahoo row summary (240 / 180 characters); the Worker's getNews() drops it so summary and evidenceText are the title (17 characters)",
        expect_py=[[240, 180]], expect_ts=[[17, 17]]),
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
            "action": v["coverage"]["recommendedNextAction"], "state": v["coverage"]["state"], "status": v.get("status")}


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
                "action": "RETRY_TRUNCATED_SOURCE", "state": "FULL", "status": None}),
    _scenario(
        "news.finnhub.25_accepted_rows_10_returned_truncated_15", ["BR-NWS-192", "BR-NWS-220"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([_frow(i, days=1 + (i % 10)) for i in range(25)]), select=_fin_truncation_view,
        expect={"returned": 10,
                "finnhub": {"status": "OK", "acceptedCount": 25, "collectedCount": 25, "returnedCount": 10, "truncatedCount": 15,
                            "hasMoreAccepted": True},
                "truncatedSources": [{"source": "finnhub", "acceptedCount": 25, "returnedCount": 10, "truncatedCount": 15}],
                "action": "RETRY_TRUNCATED_SOURCE", "state": "FULL", "status": None}),
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
    # HTTP outcomes. The statuses agree; the warning TEXT differs for auth errors, other HTTP errors and a changed payload.
    _scenario(
        "news.finnhub.http_401_is_auth_error_with_runtime_specific_message", ["BR-NWS-219"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(None, 401, "invalid token"),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "warnings": _warning_rows(v), "status": v.get("status")},
        mode="pinned", decision="FINDING: Finnhub auth-error warning text differs (Python 'HTTP 401', Worker 'FINNHUB_AUTH_ERROR:401')",
        expect_py={"finnhub": {"status": "AUTH_ERROR", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub auth error: HTTP 401"]], "status": "SOURCE_LIMITED_NOT_FOUND"},
        expect_ts={"finnhub": {"status": "AUTH_ERROR", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub auth error: FINNHUB_AUTH_ERROR:401"]], "status": "SOURCE_LIMITED_NOT_FOUND"}),
    _scenario(
        "news.finnhub.http_500_is_provider_error_with_runtime_specific_message", ["BR-NWS-219"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(None, 500, "boom"),
        # The reason phrase after 'HTTP Error 500:' comes from the harness stub, so it is cut from the compared text.
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "status": v.get("status"),
                          "warnings": [[c, re.sub(r"(HTTP Error 500):.*", r"\1", m)] for c, m in _warning_rows(v)]},
        mode="pinned", decision="FINDING: Finnhub HTTP-error warning text differs (Python 'HTTP Error 500' from urllib, Worker 'HTTP 500')",
        expect_py={"finnhub": {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub source unavailable: HTTP Error 500"]], "status": "SOURCE_LIMITED_NOT_FOUND"},
        expect_ts={"finnhub": {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub source unavailable: HTTP 500"]], "status": "SOURCE_LIMITED_NOT_FOUND"}),
    _scenario(
        "news.finnhub.non_array_payload_is_provider_changed_with_runtime_specific_message", ["BR-NWS-219"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]),
        'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub({"error": "changed"}),
        select=lambda v: {"finnhub": v["sourceStatus"]["finnhub"], "warnings": _warning_rows(v), "status": v.get("status")},
        mode="pinned", decision="FINDING: Finnhub provider-changed warning text differs (Python 'unexpected response format', Worker 'FINNHUB_PROVIDER_CHANGED: expected array, got object')",
        expect_py={"finnhub": {"status": "PROVIDER_CHANGED", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub provider changed: unexpected response format"]], "status": "SOURCE_LIMITED_NOT_FOUND"},
        expect_ts={"finnhub": {"status": "PROVIDER_CHANGED", "rawCount": 0, "filteredCount": 0},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub provider changed: FINNHUB_PROVIDER_CHANGED: expected array, got object"]],
                   "status": "SOURCE_LIMITED_NOT_FOUND"}),
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
    # Card 194 / 277 (Q7 for triage): 0 counts as 'missing' in Python (tool defaults 10 results / 14 days) but is clamped to 1 by
    # the Worker (1 result / 1 day).
    _multi_scenario(
        "news.limits.zero_means_default_in_python_and_one_in_worker", ["BR-NWS-194", "BR-NWS-277"],
        [("max_results_0", *_limit_call(0, 14)), ("lookback_0", *_limit_call(5, 0))],
        env=FINNHUB_ENV, routes=_finnhub(_ROWS_10_DAYS), select=_limit_view, mode="pinned",
        decision="FINDING: max_results=0 / lookback_days=0 mean the default (10 / 14) in Python (int(value or default)) but clamp to 1 in the Worker (clampInt)",
        expect_py={"max_results_0": {"returned": 10, "status": "OK", "acceptedCount": 120, "collectedCount": 30, "truncatedCount": 110},
                   "lookback_0": {"returned": 5, "status": "OK", "acceptedCount": 120, "collectedCount": 15, "truncatedCount": 115}},
        expect_ts={"max_results_0": {"returned": 1, "status": "OK", "acceptedCount": 120, "collectedCount": 3, "truncatedCount": 119},
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
    # Cards 215 / 216 / 236: the default news form set is 8-K, 10-Q, 10-K, S-3, DEF14A, matched exactly (case-insensitively) on
    # the whole form string. 8-K/A, 'DEF 14A' (EDGAR's spelling), 424B5, 4, PRE14A and S-1 are dropped; a form literally
    # spelled 'DEF14A' is kept and mapped to regulatory.
    _sec_scenario(
        "news.sec.default_news_forms_match_exactly_so_def_14a_is_dropped", ["BR-NWS-215", "BR-NWS-216", "BR-NWS-236", "BR-NWS-261"],
        lambda: server.get_company_news("ACME", 20, 30, ["sec"]),
        'm.yahooFinance.getCompanyNews("ACME", 20, 30, ["sec"])',
        _sec_recent(_ALL_FORMS, _ALL_FORM_DAYS), select=_sec_view,
        expect={"items": [["8-K", "regulatory", "2026-10-14T14:00:00Z", "HIGH", "000001"],
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
                          ["10-K/A", "other", "2026-10-03T14:00:00Z", "HIGH", "000012"],
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
    # Card 237: Python rejects blank filing types; the Worker has no such check (a blank type matches nothing), and an empty
    # list means the default three forms in Python but every form in the Worker.
    _multi_scenario(
        "news.sec.recent_events_filing_types_validation_differs", ["BR-NWS-237", "BR-NWS-216"],
        [("blank", lambda: server.get_sec_recent_events("ACME", ["  "], 90, 20), 'm.yahooFinance.getSecRecentEvents("ACME", ["  "], 90, 20)'),
         ("empty_list", lambda: server.get_sec_recent_events("ACME", [], 90, 20), 'm.yahooFinance.getSecRecentEvents("ACME", [], 90, 20)')],
        routes=_sec_routes(SEC_CIK, "ACME", "Acme Corp", _sec_recent(["8-K", "DEF 14A", "10-K", "424B5"], [1, 2, 3, 4])),
        submissions=_py_sec(SEC_CIK, "Acme Corp", _sec_recent(["8-K", "DEF 14A", "10-K", "424B5"], [1, 2, 3, 4])),
        select=lambda v: ({"error": v["error"]["code"], "message": v["error"]["message"]} if "error" in v and "items" not in v
                          else {"forms": [i["filingType"] for i in v["items"]], "status": v.get("status")}),
        mode="pinned",
        decision="FINDING: filing_types=['  '] is INPUT_VALIDATION_ERROR in Python but an empty NOT_FOUND result in the Worker; filing_types=[] uses 8-K/10-Q/10-K in Python and accepts every form in the Worker",
        expect_py={"blank": {"error": "INPUT_VALIDATION_ERROR", "message": "filing_types must not be empty"},
                   "empty_list": {"forms": ["8-K", "10-K"], "status": None}},
        expect_ts={"blank": {"forms": [], "status": "NOT_FOUND"},
                   "empty_list": {"forms": ["8-K", "DEF 14A", "10-K", "424B5"], "status": None}}),
    # The SEC-only event payload: the Worker adds sourceCoverage / coverage / sourceStatus, Python does not.
    _sec_scenario(
        "news.sec.recent_events_payload_has_coverage_fields_in_worker_only", ["BR-NWS-195", "BR-NWS-261"],
        lambda: server.get_sec_recent_events("ACME", ["8-K"], 90, 20),
        'm.yahooFinance.getSecRecentEvents("ACME", ["8-K"], 90, 20)',
        _sec_recent(["8-K"], [1]), select=lambda v: sorted(v.keys()), mode="pinned",
        decision="FINDING: get_sec_recent_events adds coverage, sourceCoverage and sourceStatus in the Worker only",
        expect_py=["items", "meta", "ticker", "warnings"],
        expect_ts=["coverage", "items", "meta", "sourceCoverage", "sourceStatus", "ticker", "warnings"]),
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
    # Python formats the dollar value and share count with f"{x:,.0f}" (round half to even), the Worker with Math.round
    # (half up): 18,516.5 prints $18,516 in Python and $18,517 in the Worker; 0.5 prints $0 and $1; 2.5 shares print 2 and 3.
    _form4_scenario(
        "news.sec.form4_summary_rounding_differs_on_halves",
        ["BR-NWS-255"],
        [{"shares": "1000", "price": "18.5165"}, {"shares": "1", "price": "0.5"}, {"shares": "2.5", "price": "10"}],
        select=lambda v: [i["summary"] for i in v["items"]], mode="pinned",
        decision="FINDING: Form 4 summary rounds halves to even in Python (f-string :,.0f) and half up in the Worker (Math.round)",
        expect_py=["Sale by Doe Jane: 1,000 shares, value $18,516.", "Sale by Doe Jane: 1 shares, value $0.",
                   "Sale by Doe Jane: 2 shares, value $25."],
        expect_ts=["Sale by Doe Jane: 1,000 shares, value $18,517.", "Sale by Doe Jane: 1 shares, value $1.",
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
    # Card 230: every rejection reason once; one request returns at most 3 rows. Python keeps counting accepted rows after the
    # third (acceptedCount 4); the Worker stops reading at the limit (acceptedCount 3). Rejections are the same here because
    # they all precede the third accepted row.
    _marketaux_case(
        "news.marketaux.row_rejection_reasons_and_accepted_count_at_the_3_row_limit", ["BR-NWS-229", "BR-NWS-230"],
        {"data": ["junk", _mrow(2, title=""), _mrow(3, url="http://www.prnewswire.com/x"), _mrow(4, url="https://evil.example.com/x"),
                  _mrow(5, symbol="MSFT"), _mrow(6, exchange="NYSE"), _mrow(7, days=40), _mrow(8), _mrow(9), _mrow(10), _mrow(11)]},
        mode="pinned", decision="FINDING: Marketaux acceptedCount counts every acceptable row in Python (4) but stops at the 3-row limit in the Worker (3)",
        expect_py={"items": [[f"AAPL wire headline {i}", "PROVIDER_ENTITY_SYMBOL", "LOW", "HIGH", "Apple Inc."] for i in (8, 9, 10)],
                   "marketaux": {"status": "OK", "acceptedCount": 4, "returnedCount": 3, "rejectedCount": 7,
                                 "rejectionCounts": {"INVALID_ROW": 1, "MISSING_HEADLINE": 1, "INVALID_URL": 1, "DOMAIN_NOT_ALLOWED": 1,
                                                     "ENTITY_SYMBOL_MISMATCH": 1, "ENTITY_EXCHANGE_MISMATCH": 1, "DATE_OUTSIDE_WINDOW": 1},
                                 "fallbackReason": "HIGHER_PRIORITY_EMPTY", "attempted": True, "allowedAction": "check_official_releases"},
                   "status": None, "warnings": []},
        expect_ts={"items": [[f"AAPL wire headline {i}", "PROVIDER_ENTITY_SYMBOL", "LOW", "HIGH", "Apple Inc."] for i in (8, 9, 10)],
                   "marketaux": {"status": "OK", "acceptedCount": 3, "returnedCount": 3, "rejectedCount": 7,
                                 "rejectionCounts": {"INVALID_ROW": 1, "MISSING_HEADLINE": 1, "INVALID_URL": 1, "DOMAIN_NOT_ALLOWED": 1,
                                                     "ENTITY_SYMBOL_MISMATCH": 1, "ENTITY_EXCHANGE_MISMATCH": 1, "DATE_OUTSIDE_WINDOW": 1},
                                 "fallbackReason": "HIGHER_PRIORITY_EMPTY", "attempted": True, "allowedAction": "check_official_releases"},
                   "status": None, "warnings": []}),
    _marketaux_case("news.marketaux.missing_token_is_unconfigured_and_not_attempted", ["BR-NWS-230"], {"data": [_mrow(1)]}, env={},
                    expect=_mx_failed("UNCONFIGURED", "configure_marketaux_api_token",
                                      "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it.", attempted=False)),
    _marketaux_case("news.marketaux.non_list_data_is_provider_changed", ["BR-NWS-230"], {"x": 1},
                    expect=_mx_failed("PROVIDER_CHANGED", "inspect_provider_contract", "Marketaux provider changed: expected a data array.")),
    # Card 230: 401/403 -> AUTH_ERROR, 402/429 -> RATE_LIMITED, others PROVIDER_ERROR. The status agrees; the warning text differs.
    _marketaux_case("news.marketaux.http_401_is_auth_error_with_runtime_specific_message", ["BR-NWS-230"], None, status=401, body="nope",
                    mode="pinned", decision="FINDING: Marketaux HTTP-error warning text differs (Python 'returned HTTP 401 (AUTH_ERROR)', Worker 'unavailable (AUTH_ERROR)')",
                    expect_py=_mx_failed("AUTH_ERROR", "verify_marketaux_api_token", "Marketaux wire fallback returned HTTP 401 (AUTH_ERROR)."),
                    expect_ts=_mx_failed("AUTH_ERROR", "verify_marketaux_api_token", "Marketaux wire fallback unavailable (AUTH_ERROR).")),
    _marketaux_case("news.marketaux.http_402_is_rate_limited_with_runtime_specific_message", ["BR-NWS-230"], None, status=402, body="quota",
                    mode="pinned", decision="FINDING: Marketaux HTTP-error warning text differs (Python 'returned HTTP 402 (RATE_LIMITED)', Worker 'unavailable (RATE_LIMITED)')",
                    expect_py=_mx_failed("RATE_LIMITED", "retry_after_quota_reset", "Marketaux wire fallback returned HTTP 402 (RATE_LIMITED)."),
                    expect_ts=_mx_failed("RATE_LIMITED", "retry_after_quota_reset", "Marketaux wire fallback unavailable (RATE_LIMITED).")),
    _marketaux_case("news.marketaux.http_500_is_provider_error_with_runtime_specific_message", ["BR-NWS-230"], None, status=500, body="boom",
                    mode="pinned", decision="FINDING: Marketaux HTTP-error warning text differs (Python 'returned HTTP 500 (PROVIDER_ERROR)', Worker 'unavailable (PROVIDER_ERROR)')",
                    expect_py=_mx_failed("PROVIDER_ERROR", "retry_or_use_official_sources", "Marketaux wire fallback returned HTTP 500 (PROVIDER_ERROR)."),
                    expect_ts=_mx_failed("PROVIDER_ERROR", "retry_or_use_official_sources", "Marketaux wire fallback unavailable (PROVIDER_ERROR).")),
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


CASES += [
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
        expect={"requested": None, "defaultApplied": True,
                "attempted": [{"sourceType": "yahoo_finance_news", "status": "OK"}, {"sourceType": "yahoo_finance_press_releases", "status": "EMPTY_RESULT"}],
                "sourceStatus": {"yahoo_finance_news": "OK", "yahoo_finance_press_releases": "EMPTY_RESULT", "finnhub": "NOT_ELIGIBLE",
                                 "marketaux": "UNCONFIGURED"}}),
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
    # Card 197 precedence: AUTH_ERROR is not a failure-mode input, so with Marketaux unconfigured the mode is SOURCE_UNCONFIGURED.
    # The warning text for the Finnhub auth error differs between runtimes.
    _verify_case(
        "news.verify.auth_error_alone_is_not_a_failure_mode", ["BR-NWS-197"], ["finnhub", "marketaux"], env=FINNHUB_ENV, routes=_finnhub(None, 401, "bad token"),
        mode="pinned", decision="FINDING: Finnhub auth-error warning text differs (Python 'HTTP 401', Worker 'FINNHUB_AUTH_ERROR:401')",
        expect_py={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_UNCONFIGURED", "retryable": False, "action": _VERIFY_ACTION,
                   "sourceStatus": {"finnhub": "AUTH_ERROR", "marketaux": "UNCONFIGURED"},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub auth error: HTTP 401"],
                                ["SOURCE_UNAVAILABLE", "Marketaux wire fallback is not configured; set MARKETAUX_API_TOKEN to enable it."], _VERIFY_LIMITED],
                   "next": "CHECK_OFFICIAL_RELEASES", "best": 0},
        expect_ts={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "SOURCE_UNCONFIGURED", "retryable": False, "action": _VERIFY_ACTION,
                   "sourceStatus": {"finnhub": "AUTH_ERROR", "marketaux": "UNCONFIGURED"},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub auth error: FINNHUB_AUTH_ERROR:401"],
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
        mode="pinned", decision="FINDING: Finnhub provider-changed warning text differs (Python 'unexpected response format', Worker 'FINNHUB_PROVIDER_CHANGED: expected array, got object')",
        expect_py={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "PROVIDER_CHANGED", "retryable": False, "action": _VERIFY_ACTION,
                   "sourceStatus": {"finnhub": "PROVIDER_CHANGED"},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub provider changed: unexpected response format"], _VERIFY_LIMITED],
                   "next": "CHECK_OFFICIAL_RELEASES", "best": 0},
        expect_ts={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "PROVIDER_CHANGED", "retryable": False, "action": _VERIFY_ACTION,
                   "sourceStatus": {"finnhub": "PROVIDER_CHANGED"},
                   "warnings": [["SOURCE_UNAVAILABLE", "Finnhub provider changed: FINNHUB_PROVIDER_CHANGED: expected array, got object"], _VERIFY_LIMITED],
                   "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    # Card 197 edge: a company_ir verification with no website. The Worker reports NOT_REGISTERED (2.10.0: approved IR registry only;
    # until then WEBSITE_NOT_AVAILABLE) and the result stays NOT_FOUND (that status is not a failure mode); Python serves company_ir from
    # the Yahoo feed, whose profile lookup failed, so it is IDENTITY_UNAVAILABLE and the result is source limited.
    _verify_case(
        "news.verify.company_ir_without_a_website_stays_not_found_in_the_worker", ["BR-NWS-197"], ["company_ir"], news=[], info=RuntimeError("profile unavailable"),
        identity_fails=True, mode="pinned",
        decision="FINDING: company_ir verification: the Worker (approved IR registry only since 2.10.0) stays NOT_FOUND on NOT_REGISTERED; "
                 "Python (Yahoo passthrough) is SOURCE_LIMITED_NOT_FOUND / YAHOO_IDENTITY_UNAVAILABLE",
        expect_py={"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": "YAHOO_IDENTITY_UNAVAILABLE", "retryable": False, "action": _VERIFY_ACTION,
                   "sourceStatus": {"yahoo_finance_identity": "IDENTITY_UNAVAILABLE", "company_ir": "EMPTY_RESULT"},
                   "warnings": [["SOURCE_IDENTITY_UNAVAILABLE", "Yahoo company identity lookup was unavailable; only exact ticker-token matches were retained."],
                                _VERIFY_LIMITED],
                   "next": "CHECK_OFFICIAL_RELEASES", "best": 0},
        expect_ts={"status": "NOT_FOUND", "failureMode": None, "retryable": False, "action": None,
                   "sourceStatus": {"company_ir": "NOT_REGISTERED"}, "warnings": [],
                   "next": "CHECK_OFFICIAL_RELEASES", "best": 0}),
    # Card 242 (Finding Q7, for triage): every collected item carries tickers=[ticker], so the relevance score is HIGH at its first
    # test and the low-relevance downgrade can never fire: the evidence text (a Business Wire release headline) does not contain the
    # ticker ZZTP, yet the result stays CONFIRMED with relevance HIGH and no LOW_RELEVANCE_EVIDENCE warning.
    _scenario(
        "news.verify.low_relevance_downgrade_never_fires", ["BR-NWS-242"],
        lambda: server.verify_company_event("ZZTP", "acquisition", "", "", ["yahoo_finance_press_releases"]),
        'm.yahooFinance.verifyCompanyEvent("ZZTP", "acquisition", "", "", ["yahoo_finance_press_releases"])',
        news=[_yn("Zeta Platforms announces acquisition of Beta Corp", publisher="Business Wire", when=_days_ago(2))],
        info={"shortName": "Zeta Platforms", "longName": "Zeta Platforms, Inc.", "exchange": "NMS"}, submissions=(None, None),
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
    with _py_news_env(news=[], info={}):
        return json.loads(asyncio.run(server.get_yahoo_finance_news("AAPL")))


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
    # Card 271: get_yahoo_finance_news is a Python alias of get_company_news with a fixed source list (no Marketaux); the Worker has no such tool.
    Case(name="news.validation.get_yahoo_finance_news_alias_exists_in_python_only", ids=["BR-NWS-271"],
         py=lambda: _alias_py(),
         ts={"js": _worker_js(f"return JSON.parse(await {_call_tool('get_yahoo_finance_news', {'ticker': 'AAPL'})});"), "isolate": True},
         routes=_yahoo_routes([], {}), mode="pinned",
         decision="FINDING: get_yahoo_finance_news (fixed source list without Marketaux) exists in Python only; the Worker answers 'Unknown tool name.'",
         select=lambda v: ({"requested": v["requestedSources"], "defaultApplied": v["defaultSourcesApplied"]} if "requestedSources" in v else _unwrap(v)),
         expect_py={"requested": ["yahoo_finance_news", "yahoo_finance_press_releases", "finnhub"], "defaultApplied": False},
         expect_ts={"error": "INPUT_VALIDATION_ERROR", "message": "Unknown tool name."}),
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
    # Card 233: the entitlement pattern differs. A body that only says 'current plan' is ENTITLEMENT_REQUIRED in the Worker but
    # AUTH_ERROR in Python (whose pattern is 'not available under your current plan').
    _ownership_calls_case(
        "news.provider.current_plan_body_is_entitlement_in_the_worker_only", ["BR-NWS-233", "BR-NWS-191"], ["AAA1"],
        {FINNHUB_OWNERSHIP + "*": {"status": 403, "body": json.dumps({"error": "Your current plan does not include this."})}}, FINNHUB_ENV,
        mode="pinned",
        decision="FINDING: the entitlement pattern differs (Worker 'current plan', Python 'not available under your current plan'), so the same 403 is ENTITLEMENT_REQUIRED / USE_ALPHA_VANTAGE_EXPLICITLY in the Worker and AUTH_ERROR / CHECK_PROVIDER_CREDENTIALS in Python",
        expect_py=[{"status": "AUTH_ERROR", "action": "CHECK_PROVIDER_CREDENTIALS", "alphaFallbackAllowed": False,
                    "attempts": [["finnhub", "AUTH_ERROR", True, "MISS", 403, None]]}],
        expect_ts=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "alphaFallbackAllowed": False,
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
    # Card 226 / 200: longName 'Rocket Lab USA, Inc.', shortName 'Rocket Lab'. The Worker builds aliases from long, short and company
    # names, so 'Rocket Lab target raised ... by Cantor' is accepted (CLAUSE); Python uses longName only, so the same headline is
    # rejected as another company's target.
    _valuation_tool_case(
        "news.valuation_tool.short_name_alias_exists_in_the_worker_only", ["BR-NWS-226", "BR-NWS-200", "BR-NWS-201"], 30,
        news=_RKLB_NEWS, info=_RKLB_INFO, select=_valuation_tool_view, mode="pinned",
        decision="FINDING: Python attributes targets with one issuer name (longName, else shortName); the Worker uses longName, shortName and companyName, so the same headline is CLAUSE-accepted in the Worker and rejected in Python",
        expect_py={"windowDays": 30, "warnings": [], "status": "NOT_FOUND",
                   "attribution": {"checked": True, "rejectedForOtherCompany": 2, "subjectAliases": ["rklb", "rocket lab usa", "rocket lab usa inc"]},
                   "finnhub": [], "evidence": []},
        expect_ts={"windowDays": 30, "warnings": [], "status": "METHOD_NOT_DISCLOSED",
                   "attribution": {"checked": True, "rejectedForOtherCompany": 1,
                                   "subjectAliases": ["rklb", "rocket lab", "rocket lab usa", "rocket lab usa inc"]},
                   "finnhub": [], "evidence": [["Cantor Fitzgerald", "CLAUSE", 30, "Rocket Lab price target raised to $30 by Cantor Fitzgerald"]]}),
    # Card 201 / 226: an invalid ticker makes Python's get_company_news return an error payload (NEWS_UNAVAILABLE); the Worker's news
    # call does not validate the ticker, so there is no news error.
    _valuation_tool_case(
        "news.valuation_tool.news_error_payload_raises_news_unavailable_in_python_only", ["BR-NWS-201", "BR-NWS-226", "BR-NWS-240"], 30,
        ticker="BAD TKR!", news=[], info={}, select=lambda d: {"warnings": [w["code"] for w in d["warnings"]], "status": d["status"], "windowDays": d["windowDays"]},
        mode="pinned", decision="FINDING: a news error payload (invalid ticker) raises NEWS_UNAVAILABLE in Python; the Worker does not validate the ticker so it raises nothing",
        expect_py={"warnings": ["ATTRIBUTION_UNCHECKED", "NEWS_UNAVAILABLE"], "status": "NOT_FOUND", "windowDays": 30},
        expect_ts={"warnings": ["ATTRIBUTION_UNCHECKED"], "status": "NOT_FOUND", "windowDays": 30}),
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
        "news.gnw.other_exchange_prefix_is_rejected_in_the_worker_only", ["BR-NWS-224", "BR-NWS-225", "BR-NWS-222"], "NVDA", _NVDA_INFO,
        [_gitem(1, "NASDAQ tagged", ["Nasdaq:NVDA"]), _gitem(3, "TSX tagged", ["TSX:NVDA"]), _gitem(4, "NYSE tagged", ["NYSE:NVDA"])],
        select=_gnw_view, mode="pinned",
        decision="FINDING: GlobeNewswire stock-category exchange compatibility is checked in the Worker only (NYSE:/TSX: tags for a NASDAQ company are dropped there, kept in Python)",
        expect_py={"items": [_gnw_row("NASDAQ tagged", ["Nasdaq:NVDA"], "id1"), _gnw_row("TSX tagged", ["TSX:NVDA"], "id3"),
                             _gnw_row("NYSE tagged", ["NYSE:NVDA"], "id4")],
                   "newswire": {"status": "OK", "rawCount": 3, "filteredCount": 3}, "warnings": [], "sourcesUsed": ["newswire"], "status": None},
        expect_ts={"items": [_gnw_row("NASDAQ tagged", ["Nasdaq:NVDA"], "id1")],
                   "newswire": {"status": "OK", "rawCount": 1, "filteredCount": 1}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    # Card 223: an NYSE American company whose Yahoo exchange reads 'NYSE American' normalises to NYSE, so the Worker rejects its own
    # 'NYSE American:UUUU' tag (and any other); Python accepts all three.
    _gnw_case(
        "news.gnw.nyse_american_company_loses_its_own_releases_in_the_worker", ["BR-NWS-223", "BR-NWS-224"], "UUUU",
        {"shortName": "Energy Fuels", "longName": "Energy Fuels Inc.", "exchange": "NYSE American"},
        [_gitem(1, "Energy Fuels news", ["NYSE American:UUUU"]), _gitem(2, "Energy Fuels nasdaq", ["NASDAQ:UUUU"]), _gitem(3, "Energy Fuels amex", ["AMEX:UUUU"])],
        select=_gnw_view, mode="pinned",
        decision="FINDING: an issuer whose exchangeName is 'NYSE American' normalises to NYSE in the Worker, which then rejects its own 'NYSE American:' GlobeNewswire tag; Python does not check the exchange",
        expect_py={"items": [_gnw_row("Energy Fuels news", ["NYSE American:UUUU"], "id1"), _gnw_row("Energy Fuels nasdaq", ["NASDAQ:UUUU"], "id2"),
                             _gnw_row("Energy Fuels amex", ["AMEX:UUUU"], "id3")],
                   "newswire": {"status": "OK", "rawCount": 3, "filteredCount": 3}, "warnings": [], "sourcesUsed": ["newswire"], "status": None},
        expect_ts={"items": [], "newswire": {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}, "warnings": [], "sourcesUsed": ["newswire"],
                   "status": "NOT_FOUND"}),
    # Card 223 edge: an exchange name 'ASE' maps to NYSEAMERICAN in the Worker, which then accepts 'NYSE American:' and 'AMEX:' tags.
    _gnw_case(
        "news.gnw.ase_exchange_name_maps_to_nyse_american_in_the_worker", ["BR-NWS-223"], "UUUU",
        {"shortName": "Energy Fuels", "longName": "Energy Fuels Inc.", "exchange": "ASE"},
        [_gitem(1, "Energy Fuels news", ["NYSE American:UUUU"]), _gitem(2, "Energy Fuels nasdaq", ["NASDAQ:UUUU"]), _gitem(3, "Energy Fuels amex", ["AMEX:UUUU"])],
        select=_gnw_view, mode="pinned",
        decision="FINDING: with exchange 'ASE' the Worker accepts the 'NYSE American:' and 'AMEX:' tags and rejects 'NASDAQ:'; Python accepts all three",
        expect_py={"items": [_gnw_row("Energy Fuels news", ["NYSE American:UUUU"], "id1"), _gnw_row("Energy Fuels nasdaq", ["NASDAQ:UUUU"], "id2"),
                             _gnw_row("Energy Fuels amex", ["AMEX:UUUU"], "id3")],
                   "newswire": {"status": "OK", "rawCount": 3, "filteredCount": 3}, "warnings": [], "sourcesUsed": ["newswire"], "status": None},
        expect_ts={"items": [_gnw_row("Energy Fuels news", ["NYSE American:UUUU"], "id1"), _gnw_row("Energy Fuels amex", ["AMEX:UUUU"], "id3")],
                   "newswire": {"status": "OK", "rawCount": 2, "filteredCount": 2}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    # Card 272: a feed larger than 2 MiB is rejected in both runtimes (same text).
    _gnw_case(
        "news.gnw.feed_over_2_mib_is_rejected", ["BR-NWS-272"], "NVDA", _NVDA_INFO, None, body="x" * (2 * 1024 * 1024 + 5), select=_gnw_view,
        expect=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' unavailable: GlobeNewswire RSS response exceeded size limit"]
                            for n in _GNW_FEED_NAMES])),
    # Card 272: a DOCTYPE is blocked in Python only; an ENTITY declaration is blocked in both, with different text.
    _gnw_case(
        "news.gnw.doctype_feed_is_rejected_in_python_only", ["BR-NWS-272"], "NVDA", _NVDA_INFO, None,
        body=_rss([_gitem(1, "NVIDIA reports results", ["Nasdaq:NVDA"])]).replace("<rss", "<!DOCTYPE rss [<!ELEMENT rss ANY>]><rss", 1),
        select=_gnw_view, mode="pinned",
        decision="FINDING: Python rejects any feed containing <!DOCTYPE; the Worker rejects only <!ENTITY",
        expect_py=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' parse error: unsupported XML declaration in GlobeNewswire RSS"]
                               for n in _GNW_FEED_NAMES]),
        expect_ts={"items": [_gnw_row("NVIDIA reports results", ["Nasdaq:NVDA"], "id1")],
                   "newswire": {"status": "OK", "rawCount": 1, "filteredCount": 1}, "warnings": [], "sourcesUsed": ["newswire"], "status": None}),
    _gnw_case(
        "news.gnw.entity_feed_is_rejected_with_runtime_specific_text", ["BR-NWS-272"], "NVDA", _NVDA_INFO, None,
        body=_rss([_gitem(1, "NVIDIA reports results", ["Nasdaq:NVDA"])]).replace("<rss", '<!DOCTYPE rss [<!ENTITY x "y">]><rss', 1),
        select=_gnw_view, mode="pinned",
        decision="FINDING: the parse-error text for a blocked XML declaration differs (Python 'unsupported XML declaration', Worker 'unsupported XML entity declaration')",
        expect_py=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' parse error: unsupported XML declaration in GlobeNewswire RSS"]
                               for n in _GNW_FEED_NAMES]),
        expect_ts=_gnw_failed([["SOURCE_UNAVAILABLE", f"GlobeNewswire RSS feed '{n}' parse error: unsupported XML entity declaration in GlobeNewswire RSS"]
                               for n in _GNW_FEED_NAMES])),
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
    # Card 251: Python decodes HTML entities before building the title stem, the Worker does not ('amp' stays in one stem), so
    # 'Acme &amp; Beta sign deal' and 'Acme & Beta sign deal' are one item in Python and two in the Worker.
    _scenario(
        "news.dedupe.html_entities_are_decoded_in_python_only", ["BR-NWS-251"],
        lambda: server.get_company_news("NVDA", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("NVDA", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([_fin_dup_row(8, "NVDA Acme &amp; Beta sign deal", "https://a.com/1"), _fin_dup_row(10, "NVDA Acme & Beta sign deal", "https://b.com/2")]),
        select=lambda v: [i["title"] for i in v["items"]], mode="pinned",
        decision="FINDING: Python un-escapes HTML entities in the dedupe title stem (one item); the Worker does not (two items)",
        expect_py=["NVDA Acme & Beta sign deal"], expect_ts=["NVDA Acme & Beta sign deal", "NVDA Acme &amp; Beta sign deal"]),
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


def _ir_page_case(name, ids, body, *, entry=None, content_type="text/html; charset=utf-8", max_results=100, lookback=365, select, **kw) -> Case:
    entry = entry or IR_ENTRY

    def py():
        with tempfile.TemporaryDirectory(prefix="ir-registry-") as tmp:
            path = Path(tmp) / "registry.json"
            path.write_text(json.dumps({"schemaVersion": "2026-07-10", "registryVersion": "2026-09-25.1200", "sources": [entry]}), encoding="utf-8")
            with _py_news_env(news=[], info={}), patch.object(news_tools, "_COMPANY_IR_PAGE_REGISTRY_PATH", path), \
                    url_policy.registry_override(news_tools._load_company_ir_page_registry()):
                return json.loads(asyncio.run(server.get_company_news("IQE.L", max_results, lookback, ["company_ir_page"])))

    worker_entry = {k: entry.get(k) for k in _IR_WORKER_FIELDS}
    fx = {"registry": _worker_ir_registry(worker_entry), "src": str(WORKER / "src"),
          "esbuild": (WORKER / "node_modules" / "esbuild" / "lib" / "main.js").as_uri()}
    js = _REGISTRY_BUNDLE_JS + _worker_js(f"""
const mod = await registryBundle(fx.registry, fx.src, fx.esbuild);
mod.response.setWorkerEnv({{}});
return JSON.parse(await mod.yahooFinance.getCompanyNews("IQE.L", {max_results}, {lookback}, ["company_ir_page"]));""")
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
    # Card 249: with no issuerName in the entry the issuer is the upper-cased ticker in Python and the (missing) name in the Worker.
    _ir_page_case(
        "news.ir_page.issuer_without_a_registry_name", ["BR-NWS-249"],
        "<html>" + _ir_article(1, "Acme announces Q3 earnings", date=_ir_day(1)) + "</html>", entry={k: v for k, v in IR_ENTRY.items() if k != "issuerName"},
        select=lambda v: [[i.get("issuer"), i.get("originalSource")] for i in v["items"]], mode="pinned",
        decision="FINDING: an IR-page entry without issuerName gives issuer / originalSource 'IQE.L' (upper-cased ticker) in Python and null in the Worker",
        expect_py=[["IQE.L", "IQE.L"]], expect_ts=[[None, None]]),
    _ir_page_case(
        "news.ir_page.structured_json_summary_reads_content_text_in_the_worker_only", ["BR-NWS-202"],
        json.dumps({"items": [{"title": "Json headline one", "content_text": "summary via content_text", "url": "/investors/regulatory-news/j1", "date": _ir_day(2)}]}),
        entry={**IR_ENTRY, "adapter": "structured"}, content_type="application/json",
        select=lambda v: [[i["title"], i["summary"]] for i in v["items"]], mode="pinned",
        decision="FINDING: the Worker reads content_text as the summary of a structured JSON item; Python does not (summary falls back to the title)",
        expect_py=[["Json headline one", "Json headline one"]], expect_ts=[["Json headline one", "summary via content_text"]]),
    # Card 202 edge: a feed-shaped body is parsed as RSS/Atom by the Worker whatever the adapter; Python parses it as HTML and finds nothing.
    _ir_page_case(
        "news.ir_page.feed_shaped_body_is_parsed_as_rss_in_the_worker_only", ["BR-NWS-202"], _IR_RSS,
        select=lambda v: {"titles": [i["title"] for i in v["items"]], "status": v["sourceStatus"]["company_ir_page"]["status"]}, mode="pinned",
        decision="FINDING: an RSS body served to an html_article IR page yields items in the Worker (feed parser) and none in Python (HTML parser)",
        expect_py={"titles": [], "status": "EMPTY_RESULT"}, expect_ts={"titles": ["RSS headline one here"], "status": "APPROVED_SOURCE_OK"}),
    # Card 203: an html_article entry answered with application/json: the Worker rejects the content type (SOURCE_VALIDATION_FAILED);
    # Python parses the body as JSON, finds no records and reports an empty (approved) source.
    _ir_page_case(
        "news.ir_page.html_adapter_with_json_content_type_fails_validation_in_the_worker_only", ["BR-NWS-203"], "<html>" + _ir_article(1, "Article headline one", date=_ir_day(1)) + "</html>",
        content_type="application/json", select=_ir_view, mode="pinned",
        decision="FINDING: Content-Type is validated against the adapter in the Worker only (SOURCE_VALIDATION_FAILED); Python accepts it (EMPTY_RESULT)",
        expect_py={"count": 0, "status": "EMPTY_RESULT", "warnings": [], "collection": "NOT_FOUND", "sourcesUsed": ["company_ir_page"]},
        expect_ts={"count": 0, "status": "SOURCE_VALIDATION_FAILED",
                   "warnings": [["COMPANY_IR_PAGE_VALIDATION_FAILED", "Approved company IR page validation failed: unexpected HTML content type: application/json"]],
                   "collection": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": []}),
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
# Official-site RSS discovery for `company_ir` (Worker only): website normalisation, identity grade, feed acceptance
# BR-NWS-207 / 209 / 210 / 211 / 228 / 258
# Python serves `company_ir` from the Yahoo feed (no website, no discovery), so each case pins the Worker value and the (constant)
# Python value for the same scenario. Since 2.10.0 the Worker's company_ir reads only approved company IR registry entries (the
# discovery these cards describe was removed with the fetch allow-list), so every scenario is NOT_REGISTERED and probes no host.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _company_ir_case(name, ids, website, info, *, ticker="ACME", routes=None, hosts=False, decision, expect_ts, expect_py=None, **kw) -> Case:
    def py():
        with _py_news_env(news=[], info=info):
            return json.loads(asyncio.run(server.get_company_news(ticker, 10, 14, ["company_ir"])))

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

    py_default = {"status": "EMPTY_RESULT", "website": None, "identityConfidence": None, "warnings": [], "collection": "NOT_FOUND", "items": []}
    if hosts:
        py_default["probedHosts"] = []
    return Case(name=name, ids=ids, py=py, ts={"js": js, "isolate": True}, routes={**_yahoo_routes([], info, website), **(routes or {})},
                select=view, mode="pinned", decision=decision, expect_py=expect_py if expect_py is not None else py_default, expect_ts=expect_ts, **kw)


_ACME = {"shortName": "Acme", "longName": "Acme Inc.", "exchange": "NMS"}
_NO_SITE_TS = {"status": "WEBSITE_NOT_AVAILABLE", "website": None, "identityConfidence": "MEDIUM", "collection": "SOURCE_LIMITED_NOT_FOUND", "items": []}
_BUDGET = ["COMPANY_IR_DISCOVERY_BUDGET_EXHAUSTED", "RSS/Atom discovery reached its 24-page Worker probe budget before all bounded company/IR paths were checked."]
_NO_FEED = ["COMPANY_IR_FEED_NOT_FOUND", "No RSS/Atom feed was discovered from the verified company website or common IR/news paths."]
_WORKER_ONLY = ("FINDING: company_ir is a Yahoo-feed passthrough in Python; in the Worker it is the approved company IR registry only "
                "(2.10.0: official-site RSS/Atom discovery removed with the fetch allow-list), so with no approved entry it is NOT_REGISTERED")
_TS_NOT_REGISTERED = {"status": "NOT_REGISTERED", "website": None, "identityConfidence": None, "warnings": [], "collection": "PROVIDER_ERROR",
                      "items": []}

CASES += [
    # Cards 209 / 210 / 258: an aggregator host (linkedin.com, including subdomains) is not an official domain: the website is null,
    # COMPANY_WEBSITE_NOT_AVAILABLE names the host, company_ir is WEBSITE_NOT_AVAILABLE; a blocked host counts as no website, so with a name the
    # identity grade is MEDIUM.
    _company_ir_case(
        "news.company_ir.aggregator_website_is_not_an_official_domain", ["BR-NWS-209", "BR-NWS-210", "BR-NWS-258"], "linkedin.com/company/acme", _ACME,
        decision=_WORKER_ONLY, expect_ts=_TS_NOT_REGISTERED),
    _company_ir_case(
        "news.company_ir.empty_website_is_not_available", ["BR-NWS-209", "BR-NWS-210"], None, _ACME,
        decision=_WORKER_ONLY, expect_ts=_TS_NOT_REGISTERED),
    # Cards 209 / 210 (Finding Q7): 'ftp://acme.com' is not rejected as an unsupported scheme: 'https://' is prepended, so it becomes
    # 'https://ftp://acme.com', whose host is 'ftp'; discovery then runs against https://ftp/.
    _company_ir_case(
        "news.company_ir.ftp_scheme_is_prefixed_with_https_not_rejected", ["BR-NWS-209", "BR-NWS-210"], "ftp://acme.com", _ACME,
        decision=_WORKER_ONLY,
        expect_ts=_TS_NOT_REGISTERED),
    # Cards 209 / 210: a bare domain gets https:// and is reduced to its root URL; discovery probes that host, then investor., investors.
    # and ir. of its root domain.
    _company_ir_case(
        "news.company_ir.bare_domain_gets_https_and_is_reduced_to_its_root", ["BR-NWS-209", "BR-NWS-210"], "acme.com", _ACME, hosts=True,
        decision=_WORKER_ONLY,
        expect_ts={**_TS_NOT_REGISTERED, "probedHosts": []},
        expect_py={"status": "EMPTY_RESULT", "website": None, "identityConfidence": None, "warnings": [], "collection": "NOT_FOUND", "items": [], "probedHosts": []}),
    # Card 209: path, query and fragment are dropped; the root domain strips a leading investors. so investor. / ir. variants are
    # probed after the site's own host.
    _company_ir_case(
        "news.company_ir.path_and_query_are_dropped_and_investors_prefix_is_stripped_for_the_root", ["BR-NWS-209"], "investors.acme.com/x?y", _ACME, hosts=True,
        decision=_WORKER_ONLY,
        expect_ts={**_TS_NOT_REGISTERED, "probedHosts": []},
        expect_py={"status": "EMPTY_RESULT", "website": None, "identityConfidence": None, "warnings": [], "collection": "NOT_FOUND", "items": [], "probedHosts": []}),
]


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
_NV_INFO = {"shortName": "NVIDIA", "longName": "NVIDIA Corporation", "exchange": "NMS"}
_TOKEN_ROUTES = {
    "https://www.nvidia.com/": {"body": '<html><body><a href="https://f.example-wire.com/a.xml">RSS a</a> <a href="https://f.example-wire.com/b.xml">RSS b</a> '
                                        '<a href="https://f.example-wire.com/c.xml">RSS c</a> <a href="https://f.example-wire.com/d.xml">RSS d</a></body></html>'},
    "https://f.example-wire.com/a.xml": {"body": _rss_feed("Weekly", [("Nvidia unveils new GPU", "https://f.example-wire.com/a1")])},
    "https://f.example-wire.com/b.xml": {"body": _rss_feed("Corporation digest", [("Corporation news", "https://f.example-wire.com/b1")])},
    "https://f.example-wire.com/c.xml": {"body": _rss_feed("Technologies weekly", [("Technologies news", "https://f.example-wire.com/c1")])},
    "https://f.example-wire.com/d.xml": {"body": _rss_feed("Market wrap", [("NVDA moves", "https://f.example-wire.com/d1")])},
}
_FEED_REJECTED = ["COMPANY_IR_RSS_HOST_REJECTED", "Linked external RSS/Atom feed did not match the company identity."]

CASES += [
    # Cards 207 / 228: a feed on the company's own domain (news.acmeinc.com) is accepted; its items are HIGH with a URL, LOW without,
    # tickerRelevance HIGH. An external feed titled 'Industry Digest' that names neither the ticker nor a name token is rejected
    # (COMPANY_IR_RSS_HOST_REJECTED); an external feed whose title carries the name token 'acmeinc' is accepted with MEDIUM (URL) / LOW (no URL).
    _company_ir_case(
        "news.company_ir.same_domain_feed_accepted_unrelated_external_feed_rejected", ["BR-NWS-207", "BR-NWS-228"], "https://www.acmeinc.com", _ACMEINC, routes=_FEED_ROUTES,
        decision=_WORKER_ONLY,
        expect_ts=_TS_NOT_REGISTERED),
    # Card 211: name tokens (4+ letters, not corp/inc/group/technology ...): 'Nvidia' matches; text with only 'Corporation' or
    # 'Technologies' does not; the ticker in the first 2,000 characters of the feed also matches.
    _company_ir_case(
        "news.company_ir.external_feed_matches_on_name_token_or_ticker_but_not_stopwords", ["BR-NWS-211", "BR-NWS-207", "BR-NWS-228"], "https://www.nvidia.com", _NV_INFO,
        ticker="NVDA", routes={**_TOKEN_ROUTES, SEC_TICKERS_URL: {"json": {"0": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"}}}},
        decision=_WORKER_ONLY,
        expect_ts=_TS_NOT_REGISTERED),
    # Card 258: identity grade. A name or a CIK without a usable website is MEDIUM; neither is LOW.
    _company_ir_case(
        "news.company_ir.identity_confidence_low_without_name_cik_or_website", ["BR-NWS-258"], None, {}, routes={SEC_TICKERS_URL: {"json": {}}},
        decision=_WORKER_ONLY,
        expect_py={"status": "EMPTY_RESULT", "website": None, "identityConfidence": None,
                   "warnings": [["SOURCE_IDENTITY_UNAVAILABLE", "Yahoo company identity lookup was unavailable; only exact ticker-token matches were retained."]],
                   "collection": "SOURCE_LIMITED_NOT_FOUND", "items": []},
        expect_ts=_TS_NOT_REGISTERED),
    _company_ir_case(
        "news.company_ir.identity_confidence_medium_with_cik_only", ["BR-NWS-258"], None, {},
        routes={SEC_TICKERS_URL: {"json": {"0": {"cik_str": 123, "ticker": "ACME", "title": "Acme"}}}},
        decision=_WORKER_ONLY,
        expect_py={"status": "EMPTY_RESULT", "website": None, "identityConfidence": None,
                   "warnings": [["SOURCE_IDENTITY_UNAVAILABLE", "Yahoo company identity lookup was unavailable; only exact ticker-token matches were retained."]],
                   "collection": "SOURCE_LIMITED_NOT_FOUND", "items": []},
        expect_ts=_TS_NOT_REGISTERED),
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
    # Card 267: the payload fields that differ: the Worker adds doctrineUse 'ALLOWED' and the SEC evidence cikInt, and words
    # decisionGradeBasis differently ('SEC EX-99 press-release exhibit' vs 'SEC EX-99 or approved IR-page evidence').
    _sec_scenario(
        "news.press_releases.payload_has_doctrine_use_and_different_basis_text_in_the_worker", ["BR-NWS-267"],
        lambda: server.get_company_press_releases("ACME", 90, 20, ["sec"]),
        'm.yahooFinance.getCompanyPressReleases("ACME", 90, 20, ["sec"])',
        _PR_SEC, news=[], info=_ACME_CORP,
        select=lambda d: {"doctrineUse": d.get("doctrineUse"), "basis": d["decisionGradeBasis"], "capabilityStatus": d["capabilityStatus"],
                          "secEvidenceKeys": sorted(d["secEvidence"][0].keys()), "failureMode": d["failureMode"]},
        mode="pinned",
        decision="FINDING: get_company_press_releases adds doctrineUse='ALLOWED' and secEvidence[].cikInt in the Worker only and words decisionGradeBasis differently",
        expect_py={"doctrineUse": None, "basis": "No resolved SEC EX-99 or approved IR-page evidence in this call; use for verification/context only.",
                   "capabilityStatus": "ACTIVE", "secEvidenceKeys": ["acceptedAt", "accessionNumber", "documentUrl", "filingDate", "filingType"],
                   "failureMode": "SEC_8K_FOUND_EX99_NOT_FOUND"},
        expect_ts={"doctrineUse": "ALLOWED", "basis": "No resolved SEC EX-99 press-release exhibit in this call; use for verification/context only.",
                   "capabilityStatus": "ACTIVE", "secEvidenceKeys": ["acceptedAt", "accessionNumber", "cikInt", "documentUrl", "filingDate", "filingType"],
                   "failureMode": "SEC_8K_FOUND_EX99_NOT_FOUND"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Declared output schema vs runtime statuses (BR-NWS-214)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

_NEWS_TOOL_NAMES = ["get_company_news", "search_company_news", "get_public_event_timeline", "verify_company_event",
                    "get_company_press_releases", "get_sec_recent_events"]


def _declared_status_py():
    out = {}
    for tool in _NEWS_TOOL_NAMES:
        schema = server._TOOL_OUTPUT_SCHEMAS.get("get_yahoo_finance_news" if tool == "get_company_news" else tool) or {}
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

CASES += [
    # Card 253: keyword classification (first matching group wins: 'quarterly' beats 'raises'/'outlook'). Substring matching causes
    # false hits (Finding Q7, both runtimes): 'steps' -> eps -> earnings, 'dealer' -> deal -> contract, 'denote' -> note -> financing,
    # 'corporates' -> rates -> macro, 'sector' -> sec -> regulatory.
    _scenario(
        "news.classification.keyword_groups_and_substring_false_hits_agree", ["BR-NWS-253"],
        lambda: server.get_company_news("ACME", 100, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("ACME", 100, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(_EVENT_TITLES_SHARED)), select=_event_types,
        expect=sorted([t, e] for t, e in _EVENT_TYPES_SHARED.items())),
    # Card 253: the keyword lists differ ('announces new', 'annual report' and 'credit facility' are Python-only keywords; in the Worker
    # 'secures' contains 'sec').
    _scenario(
        "news.classification.python_only_keywords", ["BR-NWS-253"],
        lambda: server.get_company_news("ACME", 100, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("ACME", 100, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(["Acme announces new unit", "Acme posts annual report", "Acme secures credit facility"])),
        select=_event_types, mode="pinned",
        decision="FINDING: the event-type keyword lists differ ('announces new', 'annual report', 'credit facility' only in Python), so the same headline is typed differently",
        expect_py=[["Acme announces new unit", "product"], ["Acme posts annual report", "earnings"], ["Acme secures credit facility", "financing"]],
        expect_ts=[["Acme announces new unit", "other"], ["Acme posts annual report", "other"], ["Acme secures credit facility", "regulatory"]]),
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


_LABELS_SHARED = [
    ["AAPL news one", "CONTEXTUAL_NEWS", "EXPLICIT", "PROVIDER", "CONTEXT_ONLY"],
    ["Sector five", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "UNKNOWN", "CHECK_OFFICIAL_RELEASES"],
    ["Sector headline", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PROVIDER", "CHECK_OFFICIAL_RELEASES"],
    ["Sector three", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PROVIDER", "CHECK_OFFICIAL_RELEASES"],
    ["Sector two", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PUBLISHER", "CHECK_OFFICIAL_RELEASES"],
]

CASES += [
    # Card 254: a Finnhub item is CONTEXTUAL_NEWS; its ticker match is EXPLICIT when the text holds the ticker, else SOURCE_SCOPED; a
    # finnhub.io or finance.yahoo.com host is PROVIDER, another host PUBLISHER, no URL UNKNOWN. ('Sector ...' headlines contain 'sec', so they
    # are typed regulatory and carry CHECK_OFFICIAL_RELEASES, BR-NWS-253.) A URL that does not parse is UNKNOWN in the Worker and PUBLISHER in Python.
    _scenario(
        "news.labels.evidence_class_ticker_match_and_url_provenance", ["BR-NWS-254"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([r for r in _LABEL_ROWS if r["headline"] != "Sector four"]), select=_labels, expect=_LABELS_SHARED),
    _scenario(
        "news.labels.unparseable_url_provenance_differs", ["BR-NWS-254"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub([r for r in _LABEL_ROWS if r["headline"] == "Sector four"]), select=_labels, mode="pinned",
        decision="FINDING: an unparseable URL has urlProvenance UNKNOWN in the Worker and PUBLISHER in Python",
        expect_py=[["Sector four", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "PUBLISHER", "CHECK_OFFICIAL_RELEASES"]],
        expect_ts=[["Sector four", "CONTEXTUAL_NEWS", "SOURCE_SCOPED", "UNKNOWN", "CHECK_OFFICIAL_RELEASES"]]),
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
    # Card 256 edge: Python slices by code points, the Worker by UTF-16 units: an emoji straddling the cut is kept whole in Python and
    # split into a lone surrogate in the Worker.
    _scenario(
        "news.text.cut_position_counts_code_points_in_python_and_utf16_units_in_the_worker", ["BR-NWS-256"],
        lambda: server.get_company_news("AAPL", 20, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 20, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": "AAPL astral", "summary": "x" * 239 + "\U0001F600" + "tail", "source": "W", "url": "https://e.com/1",
                          "datetime": _epoch(_days_ago(1, hour=1))}]),
        select=lambda v: [[len(i["summary"]), ascii(i["summary"][-3:]), len(i["evidenceText"])] for i in v["items"]], mode="pinned",
        decision="FINDING: summary is cut at 240 code points in Python and 240 UTF-16 units in the Worker, which can leave a lone surrogate",
        expect_py=[[240, "'xx\\U0001f600'", 180]], expect_ts=[[240, "'xx\\ud83d'", 180]]),
]

CASES += [
    # Card 257 (Finding Q7, both runtimes): with the legacy yahoo_finance source the news and press-release passes read the same 20 stories,
    # so rawCount and retrievedCount are 40, and the press-release pass rejects all 20 as NOT_ISSUER_WIRE_RELEASE.
    _scenario(
        "news.yahoo.legacy_aggregate_counts_the_same_feed_twice", ["BR-NWS-257"],
        lambda: server.get_company_news("AMD", 100, 14, ["yahoo_finance"]), 'm.yahooFinance.getCompanyNews("AMD", 100, 14, ["yahoo_finance"])',
        news=[_yn(f"AMD story {i}", when=_days_ago(1 + i % 10)) for i in range(20)], info=AMD_INFO,
        select=lambda v: {"returned": len(v["items"]), **{k: v["sourceStatus"]["yahoo_finance"][k] for k in
                                                         ("status", "rawCount", "retrievedCount", "filteredCount", "acceptedCount", "returnedCount",
                                                          "rejectedCount", "rejectionCounts", "identityStatus", "attempted")}},
        expect={"returned": 20, "status": "OK", "rawCount": 40, "retrievedCount": 40, "filteredCount": 20, "acceptedCount": 20, "returnedCount": 20,
                "rejectedCount": 20, "rejectionCounts": {"NOT_ISSUER_WIRE_RELEASE": 20}, "identityStatus": "RESOLVED", "attempted": True}),
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
    # Card 262: the raw-substring relevance test: the ticker 'A' is found inside 'MARKETS RALLY', so a Finnhub row about nothing in
    # particular is HIGH and MEDIUM confidence (Finding Q7, both runtimes).
    _scenario(
        "news.ranking.single_letter_ticker_is_found_inside_any_word", ["BR-NWS-262"],
        lambda: server.get_company_news("A", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("A", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV, routes=_finnhub(_finnhub_rows(["Markets rally", "Zzz yyy"], summary="")),
        select=lambda v: sorted([i["title"], i["tickerRelevance"], i["confidence"]] for i in v["items"]),
        expect=[["Markets rally", "HIGH", "MEDIUM"], ["Zzz yyy", "LOW", "MEDIUM"]]),
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
    # Card 278: timestamp format. Python writes 2026-10-14T09:00:00Z, the Worker 2026-10-14T09:00:00.000Z for epoch-second and ISO inputs;
    # compact YYYYMMDD / YYYYMMDDHHMMSS are alike in both. A Yahoo row with providerPublishTime 0 falls through to content.pubDate in Python
    # and is dropped in the Worker (whose getNews() keeps no pubDate).
    _scenario(
        "news.timestamps.yahoo_epoch_zero_and_millisecond_suffix_differ", ["BR-NWS-278"],
        lambda: server.get_company_news("AMD", 100, 14, ["yahoo_finance_news"]), 'm.yahooFinance.getCompanyNews("AMD", 100, 14, ["yahoo_finance_news"])',
        news=[{"title": "AMD tick", "publisher": "R", "link": "https://e.com/a", "providerPublishTime": _YAHOO_TS},
              {"title": "AMD zero", "publisher": "R", "link": "https://e.com/b", "providerPublishTime": 0, "content": {"pubDate": "2026-10-13T00:00:00Z"}}],
        info=AMD_INFO, select=lambda v: [[i["title"], i["publishedAt"], v["meta"]["watermark"]] for i in v["items"]], mode="pinned",
        decision="FINDING: publishedAt carries '.000Z' milliseconds in the Worker and none in Python; an epoch of 0 falls through to content.pubDate in Python only",
        expect_py=[["AMD tick", "2026-10-14T09:00:00Z", NOW_ISO], ["AMD zero", "2026-10-13T00:00:00Z", NOW_ISO]],
        expect_ts=[["AMD tick", "2026-10-14T09:00:00.000Z", "2026-10-15T12:00:00.000Z"]]),
    _scenario(
        "news.timestamps.provider_date_strings_parse_differently", ["BR-NWS-278"],
        lambda: server.get_company_news("AAPL", 10, 14, ["finnhub"]), 'm.yahooFinance.getCompanyNews("AAPL", 10, 14, ["finnhub"])',
        env=FINNHUB_ENV,
        routes=_finnhub([{"headline": "AAPL rfc date", "summary": "s", "source": "W", "url": "https://e.com/1", "datetime": "Tue, 13 Oct 2026 10:00:00 GMT"},
                         {"headline": "AAPL iso date", "summary": "s", "source": "W", "url": "https://e.com/2", "datetime": "2026-10-13T10:00:00Z"},
                         {"headline": "AAPL compact", "summary": "s", "source": "W", "url": "https://e.com/3", "datetime": "20261012"},
                         {"headline": "AAPL compact14", "summary": "s", "source": "W", "url": "https://e.com/4", "datetime": "20261011101010"}]),
        select=lambda v: [[i["title"], i["publishedAt"]] for i in v["items"]], mode="pinned",
        decision="FINDING: an RFC 2822 date string parses in the Worker only (Python gives null and the date window drops the row); ISO output has '.000Z' only in the Worker",
        expect_py=[["AAPL iso date", "2026-10-13T10:00:00Z"], ["AAPL compact", "2026-10-12T00:00:00Z"], ["AAPL compact14", "2026-10-11T10:10:10Z"]],
        expect_ts=[["AAPL rfc date", "2026-10-13T10:00:00.000Z"], ["AAPL iso date", "2026-10-13T10:00:00.000Z"], ["AAPL compact", "2026-10-12T00:00:00Z"],
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
        news=[], info={}, select=_CANDIDATE_VIEW, mode="pinned",
        decision="FINDING: a candidate-only IR page emits warning COMPANY_IR_PAGE_CANDIDATE_AVAILABLE and collection status SOURCE_LIMITED_NOT_FOUND in Python; the Worker emits no warning and PROVIDER_ERROR",
        expect_py={**_CANDIDATE_COMMON, "warnings": ["COMPANY_IR_PAGE_CANDIDATE_AVAILABLE"], "collection": "SOURCE_LIMITED_NOT_FOUND"},
        expect_ts={**_CANDIDATE_COMMON, "warnings": [], "collection": "PROVIDER_ERROR"}),
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


_ENTITY_FEED_ROUTES = {
    "https://www.acmeinc.com/": {"body": '<html><head><link rel="alternate" type="application/rss+xml" href="https://news.acmeinc.com/feed.xml"></head></html>'},
    "https://news.acmeinc.com/feed.xml": {"body": '<?xml version="1.0"?><!DOCTYPE rss [<!ENTITY x "y">]><rss version="2.0"><channel><title>Acmeinc News</title></channel></rss>'},
}
CASES += [
    # Cards 207 / 228: a feed containing <!ENTITY is a parse error (COMPANY_IR_RSS_PARSE_ERROR), not an accepted or host-rejected feed.
    _company_ir_case(
        "news.company_ir.feed_with_an_entity_declaration_is_a_parse_error", ["BR-NWS-207", "BR-NWS-228"], "https://www.acmeinc.com", _ACMEINC,
        routes=_ENTITY_FEED_ROUTES, decision=_WORKER_ONLY,
        expect_ts=_TS_NOT_REGISTERED),
]
