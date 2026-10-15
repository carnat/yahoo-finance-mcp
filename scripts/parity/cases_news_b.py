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

from scripts.parity.harness import Case

# Registers the tools (server.py imports the yfmcp modules) the way the existing suites do.
import server  # noqa: F401,E402
from yfmcp import capital_structure as cs  # noqa: E402
from yfmcp import cache as pycache  # noqa: E402
from yfmcp.clients import edgar  # noqa: E402

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
            stack.enter_context(patch.object(server, "datetime", _DT_SHIM))
            stack.enter_context(patch.object(server, "_utc_now_iso", lambda: NOW_ISO))
        stack.enter_context(patch.object(server, "_yahoo_search_news", lambda ticker: copy.deepcopy(news or [])))
        stack.enter_context(patch.object(server.yf, "Ticker", lambda ticker, *a, **k: _FakeTicker(info if info is not None else {})))
        if submissions is not None:
            stack.enter_context(patch.object(server, "_get_submissions_for_ticker", AsyncMock(return_value=submissions)))
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
    # 7 rows stay news. company_ir differs: Python serves it from the Yahoo feed, the Worker from official-site RSS discovery
    # (no website in the profile here, so nothing).
    _multi_scenario(
        "news.yahoo.source_selector_item_sets", ["BR-NWS-198", "BR-NWS-246"],
        [(label, (lambda s=srcs: server.get_company_news("AMD", 10, 14, s)),
          f"m.yahooFinance.getCompanyNews(\"AMD\", 10, 14, {json.dumps(srcs)})")
         for label, srcs in (("yahoo_finance_news", ["yahoo_finance_news"]), ("yahoo_finance", ["yahoo_finance"]),
                             ("company_ir", ["company_ir"]), ("yahoo_finance_press_releases", ["yahoo_finance_press_releases"]))],
        news=_SEL_FEED, info=AMD_INFO, select=_selector_view, mode="pinned",
        decision="FINDING: Python relabels contentType=PRESS_RELEASE rows and drops them from a yahoo_finance_news selection (accepted 7, returned 5); "
                 "the Worker's getNews() drops contentType so it returns all 7 as news; company_ir is a Yahoo passthrough in Python, RSS discovery in the Worker",
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
            "company_ir": {"items": [], "sourcesUsed": [], "coverage": "PARTIAL", "status": "SOURCE_LIMITED_NOT_FOUND",
                           "news": {}, "sourceStatus": {"company_ir": "WEBSITE_NOT_AVAILABLE"}},
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
    "Acme (TSXV: ABC) announces results", "Listing on NYSE: ABC planned", "Rally in ASX: ABC shares", "Zeta (CVE: ABC) update", "LSE: ABC dips"]]

CASES += [
    # A NASDAQ (NMS) company: a bare mention, 'Nasdaq:' and an unknown prefix ('OTC:') match; TSXV / NYSE / ASX / CVE / LSE
    # prefixes are incompatible (the first headline also has a bare mention, so it matches).
    _collector_case(
        "news.yahoo.exchange_prefix_incompatible_for_nasdaq_company", ["BR-NWS-217", "BR-NWS-218", "BR-NWS-243"], "ABC", _PREFIX_FEED, _ZETA,
        view=_collector_view,
        expect=_collected([_kept("Shares of ABC (TSX: ABC) rose"), _kept("Nasdaq: ABC climbs"), _kept("ABC wins contract"),
                           _kept("ABC (OTC: ABC) quoted")], raw=9, rejections={"IDENTITY_MISMATCH": 5})),
    # Unknown company exchange: any word-boundary token match counts, whatever the prefix.
    _collector_case(
        "news.yahoo.exchange_prefix_ignored_when_company_exchange_unknown", ["BR-NWS-217", "BR-NWS-218"], "ABC", _PREFIX_FEED,
        {**_ZETA, "exchange": None}, view=_collector_view,
        expect=_collected([_kept(t["title"]) for t in _PREFIX_FEED], raw=9, rejections={})),
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
    # Card 192 order of precedence: a failed source outranks truncation, and a retryable failure outranks both.
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
    _marketaux_case("news.marketaux.entity_on_matching_exchange_is_accepted_with_low_confidence", ["BR-NWS-229", "BR-NWS-230", "BR-NWS-184"],
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
    _marketaux_case("news.marketaux.missing_token_is_unconfigured_and_not_attempted", ["BR-NWS-230", "BR-NWS-184"], {"data": [_mrow(1)]}, env={},
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
