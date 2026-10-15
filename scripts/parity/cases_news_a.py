"""Parity cases: News domain, rules BR-NWS-101 .. BR-NWS-190 (modernization Phase 2, round Q7).

These cases pin what the Python runtime (server.py, yfmcp/) and the Cloudflare Worker (worker/src/*.ts) do TODAY with
the News code, so the planned pure refactor that moves it into yfmcp/news.py and worker/src/news.ts can be proven
byte-identical. They do not pick winners: where the runtimes differ the case is `pinned` with a
`FINDING: ...` decision and both values; where they agree but the card says the behaviour looks wrong the case is
`equal` with a `# Finding (Q7, for triage)` comment.

How the two runtimes are driven (nothing reaches the network)
- Pure helpers with a Worker export (analyst valuation text extraction, sourceRequestFields) are called directly.
- Everything else sits behind unexported Worker functions, so it is driven at the tool boundary
  (getCompanyNews / searchCompanyNews / getCompanyPressReleases / getSecRecentEvents / getPublicEventTimeline /
  verifyCompanyEvent against server.py's tools of the same name) with one scenario feeding both runtimes:
    * Worker: canned HTTP in `routes` (Yahoo auth + quoteSummary profile + search feed, Finnhub, Marketaux,
      GlobeNewswire, SEC submissions/documents, IR pages); `Date` is replaced by a frozen clock and
      `setWorkerEnv` supplies (fake) provider keys; `isolate` gives empty module caches.
    * Python: the same canned HTTP through the harness urllib stub; yfinance is replaced by a fake
      `yf.Ticker(...).info` and `server._yahoo_search_news`; `server.datetime` / `server._utc_now_iso` are frozen;
      the process caches the scenario touches (tool cache, EDGAR caches) are patched empty and restored.
- The registry-backed company_ir_page source reads a static JSON file in both runtimes (the Worker bundles it at
  build time). For approved/disabled/stale entries the Python side patches `_load_company_ir_page_registry` and the
  Worker side esbuild-bundles worker/src with the same test registry substituted for company-ir-page-registry.json
  (production code is unchanged; only the data file differs). The shipped registry (two candidate entries) is used
  as is for the candidate cases.

Conventions: provider keys are fake literals (`fake-...`); duplicateGroupId, retrievedAt and watermark are never
compared because the runtimes hash differently (BR-NWS-119) or stamp the clock in different formats (BR-NWS-136);
timestamps are compared with the Worker's ".000Z" milliseconds removed unless the case is about that difference.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import datetime
import hashlib
import json
import os
import re
import types
from typing import Any
from unittest.mock import patch

from scripts.parity.harness import WORKER, Case

import server
from yfmcp import capital_structure as cs
from yfmcp.clients import edgar

CASES: list[Case] = []

NOW = "2026-10-15T12:00:00Z"
# Provider key variables both runtimes read; every scenario starts without them.
_PROVIDER_ENV = ("FINNHUB_API_KEY", "FINNHUB_TOKEN", "ALPHA_VANTAGE_API_KEY", "MARKETAUX_API_TOKEN", "MARKETAUX_API_KEY")
FAKE_FINNHUB = {"FINNHUB_API_KEY": "fake-finnhub-key"}                  # clearly fake, same shape as a real key
FAKE_MARKETAUX = {"MARKETAUX_API_TOKEN": "fake-marketaux-token"}

YAHOO_AUTH_ROUTES = {
    "https://fc.yahoo.com": {"headers": {"set-cookie": "A3=fake-cookie; Path=/"}},
    "https://query1.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
    "https://query2.finance.yahoo.com/v1/test/getcrumb": {"body": "fake-crumb"},
}
YAHOO_QUOTE_SUMMARY = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/*"
YAHOO_SEARCH = "https://query1.finance.yahoo.com/v1/finance/search*"
FINNHUB_NEWS = "https://finnhub.io/api/v1/company-news*"
MARKETAUX_NEWS = "https://api.marketaux.com/v1/news/all*"
GNW_FEEDS = [(name, url) for name, url in server._GLOBENEWSWIRE_RSS_FEEDS]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Clock, scenario and tool plumbing
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _frozen_datetime_module(now: str):
    """A stand-in for the `datetime` module whose datetime.now() is fixed (everything else is the real module)."""
    frozen = datetime.datetime.fromisoformat(now.replace("Z", "+00:00"))

    class FrozenDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: D401 - mirrors datetime.datetime.now
            return frozen.astimezone(tz) if tz is not None else frozen.replace(tzinfo=None)

    shim = types.SimpleNamespace(**{k: getattr(datetime, k) for k in dir(datetime) if not k.startswith("__")})
    shim.datetime = FrozenDateTime
    return shim


class _FakeTicker:
    """yf.Ticker stand-in: `.info` is a dict, or raises when the scenario says the profile is unavailable."""

    def __init__(self, info):
        self._info = info

    @property
    def info(self):
        if isinstance(self._info, Exception):
            raise self._info
        return self._info


@contextlib.contextmanager
def _python_world(scn: dict):
    """Everything a Python tool call may touch, frozen, faked or emptied; restored on exit."""
    now = scn.get("now", NOW)
    news = scn.get("news", [])
    info = scn.get("info", {})
    if scn.get("cik") is not None and not isinstance(info, Exception):
        info = {**info, "cik": scn["cik"]}          # yfinance's `info["cik"]`; the Worker reads quoteSummary secFilings.cik
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, {}))          # snapshot; restored on exit
        for var in _PROVIDER_ENV:
            os.environ.pop(var, None)
        os.environ.update(scn.get("env", {}))
        stack.enter_context(patch.object(server, "datetime", _frozen_datetime_module(now)))
        stack.enter_context(patch.object(server, "_utc_now_iso", lambda: now))
        stack.enter_context(patch.dict(server._tool_cache._store, {}, clear=True))
        for cache in (edgar._FILING_CIK_CACHE, edgar._EDGAR_SUBS_CACHE, edgar._EDGAR_HTML_CACHE):
            stack.enter_context(patch.dict(cache, {}, clear=True))
        saved = (edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT)
        edgar._EDGAR_TICKERS, edgar._EDGAR_TICKERS_LOADED_AT = None, 0.0
        stack.callback(lambda: setattr(edgar, "_EDGAR_TICKERS", saved[0]))
        stack.callback(lambda: setattr(edgar, "_EDGAR_TICKERS_LOADED_AT", saved[1]))
        stack.enter_context(patch.object(server.yf, "Ticker", lambda _symbol: _FakeTicker(info)))

        def search(_ticker):
            if isinstance(news, Exception):
                raise news
            return copy.deepcopy(news)

        stack.enter_context(patch.object(server, "_yahoo_search_news", search))
        if scn.get("registry") is not None:
            registry = scn["registry"]
            stack.enter_context(patch.object(server, "_load_company_ir_page_registry", lambda: copy.deepcopy(registry)))
        yield


def _profile_result(info: dict | Exception, cik: int | None = None) -> dict:
    """The Worker's quoteSummary payload for the same company the Python fake `info` describes."""
    if isinstance(info, Exception):
        return {"quoteSummary": {"result": None, "error": {"code": "Not Found"}}}
    price = {k: v for k, v in (("shortName", info.get("shortName")), ("longName", info.get("longName")),
                              ("exchangeName", info.get("exchange"))) if v is not None}
    result: dict[str, Any] = {"price": price}
    if info.get("website"):
        result["summaryProfile"] = {"website": info["website"]}
    if cik is not None:
        result["secFilings"] = {"cik": cik}
    return {"quoteSummary": {"result": [result], "error": None}}


def _routes_for(scn: dict) -> dict:
    routes: dict = dict(YAHOO_AUTH_ROUTES)
    info = scn.get("info", {})
    routes[YAHOO_QUOTE_SUMMARY] = ({"status": 404, "body": "{}"} if isinstance(info, Exception)
                                   else {"json": _profile_result(info, scn.get("cik"))})
    news = scn.get("news", [])
    routes[YAHOO_SEARCH] = {"status": 500, "body": "{}"} if isinstance(news, Exception) else {"json": {"news": news}}
    routes.update(scn.get("routes", {}))
    return routes


# The Worker case body: frozen clock, fake provider env, optional registry-substituted bundle.
_REGISTRY_BUNDLE_JS = """
async function registryBundle(registry, src, esbuildMain) {
  globalThis.__nwsRegistryBundles ??= new Map();
  const key = JSON.stringify(registry);
  if (!globalThis.__nwsRegistryBundles.has(key)) {
    const esbuild = await import(esbuildMain);
    const build = esbuild.build ?? esbuild.default.build;
    const entry = `export * as yahooFinance from ${JSON.stringify(src + "/yahoo-finance.ts")};\\n`
      + `export * as response from ${JSON.stringify(src + "/response.ts")};\\n`;
    const out = await build({
      stdin: { contents: entry, resolveDir: src, loader: "ts" }, bundle: true, format: "esm", platform: "node",
      write: false, logLevel: "silent",
      plugins: [{ name: "test-registry", setup(b) {
        b.onLoad({ filter: /company-ir-page-registry\\.json$/ }, () => ({ contents: JSON.stringify(registry), loader: "json" }));
      } }],
    });
    const fs = await import("node:fs"); const os = await import("node:os"); const path = await import("node:path");
    const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "nws-registry-")), "bundle.mjs");
    fs.writeFileSync(file, out.outputFiles[0].text);
    globalThis.__nwsRegistryBundles.set(key, await import("file://" + file));
  }
  return globalThis.__nwsRegistryBundles.get(key);
}
"""

_TOOL_JS = """
const RealDate = Date;
const FIXED = RealDate.parse(fx.now);
class FrozenDate extends RealDate {
  constructor(...a) { if (a.length === 0) super(FIXED); else super(...a); }
  static now() { return FIXED; }
}
const mod = fx.registry ? await registryBundle(fx.registry, fx.src, fx.esbuild) : m;
mod.response.setWorkerEnv(fx.env || {});
globalThis.Date = FrozenDate;
try {
  const out = await mod.yahooFinance[fx.fn](...args);
  return typeof out === "string" ? JSON.parse(out) : out;
} finally { globalThis.Date = RealDate; mod.response.setWorkerEnv({}); }
"""

# Python tool name -> Worker export; the positional arguments are the same on both sides.
TOOLS = {
    "news": ("get_company_news", "getCompanyNews"),                      # ticker, max_results, lookback_days, sources
    "search": ("search_company_news", "searchCompanyNews"),              # ticker, query, start, end, sources, max_results
    "press": ("get_company_press_releases", "getCompanyPressReleases"),  # ticker, lookback_days, max_results, sources
    "sec": ("get_sec_recent_events", "getSecRecentEvents"),              # ticker, filing_types, lookback_days, max_results
    "timeline": ("get_public_event_timeline", "getPublicEventTimeline"),  # ticker, start, end, sources, max, newest_first
    "verify": ("verify_company_event", "verifyCompanyEvent"),            # ticker, event_query, start, end, sources
}


def tool_case(name: str, ids: list[str], tool: str, args: list, scn: dict | None = None, **kw) -> Case:
    """One tool call against one scenario, run in both runtimes. `select` shapes what is compared."""
    scn = dict(scn or {})
    py_name, ts_name = TOOLS[tool]
    registry = scn.get("registry")

    def py():
        with _python_world(scn):
            return json.loads(asyncio.run(getattr(server, py_name)(*copy.deepcopy(args))))

    fx = {"now": scn.get("now", NOW), "env": scn.get("env", {}), "fn": ts_name}
    js = _TOOL_JS
    if registry is not None:
        fx.update(registry=registry, src=str(WORKER / "src"), esbuild=(WORKER / "node_modules" / "esbuild" / "lib" / "main.js").as_uri())
        js = _REGISTRY_BUNDLE_JS + _TOOL_JS
    return Case(name=name, ids=ids, py=py, ts={"js": js, "args": args, "fx": fx, "isolate": True},
                routes=_routes_for(scn), **kw)


# ── views: what the cases compare ─────────────────────────────────────────────────────────────

_VOLATILE = {"retrievedAt", "watermark", "duplicateGroupId"}


def ts_norm(value: Any) -> Any:
    """Drop volatile keys and the Worker's milliseconds (".000Z" -> "Z") so timestamps compare by instant."""
    if isinstance(value, dict):
        return {k: ts_norm(v) for k, v in value.items() if k not in _VOLATILE}
    if isinstance(value, list):
        return [ts_norm(v) for v in value]
    if isinstance(value, str):
        return re.sub(r"\.000Z$", "Z", value)
    return value


ITEM_BASE = ("title", "source", "sourceType", "eventType", "confidence", "tickerRelevance", "url")
ITEM_LABELS = ("evidenceClass", "tickerMatch", "urlProvenance", "decisionUse", "matchBasis")


def item_rows(v: dict, fields: tuple = ITEM_BASE, key: str = "items") -> list[dict]:
    rows = []
    for it in v.get(key) or []:
        rows.append({f: ts_norm(it.get(f)) for f in fields})
    return rows


def codes(v: dict) -> list[str]:
    return [w.get("code") for w in v.get("warnings") or []]


def status_row(info: dict | None, *fields: str) -> dict | None:
    return None if info is None else {f: ts_norm(info.get(f)) for f in fields}


def coverage_view(v: dict) -> dict:
    cov = v.get("coverage") or {}
    return {
        "sourceCoverage": v.get("sourceCoverage"),
        "state": cov.get("state"),
        "failed": [[f["source"], f["status"]] for f in cov.get("failedSources") or []],
        "skipped": [[f["source"], f["status"]] for f in cov.get("skippedSources") or []],
        "truncated": [[t["source"], t["acceptedCount"], t["returnedCount"], t["truncatedCount"]] for t in cov.get("truncatedSources") or []],
        "action": cov.get("recommendedNextAction"),
    }


def news_view(v: dict, *, items: tuple = ITEM_BASE, sources: tuple[str, ...] = (), source_fields: tuple = ("status",),
              extra: tuple = ("status",)) -> dict:
    """status + coverage + warning codes + chosen items + chosen sourceStatus rows."""
    out: dict[str, Any] = {k: v.get(k) for k in extra}
    out["coverage"] = coverage_view(v)
    out["warnings"] = codes(v)
    if items:
        out["items"] = item_rows(v, items)
    ss = v.get("sourceStatus") or {}
    out["sourceStatus"] = {s: status_row(ss.get(s), *source_fields) for s in sources}
    return out


# ── scenario data builders ────────────────────────────────────────────────────────────────────

def epoch(day_iso: str) -> int:
    return int(datetime.datetime.fromisoformat(day_iso.replace("Z", "+00:00")).timestamp())


def yahoo_row(title: str, when: str, *, publisher: str = "Reuters", link: str | None = "auto") -> dict:
    """One row of Yahoo's search news feed, in the shape both runtimes read (title, publisher, link, epoch time)."""
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:40]
    return {"title": title, "publisher": publisher, "providerPublishTime": epoch(when),
            "link": f"https://finance.yahoo.com/news/{slug}.html" if link == "auto" else link}


def finnhub_row(headline: str, when: str, *, summary: str = "", source: str = "Reuters", url: str | None = "auto") -> dict:
    slug = re.sub(r"[^a-z0-9]+", "-", headline.lower()).strip("-")[:40]
    return {"headline": headline, "summary": summary, "source": source, "datetime": epoch(when),
            "url": f"https://finnhub.io/api/news/{slug}" if url == "auto" else url}


def mx_row(title: str, when: str, *, host: str = "www.businesswire.com", symbol: str = "NVDA", exchange: str | None = "NASDAQ",
           name: str = "NVIDIA Corporation", uuid: str = "u1") -> dict:
    entity = {"symbol": symbol, "name": name, "match_score": 12.5}
    if exchange is not None:
        entity["exchange"] = exchange
    return {"uuid": uuid, "title": title, "snippet": title + " details", "url": f"https://{host}/news/{uuid}",
            "source": host, "published_at": when.replace("Z", ".000000Z"), "entities": [entity]}


def rss(items: list[dict], title: str = "Feed") -> str:
    body = "".join(
        "<item>" + "".join(f"<{k}>{v}</{k}>" for k, v in it.items() if k != "categories")
        + "".join(f'<category domain="{d}">{c}</category>' for d, c in it.get("categories", []))
        + "</item>" for it in items)
    return ('<?xml version="1.0" encoding="UTF-8"?><rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">'
            f"<channel><title>{title}</title>{body}</channel></rss>")


def gnw_item(title: str, when_rfc: str, stock: str, *, contributor: str | None = None, link: str | None = None, desc: str = "") -> dict:
    it: dict[str, Any] = {"title": title, "link": link or f"https://www.globenewswire.com/news-release/{re.sub(r'[^a-z0-9]+', '-', title.lower())[:30]}",
                          "description": desc or title, "pubDate": when_rfc,
                          "categories": [("https://www.globenewswire.com/rss/stock", stock)]}
    if contributor:
        it["dc:contributor"] = contributor
    return it


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Evidence labels per item: BR-NWS-104 (P0), BR-NWS-173, BR-NWS-175
# A Finnhub item (sourceType company_news) is contextual news; decisionUse depends only on its event type.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

LABEL_FIELDS = ("title", "sourceType", "eventType", "evidenceClass", "tickerMatch", "urlProvenance", "decisionUse")
FINNHUB_LABEL_ROWS = [
    finnhub_row("Acme analyst upgrade after rating review", "2026-10-14T10:00:00Z"),
    finnhub_row("Acme quarterly earnings beat", "2026-10-13T10:00:00Z", url="https://www.reuters.com/acme-earnings"),
    finnhub_row("Markets rally on chip demand", "2026-10-12T10:00:00Z", url=None),
]

CASES += [
    tool_case("news.labels.finnhub_items_are_contextual_news_with_event_type_decision_use",
              ["BR-NWS-104", "BR-NWS-173", "BR-NWS-175"], "news", ["ACME", 5, 14, ["finnhub"]],
              {"env": FAKE_FINNHUB, "routes": {FINNHUB_NEWS: {"json": FINNHUB_LABEL_ROWS}}},
              select=lambda v: item_rows(v, LABEL_FIELDS),
              expect=[
                  {"title": "Acme analyst upgrade after rating review", "sourceType": "company_news", "eventType": "analyst",
                   "evidenceClass": "CONTEXTUAL_NEWS", "tickerMatch": "EXPLICIT", "urlProvenance": "PROVIDER", "decisionUse": "CONTEXT_ONLY"},
                  {"title": "Acme quarterly earnings beat", "sourceType": "company_news", "eventType": "earnings",
                   "evidenceClass": "CONTEXTUAL_NEWS", "tickerMatch": "EXPLICIT", "urlProvenance": "PUBLISHER", "decisionUse": "CHECK_OFFICIAL_RELEASES"},
                  {"title": "Markets rally on chip demand", "sourceType": "company_news", "eventType": "other",
                   "evidenceClass": "CONTEXTUAL_NEWS", "tickerMatch": "SOURCE_SCOPED", "urlProvenance": "UNKNOWN", "decisionUse": "CONTEXT_ONLY"},
              ]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Finnhub source state: BR-NWS-176 (HTTP outcome), 181 (key), 178/179 (market eligibility), 153 (truncation),
# 144/157/169 (collection status), 161 (coverage action)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

ACME_INFO = {"shortName": "Acme", "longName": "Acme Corporation Inc."}
FINNHUB_VIEW_FIELDS = ("status", "rawCount", "filteredCount")


def finnhub_state_view(v: dict) -> dict:
    out = news_view(v, items=(), extra=("status", "sourceCoverage"))
    out["finnhub"] = (v.get("sourceStatus") or {}).get("finnhub")
    return out


def _finnhub_http_case(code: int, status: str, action: str, ids: list[str], body: Any = "{}") -> Case:
    return tool_case(
        f"news.finnhub.http_{code}_gives_{status.lower()}", ids, "news", ["ACME", 5, 14, ["finnhub"]],
        {"env": FAKE_FINNHUB, "routes": {FINNHUB_NEWS: {"status": code, "body": body}}},
        select=finnhub_state_view,
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL",
                "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["finnhub", status]], "skipped": [],
                             "truncated": [], "action": action},
                "warnings": ["SOURCE_UNAVAILABLE"], "sourceStatus": {},
                "finnhub": {"status": status, "rawCount": 0, "filteredCount": 0}})


CASES += [
    # Card 176: HTTP 429 -> RATE_LIMITED, and a rate-limited source makes the next action "retry".
    _finnhub_http_case(429, "RATE_LIMITED", "RETRY_RETRYABLE_SOURCES", ["BR-NWS-176", "BR-NWS-161"]),
    _finnhub_http_case(401, "AUTH_ERROR", "CHECK_OFFICIAL_RELEASES", ["BR-NWS-176"]),
    _finnhub_http_case(403, "AUTH_ERROR", "CHECK_OFFICIAL_RELEASES", ["BR-NWS-176"]),
    # Only 401/403/429 are classified; any other HTTP error (including 402) is a generic provider error.
    _finnhub_http_case(402, "PROVIDER_ERROR", "CHECK_OFFICIAL_RELEASES", ["BR-NWS-176"]),
    _finnhub_http_case(500, "PROVIDER_ERROR", "CHECK_OFFICIAL_RELEASES", ["BR-NWS-176"]),
    # A 200 whose body is not a list reads as a changed provider contract.
    tool_case("news.finnhub.non_list_body_gives_provider_changed", ["BR-NWS-176"], "news", ["ACME", 5, 14, ["finnhub"]],
              {"env": FAKE_FINNHUB, "routes": {FINNHUB_NEWS: {"json": {"error": "changed"}}}}, select=finnhub_state_view,
              expect={"status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL",
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["finnhub", "PROVIDER_CHANGED"]],
                                   "skipped": [], "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"},
                      "warnings": ["SOURCE_UNAVAILABLE"], "sourceStatus": {},
                      "finnhub": {"status": "PROVIDER_CHANGED", "rawCount": 0, "filteredCount": 0}}),
    # Card 181: no FINNHUB_API_KEY / FINNHUB_TOKEN -> nothing is called, UNCONFIGURED, source-limited empty result
    # (also cards 144, 157, 169: an empty result with a SOURCE_UNAVAILABLE warning is SOURCE_LIMITED_NOT_FOUND).
    tool_case("news.finnhub.no_key_is_unconfigured_and_empty_result_is_source_limited",
              ["BR-NWS-181", "BR-NWS-144", "BR-NWS-157", "BR-NWS-169"], "news", ["ACME", 5, 14, ["finnhub"]],
              {"routes": {}},
              select=lambda v: {**finnhub_state_view(v),
                                "messages": [w.get("message") for w in v.get("warnings") or []]},
              expect={"status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL",
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["finnhub", "UNCONFIGURED"]],
                                   "skipped": [], "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"},
                      "warnings": ["SOURCE_UNAVAILABLE"], "sourceStatus": {},
                      "finnhub": {"status": "UNCONFIGURED"},
                      "messages": ["Finnhub company-news source is not configured; skipped."]}),
]


def _eligibility_case(ticker: str, ids: list[str]) -> Case:
    reason = "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT"
    return tool_case(
        f"news.finnhub.ineligible_{re.sub(r'[^a-z0-9]+', '_', ticker.lower())}_is_skipped_without_a_request", ids,
        "news", [ticker, 5, 14, ["finnhub"]], {"env": FAKE_FINNHUB, "routes": {}},
        select=lambda v: {**finnhub_state_view(v),
                          "warning": [{k: w.get(k) for k in ("code", "source", "attempted", "reasonCode")}
                                      for w in v.get("warnings") or []]},
        expect={"status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL",
                "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [], "skipped": [["finnhub", "NOT_ELIGIBLE"]],
                             "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"},
                "warnings": ["SOURCE_NOT_ELIGIBLE"], "sourceStatus": {},
                "finnhub": {"status": "NOT_ELIGIBLE", "rawCount": 0, "filteredCount": 0, "attempted": False,
                            "reasonCode": reason, "allowedAction": "use_yahoo_or_official_sources"},
                "warning": [{"code": "SOURCE_NOT_ELIGIBLE", "source": "finnhub", "attempted": False, "reasonCode": reason}]})


CASES += [
    # Cards 178/179: ineligible suffixes (.ST .DE .L .TWO) and tickers (FOCI) are skipped with a reason code, no request.
    _eligibility_case("ERIC-B.ST", ["BR-NWS-178"]),
    _eligibility_case("VOLV-B.ST", ["BR-NWS-179"]),
    _eligibility_case("FOCI", ["BR-NWS-178"]),
    _eligibility_case("BRK.L", ["BR-NWS-178", "BR-NWS-179"]),
    # The suffix test is a plain endsWith: 'ALL' has no dot and a '.LN' ticker does not end with '.L', so both are eligible
    # (no key here, so they stop at UNCONFIGURED instead of NOT_ELIGIBLE).
    tool_case("news.finnhub.suffix_test_is_endswith_so_all_and_ln_tickers_stay_eligible", ["BR-NWS-178", "BR-NWS-179"], "news",
              ["ALL", 5, 14, ["finnhub"]], {"routes": {}}, select=lambda v: v["sourceStatus"]["finnhub"]["status"], expect="UNCONFIGURED"),
    tool_case("news.finnhub.dotted_ln_ticker_stays_eligible", ["BR-NWS-179"], "news",
              ["ABC.LN", 5, 14, ["finnhub"]], {"routes": {}}, select=lambda v: v["sourceStatus"]["finnhub"]["status"], expect="UNCONFIGURED"),
]

# Card 153: 40 in-window rows; the collection pool for max_results=10 is 30, and 10 are returned.
FINNHUB_MANY = [finnhub_row(f"Acme headline {n:02d}", f"2026-10-14T{23 - n // 4:02d}:{(n % 4) * 15:02d}:00Z") for n in range(40)]
CASES += [
    tool_case("news.finnhub.accepted_more_than_returned_reports_truncation", ["BR-NWS-153", "BR-NWS-172", "BR-NWS-161"], "news",
              ["ACME", 10, 14, ["finnhub"]], {"env": FAKE_FINNHUB, "routes": {FINNHUB_NEWS: {"json": FINNHUB_MANY}}},
              select=lambda v: {"finnhub": status_row((v["sourceStatus"] or {}).get("finnhub"), "status", "rawCount", "acceptedCount", "collectedCount",
                                                      "returnedCount", "truncatedCount", "hasMoreAccepted", "rejectedCount", "attempted"),
                                "returned": len(v["items"]), "coverage": coverage_view(v), "status": v.get("status")},
              expect={"finnhub": {"status": "OK", "rawCount": 40, "acceptedCount": 40, "collectedCount": 30, "returnedCount": 10,
                                  "truncatedCount": 30, "hasMoreAccepted": True, "rejectedCount": 0, "attempted": True},
                      "returned": 10,
                      "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [], "skipped": [],
                                   "truncated": [["finnhub", 40, 10, 30]], "action": "RETRY_TRUNCATED_SOURCE"},
                      "status": None}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Event-type classification: BR-NWS-120, 130, 253 (substring keyword lists, priority order, Python/Worker list differences)
# Every headline names ACME so the Yahoo identity gate accepts it; the case compares {title: eventType}.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _yahoo_titles_case(name: str, ids: list[str], titles: list[str], select, info: dict | None = None, **kw) -> Case:
    rows = [yahoo_row(t, f"2026-10-{14 - i:02d}T09:00:00Z") for i, t in enumerate(titles)]
    return tool_case(name, ids, "news", ["ACME", 30, 14, ["yahoo_finance_news"]],
                     {"info": info or ACME_INFO, "news": rows}, select=select, **kw)


def _event_types(v: dict) -> dict:
    return {it["title"]: it["eventType"] for it in v["items"]}


AGREED_EVENT_TITLES = {
    "Acme raises $50M in private placement": "guidance",          # card 130: 'raises' (guidance) precedes 'offering/financing'
    "Acme to host quarterly call": "earnings",
    "Acme second-quarter steps announced": "earnings",            # 'eps' inside 'steps' (substring match, not whole word)
    "Acme sector update": "regulatory",                           # 'sec' inside 'sector'
    "Acme dealer network expands": "contract",                    # 'deal' inside 'dealer'
    "Acme operates new facility": "macro",                        # 'rates' inside 'operates'
    "Acme notebook line launch": "financing",                     # 'note' inside 'notebook' is checked before 'launch' (product)
    "Acme files 10-K": "earnings",                                # '10-k' sits in the earnings list, so the regulatory entry is unreachable
    "Acme CEO resigns": "other",
}
# Finding (Q7, for triage): matching is by substring in both runtimes, so 'steps' -> earnings, 'sector' -> regulatory,
# 'dealer' -> contract, 'operates' -> macro, 'notebook' -> financing; eventType drives decisionUse.
CASES += [
    _yahoo_titles_case("news.event_type.keyword_priority_and_substring_matching_agree", ["BR-NWS-130", "BR-NWS-120", "BR-NWS-253"],
                       list(AGREED_EVENT_TITLES), _event_types, expect=AGREED_EVENT_TITLES),
    # Python's list has extra phrases ('announces new', 'annual report', 'credit facility') the Worker's lacks.
    _yahoo_titles_case("news.event_type.python_only_phrases_differ_from_worker", ["BR-NWS-120", "BR-NWS-130"],
                       ["Acme announces new CEO", "Acme files annual report", "Acme closes $50M credit facility"], _event_types,
                       mode="pinned", decision="FINDING: Python event-type keywords include 'announces new', 'annual report' and 'credit facility'; the Worker's do not",
                       expect_py={"Acme announces new CEO": "product", "Acme files annual report": "earnings",
                                  "Acme closes $50M credit facility": "financing"},
                       expect_ts={"Acme announces new CEO": "other", "Acme files annual report": "other",
                                  "Acme closes $50M credit facility": "other"}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Yahoo company identity: BR-NWS-139, 140 (aliases and acronyms), 166 (identity unavailable), 158 (feed state), 156 (outage)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

AMD_INFO = {"shortName": "Advanced Micro Devices, Inc.", "longName": "Advanced Micro Devices, Inc."}


def _match_basis(v: dict) -> dict:
    return {it["title"]: it.get("matchBasis") for it in v["items"]}


def _yahoo_state(v: dict) -> dict:
    row = (v["sourceStatus"] or {}).get("yahoo_finance_news") or {}
    return {k: row.get(k) for k in ("status", "rawCount", "acceptedCount", "returnedCount", "rejectedCount", "rejectionCounts",
                                    "identityStatus", "attempted")}


def _amdz_case() -> Case:
    rows = [yahoo_row("Advanced Micro Devices, Inc. ships MI400", "2026-10-14T09:00:00Z"),
            yahoo_row("AMD beats estimates", "2026-10-13T09:00:00Z"),
            yahoo_row("Intel unveils new chip", "2026-10-12T09:00:00Z"),
            yahoo_row("AMDZ rallies", "2026-10-11T09:00:00Z")]
    return tool_case("news.identity.alias_acronym_and_ticker_matches_are_labelled", ["BR-NWS-139", "BR-NWS-140"], "news",
                     ["AMDZ", 10, 14, ["yahoo_finance_news"]], {"info": AMD_INFO, "news": rows},
                     select=lambda v: {"basis": _match_basis(v), "state": _yahoo_state(v)},
                     expect={"basis": {"Advanced Micro Devices, Inc. ships MI400": "ISSUER_NAME", "AMD beats estimates": "ISSUER_ACRONYM",
                                       "AMDZ rallies": "TICKER_TOKEN"},
                             "state": {"status": "OK", "rawCount": 4, "acceptedCount": 3, "returnedCount": 3, "rejectedCount": 1,
                                       "rejectionCounts": {"IDENTITY_MISMATCH": 1}, "identityStatus": "RESOLVED", "attempted": True}})


CASES.append(_amdz_case())

IDENTITY_UNAVAILABLE_ROW = {"status": "IDENTITY_UNAVAILABLE", "attempted": True, "reasonCode": "YAHOO_PROFILE_IDENTITY_UNAVAILABLE",
                            "allowedAction": "retry_or_use_explicit_ticker_matches_only"}
CASES += [
    # Card 166 (and 140's fallback): no company name -> UNAVAILABLE identity, only ticker-token matches survive.
    tool_case("news.identity.unavailable_keeps_only_ticker_token_matches_and_degrades_coverage", ["BR-NWS-166", "BR-NWS-140"], "news",
              ["ACME", 10, 14, ["yahoo_finance_news"]],
              {"info": {}, "news": [yahoo_row("ACME wins order", "2026-10-14T09:00:00Z"), yahoo_row("Unrelated firm news", "2026-10-13T09:00:00Z")]},
              select=lambda v: {"titles": [it["title"] for it in v["items"]], "state": _yahoo_state(v), "warnings": codes(v),
                                "identity": (v["sourceStatus"] or {}).get("yahoo_finance_identity"), "status": v.get("status"),
                                "coverage": coverage_view(v)},
              expect={"titles": ["ACME wins order"],
                      "state": {"status": "OK", "rawCount": 2, "acceptedCount": 1, "returnedCount": 1, "rejectedCount": 1,
                                "rejectionCounts": {"IDENTITY_UNAVAILABLE_TICKER_NOT_FOUND": 1}, "identityStatus": "UNAVAILABLE", "attempted": True},
                      "warnings": ["SOURCE_IDENTITY_UNAVAILABLE"], "identity": IDENTITY_UNAVAILABLE_ROW, "status": "PARTIAL",
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["yahoo_finance_identity", "IDENTITY_UNAVAILABLE"]],
                                   "skipped": [], "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"}}),
    # Card 139: a Yahoo shortName that is just the ticker. Python takes it as an issuer alias (RESOLVED); the Worker
    # drops an alias equal to the ticker, so its identity is UNAVAILABLE, a warning is raised and coverage is PARTIAL.
    tool_case("news.identity.ticker_only_short_name_resolves_in_python_not_in_worker", ["BR-NWS-139"], "news",
              ["TSTX", 10, 14, ["yahoo_finance_news"]],
              {"info": {"shortName": "TSTX"}, "news": [yahoo_row("TSTX wins order", "2026-10-14T09:00:00Z")]},
              select=lambda v: {"identityStatus": _yahoo_state(v)["identityStatus"], "warnings": codes(v), "status": v.get("status"),
                                "coverageState": v["coverage"]["state"], "titles": [it["title"] for it in v["items"]]},
              mode="pinned", decision="FINDING: Worker excludes a ticker-equal Yahoo shortName from issuer aliases (identity UNAVAILABLE); Python accepts it (RESOLVED)",
              expect_py={"identityStatus": "RESOLVED", "warnings": [], "status": None, "coverageState": "FULL", "titles": ["TSTX wins order"]},
              expect_ts={"identityStatus": "UNAVAILABLE", "warnings": ["SOURCE_IDENTITY_UNAVAILABLE"], "status": "PARTIAL",
                         "coverageState": "PARTIAL", "titles": ["TSTX wins order"]}),
]

# Card 158: 20 raw Yahoo items, none accepted, collector completed -> EMPTY_RESULT with the rejection breakdown.
_OLD = [yahoo_row(f"Acme old story {n}", f"2026-09-{n + 1:02d}T09:00:00Z") for n in range(12)]
_OTHER = [yahoo_row(f"Other firm story {n}", f"2026-10-{n + 1:02d}T09:00:00Z") for n in range(8)]
CASES += [
    tool_case("news.yahoo.completed_feed_with_nothing_accepted_is_empty_result_with_rejection_counts", ["BR-NWS-158"], "news",
              ["ACME", 10, 14, ["yahoo_finance_news"]], {"info": ACME_INFO, "news": _OLD + _OTHER},
              select=lambda v: {"state": _yahoo_state(v), "status": v.get("status"), "coverage": coverage_view(v)},
              expect={"state": {"status": "EMPTY_RESULT", "rawCount": 20, "acceptedCount": 0, "returnedCount": 0, "rejectedCount": 20,
                                "rejectionCounts": {"DATE_WINDOW": 12, "IDENTITY_MISMATCH": 8}, "identityStatus": "RESOLVED", "attempted": True},
                      "status": "NOT_FOUND",
                      "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [], "skipped": [], "truncated": [], "action": "USE_RETURNED_CONTEXT"}}),
    # Card 156 (tool-level form): a Yahoo outage marks both selected Yahoo sub-sources PROVIDER_ERROR.
    tool_case("news.yahoo.outage_marks_both_selected_yahoo_sources_provider_error", ["BR-NWS-156", "BR-NWS-158"], "news",
              ["ACME", 10, 14, ["yahoo_finance_news", "yahoo_finance_press_releases"]],
              {"info": ACME_INFO, "news": RuntimeError("yahoo down")},
              select=lambda v: {"status": v.get("status"), "warnings": codes(v),
                                "rows": {s: status_row(v["sourceStatus"].get(s), "status", "rawCount", "filteredCount", "acceptedCount", "attempted")
                                         for s in ("yahoo_finance_news", "yahoo_finance_press_releases")},
                                "coverage": coverage_view(v)},
              expect={"status": "SOURCE_LIMITED_NOT_FOUND", "warnings": ["SOURCE_UNAVAILABLE"],
                      "rows": {s: {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0, "acceptedCount": 0, "attempted": True}
                               for s in ("yahoo_finance_news", "yahoo_finance_press_releases")},
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL",
                                   "failed": [["yahoo_finance_news", "PROVIDER_ERROR"], ["yahoo_finance_press_releases", "PROVIDER_ERROR"]],
                                   "skipped": [], "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"}}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# De-duplication: BR-NWS-114..119, 174, 251 (group key, survivor by source priority, merge metadata)
# duplicateGroupId itself is never equal across runtimes (sha256[:16] vs a 32-bit hash), so survivors are compared.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _js_hash32(key: str) -> str:
    """The Worker's makeDupGroupId hash (Java-style 31x rolling 32-bit hash, 8 hex digits), computed independently."""
    h = 0
    for ch in key:
        h = ((h << 5) - h + ord(ch)) & 0xFFFFFFFF
    return format(h, "08x")


def _survivors(v: dict) -> list[dict]:
    out = []
    for it in v.get("items") or []:
        out.append({"title": it["title"], "source": it["source"], "publishedAt": ts_norm(it["publishedAt"]),
                    "refs": [{"source": r.get("source"), "sourceType": r.get("sourceType"), "publishedAt": ts_norm(r.get("publishedAt"))}
                             for r in it.get("sourceRefs") or []]})
    return out


def _variance(v: dict) -> list[dict]:
    return [{"code": w["code"], "statusImpact": w.get("statusImpact")} for w in v.get("warnings") or [] if w.get("code") == "TIMESTAMP_VARIANCE"]


def _dedupe_view(v: dict) -> dict:
    return {"items": _survivors(v), "variance": _variance(v)}


FINNHUB_AND_YAHOO = ["yahoo_finance_news", "finnhub"]
CASES += [
    # Card 116: the same headline from Finnhub (rank 4) and Yahoo news (rank 5): Finnhub is kept, Yahoo goes to sourceRefs.
    tool_case("news.dedupe.finnhub_beats_yahoo_news_for_the_same_headline", ["BR-NWS-116", "BR-NWS-117", "BR-NWS-115"], "news",
              ["ACME", 10, 14, FINNHUB_AND_YAHOO],
              {"env": FAKE_FINNHUB, "info": ACME_INFO,
               "news": [yahoo_row("Acme wins supply contract", "2026-10-10T13:00:00Z")],
               "routes": {FINNHUB_NEWS: {"json": [finnhub_row("Acme wins supply contract", "2026-10-10T13:00:00Z")]}}},
              select=_dedupe_view,
              expect={"items": [{"title": "Acme wins supply contract", "source": "finnhub", "publishedAt": "2026-10-10T13:00:00Z",
                                 "refs": [{"source": "yahoo_finance_news", "sourceType": "yahoo_finance_news",
                                           "publishedAt": "2026-10-10T13:00:00Z"}]}],
                      "variance": []}),
    # Different timestamps in one group: the survivor keeps its own time and a TIMESTAMP_VARIANCE warning (statusImpact false) is added.
    tool_case("news.dedupe.timestamp_variance_is_flagged_without_status_impact", ["BR-NWS-117", "BR-NWS-252"], "news",
              ["ACME", 10, 14, FINNHUB_AND_YAHOO],
              {"env": FAKE_FINNHUB, "info": ACME_INFO,
               "news": [yahoo_row("Acme wins supply contract", "2026-10-10T13:05:00Z")],
               "routes": {FINNHUB_NEWS: {"json": [finnhub_row("Acme wins supply contract", "2026-10-10T13:00:00Z")]}}},
              select=_dedupe_view,
              expect={"items": [{"title": "Acme wins supply contract", "source": "finnhub", "publishedAt": "2026-10-10T13:00:00Z",
                                 "refs": [{"source": "yahoo_finance_news", "sourceType": "yahoo_finance_news",
                                           "publishedAt": "2026-10-10T13:05:00Z"}]}],
                      "variance": [{"code": "TIMESTAMP_VARIANCE", "statusImpact": False}]}),
    # Card 117: a Yahoo press release (rank 2) beats Finnhub (rank 4) although it was published later.
    tool_case("news.dedupe.yahoo_press_release_beats_finnhub", ["BR-NWS-117"], "news",
              ["ACME", 10, 14, ["yahoo_finance_press_releases", "finnhub"]],
              {"env": FAKE_FINNHUB, "info": ACME_INFO,
               "news": [yahoo_row("Acme wins supply contract", "2026-10-10T10:05:00Z", publisher="Business Wire")],
               "routes": {FINNHUB_NEWS: {"json": [finnhub_row("Acme wins supply contract", "2026-10-10T10:00:00Z")]}}},
              select=_dedupe_view,
              expect={"items": [{"title": "Acme wins supply contract", "source": "yahoo_finance_press_releases", "publishedAt": "2026-10-10T10:05:00Z",
                                 "refs": [{"source": "finnhub", "sourceType": "company_news", "publishedAt": "2026-10-10T10:00:00Z"}]}],
                      "variance": [{"code": "TIMESTAMP_VARIANCE", "statusImpact": False}]}),
    # Card 115: corp/globenewswire stop words and the ticker word are dropped from the stem, so these two titles group.
    tool_case("news.dedupe.stop_words_and_ticker_are_dropped_from_the_title_stem", ["BR-NWS-115", "BR-NWS-119"], "news",
              ["NVDA", 10, 14, ["finnhub"]],
              {"env": FAKE_FINNHUB, "routes": {FINNHUB_NEWS: {"json": [
                  finnhub_row("NVIDIA Corp Announces Q3 Results - GlobeNewswire", "2026-10-10T13:00:00Z"),
                  finnhub_row("Nvidia announces Q3 results", "2026-10-10T13:02:00Z")]}}},
              select=_dedupe_view,
              expect={"items": [{"title": "Nvidia announces Q3 results", "source": "finnhub", "publishedAt": "2026-10-10T13:02:00Z",
                                 "refs": [{"source": "finnhub", "sourceType": "company_news", "publishedAt": "2026-10-10T13:00:00Z"}]}],
                      "variance": [{"code": "TIMESTAMP_VARIANCE", "statusImpact": False}]}),
    # Card 174: newswire (rank 3) beats Yahoo news (rank 5).
    tool_case("news.dedupe.newswire_beats_yahoo_news", ["BR-NWS-174", "BR-NWS-116"], "news",
              ["ACME", 10, 14, ["newswire", "yahoo_finance_news"]],
              {"info": ACME_INFO, "news": [yahoo_row("Acme wins supply contract", "2026-10-10T10:05:00Z")],
               "routes": {GNW_FEEDS[0][1]: {"body": rss([gnw_item("Acme wins supply contract", "Sat, 10 Oct 2026 10:00:00 +0000", "Nasdaq:ACME")])},
                          "https://www.globenewswire.com/RssFeed/*": {"body": rss([])}}},
              select=_dedupe_view,
              expect={"items": [{"title": "Acme wins supply contract", "source": "newswire", "publishedAt": "2026-10-10T10:00:00Z",
                                 "refs": [{"source": "yahoo_finance_news", "sourceType": "yahoo_finance_news", "publishedAt": "2026-10-10T10:05:00Z"}]}],
                      "variance": [{"code": "TIMESTAMP_VARIANCE", "statusImpact": False}]}),
    # Card 118 / 251: Python unescapes HTML entities before building the stem; the Worker does not ('amp' stays in its stem).
    tool_case("news.dedupe.html_entity_in_title_groups_in_python_not_in_worker", ["BR-NWS-118", "BR-NWS-251"], "news",
              ["SMCO", 10, 14, FINNHUB_AND_YAHOO],
              {"env": FAKE_FINNHUB, "info": {"shortName": "Smith & Co"},
               "news": [yahoo_row("Smith & Co. wins contract", "2026-10-10T13:00:00Z")],
               "routes": {FINNHUB_NEWS: {"json": [finnhub_row("Smith &amp; Co. wins contract", "2026-10-10T13:00:00Z")]}}},
              select=lambda v: {"sources": sorted(it["source"] for it in v["items"]), "count": len(v["items"])},
              mode="pinned", decision="FINDING: Python HTML-unescapes titles before the dedupe stem and the Worker does not, so the same story stays two items in the Worker",
              expect_py={"sources": ["finnhub"], "count": 1}, expect_ts={"sources": ["finnhub", "yahoo_finance_news"], "count": 2}),
]

# Cards 114 / 119: the group id itself. Python = first 16 hex of sha256 over "stem|day|ISSUER-or-TICKER"; the Worker = a
# 31x rolling 32-bit hash of the same string as 8 hex digits. The GlobeNewswire contributor is the item's issuer.
_GID_KEY = "announces q3 results|2026-10-01|ACME CORP"
CASES += [
    tool_case("news.dedupe.group_id_format_differs_between_runtimes", ["BR-NWS-114", "BR-NWS-119"], "news",
              ["ACME", 10, 14, ["newswire"]],
              {"info": ACME_INFO,
               "routes": {GNW_FEEDS[0][1]: {"body": rss([gnw_item("ACME Corp Announces Q3 Results", "Thu, 01 Oct 2026 13:00:00 +0000",
                                                                  "Nasdaq:ACME", contributor="Acme Corp")])},
                          "https://www.globenewswire.com/RssFeed/*": {"body": rss([])}}},
              select=lambda v: [it["duplicateGroupId"] for it in v["items"]],
              mode="pinned", decision="FINDING: duplicateGroupId is a 16-hex sha256 prefix in Python and an 8-hex 32-bit hash in the Worker",
              expect_py=[hashlib.sha256(_GID_KEY.encode()).hexdigest()[:16]], expect_ts=[_js_hash32(_GID_KEY)]),
]

# Card 129: the merged list is capped to the newest N across sources, whatever the source. (Each collector first keeps its
# own first N: Finnhub in feed order, which is newest first for the real feed.)
_CAP_YAHOO = [yahoo_row(f"Acme alpha story {n}", f"2026-10-1{n}T09:00:00Z") for n in (4, 3, 2, 1)]            # Oct 14..11 09:00
_CAP_FINNHUB = [finnhub_row(f"Acme beta story {n}", f"2026-10-1{n}T15:00:00Z") for n in (4, 3, 2, 1)]         # Oct 14..11 15:00
CASES += [
    tool_case("news.cap.merged_sources_keep_the_newest_n_after_dedupe", ["BR-NWS-129"], "timeline",
              ["ACME", "", "", FINNHUB_AND_YAHOO, 3, True],
              {"env": FAKE_FINNHUB, "info": ACME_INFO, "news": _CAP_YAHOO, "routes": {FINNHUB_NEWS: {"json": _CAP_FINNHUB}}},
              select=lambda v: [(e["title"], e["source"], ts_norm(e["timestamp"])) for e in v["timeline"]],
              expect=[("Acme beta story 4", "finnhub", "2026-10-14T15:00:00Z"),
                      ("Acme alpha story 4", "yahoo_finance_news", "2026-10-14T09:00:00Z"),
                      ("Acme beta story 3", "finnhub", "2026-10-13T15:00:00Z")]),
]

# Card 136: epoch seconds -> '...Z' in Python, '....000Z' in the Worker; compact 8-digit dates agree.
CASES += [
    tool_case("news.timestamp.epoch_publishedat_has_milliseconds_only_in_the_worker", ["BR-NWS-136"], "news",
              ["ACME", 10, 14, ["finnhub"]],
              {"env": FAKE_FINNHUB, "now": "2025-10-10T00:00:00Z",
               "routes": {FINNHUB_NEWS: {"json": [{"headline": "Acme epoch story", "summary": "", "source": "Wire", "datetime": 1760000000,
                                                   "url": "https://finnhub.io/api/news/epoch"},
                                                  {"headline": "Acme compact story", "summary": "", "source": "Wire", "datetime": "20251008",
                                                   "url": "https://finnhub.io/api/news/compact"}]}}},
              select=lambda v: {it["title"]: it["publishedAt"] for it in v["items"]},
              mode="pinned", decision="FINDING: publishedAt carries milliseconds ('.000Z') in the Worker and none in Python for epoch input",
              expect_py={"Acme epoch story": "2025-10-09T08:53:20Z", "Acme compact story": "2025-10-08T00:00:00Z"},
              expect_ts={"Acme epoch story": "2025-10-09T08:53:20.000Z", "Acme compact story": "2025-10-08T00:00:00Z"}),
]

# Card 132: requested vs attempted source accounting (direct: sourceRequestFields is a Worker export).
_STATUS_132 = {"finnhub": {"status": "NOT_ELIGIBLE"}, "marketaux": {"status": "NOT_NEEDED"}, "yahoo_finance_news": {"status": "OK"},
               "bad": "not-a-dict", "sec": {"status": "EMPTY_RESULT", "attempted": False}}
CASES += [
    Case(name="news.source_request.default_sources_report_only_attempted_sources", ids=["BR-NWS-132"],
         py=lambda: server._source_request_fields(None, _STATUS_132),
         ts={"js": "return m.yahooFinance.sourceRequestFields(null, args[0]);", "args": [_STATUS_132]},
         expect={"requestedSources": None, "defaultSourcesApplied": True,
                 "attemptedSources": [{"sourceType": "yahoo_finance_news", "status": "OK"}]}),
    Case(name="news.source_request.explicit_empty_list_is_not_the_default", ids=["BR-NWS-132"],
         py=lambda: server._source_request_fields([], {}),
         ts={"js": "return m.yahooFinance.sourceRequestFields([], {});"},
         expect={"requestedSources": [], "defaultSourcesApplied": False, "attemptedSources": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Marketaux wire fallback: BR-NWS-184, 185, 186, 187, 188, 189, 190 (gating, bounds), 168 (Finnhub limits force it),
# 155 (status mapping), 121 (exchange normalisation)
# The exact request URL is the route key, so a changed limit, page, domain list or window is an UNSTUBBED request.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from urllib.parse import urlencode  # noqa: E402

MX_TOKEN = "fake-marketaux-token"
MX_DOMAINS = "businesswire.com,prnewswire.com,globenewswire.com"
NVDA_INFO = {"shortName": "NVIDIA Corporation", "longName": "NVIDIA Corporation", "exchange": "NMS"}


def mx_url(symbol: str = "NVDA", after: str = "2026-10-01", before: str = "2026-10-15", limit: int = 3, search: str | None = None) -> str:
    params = {"symbols": symbol, "filter_entities": "true", "language": "en", "domains": MX_DOMAINS,
              "published_after": f"{after}T00:00:00", "published_before": f"{before}T23:59:59", "limit": str(limit), "page": "1"}
    if search:
        params["search"] = search
    return "https://api.marketaux.com/v1/news/all?" + urlencode({**params, "api_token": MX_TOKEN})


MX_FIELDS = ("status", "rawCount", "acceptedCount", "returnedCount", "rejectedCount", "rejectionCounts", "attempted", "reasonCode",
             "fallbackReason", "quotaPolicy", "domains", "allowedAction")


def mx_view(v: dict) -> dict:
    return {"marketaux": status_row((v.get("sourceStatus") or {}).get("marketaux"), *MX_FIELDS),
            "items": item_rows(v, ("title", "sourceType", "matchBasis", "confidence", "tickerRelevance")), "coverage": coverage_view(v)}


MX_QUOTA = {"dailyLimit": 100, "maxArticlesPerRequest": 3, "pagesRequested": 1, "automaticPagination": False}
MX_DOMAIN_LIST = ["businesswire.com", "prnewswire.com", "globenewswire.com"]
MX_MIXED_ROWS = [
    mx_row("NVIDIA announces wire result", "2026-10-12T09:00:00Z", uuid="a1"),
    mx_row("Reuters piece on NVIDIA", "2026-10-12T10:00:00Z", host="www.reuters.com", uuid="a2"),
    {**mx_row("Insecure link", "2026-10-12T11:00:00Z", uuid="a3"), "url": "http://www.businesswire.com/news/a3"},
    mx_row("AMD announces wire result", "2026-10-12T12:00:00Z", symbol="AMD", uuid="a4"),
    mx_row("Old NVIDIA wire result", "2026-08-01T12:00:00Z", uuid="a5"),
]

CASES += [
    # Cards 187/188/189: one page of at most 3 rows from the three wire domains for the lookback window; rows are filtered locally.
    tool_case("news.marketaux.explicit_source_makes_one_bounded_request_and_filters_rows",
              ["BR-NWS-187", "BR-NWS-188", "BR-NWS-189"], "news", ["NVDA", 10, 14, ["marketaux"]],
              {"env": FAKE_MARKETAUX, "routes": {mx_url(): {"json": {"data": MX_MIXED_ROWS}}}}, select=mx_view,
              expect={"marketaux": {"status": "OK", "rawCount": 5, "acceptedCount": 1, "returnedCount": 1, "rejectedCount": 4,
                                    "rejectionCounts": {"DOMAIN_NOT_ALLOWED": 1, "INVALID_URL": 1, "ENTITY_SYMBOL_MISMATCH": 1, "DATE_OUTSIDE_WINDOW": 1},
                                    "attempted": True, "reasonCode": None, "fallbackReason": "EXPLICIT_SOURCE_SELECTION",
                                    "quotaPolicy": MX_QUOTA, "domains": MX_DOMAIN_LIST, "allowedAction": "check_official_releases"},
                      "items": [{"title": "NVIDIA announces wire result", "sourceType": "marketaux_wire", "matchBasis": "PROVIDER_ENTITY_SYMBOL",
                                 "confidence": "LOW", "tickerRelevance": "HIGH"}],
                      "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [], "skipped": [], "truncated": [],
                                   "action": "USE_RETURNED_CONTEXT"}}),
    # Explicit dates replace the default window in the request (published_after / published_before).
    tool_case("news.marketaux.search_query_and_explicit_dates_are_sent_to_the_provider", ["BR-NWS-187", "BR-NWS-185"], "search",
              ["NVDA", "earnings", "2026-09-20", "2026-10-05", ["marketaux"], 5],
              {"env": FAKE_MARKETAUX, "routes": {mx_url(after="2026-09-20", before="2026-10-05", search="earnings"): {"json": {"data": []}}}},
              select=lambda v: {"status": v["sourceStatus"]["marketaux"]["status"], "attempted": v["sourceStatus"]["marketaux"]["attempted"]},
              expect={"status": "EMPTY_RESULT", "attempted": True}),
    # Cards 184/186/190: Yahoo and Finnhub found evidence and nothing is limited -> no Marketaux call, no quota spent.
    tool_case("news.marketaux.not_called_when_higher_priority_sources_found_evidence",
              ["BR-NWS-184", "BR-NWS-186", "BR-NWS-190"], "news", ["ACME", 10, 14, None],
              {"env": {**FAKE_FINNHUB, **FAKE_MARKETAUX}, "info": ACME_INFO,
               "news": [yahoo_row(f"Acme story {w}", f"2026-10-1{n}T09:00:00Z") for n, w in ((4, "alpha"), (3, "beta"), (2, "gamma"))],
               "routes": {FINNHUB_NEWS: {"json": []}}},
              select=mx_view,
              expect={"marketaux": {"status": "NOT_NEEDED", "rawCount": 0, "acceptedCount": 0, "returnedCount": 0, "rejectedCount": 0,
                                    "rejectionCounts": {}, "attempted": False, "reasonCode": "HIGHER_PRIORITY_EVIDENCE_FOUND",
                                    "fallbackReason": None,
                                    "quotaPolicy": {**MX_QUOTA, "pagesRequested": 0}, "domains": MX_DOMAIN_LIST, "allowedAction": "none"},
                      "items": [{"title": f"Acme story {w}", "sourceType": "yahoo_finance_news", "matchBasis": "TICKER_TOKEN",
                                 "confidence": "MEDIUM", "tickerRelevance": "HIGH"} for w in ("alpha", "beta", "gamma")],
                      "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [],
                                   "skipped": [["marketaux", "NOT_NEEDED"]], "truncated": [], "action": "USE_RETURNED_CONTEXT"}}),
    # Card 168: a Finnhub NOT_ELIGIBLE skip (a deliberate policy skip) still counts as limited coverage and spends a Marketaux call.
    tool_case("news.marketaux.finnhub_policy_skip_forces_the_fallback_even_with_yahoo_evidence", ["BR-NWS-168", "BR-NWS-186"], "news",
              ["ERIC-B.ST", 10, 14, None],
              {"env": {**FAKE_FINNHUB, **FAKE_MARKETAUX}, "info": {"shortName": "Ericsson", "longName": "Telefonaktiebolaget LM Ericsson"},
               "news": [yahoo_row("ERIC-B.ST story alpha", "2026-10-14T09:00:00Z"), yahoo_row("ERIC-B.ST story beta", "2026-10-13T09:00:00Z")],
               "routes": {mx_url(symbol="ERIC-B.ST"): {"json": {"data": []}}}},
              select=lambda v: {"marketaux": status_row(v["sourceStatus"]["marketaux"], "status", "attempted", "fallbackReason", "reasonCode", "quotaPolicy"),
                                "finnhub": status_row(v["sourceStatus"]["finnhub"], "status", "attempted"),
                                "warnings": codes(v), "items": [it["title"] for it in v["items"]]},
              expect={"marketaux": {"status": "EMPTY_RESULT", "attempted": True, "fallbackReason": "HIGHER_PRIORITY_COVERAGE_LIMITED",
                                    "reasonCode": None, "quotaPolicy": MX_QUOTA},
                      "finnhub": {"status": "NOT_ELIGIBLE", "attempted": False},
                      "warnings": ["SOURCE_NOT_ELIGIBLE"], "items": ["ERIC-B.ST story alpha", "ERIC-B.ST story beta"]}),
    # Card 185: for search_company_news the priority items must contain the query, otherwise Marketaux runs (HIGHER_PRIORITY_EMPTY).
    tool_case("news.marketaux.search_fallback_runs_when_no_priority_item_contains_the_query", ["BR-NWS-185"], "search",
              ["ACME", "buyback", "", "", None, 10],
              {"env": {**FAKE_FINNHUB, **FAKE_MARKETAUX}, "info": ACME_INFO,
               "news": [yahoo_row("Acme quarterly call set", "2026-10-14T09:00:00Z"), yahoo_row("Acme opens plant", "2026-10-13T09:00:00Z")],
               "routes": {FINNHUB_NEWS: {"json": []}, mx_url(symbol="ACME", search="buyback"): {"json": {"data": []}}}},
              select=lambda v: status_row(v["sourceStatus"]["marketaux"], "status", "attempted", "fallbackReason", "reasonCode"),
              expect={"status": "EMPTY_RESULT", "attempted": True, "fallbackReason": "HIGHER_PRIORITY_EMPTY", "reasonCode": None}),
    tool_case("news.marketaux.search_fallback_skipped_when_a_priority_item_contains_the_query", ["BR-NWS-185"], "search",
              ["ACME", "buy", "", "", None, 10],
              {"env": {**FAKE_FINNHUB, **FAKE_MARKETAUX}, "info": ACME_INFO,
               "news": [yahoo_row("Acme announces buyback", "2026-10-14T09:00:00Z")],
               "routes": {FINNHUB_NEWS: {"json": []}}},
              select=lambda v: status_row(v["sourceStatus"]["marketaux"], "status", "attempted", "fallbackReason", "reasonCode"),
              expect={"status": "NOT_NEEDED", "attempted": False, "fallbackReason": None, "reasonCode": "HIGHER_PRIORITY_EVIDENCE_FOUND"}),
]


def _mx_status_case(label: str, route: dict | None, env: dict, status: str, action: str, ids: list[str], cov_action: str) -> Case:
    routes = {} if route is None else {MARKETAUX_NEWS: route}
    return tool_case(
        f"news.marketaux.{label}", ids, "news", ["NVDA", 10, 14, ["marketaux"]], {"env": env, "routes": routes},
        select=lambda v: {"marketaux": status_row(v["sourceStatus"]["marketaux"], "status", "allowedAction", "attempted", "fallbackReason"),
                          "coverage": coverage_view(v), "status": v.get("status"), "warnings": codes(v)},
        expect={"marketaux": {"status": status, "allowedAction": action, "attempted": status != "UNCONFIGURED",
                              "fallbackReason": "EXPLICIT_SOURCE_SELECTION"},
                "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["marketaux", status]], "skipped": [],
                             "truncated": [], "action": cov_action},
                "status": "SOURCE_LIMITED_NOT_FOUND", "warnings": ["SOURCE_UNAVAILABLE"]})


CASES += [
    # Card 155: HTTP and parse outcomes -> status -> allowedAction -> coverage action.
    _mx_status_case("http_402_is_rate_limited_and_retryable", {"status": 402, "body": "{}"}, FAKE_MARKETAUX, "RATE_LIMITED",
                    "retry_after_quota_reset", ["BR-NWS-155"], "RETRY_RETRYABLE_SOURCES"),
    _mx_status_case("http_429_is_rate_limited", {"status": 429, "body": "{}"}, FAKE_MARKETAUX, "RATE_LIMITED",
                    "retry_after_quota_reset", ["BR-NWS-155"], "RETRY_RETRYABLE_SOURCES"),
    _mx_status_case("http_401_is_auth_error", {"status": 401, "body": "{}"}, FAKE_MARKETAUX, "AUTH_ERROR",
                    "verify_marketaux_api_token", ["BR-NWS-155"], "CHECK_OFFICIAL_RELEASES"),
    _mx_status_case("http_500_is_provider_error", {"status": 500, "body": "{}"}, FAKE_MARKETAUX, "PROVIDER_ERROR",
                    "retry_or_use_official_sources", ["BR-NWS-155"], "CHECK_OFFICIAL_RELEASES"),
    _mx_status_case("body_without_data_array_is_provider_changed", {"json": {"meta": {}}}, FAKE_MARKETAUX, "PROVIDER_CHANGED",
                    "inspect_provider_contract", ["BR-NWS-155"], "CHECK_OFFICIAL_RELEASES"),
    _mx_status_case("no_token_is_unconfigured", None, {}, "UNCONFIGURED", "configure_marketaux_api_token", ["BR-NWS-155", "BR-NWS-189"],
                    "CHECK_OFFICIAL_RELEASES"),
]

# Card 121: 'NYSE American' normalises to NYSE (the NYSE test runs before the AMEX test), so an 'AMEX' entity is a mismatch
# and a 'NYSE' entity is not. Unknown exchanges ('LSE') and a missing exchange skip the check.
# Finding (Q7, for triage): NYSE American-listed issuers are indistinguishable from NYSE ones in both runtimes.
_NYSEA_ROWS = [mx_row("Row with NYSE entity", "2026-10-12T09:00:00Z", symbol="NYSA", exchange="NYSE", uuid="b1"),
               mx_row("Row with AMEX entity", "2026-10-12T10:00:00Z", symbol="NYSA", exchange="AMEX", uuid="b2"),
               mx_row("Row with LSE entity", "2026-10-12T11:00:00Z", symbol="NYSA", exchange="LSE", uuid="b3"),
               mx_row("Row with no exchange", "2026-10-12T12:00:00Z", symbol="NYSA", exchange=None, uuid="b4")]
CASES += [
    tool_case("news.marketaux.nyse_american_normalises_to_nyse_before_the_exchange_check", ["BR-NWS-121"], "news",
              ["NYSA", 10, 14, ["yahoo_finance_news", "marketaux"]],
              {"env": FAKE_MARKETAUX, "info": {"shortName": "Nysa Corp", "longName": "Nysa Corp", "exchange": "NYSE American"},
               "news": [], "routes": {mx_url(symbol="NYSA"): {"json": {"data": _NYSEA_ROWS}}}},
              select=lambda v: {"accepted": sorted(it["title"] for it in v["items"]),
                                "marketaux": status_row(v["sourceStatus"]["marketaux"], "acceptedCount", "rejectedCount", "rejectionCounts", "fallbackReason")},
              expect={"accepted": ["Row with LSE entity", "Row with NYSE entity", "Row with no exchange"],
                      "marketaux": {"acceptedCount": 3, "rejectedCount": 1, "rejectionCounts": {"ENTITY_EXCHANGE_MISMATCH": 1},
                                    "fallbackReason": "HIGHER_PRIORITY_EMPTY"}}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# SEC source: BR-NWS-159/160 (status), 133/134 (event URL, estimated time, confidence), 123..128 and 255 (Form 4 parsing)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def sec_filing(form: str, accession: str, doc: str, filed: str = "2026-10-01", accepted: str | None = "auto") -> dict:
    return {"form": form, "accession": accession, "doc": doc, "filed": filed,
            "accepted": f"{filed}T16:30:00.000Z" if accepted == "auto" else (accepted or "")}


def sec_routes(cik: int, filings: list[dict], docs: dict[str, str] | None = None, name: str = "Test Co Inc") -> dict:
    recent = {"form": [f["form"] for f in filings], "filingDate": [f["filed"] for f in filings],
              "acceptanceDateTime": [f["accepted"] for f in filings], "accessionNumber": [f["accession"] for f in filings],
              "primaryDocument": [f["doc"] for f in filings]}
    routes: dict = {f"https://data.sec.gov/submissions/CIK{cik:010d}.json": {"json": {"name": name, "filings": {"recent": recent}}}}
    for url, body in (docs or {}).items():
        routes[url] = {"body": body}
    return routes


def sec_scn(cik: int, filings: list[dict], docs: dict[str, str] | None = None, **extra) -> dict:
    return {"cik": cik, "info": extra.pop("info", {}), "routes": sec_routes(cik, filings, docs), **extra}


def doc_url(cik: int, accession: str, doc: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/{doc}"


CASES += [
    # Cards 159/160: SEC selected, no CIK / no submissions -> PROVIDER_ERROR (not absence); the empty result is source-limited.
    tool_case("news.sec.no_cik_is_provider_error_and_empty_result_is_source_limited", ["BR-NWS-160", "BR-NWS-159"], "news",
              ["ZZSEC", 10, 14, ["sec"]], {"info": {}},
              select=lambda v: {"sec": v["sourceStatus"]["sec"], "status": v.get("status"), "coverage": coverage_view(v),
                                "warnings": [(w["code"], w["message"]) for w in v["warnings"]]},
              expect={"sec": {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}, "status": "SOURCE_LIMITED_NOT_FOUND",
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["sec", "PROVIDER_ERROR"]], "skipped": [],
                                   "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"},
                      "warnings": [("SOURCE_UNAVAILABLE", "SEC submissions source unavailable.")]}),
    # SEC reachable but nothing in the selected form types -> used, EMPTY_RESULT, NOT_FOUND.
    tool_case("news.sec.reachable_with_no_matching_form_is_empty_result_not_error", ["BR-NWS-159"], "news",
              ["SECA", 10, 14, ["sec"]], sec_scn(910101, [sec_filing("SC 13G", "0000910101-26-000001", "x.htm")]),
              select=lambda v: {"sec": v["sourceStatus"]["sec"], "status": v.get("status"), "sourcesUsed": v["meta"]["sourcesUsed"]},
              expect={"sec": {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}, "status": "NOT_FOUND", "sourcesUsed": ["sec"]}),
]

# Cards 133/134 (+261): the archive URL is built from CIK, accession and primary document; the index page when there is no
# document; an 8-K without an acceptance time takes its filing date at 00:00:00Z (PUBLISHED_AT_ESTIMATED, MEDIUM).
_EVT_ACC, _EVT_ACC2 = "0000910102-26-000010", "0000910102-26-000011"
_EVT_FILINGS = [sec_filing("8-K", _EVT_ACC, "a8k.htm", accepted=None), sec_filing("10-Q", _EVT_ACC2, "", accepted="auto", filed="2026-10-02")]
CASES += [
    tool_case("news.sec.event_url_estimated_time_confidence_and_event_type", ["BR-NWS-133", "BR-NWS-134", "BR-NWS-261"], "sec",
              ["SECB", ["8-K", "10-Q"], 30, 10], sec_scn(910102, _EVT_FILINGS),
              select=lambda v: {"items": [{"title": it["title"], "url": it["url"], "publishedAt": ts_norm(it["publishedAt"]),
                                           "confidence": it["confidence"], "eventType": it["eventType"], "filingType": it["filingType"]}
                                          for it in v["items"]],
                                "warnings": codes(v)},
              expect={"items": [{"title": "8-K filed", "url": "https://www.sec.gov/Archives/edgar/data/910102/000091010226000010/a8k.htm",
                                 "publishedAt": "2026-10-01T00:00:00Z", "confidence": "MEDIUM", "eventType": "regulatory", "filingType": "8-K"},
                                {"title": "10-Q filed",
                                 "url": "https://www.sec.gov/Archives/edgar/data/910102/000091010226000011/0000910102-26-000011-index.htm",
                                 "publishedAt": "2026-10-02T16:30:00Z", "confidence": "HIGH", "eventType": "earnings", "filingType": "10-Q"}],
                      "warnings": ["PUBLISHED_AT_ESTIMATED"]}),
    # Form-to-event-type map: 'DEF14A'/'PRE14A' -> regulatory, S-3/424B -> financing, 4 -> insider, '424B5' -> other.
    tool_case("news.sec.form_type_to_event_type_map", ["BR-NWS-134"], "sec",
              ["SECC", ["S-3", "424B", "424B5", "DEF14A", "10-K"], 30, 10],
              sec_scn(910103, [sec_filing(f, f"0000910103-26-0000{n:02d}", "d.htm", filed=f"2026-10-{n + 1:02d}")
                               for n, f in enumerate(["S-3", "424B", "424B5", "DEF14A", "10-K"], start=1)]),
              select=lambda v: {it["filingType"]: it["eventType"] for it in v["items"]},
              expect={"S-3": "financing", "424B": "financing", "424B5": "other", "DEF14A": "regulatory", "10-K": "earnings"}),
    # An item with no accession/document has no archive URL: confidence LOW. In the news path the Worker also warns
    # SEC_URL_INVALID and Python does not (get_sec_recent_events warns in both).
    tool_case("news.sec.filing_without_url_warns_only_in_the_worker_news_path", ["BR-NWS-133"], "news",
              ["SECD", 10, 30, ["sec"]], sec_scn(910104, [sec_filing("8-K", "", "", accepted=None)]),
              select=lambda v: {"items": [{"url": it["url"], "confidence": it["confidence"]} for it in v["items"]], "warnings": codes(v)},
              mode="pinned", decision="FINDING: the Worker raises SEC_URL_INVALID for an SEC event without an archive URL in the news path; Python does not",
              expect_py={"items": [{"url": None, "confidence": "LOW"}], "warnings": ["PUBLISHED_AT_ESTIMATED"]},
              expect_ts={"items": [{"url": None, "confidence": "LOW"}], "warnings": ["PUBLISHED_AT_ESTIMATED", "SEC_URL_INVALID"]}),
    tool_case("news.sec.filing_without_url_warns_in_both_runtimes_from_get_sec_recent_events", ["BR-NWS-133"], "sec",
              ["SECE", ["8-K"], 30, 10], sec_scn(910105, [sec_filing("8-K", "", "", accepted=None)]),
              select=lambda v: {"items": [{"url": it["url"], "confidence": it["confidence"]} for it in v["items"]], "warnings": codes(v)},
              expect={"items": [{"url": None, "confidence": "LOW"}], "warnings": ["PUBLISHED_AT_ESTIMATED", "SEC_URL_INVALID"]}),
]


# ── Form 4 documents ─────────────────────────────────────────────────────────────────────────

def f4_xml(owner: str = "Doe Jane", flags: str = "", title: str | None = None, rows: str = "") -> str:
    title_xml = f"<officerTitle>{title}</officerTitle>" if title is not None else ""
    return (f'<?xml version="1.0"?><ownershipDocument><reportingOwner><reportingOwnerId><rptOwnerName>{owner}</rptOwnerName></reportingOwnerId>'
            f"<reportingOwnerRelationship>{flags}{title_xml}</reportingOwnerRelationship></reportingOwner>"
            f"<nonDerivativeTable>{rows}</nonDerivativeTable></ownershipDocument>")


def f4_row(code: str | None, shares: str, price: str | None, date: str = "2026-10-01") -> str:
    code_xml = f"<transactionCode>{code}</transactionCode>" if code is not None else ""
    price_xml = f"<transactionPricePerShare><value>{price}</value></transactionPricePerShare>" if price is not None else ""
    return (f"<nonDerivativeTransaction><transactionDate><value>{date}</value></transactionDate>"
            f"<transactionCoding>{code_xml}</transactionCoding>"
            f"<transactionAmounts><transactionShares><value>{shares}</value></transactionShares>{price_xml}</transactionAmounts>"
            f"<ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature></nonDerivativeTransaction>")


def f4_html(rows: list[list[str]], *, owner: str = "Doe Jane", roles: tuple = (("X", "Director"),), derivative: bool = False) -> str:
    """An HTML-rendered Form 4 (not well-formed XML: &nbsp; and unclosed <br>), with the SEC's table captions."""
    role_cells = "".join(f"<tr><td>{x}</td><td>{label}</td></tr>" for x, label in roles)
    caption = "Table II - Derivative Securities Acquired" if derivative else "Table I - Non-Derivative Securities Acquired"
    body = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>" for row in rows)
    return (f"<html><body>&nbsp;<br>Name and Address of Reporting Person <a href='/x'>{owner}</a><br>"
            f"<table><tr><td>Relationship of Reporting Person(s) to Issuer</td></tr>{role_cells}</table>"
            f"Individual or Joint/Group Filing<br>"
            f"<table><tr><td>{caption}</td></tr>{body}</table></body></html>")


def f4_nd_row(code: str, shares: str, price: str, date: str = "10/01/2026", own: str = "D") -> list[str]:
    # date col1, code col3, shares col5, price col7, ownership col9 (10 cells)
    return ["Common Stock", date, "", code, "", shares, "A", price, "10,000", own]


def f4_d_row(code: str, shares: str, price: str, date: str = "10/02/2026", own: str = "D") -> list[str]:
    # price col1, date col2, code col4, shares col6, ownership col14 (15 cells)
    return ["Option", price, date, "", code, "", shares, "", "", "", "", "", "", "", own]


def _form4_view(v: dict) -> list[dict]:
    out = []
    for it in v.get("items") or []:
        tx = it.get("insiderTransaction")
        row: dict[str, Any] = {"title": it["title"], "summary": it.get("summary")}
        if tx:
            row["tx"] = {k: tx.get(k) for k in ("owner", "role", "transactionCode", "transactionLabel", "shares", "price", "value",
                                                "ownershipForm", "transactionDate")}
        out.append(row)
    return out


def form4_case(name: str, ids: list[str], cik: int, docs: list[str], **kw) -> Case:
    """One Form 4 filing per document, newest first; all fetched through get_sec_recent_events(['4'])."""
    filings, doc_map = [], {}
    for n, body in enumerate(docs):
        acc = f"{cik:010d}-26-{n + 1:06d}"
        filings.append(sec_filing("4", acc, f"f4_{n}.xml", filed=f"2026-10-{10 - n:02d}"))
        doc_map[doc_url(cik, acc, f"f4_{n}.xml")] = body
    kw.setdefault("select", _form4_view)
    return tool_case(name, ids, "sec", [f"F4{cik}", ["4"], 30, 10], sec_scn(cik, filings, doc_map), **kw)


def tx(owner="Doe Jane", role=None, code=None, label="Other/Unclassified", shares=None, price=None, value=None, own="D", date="2026-10-01") -> dict:
    return {"owner": owner, "role": role, "transactionCode": code, "transactionLabel": label, "shares": shares, "price": price,
            "value": value, "ownershipForm": own, "transactionDate": date}


CASES += [
    # Cards 125/255: S, 10,000 shares at $152.375 -> $1,523,750.00; title and summary are rewritten from the parsed details.
    form4_case("news.form4.sale_value_title_and_summary", ["BR-NWS-125", "BR-NWS-255", "BR-NWS-123"], 910201,
               [f4_xml(flags="<isOfficer>1</isOfficer>", title="Chief Executive Officer", rows=f4_row("S", "10000", "152.375"))],
               expect=[{"title": "Form 4: Sale by Doe Jane", "summary": "Sale by Doe Jane: 10,000 shares, value $1,523,750.",
                        "tx": tx(role="Chief Executive Officer", code="S", label="Sale", shares=10000, price=152.375, value=1523750)}]),
    # Card 127: no officer title -> the flags give the role; lower-case code 'p' is kept as written but labelled Purchase.
    form4_case("news.form4.role_from_flags_and_lowercase_code_label", ["BR-NWS-127", "BR-NWS-124"], 910202,
               [f4_xml(flags="<isDirector>1</isDirector><isTenPercentOwner>true</isTenPercentOwner>", title="", rows=f4_row("p", "1000", "10"))],
               expect=[{"title": "Form 4: Purchase by Doe Jane", "summary": "Purchase by Doe Jane: 1,000 shares, value $10,000.",
                        "tx": tx(role="director, ten_percent_owner", code="p", label="Purchase", shares=1000, price=10, value=10000)}]),
    form4_case("news.form4.officer_title_wins_over_flags_and_no_flags_gives_no_role", ["BR-NWS-127"], 910203,
               [f4_xml(flags="<isDirector>1</isDirector>", title="Chief Executive Officer", rows=f4_row("S", "5", "2")),
                f4_xml(flags="<isDirector>0</isDirector>", rows=f4_row("S", "5", "2"))],
               expect=[{"title": "Form 4: Sale by Doe Jane", "summary": "Sale by Doe Jane: 5 shares, value $10.",
                        "tx": tx(role="Chief Executive Officer", code="S", label="Sale", shares=5, price=2, value=10)},
                       {"title": "Form 4: Sale by Doe Jane", "summary": "Sale by Doe Jane: 5 shares, value $10.",
                        "tx": tx(role=None, code="S", label="Sale", shares=5, price=2, value=10)}]),
    # Card 124: any code outside P S A D M F G is 'Other/Unclassified'; a missing code leaves transactionCode null.
    form4_case("news.form4.code_to_label_mapping", ["BR-NWS-124"], 910204,
               [f4_xml(rows=f4_row(c, "10", "1")) for c in ("J", "A", "D", "M", "F", "G")] + [f4_xml(rows=f4_row(None, "10", "1"))],
               expect=[{"title": f"Form 4: {label} by Doe Jane", "summary": f"{label} by Doe Jane: 10 shares, value $10.",
                        "tx": tx(code=c, label=label, shares=10, price=1, value=10)}
                       for c, label in (("J", "Other/Unclassified"), ("A", "Grant/Award"), ("D", "Disposition"), ("M", "Option exercise/conversion"),
                                        ("F", "Tax withholding/payment"), ("G", "Gift"), (None, "Other/Unclassified"))]),
    # Cards 125/128: a price of null gives a null value; a price of 0 gives 0, not null; fractional products round to cents.
    form4_case("news.form4.value_is_null_without_price_zero_with_zero_price_and_rounded_to_cents", ["BR-NWS-128", "BR-NWS-125"], 910205,
               [f4_xml(rows=f4_row("A", "500", None)), f4_xml(rows=f4_row("A", "500", "0")), f4_xml(rows=f4_row("S", "1500", "12.345"))],
               expect=[{"title": "Form 4: Grant/Award by Doe Jane", "summary": "Grant/Award by Doe Jane: 500 shares.",
                        "tx": tx(code="A", label="Grant/Award", shares=500, price=None, value=None)},
                       {"title": "Form 4: Grant/Award by Doe Jane", "summary": "Grant/Award by Doe Jane: 500 shares, value $0.",
                        "tx": tx(code="A", label="Grant/Award", shares=500, price=0, value=0)},
                       {"title": "Form 4: Sale by Doe Jane", "summary": "Sale by Doe Jane: 1,500 shares, value $18,518.",
                        "tx": tx(code="S", label="Sale", shares=1500, price=12.345, value=18517.5)}]),
    # Card 128 defect: on an exact half-cent product Python's round() goes to even (0.12), the Worker's Math.round goes up (0.13).
    form4_case("news.form4.half_cent_value_rounds_differently", ["BR-NWS-128"], 910206, [f4_xml(rows=f4_row("S", "1", "0.125"))],
               select=lambda v: [it["insiderTransaction"]["value"] for it in v["items"]],
               mode="pinned", decision="FINDING: Python round() takes 0.125 to 0.12 (half-even); the Worker's Math.round takes it to 0.13",
               expect_py=[0.12], expect_ts=[0.13]),
]

_XHTML_FORM4 = (
    '<html><body><p>Name and Address of Reporting Person <a href="/x">Doe Jane</a></p>'
    "<table><tr><td>Relationship of Reporting Person(s) to Issuer</td></tr><tr><td>X</td><td>Director</td></tr></table>"
    "<p>Individual or Joint/Group Filing</p>"
    "<table><tr><td>Table I - Non-Derivative Securities Acquired</td></tr>"
    "<tr><td>Common</td><td>10/01/2026</td><td></td><td>S</td><td></td><td>1,500</td><td>A</td><td>$12.345</td><td>10,000</td><td>D</td></tr>"
    "</table></body></html>")

CASES += [
    # Card 123: HTML layout: non-derivative date col1, code col3, shares col5, price col7, ownership col9 (10+ cells);
    # derivative price col1 (the conversion/exercise price), date col2, code col4, shares col6, ownership col14 (15+ cells).
    form4_case("news.form4.html_layout_non_derivative_and_derivative_columns", ["BR-NWS-123", "BR-NWS-127"], 910207,
               [f4_html([f4_nd_row("S", "1,500", "$12.345")], roles=(("X", "Director"), ("", "Officer (give title below)"), ("X", "10% Owner"))),
                f4_html([f4_d_row("M", "2,000", "$5.00")], derivative=True)],
               expect=[{"title": "Form 4: Sale by Doe Jane", "summary": "Sale by Doe Jane: 1,500 shares, value $18,518.",
                        "tx": tx(role="director, ten_percent_owner", code="S", label="Sale", shares=1500, price=12.345, value=18517.5)},
                       {"title": "Form 4: Option exercise/conversion by Doe Jane",
                        "summary": "Option exercise/conversion by Doe Jane: 2,000 shares, value $10,000.",
                        "tx": tx(role="director", code="M", label="Option exercise/conversion", shares=2000, price=5, value=10000,
                                 date="2026-10-02")}]),
    # Card 126: accounting negatives, currency, thousands separators and footnote markers; the first row without a number is skipped.
    form4_case("news.form4.numeric_cells_accounting_negative_currency_and_footnote_markers", ["BR-NWS-126", "BR-NWS-123"], 910208,
               [f4_html([f4_nd_row("S", "N/A", "$1"), f4_nd_row("P", "<b>12</b> (1)", "$1,234.50")]),
                f4_html([f4_nd_row("S", "(500)", "$10.00")])],
               expect=[{"title": "Form 4: Purchase by Doe Jane", "summary": "Purchase by Doe Jane: 12 shares, value $14,814.",
                        "tx": tx(role="director", code="P", label="Purchase", shares=12, price=1234.5, value=14814)},
                       {"title": "Form 4: Sale by Doe Jane", "summary": "Sale by Doe Jane: -500 shares, value $-5,000.",
                        "tx": tx(role="director", code="S", label="Sale", shares=-500, price=10, value=-5000)}]),
    # Card 123 defect: a well-formed XHTML primary document with no <nonDerivativeTransaction> node. Python falls back to the
    # HTML parser only when XML parsing raises, so it reports nothing; the Worker falls back whenever the XML node is missing.
    form4_case("news.form4.well_formed_xhtml_document_is_parsed_only_by_the_worker", ["BR-NWS-123"], 910209, [_XHTML_FORM4],
               select=lambda v: {"parsed": [bool(it.get("insiderTransaction")) for it in v["items"]], "warnings": codes(v),
                                 "titles": [it["title"] for it in v["items"]]},
               mode="pinned", decision="FINDING: Python falls back to the HTML Form 4 parser only when XML parsing raises; the Worker whenever no XML transaction node exists",
               expect_py={"parsed": [False], "warnings": ["FORM4_PARSE_UNAVAILABLE"], "titles": ["4 filed"]},
               expect_ts={"parsed": [True], "warnings": [], "titles": ["Form 4: Sale by Doe Jane"]}),
]
