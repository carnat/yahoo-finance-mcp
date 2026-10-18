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

Not driven (no tool path, or the stubs cannot express it)
- A redirect to an off-list host for an IR page (BR-NWS-108, second half): neither stub can set the final URL of a response.
- Failure modes that need a thrown timeout or a Cloudflare subrequest error: WORKER_SUBREQUEST_LIMIT, PROVIDER_TIMEOUT, Finnhub/Marketaux
  TIMEOUT (BR-NWS-106, 155, 163); the stubs only return responses.
- computeSourceStatus / _compute_source_status branches that run without collector diagnostics (BR-NWS-156 "no diagnostics",
  BR-NWS-159 company_ir via a "yahoo finance" warning): every collector supplies diagnostics, so no tool call reaches them.
- The documentUrl / primaryDocumentUrl branch of SEC event URL building (BR-NWS-133): the collectors never pass those fields.
- Host-local interpretation of naive timestamps (BR-NWS-136, 171): the result depends on the machine time zone.

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

import server
from scripts.parity.harness import WORKER, Case
from yfmcp import capital_structure as cs
from yfmcp import url_policy
from yfmcp.clients import edgar
from yfmcp.tools import news as news_tools
from yfmcp.news import shared as news_shared
from yfmcp.news.providers import company_ir_page as ir_page_provider, finnhub as finnhub_provider, marketaux as marketaux_provider, sec as sec_provider, yahoo as yahoo_provider

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
    frozen = datetime.datetime.fromisoformat(now)

    class FrozenDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
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
        frozen = _frozen_datetime_module(now)
        for clock_owner in (news_tools, news_shared, ir_page_provider, finnhub_provider, marketaux_provider):
            stack.enter_context(patch.object(clock_owner, "datetime", frozen))
        stack.enter_context(patch.object(news_tools, "_utc_now_iso", lambda: now))
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

        stack.enter_context(patch.object(yahoo_provider, "_yahoo_search_news", search))
        if scn.get("registry") is not None:
            registry = scn["registry"]
            stack.enter_context(patch.object(ir_page_provider, "_load_company_ir_page_registry", lambda: copy.deepcopy(registry)))
            stack.enter_context(url_policy.registry_override(registry))     # the fetch allow-list holds approved registry hosts
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
    return int(datetime.datetime.fromisoformat(day_iso).timestamp())


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

from urllib.parse import urlencode

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

_NOT_CALLED_EXPECT_2150 = {"marketaux": {"status": "NOT_NEEDED", "rawCount": 0, "acceptedCount": 0, "returnedCount": 0, "rejectedCount": 0,
                                    "rejectionCounts": {}, "attempted": False, "reasonCode": "HIGHER_PRIORITY_EVIDENCE_FOUND",
                                    "fallbackReason": None,
                                    "quotaPolicy": {**MX_QUOTA, "pagesRequested": 0}, "domains": MX_DOMAIN_LIST, "allowedAction": "none"},
                      "items": [{"title": f"Acme story {w}", "sourceType": "yahoo_finance_news", "matchBasis": "TICKER_TOKEN",
                                 "confidence": "MEDIUM", "tickerRelevance": "HIGH"} for w in ("alpha", "beta", "gamma")],
                      "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [],
                                   "skipped": [["marketaux", "NOT_NEEDED"]], "truncated": [], "action": "USE_RETURNED_CONTEXT"}}
# 2.16.0: the same, plus the default sec source failing for lack of an SEC stub (coverage PARTIAL, CHECK_OFFICIAL_RELEASES).
_NOT_CALLED_EXPECT_2160 = {**_NOT_CALLED_EXPECT_2150,
                           "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["sec", "PROVIDER_ERROR"]],
                                        "skipped": [["marketaux", "NOT_NEEDED"]], "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"}}

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
    # 2.16.0 (RNEWS2 §4.1): the defaults now include sec. ACME has no SEC ticker-map stub, so the Worker reports
    # sec PROVIDER_ERROR (as for an explicit sec on a ticker without a CIK) and coverage reads PARTIAL; the Marketaux
    # gate is unchanged. TODO(python-mirror): flip to equal with expect=_NOT_CALLED_EXPECT_2160 once Python reads the
    # registry defaults.
    tool_case("news.marketaux.not_called_when_higher_priority_sources_found_evidence",
              ["BR-NWS-184", "BR-NWS-186", "BR-NWS-190"], "news", ["ACME", 10, 14, None],
              {"env": {**FAKE_FINNHUB, **FAKE_MARKETAUX}, "info": ACME_INFO,
               "news": [yahoo_row(f"Acme story {w}", f"2026-10-1{n}T09:00:00Z") for n, w in ((4, "alpha"), (3, "beta"), (2, "gamma"))],
               "routes": {FINNHUB_NEWS: {"json": []}}},
              select=mx_view, mode="pinned", decision="RNEWS2 2.16.0 (python-mirror port pending)",
              expect_py=_NOT_CALLED_EXPECT_2150, expect_ts=_NOT_CALLED_EXPECT_2160),
    # Card 168: a Finnhub NOT_ELIGIBLE skip (a deliberate policy skip) still counts as limited coverage and spends a Marketaux call.
    tool_case("news.marketaux.finnhub_policy_skip_forces_the_fallback_even_with_yahoo_evidence", ["BR-NWS-168", "BR-NWS-186"], "news",
              ["ERIC-B.ST", 10, 14, None],
              {"env": {**FAKE_FINNHUB, **FAKE_MARKETAUX}, "info": {"shortName": "Ericsson", "longName": "Telefonaktiebolaget LM Ericsson"},
               "news": [yahoo_row("ERIC-B.ST story alpha", "2026-10-14T09:00:00Z"), yahoo_row("ERIC-B.ST story beta", "2026-10-13T09:00:00Z")],
               "routes": {mx_url(symbol="ERIC-B.ST"): {"json": {"data": []}}}},
              select=lambda v: {"marketaux": status_row(v["sourceStatus"]["marketaux"], "status", "attempted", "fallbackReason", "reasonCode", "quotaPolicy"),
                                "finnhub": status_row(v["sourceStatus"]["finnhub"], "status", "attempted"),
                                "warnings": codes(v), "items": [it["title"] for it in v["items"]]},
              # 2.16.0 (RNEWS2 §4.1): the default sec source has no CIK for ERIC-B.ST, so the Worker adds its SOURCE_UNAVAILABLE
              # warning first. TODO(python-mirror): flip to equal with expect = the expect_ts value below.
              mode="pinned", decision="RNEWS2 2.16.0 (python-mirror port pending)",
              expect_py={"marketaux": {"status": "EMPTY_RESULT", "attempted": True, "fallbackReason": "HIGHER_PRIORITY_COVERAGE_LIMITED",
                                       "reasonCode": None, "quotaPolicy": MX_QUOTA},
                         "finnhub": {"status": "NOT_ELIGIBLE", "attempted": False},
                         "warnings": ["SOURCE_NOT_ELIGIBLE"], "items": ["ERIC-B.ST story alpha", "ERIC-B.ST story beta"]},
              expect_ts={"marketaux": {"status": "EMPTY_RESULT", "attempted": True, "fallbackReason": "HIGHER_PRIORITY_COVERAGE_LIMITED",
                                       "reasonCode": None, "quotaPolicy": MX_QUOTA},
                         "finnhub": {"status": "NOT_ELIGIBLE", "attempted": False},
                         "warnings": ["SOURCE_UNAVAILABLE", "SOURCE_NOT_ELIGIBLE"], "items": ["ERIC-B.ST story alpha", "ERIC-B.ST story beta"]}),
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
    # Cards 159/160: SEC selected, no CIK resolved / no submissions -> PROVIDER_ERROR (not absence); the empty result is source-limited.
    # 2.16.0: no SEC ticker-index stub here, so the lookup failed rather than finding no CIK (RNEWS2 §8.7 keeps PROVIDER_ERROR).
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


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# get_company_press_releases: BR-NWS-105 (P0 status / decisionGrade precedence), 109 (P0 EX-99.1 identification),
# 147 (empty status wording), 148 (candidate-only IR page)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def index_page(cik: int, accession: str, exhibits: list[tuple[str, str, str]]) -> str:
    """An EDGAR filing index page: (type, description, document) rows after the primary 8-K document."""
    nodash = accession.replace("-", "")
    rows = [f"<tr><td>1</td><td>8-K</td><td><a href=\"/Archives/edgar/data/{cik}/{nodash}/a8k.htm\">a8k.htm</a></td><td>8-K</td><td>10</td></tr>"]
    for n, (typ, desc, doc) in enumerate(exhibits, start=2):
        link = f'<a href="/Archives/edgar/data/{cik}/{nodash}/{doc}">{doc}</a>' if doc else ""
        rows.append(f"<tr><td>{n}</td><td>{desc}</td><td>{link}</td><td>{typ}</td><td>5</td></tr>")
    return ("<html><body><table><tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
            + "".join(rows) + "</table></body></html>")


def press_scn(cik: int, exhibits: list[tuple[str, str, str]] | None, **extra) -> dict:
    acc = f"{cik:010d}-26-000001"
    filings = [sec_filing("8-K", acc, "a8k.htm")]
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{acc}-index.htm"
    docs = {} if exhibits is None else {index_url: index_page(cik, acc, exhibits)}
    return sec_scn(cik, filings, docs, info=extra.pop("info", ACME_INFO), **extra)


def press_view(v: dict) -> dict:
    return {"status": v.get("status"), "coverageStatus": v.get("coverageStatus"), "decisionGrade": v.get("decisionGrade"),
            "failureMode": v.get("failureMode"), "coverageAction": v["coverage"]["recommendedNextAction"],
            "items": [{"sourceType": it["sourceType"], "title": it["title"], "url": it["url"], "decisionUse": it.get("decisionUse")}
                      for it in v.get("items") or []],
            "warnings": [(w["code"], w.get("filingsSearched")) for w in v.get("warnings") or []]}


def _press_case(name: str, ids: list[str], cik: int, exhibits, **kw) -> Case:
    kw.setdefault("select", press_view)
    return tool_case(name, ids, "press", [f"PR{cik}", 90, 20, ["sec"]], press_scn(cik, exhibits), **kw)


_NOT_FOUND_EXPECT = {"status": "SEC_8K_FOUND_EX99_NOT_FOUND", "coverageStatus": "SEC_8K_FOUND_EX99_NOT_FOUND", "decisionGrade": False,
                     "failureMode": "SEC_8K_FOUND_EX99_NOT_FOUND", "coverageAction": "USE_RETURNED_CONTEXT"}
CASES += [
    # Card 105: one 8-K, no resolvable EX-99.1, no approved IR page -> not decision grade; warning counts the filing.
    _press_case("press.status.8k_without_ex99_is_not_decision_grade", ["BR-NWS-105"], 910301, [("EX-101", "XBRL", "x.xml")],
                expect={**_NOT_FOUND_EXPECT,
                        "items": [{"sourceType": "sec_filing", "title": "8-K filed",
                                   "url": "https://www.sec.gov/Archives/edgar/data/910301/000091030126000001/a8k.htm", "decisionUse": "CHECK_OFFICIAL_RELEASES"}],
                        "warnings": [("SEC_8K_FOUND_EX99_NOT_FOUND", 1)]}),
    # A resolved EX-99.1 rewrites the item (sec_ex99_found, HIGH) and makes the payload decision grade.
    _press_case("press.status.resolved_ex99_1_is_decision_grade", ["BR-NWS-105", "BR-NWS-109"], 910302, [("EX-99.1", "Press Release", "ex99.htm")],
                expect={"status": "SEC_EX99_FOUND", "coverageStatus": "SEC_EX99_RESOLVED", "decisionGrade": True, "failureMode": None,
                        "coverageAction": "USE_OFFICIAL_EVIDENCE",
                        "items": [{"sourceType": "sec_ex99_found", "title": "EX-99.1 exhibit found in 8-K",
                                   "url": "https://www.sec.gov/Archives/edgar/data/910302/000091030226000001/ex99.htm",
                                   "decisionUse": "USE_OFFICIAL_EVIDENCE"}],
                        "warnings": []}),
    # Card 109: a bare 'EX-99' typed exhibit described as a press release resolves in both runtimes. 2.14.0 (R4B1_SPEC
    # section 1): an EX-99 type is accepted only with PRESS/EARNINGS/RESULTS RELEASE wording, in both runtimes.
    _press_case("press.ex99.bare_ex99_with_press_release_description_resolves_in_both", ["BR-NWS-109"], 910303,
                [("EX-99", "Press Release", "ex99.htm")],
                select=lambda v: {"status": v.get("status"), "decisionGrade": v.get("decisionGrade")},
                expect={"status": "SEC_EX99_FOUND", "decisionGrade": True}),
    # Card 109 on the same 8-K, 2.14.0 (R4B1_SPEC section 1): the type must be EX-99.1 / EX-99.01 (or the 99.1 alias),
    # or an EX-99 type with release wording; a file name alone is not accepted.
    _press_case("press.ex99.bare_ex99_with_other_description_is_not_found_in_both", ["BR-NWS-109"], 910304,
                [("EX-99", "Exhibit", "ex99.htm")], select=lambda v: {"status": v.get("status"), "decisionGrade": v.get("decisionGrade")},
                expect={"status": "SEC_8K_FOUND_EX99_NOT_FOUND", "decisionGrade": False}),
    _press_case("press.ex99.zero_padded_type_resolves_in_both", ["BR-NWS-109"], 910305,
                [("EX-99.01", "Press Release", "ex99.htm")], select=lambda v: {"status": v.get("status"), "decisionGrade": v.get("decisionGrade")},
                expect={"status": "SEC_EX99_FOUND", "decisionGrade": True}),
    _press_case("press.ex99.file_name_match_without_a_type_is_not_found_in_both", ["BR-NWS-109"], 910306,
                [("", "Exhibit", "ex99_1.htm")], select=lambda v: {"status": v.get("status"), "decisionGrade": v.get("decisionGrade")},
                expect={"status": "SEC_8K_FOUND_EX99_NOT_FOUND", "decisionGrade": False}),
]


def _empty_press_view(v: dict) -> dict:
    return {"status": v.get("status"), "coverageStatus": v.get("coverageStatus"), "decisionGrade": v.get("decisionGrade"),
            "warnings": codes(v), "candidateSources": v.get("candidateSources"),
            "sec": (v.get("sourceStatus") or {}).get("sec", {}).get("status")}


IQE_CANDIDATE = {
    "ticker": "IQE.L", "issuerName": "IQE plc", "canonicalUrl": "https://www.iqep.com/investors/regulatory-news/", "adapter": "html_article",
    "candidateReason": ("Proposed for 2.3.0 non-US coverage; not fetched during review because the build sandbox cannot reach the host. "
                        "Confirm the page lists the issuer's regulatory announcements, then set status approved with reviewer metadata."),
    "discoveredAt": "2026-09-25T12:00:00Z", "decisionGrade": False}
_IQE_PRESS = {"coverageStatus": "NO_OFFICIAL_RELEASE_SOURCE", "decisionGrade": False, "candidateSources": [IQE_CANDIDATE],
              "company_ir_page": {"status": "CANDIDATE_AVAILABLE", "allowedAction": "review_and_promote", "candidates": [IQE_CANDIDATE]}}

# Card 147: nothing found because SEC is unavailable, yet the status reads as plain absence (NO_YAHOO_PRESS_RELEASE) and the
# coverage status is NO_OFFICIAL_RELEASE_SOURCE; the outage shows only in sourceStatus.sec.
# Finding (Q7, for triage): a provider outage on an empty press-release result is labelled like genuine absence.
CASES += [
    tool_case("press.empty.sec_outage_with_yahoo_selected_reads_as_no_yahoo_press_release", ["BR-NWS-147"], "press",
              ["ZZPR", 90, 20, None], {"info": ACME_INFO, "news": []}, select=_empty_press_view,
              expect={"status": "NO_YAHOO_PRESS_RELEASE", "coverageStatus": "NO_OFFICIAL_RELEASE_SOURCE", "decisionGrade": False,
                      "warnings": ["SOURCE_UNAVAILABLE", "NO_OFFICIAL_RELEASE_SOURCE"], "candidateSources": None, "sec": "PROVIDER_ERROR"}),
    # Precedence of the empty-result status: Yahoo press releases > IR page > company_ir RSS > NOT_FOUND.
    tool_case("press.empty.status_for_unregistered_ir_page_only", ["BR-NWS-147", "BR-NWS-148"], "press",
              ["ZZPR", 90, 20, ["company_ir_page"]], {"info": {}}, select=_empty_press_view,
              expect={"status": "COMPANY_IR_PAGE_NOT_APPROVED", "coverageStatus": "NO_OFFICIAL_RELEASE_SOURCE", "decisionGrade": False,
                      "warnings": ["NO_OFFICIAL_RELEASE_SOURCE"], "candidateSources": None, "sec": None}),
    # Card 148: only an unapproved candidate exists (IQE.L is a shipped candidate). Python says NOT_APPROVED, the Worker CANDIDATE_ONLY;
    # both list the candidate in candidateSources, built from different inputs.
    tool_case("press.empty.candidate_only_ir_page_status_differs", ["BR-NWS-148", "BR-NWS-146", "BR-NWS-101"], "press",
              ["IQE.L", 90, 20, ["company_ir_page"]], {"info": {}},
              select=lambda v: {"status": v.get("status"), "coverageStatus": v.get("coverageStatus"), "decisionGrade": v.get("decisionGrade"),
                                "candidateSources": v.get("candidateSources"),
                                "company_ir_page": status_row(v["sourceStatus"].get("company_ir_page"), "status", "allowedAction", "candidates")},
              mode="pinned", decision="FINDING: with only an unapproved IR-page candidate the empty press-release status is COMPANY_IR_PAGE_NOT_APPROVED in Python and COMPANY_IR_PAGE_CANDIDATE_ONLY in the Worker",
              expect_py={**_IQE_PRESS, "status": "COMPANY_IR_PAGE_NOT_APPROVED"}, expect_ts={**_IQE_PRESS, "status": "COMPANY_IR_PAGE_CANDIDATE_ONLY"}),
    tool_case("press.empty.yahoo_press_release_selection_outranks_the_ir_page_status", ["BR-NWS-148"], "press",
              ["IQE.L", 90, 20, ["company_ir_page", "yahoo_finance_press_releases"]], {"info": {}, "news": []},
              select=lambda v: v.get("status"), expect="NO_YAHOO_PRESS_RELEASE"),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# verify_company_event: BR-NWS-106 (P0 NOT_FOUND upgrade), 163 (failure-mode order, retryable), 164 (status precedence),
# 138 (query term threshold)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

VERIFY_ACTION = ("Inspect sourceStatus and retry with a narrower sources list that excludes failed providers; "
                 "do not treat this result as confirmed absence.")


def verify_view(v: dict) -> dict:
    return {"status": v.get("status"), "failureMode": v.get("failureMode"), "retryable": v.get("retryable"),
            "recommendedAction": v.get("recommendedAction"), "warnings": codes(v), "sourceCoverage": v.get("sourceCoverage"),
            "bestEvidence": [{"sourceType": e["sourceType"], "confidence": e["confidence"], "method": e["queryMatch"]["method"],
                              "relevance": e["relevance"]} for e in v.get("bestEvidence") or []],
            "matched": v["queryPolicy"]["matchedEvidenceCount"]}


def _limited(mode: str | None, retryable: bool, **over) -> dict:
    out = {"status": "SOURCE_LIMITED_NOT_FOUND", "failureMode": mode, "retryable": retryable,
           "recommendedAction": VERIFY_ACTION, "warnings": ["SOURCE_UNAVAILABLE", "SOURCE_LIMITED_NOT_FOUND"],
           "sourceCoverage": "PARTIAL", "bestEvidence": [], "matched": 0}
    out.update(over)
    return out


def _verify_case(name: str, ids: list[str], sources: list[str], env: dict, routes: dict, expect: dict, ticker: str = "ACME", **scn) -> Case:
    return tool_case(name, ids, "verify", [ticker, "buyback announcement", "", "", sources],
                     {"env": env, "info": scn.pop("info", ACME_INFO), "news": scn.pop("news", []), "routes": routes, **scn},
                     select=verify_view, expect=expect)


CASES += [
    # Card 106 (P0): nothing matched and Finnhub rate-limited -> not "NOT_FOUND" but SOURCE_LIMITED_NOT_FOUND, retryable.
    _verify_case("verify.not_found_with_rate_limited_source_is_source_limited_and_retryable", ["BR-NWS-106", "BR-NWS-163"], ["finnhub"],
                 FAKE_FINNHUB, {FINNHUB_NEWS: {"status": 429, "body": "{}"}}, _limited("RATE_LIMITED", True)),
    # Card 163: RATE_LIMITED outranks UNCONFIGURED; UNCONFIGURED alone is not retryable.
    _verify_case("verify.rate_limited_outranks_unconfigured", ["BR-NWS-163"], ["finnhub", "marketaux"], FAKE_FINNHUB,
                 {FINNHUB_NEWS: {"status": 429, "body": "{}"}},
                 _limited("RATE_LIMITED", True, warnings=["SOURCE_UNAVAILABLE", "SOURCE_UNAVAILABLE", "SOURCE_LIMITED_NOT_FOUND"])),
    _verify_case("verify.unconfigured_alone_is_not_retryable", ["BR-NWS-163", "BR-NWS-106"], ["finnhub"], {}, {}, _limited("SOURCE_UNCONFIGURED", False)),
    _verify_case("verify.not_eligible_source_gives_source_not_eligible", ["BR-NWS-163"], ["finnhub"], FAKE_FINNHUB, {},
                 _limited("SOURCE_NOT_ELIGIBLE", False, warnings=["SOURCE_NOT_ELIGIBLE", "SOURCE_LIMITED_NOT_FOUND"]), ticker="ERIC-B.ST"),
    _verify_case("verify.provider_error_is_not_retryable", ["BR-NWS-163"], ["finnhub"], FAKE_FINNHUB,
                 {FINNHUB_NEWS: {"status": 500, "body": "{}"}}, _limited("PROVIDER_ERROR", False)),
    _verify_case("verify.provider_changed_is_not_retryable", ["BR-NWS-163"], ["finnhub"], FAKE_FINNHUB,
                 {FINNHUB_NEWS: {"json": {"oops": 1}}}, _limited("PROVIDER_CHANGED", False)),
    _verify_case("verify.identity_unavailable_gives_yahoo_identity_failure_mode", ["BR-NWS-163"], ["yahoo_finance_news"], {}, {},
                 _limited("YAHOO_IDENTITY_UNAVAILABLE", False, warnings=["SOURCE_IDENTITY_UNAVAILABLE", "SOURCE_LIMITED_NOT_FOUND"]), info={}),
    # Finding (Q7, for triage): AUTH_ERROR is a real outage but yields no failure mode, so NOT_FOUND is not upgraded.
    _verify_case("verify.auth_error_does_not_upgrade_not_found", ["BR-NWS-163", "BR-NWS-106"], ["finnhub"], FAKE_FINNHUB,
                 {FINNHUB_NEWS: {"status": 401, "body": "{}"}},
                 {"status": "NOT_FOUND", "failureMode": None, "retryable": False, "recommendedAction": None, "warnings": ["SOURCE_UNAVAILABLE"],
                  "sourceCoverage": "PARTIAL", "bestEvidence": [], "matched": 0}),
    # Card 138: the term threshold. 'announced' is a stop word; 4 meaningful terms need ceil(4 * 0.67) = 3 of them.
    tool_case("verify.query_policy.four_meaningful_terms_require_three_matches", ["BR-NWS-138"], "verify",
              ["ACME", "BlueBird satellite launch contract announced", "", "", []],
              {"info": ACME_INFO}, select=lambda v: v["queryPolicy"],
              expect={"meaningfulTerms": ["bluebird", "satellite", "launch", "contract"], "requiredTermMatches": 3, "matchedEvidenceCount": 0}),
    tool_case("verify.query_policy.up_to_two_terms_must_all_match", ["BR-NWS-138"], "verify", ["ACME", "dividend cut", "", "", []],
              {"info": ACME_INFO}, select=lambda v: v["queryPolicy"],
              expect={"meaningfulTerms": ["dividend", "cut"], "requiredTermMatches": 2, "matchedEvidenceCount": 0}),
    tool_case("verify.query_policy.generic_query_has_no_terms_and_warns", ["BR-NWS-138"], "verify", ["ACME", "announced results", "", "", []],
              {"info": ACME_INFO}, select=lambda v: {"policy": v["queryPolicy"], "warnings": codes(v)},
              expect={"policy": {"meaningfulTerms": [], "requiredTermMatches": 0, "matchedEvidenceCount": 0},
                      "warnings": ["EVENT_QUERY_TOO_GENERIC"]}),
]

# Card 164: status precedence CONFLICTING > CONFIRMED (official, in range) > PARTIAL (matched in range) > STALE > NOT_FOUND.
_PR_ROW = yahoo_row("Acme announces supply contract", "2026-10-05T09:00:00Z", publisher="Business Wire")
_NEWS_ROW = yahoo_row("Acme supply contract talks", "2026-10-05T09:00:00Z")


def _status_case(name: str, ids: list[str], news: list[dict], sources: list[str], start: str, end: str, expect: dict,
                 query: str = "supply contract", **kw) -> Case:
    if expect:
        kw["expect"] = expect
    return tool_case(name, ids, "verify", ["ACME", query, start, end, sources], {"info": ACME_INFO, "news": news},
                     select=lambda v: {"status": v["status"], "matched": v["queryPolicy"]["matchedEvidenceCount"],
                                       "best": [(e["sourceType"], e["queryMatch"]["method"]) for e in v["bestEvidence"]]}, **kw)


CASES += [
    _status_case("verify.status.official_yahoo_press_release_in_range_is_confirmed", ["BR-NWS-164"], [_PR_ROW], ["yahoo_finance_press_releases"],
                 "2026-10-01", "", {"status": "CONFIRMED", "matched": 1, "best": [("yahoo_finance_press_releases", "PHRASE")]}),
    _status_case("verify.status.contextual_news_only_is_partial", ["BR-NWS-164"], [_NEWS_ROW], ["yahoo_finance_news"], "2026-10-01", "",
                 {"status": "PARTIAL", "matched": 1, "best": [("yahoo_finance_news", "PHRASE")]}),
    # Card 164: STALE needs a match outside the requested range that is also older than 90 days. The Worker passes the range into
    # the collection window, so such an item is never collected and its status stays NOT_FOUND (STALE is unreachable there).
    _status_case("verify.status.match_outside_the_date_range_and_older_than_90_days_is_stale_only_in_python", ["BR-NWS-164"],
                 [yahoo_row("Acme supply contract talks", "2026-03-01T09:00:00Z")], ["yahoo_finance_news"], "2026-10-01", "",
                 {}, mode="pinned", decision="FINDING: verify_company_event can return STALE in Python but not in the Worker, which applies start_date/end_date while collecting",
                 expect_py={"status": "STALE", "matched": 1, "best": [("yahoo_finance_news", "PHRASE")]},
                 expect_ts={"status": "NOT_FOUND", "matched": 0, "best": []}),
    _status_case("verify.status.no_match_is_not_found", ["BR-NWS-164"], [yahoo_row("Acme opens a plant", "2026-10-05T09:00:00Z")],
                 ["yahoo_finance_news"], "2026-10-01", "", {"status": "NOT_FOUND", "matched": 0, "best": []}),
    # Finding (Q7, for triage): without a date range every match counts as in range, so a 200-day-old official release is
    # CONFIRMED and STALE cannot occur.
    _status_case("verify.status.old_official_release_without_dates_is_still_confirmed", ["BR-NWS-164"],
                 [yahoo_row("Acme announces supply contract", "2026-03-28T09:00:00Z", publisher="Business Wire")], ["yahoo_finance_press_releases"], "", "",
                 {"status": "CONFIRMED", "matched": 1, "best": [("yahoo_finance_press_releases", "PHRASE")]}),
    # Card 164: bestEvidence.confidence. The Worker derives it from the source type (contextual Yahoo news is always LOW); Python
    # reports the item's own confidence (MEDIUM when it has a URL).
    tool_case("verify.best_evidence_confidence_for_contextual_news_differs", ["BR-NWS-164"], "verify",
              ["ACME", "supply contract", "2026-10-01", "", ["yahoo_finance_news"]], {"info": ACME_INFO, "news": [_NEWS_ROW]},
              select=lambda v: [(e["sourceType"], e["confidence"]) for e in v["bestEvidence"]],
              mode="pinned", decision="FINDING: verify bestEvidence confidence is the item's own in Python and source-type based in the Worker",
              expect_py=[("yahoo_finance_news", "MEDIUM")], expect_ts=[("yahoo_finance_news", "LOW")]),
    tool_case("verify.best_evidence_confidence_for_official_press_release_agrees", ["BR-NWS-164"], "verify",
              ["ACME", "supply contract", "2026-10-01", "", ["yahoo_finance_press_releases"]], {"info": ACME_INFO, "news": [_PR_ROW]},
              select=lambda v: [(e["sourceType"], e["confidence"]) for e in v["bestEvidence"]],
              expect=[("yahoo_finance_press_releases", "MEDIUM")]),
    # Finding (Q7, for triage, card 138): an exact PHRASE match is accepted even when the query is all stop words, so a too-generic
    # query can still produce a match beside the EVENT_QUERY_TOO_GENERIC warning.
    tool_case("verify.query_policy.generic_phrase_still_matches_despite_the_warning", ["BR-NWS-138"], "verify",
              ["ACME", "announced results", "", "", ["yahoo_finance_news"]],
              {"info": ACME_INFO, "news": [yahoo_row("Acme announced results", "2026-10-05T09:00:00Z")]},
              select=lambda v: {"status": v["status"], "warnings": codes(v), "policy": v["queryPolicy"],
                                "methods": [e["queryMatch"]["method"] for e in v["bestEvidence"]]},
              expect={"status": "PARTIAL", "warnings": ["EVENT_QUERY_TOO_GENERIC"],
                      "policy": {"meaningfulTerms": [], "requiredTermMatches": 0, "matchedEvidenceCount": 1}, "methods": ["PHRASE"]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# company_ir_page registry: BR-NWS-101, 102 (P0 lifecycle), 108 (P0 allow-list), 113 (record extraction), 142 (allowedAction),
# 146 (candidate), 149 (empty-result status), 171 (revalidation deadline), 263 (shipped entries)
# The test registry below replaces the shipped file in BOTH runtimes (see the module docstring); the shipped candidate
# entries (IQE.L) are exercised as they are.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def ir_entry(ticker: str, status: str = "approved", adapter: str = "html_article", revalidate: str | None = None,
             canonical: str | None = None) -> dict:
    host = f"ir.{ticker.lower()}.example"
    return {"ticker": ticker, "issuerName": f"{ticker} Corp", "status": status, "adapter": adapter,
            "canonicalUrl": canonical or f"https://{host}/investors/news/", "allowedHosts": [host], "allowedPathPrefixes": ["/investors/"],
            "candidateReason": "test registry entry", "discoveredAt": "2026-09-01T00:00:00Z", "reviewedBy": "reviewer",
            "reviewedAt": "2026-09-02T00:00:00Z", "revalidateAfter": revalidate}


def ir_page_url(ticker: str) -> str:
    return f"https://ir.{ticker.lower()}.example/investors/news/"


IR_REGISTRY = {"schemaVersion": "2026-07-10", "registryVersion": "2026-10-15.0900", "sources": [
    ir_entry("IRDIS", "disabled"),
    ir_entry("IRCAND", "candidate"),
    ir_entry("IRSTALE", revalidate="2026-10-14T00:00:00Z"),
    ir_entry("IREDGE", revalidate="2026-10-15T12:00:00Z"),
    ir_entry("IRGARB", revalidate="not-a-date"),
    ir_entry("IRFUT", revalidate="2027-01-01T00:00:00Z"),
    ir_entry("IROKH"),
    ir_entry("IROKJ", adapter="structured"),
    ir_entry("IRREJ", "rejected"),
    ir_entry("IRBADC", canonical="https://evil.example/investors/news/"),
    ir_entry("IRFAIL"),
    ir_entry("IR50"),
    ir_entry("IRLINKS"),
    ir_entry("IRPR"),
]}


def _article(title: str, href: str, when: str | None, heading: bool = True) -> str:
    head = f"<h2>{title}</h2><a href=\"{href}\">Read more</a>" if heading else f"<a href=\"{href}\">{title}</a>"
    time_tag = f'<time datetime="{when}">{when}</time>' if when else ""
    return f"<article>{head}{time_tag}</article>"


_OKH_PAGE = "<html><body>" + "".join([
    _article("IROKH reports quarterly earnings", "/investors/news/q3", "2026-10-12"),
    _article("IROKH announces new product launch", "/investors/news/product", "2026-10-10T09:00:00Z", heading=False),
    _article("IROKH old release from August", "/investors/news/old", "2026-08-01"),
    _article("IROKH release without a date", "/investors/news/nodate", None),
    _article("Tiny", "/investors/news/tiny", "2026-10-11"),
]) + "</body></html>"
_HTML = {"headers": {"content-type": "text/html; charset=utf-8"}}
_JSON = {"headers": {"content-type": "application/json"}}


def ir_routes(ticker: str, body: str, kind: dict = _HTML) -> dict:
    return {ir_page_url(ticker): {**kind, "body": body}}


def ir_news_case(name: str, ids: list[str], ticker: str, routes: dict, select, tool: str = "news", args: list | None = None, **kw) -> Case:
    return tool_case(name, ids, tool, args or [ticker, 100, 14, ["company_ir_page"]],
                     {"registry": IR_REGISTRY, "info": {}, "routes": routes}, select=select, **kw)


IR_ITEM_FIELDS = ("title", "url", "sourceType", "approvalStatus", "decisionUse", "evidenceClass", "urlProvenance", "eventType",
                  "confidence", "tickerRelevance", "issuer", "publishedAt")


def ir_items(v: dict) -> list[dict]:
    return item_rows(v, IR_ITEM_FIELDS)


def ir_row(v: dict, *fields: str) -> dict | None:
    return status_row((v.get("sourceStatus") or {}).get("company_ir_page"), *fields)


def ir_item(ticker: str, title: str, path: str, when: str, event: str) -> dict:
    return {"title": title, "url": f"https://ir.{ticker.lower()}.example{path}", "sourceType": "company_ir_page", "approvalStatus": "approved",
            "decisionUse": "USE_OFFICIAL_EVIDENCE", "evidenceClass": "APPROVED_IR_PAGE", "urlProvenance": "OFFICIAL", "eventType": event,
            "confidence": "HIGH", "tickerRelevance": "HIGH", "issuer": f"{ticker} Corp", "publishedAt": when}


CASES += [
    # Cards 101/102/113: approved + fresh -> fetched; HTML block title from the first h1-h4 else the first anchor; the date from
    # <time datetime>; titles under 6 characters, undated items and items outside the window are dropped.
    ir_news_case("ir_page.approved_html_page_is_fetched_and_parsed", ["BR-NWS-101", "BR-NWS-102", "BR-NWS-113"], "IROKH",
                 ir_routes("IROKH", _OKH_PAGE),
                 lambda v: {"items": ir_items(v), "row": ir_row(v, "status", "allowedAction", "rawCount", "filteredCount"), "status": v.get("status"),
                            "warnings": codes(v), "coverage": coverage_view(v)},
                 expect={"items": [ir_item("IROKH", "IROKH reports quarterly earnings", "/investors/news/q3", "2026-10-12T00:00:00Z", "earnings"),
                                   ir_item("IROKH", "IROKH announces new product launch", "/investors/news/product", "2026-10-10T09:00:00Z", "product")],
                         "row": {"status": "APPROVED_SOURCE_OK", "allowedAction": "fetch_configured_source", "rawCount": 2, "filteredCount": 2},
                         "status": None, "warnings": [],
                         "coverage": {"sourceCoverage": "FULL", "state": "FULL", "failed": [], "skipped": [], "truncated": [], "action": "USE_RETURNED_CONTEXT"}}),
    # Card 113: JSON list keys are tried in order (an empty 'items' falls through to 'releases'); title keys title/headline/name;
    # date keys publishedAt/datePublished/...; a record without a title is skipped.
    ir_news_case("ir_page.structured_json_page_uses_first_non_empty_list_and_key_aliases", ["BR-NWS-113"], "IROKJ",
                 ir_routes("IROKJ", json.dumps({"items": [], "releases": [
                     {"headline": "IROKJ announces financing", "url": "https://ir.irokj.example/investors/news/fin", "datePublished": "2026-10-13T10:00:00Z"},
                     {"name": "IROKJ contract award", "link": "/investors/news/award", "date": "2026-10-11"},
                     {"url": "https://ir.irokj.example/investors/news/untitled", "date": "2026-10-11"},
                     {"title": "IROKJ off host item", "url": "https://elsewhere.example/investors/news/x", "date": "2026-10-11"}]}), _JSON),
                 lambda v: {"items": [(it["title"], it["url"], it["publishedAt"], it["eventType"]) for it in ir_items(v)],
                            "row": ir_row(v, "status", "rawCount")},
                 expect={"items": [("IROKJ announces financing", "https://ir.irokj.example/investors/news/fin", "2026-10-13T10:00:00Z", "financing"),
                                   ("IROKJ contract award", "https://ir.irokj.example/investors/news/award", "2026-10-11T00:00:00Z", "contract")],
                         "row": {"status": "APPROVED_SOURCE_OK", "rawCount": 2}}),
    # Card 108 (P0): item URLs must be https on an allowed host and path prefix; a relative link resolves against the page.
    ir_news_case("ir_page.item_urls_outside_the_allow_list_are_dropped", ["BR-NWS-108"], "IRLINKS",
                 ir_routes("IRLINKS", "<html><body>" + "".join([
                     _article("IRLINKS good absolute link", "https://ir.irlinks.example/investors/news/a", "2026-10-12"),
                     _article("IRLINKS good relative link", "b", "2026-10-11"),
                     _article("IRLINKS insecure http link", "http://ir.irlinks.example/investors/news/c", "2026-10-10"),
                     _article("IRLINKS other host link", "https://evil.example/investors/news/d", "2026-10-09"),
                     _article("IRLINKS path outside prefix", "https://ir.irlinks.example/careers/e", "2026-10-08"),
                     _article("IRLINKS www host variant", "https://www.ir.irlinks.example/investors/news/f", "2026-10-07")]) + "</body></html>"),
                 lambda v: [(it["title"], it["url"]) for it in ir_items(v)],
                 expect=[("IRLINKS good absolute link", "https://ir.irlinks.example/investors/news/a"),
                         ("IRLINKS good relative link", "https://ir.irlinks.example/investors/news/b"),
                         ("IRLINKS www host variant", "https://www.ir.irlinks.example/investors/news/f")]),
    # A registry canonical URL outside its own allow-list fails the whole source with COMPANY_IR_PAGE_VALIDATION_FAILED (same text in
    # both runtimes). Only the allowedAction wording differs (see the FINDING case below).
    ir_news_case("ir_page.canonical_url_outside_allow_list_fails_validation", ["BR-NWS-108", "BR-NWS-101"], "IRBADC", {},
                 lambda v: {"warnings": [(w["code"], w["message"]) for w in v["warnings"]], "status": v.get("status"),
                            "row": ir_row(v, "status", "rawCount")},
                 expect={"warnings": [("COMPANY_IR_PAGE_VALIDATION_FAILED",
                                       "Approved company IR page validation failed: registry canonical URL is outside its allow-list")],
                         "status": "SOURCE_LIMITED_NOT_FOUND", "row": {"status": "SOURCE_VALIDATION_FAILED", "rawCount": 0}}),
    ir_news_case("ir_page.fetch_failure_fails_validation", ["BR-NWS-101", "BR-NWS-102"], "IRFAIL", {ir_page_url("IRFAIL"): {"status": 500, "body": "oops"}},
                 lambda v: {"codes": codes(v), "status": v.get("status"), "row": ir_row(v, "status", "rawCount")},
                 expect={"codes": ["COMPANY_IR_PAGE_VALIDATION_FAILED"], "status": "SOURCE_LIMITED_NOT_FOUND",
                         "row": {"status": "SOURCE_VALIDATION_FAILED", "rawCount": 0}}),
]

# Card 113: only the first 50 qualifying <li> blocks are examined when a page has no <article> tags.
_LI_PAGE = ("<html><body><ul>" + "".join(f"<li><a href=\"/investors/other/{n}\">Contact page {n}</a></li>" for n in range(20))
            + "".join(f'<li><a href="/investors/news/{n}">IR50 announces item {n:02d}</a> <time datetime="2026-10-{2 + n % 12:02d}">d</time></li>'
                      for n in range(1, 61)) + "</ul></body></html>")
CASES += [
    ir_news_case("ir_page.list_items_are_capped_at_the_first_50_qualifying_blocks", ["BR-NWS-113"], "IR50", ir_routes("IR50", _LI_PAGE),
                 lambda v: {"count": len(v["items"]), "has_50": any(it["title"].endswith("item 50") for it in v["items"]),
                            "has_51": any(it["title"].endswith("item 51") for it in v["items"])},
                 expect={"count": 50, "has_50": True, "has_51": False}),
    # 2.10.0 (Q2a, planner decision): only an approved entry is fetched. An entry with any other status (here 'rejected') is not
    # fetched in either runtime: no items, and the same COMPANY_IR_PAGE_VALIDATION_FAILED warning (the page host is not on the
    # fetch allow-list, which holds the hosts of approved entries only). Until then both fetched it and stamped it approved.
    ir_news_case("ir_page.non_approved_status_is_not_fetched", ["BR-NWS-101", "BR-NWS-102"], "IRREJ",
                 ir_routes("IRREJ", _article("IRREJ reports quarterly earnings", "/investors/news/q3", "2026-10-12")),
                 lambda v: {"items": [(it["title"], it["approvalStatus"], it["decisionUse"]) for it in ir_items(v)],
                            "warnings": [(w["code"], w["message"]) for w in v["warnings"]], "row": ir_row(v, "status", "rawCount")},
                 expect={"items": [],
                         "warnings": [("COMPANY_IR_PAGE_VALIDATION_FAILED", "Approved company IR page validation failed: URL host not allowed")],
                         "row": {"status": "SOURCE_VALIDATION_FAILED", "rawCount": 0}}),
    # Card 171: the revalidation deadline. An absent deadline (IROKH) and a future one (IRFUT) are fresh; so is a deadline equal to
    # now; a past one and an unparseable one are stale and the source is skipped.
    *[ir_news_case(f"ir_page.revalidation_deadline_{label}", ["BR-NWS-171", "BR-NWS-102"], ticker,
                   ir_routes(ticker, _article(f"{ticker} reports quarterly earnings", "/investors/news/q3", "2026-10-12")),
                   lambda v: {"row": ir_row(v, "status"), "codes": codes(v), "items": len(v["items"])}, expect=expect)
      for label, ticker, expect in (
          ("in_the_future_is_fresh", "IRFUT", {"row": {"status": "APPROVED_SOURCE_OK"}, "codes": [], "items": 1}),
          ("equal_to_now_is_fresh", "IREDGE", {"row": {"status": "APPROVED_SOURCE_OK"}, "codes": [], "items": 1}),
          ("in_the_past_is_stale", "IRSTALE", {"row": {"status": "REVALIDATION_DUE"}, "codes": ["COMPANY_IR_PAGE_REVALIDATION_DUE"], "items": 0}),
          ("unparseable_is_stale", "IRGARB", {"row": {"status": "REVALIDATION_DUE"}, "codes": ["COMPANY_IR_PAGE_REVALIDATION_DUE"], "items": 0}))],
]

# Cards 101/102/146: a shipped candidate entry is never fetched (no route exists for its page); the source reports
# CANDIDATE_AVAILABLE / review_and_promote and lists the candidate with decisionGrade false. Both runtimes agree on that.
CASES += [
    tool_case("ir_page.candidate_entry_is_not_fetched_and_reports_review_and_promote", ["BR-NWS-101", "BR-NWS-102", "BR-NWS-146", "BR-NWS-263"], "news",
              ["IQE.L", 10, 14, ["company_ir_page"]], {"info": {}},
              select=lambda v: {"row": ir_row(v, "status", "allowedAction", "candidates", "rawCount", "filteredCount"), "items": v["items"],
                                "coverage": coverage_view(v)},
              expect={"row": {"status": "CANDIDATE_AVAILABLE", "allowedAction": "review_and_promote", "candidates": [IQE_CANDIDATE],
                              "rawCount": 0, "filteredCount": 0},
                      "items": [],
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["company_ir_page", "CANDIDATE_AVAILABLE"]],
                                   "skipped": [], "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"}}),
    # Card 149: Python raises an info warning COMPANY_IR_PAGE_CANDIDATE_AVAILABLE, which makes the empty result source-limited;
    # the Worker raises no warning for a candidate, so nothing marks the source as used or limited (PROVIDER_ERROR).
    tool_case("ir_page.candidate_empty_result_status_differs", ["BR-NWS-149", "BR-NWS-144"], "news",
              ["IQE.L", 10, 14, ["sec", "company_ir_page"]], {"info": {}},
              select=lambda v: {"status": v.get("status"), "warnings": codes(v)},
              mode="pinned", decision="FINDING: an unreviewed IR-page candidate makes the empty collection status SOURCE_LIMITED_NOT_FOUND in Python (info warning) and not in the Worker",
              expect_py={"status": "SOURCE_LIMITED_NOT_FOUND", "warnings": ["SOURCE_UNAVAILABLE", "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE"]},
              expect_ts={"status": "SOURCE_LIMITED_NOT_FOUND", "warnings": ["SOURCE_UNAVAILABLE"]}),
    tool_case("ir_page.candidate_alone_empty_result_status_differs", ["BR-NWS-149"], "news",
              ["IQE.L", 10, 14, ["company_ir_page"]], {"info": {}},
              select=lambda v: {"status": v.get("status"), "warnings": codes(v)},
              mode="pinned", decision="FINDING: an unreviewed IR-page candidate makes the empty collection status SOURCE_LIMITED_NOT_FOUND in Python (info warning) and PROVIDER_ERROR in the Worker",
              expect_py={"status": "SOURCE_LIMITED_NOT_FOUND", "warnings": ["COMPANY_IR_PAGE_CANDIDATE_AVAILABLE"]},
              expect_ts={"status": "PROVIDER_ERROR", "warnings": []}),
    # Card 142 / 149: a disabled entry. Python warns (info) and says registry_review_required; the Worker is silent and says 'none'.
    ir_news_case("ir_page.disabled_entry_status_and_allowed_action_differ", ["BR-NWS-142", "BR-NWS-149", "BR-NWS-102", "BR-NWS-101"], "IRDIS", {},
                 lambda v: {"row": ir_row(v, "status", "allowedAction"), "status": v.get("status"), "warnings": codes(v), "items": len(v["items"])},
                 mode="pinned", decision="FINDING: DISABLED IR page entry: allowedAction registry_review_required + info warning + SOURCE_LIMITED_NOT_FOUND in Python; none + no warning + PROVIDER_ERROR in the Worker",
                 expect_py={"row": {"status": "DISABLED", "allowedAction": "registry_review_required"}, "status": "SOURCE_LIMITED_NOT_FOUND",
                            "warnings": ["COMPANY_IR_PAGE_DISABLED"], "items": 0},
                 expect_ts={"row": {"status": "DISABLED", "allowedAction": "none"}, "status": "PROVIDER_ERROR", "warnings": [], "items": 0}),
    ir_news_case("ir_page.stale_entry_allowed_action_differs", ["BR-NWS-142", "BR-NWS-171"], "IRSTALE", {},
                 lambda v: {"row": ir_row(v, "status", "allowedAction"), "status": v.get("status"), "warnings": codes(v)},
                 mode="pinned", decision="FINDING: REVALIDATION_DUE allowedAction is registry_review_required in Python and revalidate in the Worker",
                 expect_py={"row": {"status": "REVALIDATION_DUE", "allowedAction": "registry_review_required"}, "status": "SOURCE_LIMITED_NOT_FOUND",
                            "warnings": ["COMPANY_IR_PAGE_REVALIDATION_DUE"]},
                 expect_ts={"row": {"status": "REVALIDATION_DUE", "allowedAction": "revalidate"}, "status": "SOURCE_LIMITED_NOT_FOUND",
                            "warnings": ["COMPANY_IR_PAGE_REVALIDATION_DUE"]}),
    ir_news_case("ir_page.validation_failure_allowed_action_differs", ["BR-NWS-142", "BR-NWS-101"], "IRBADC", {},
                 lambda v: {"row": ir_row(v, "status", "allowedAction")},
                 mode="pinned", decision="FINDING: SOURCE_VALIDATION_FAILED allowedAction is registry_review_required in Python and review_registry_entry in the Worker",
                 expect_py={"row": {"status": "SOURCE_VALIDATION_FAILED", "allowedAction": "registry_review_required"}},
                 expect_ts={"row": {"status": "SOURCE_VALIDATION_FAILED", "allowedAction": "review_registry_entry"}}),
    # Card 142: an approved source that returned nothing in the window is EMPTY_RESULT with registry_review_required in Python; the
    # Worker's diagnostic keeps fetch_configured_source (its status flips to EMPTY_RESULT but not the action).
    ir_news_case("ir_page.approved_page_with_nothing_in_window_allowed_action_differs", ["BR-NWS-142", "BR-NWS-146"], "IRPR",
                 ir_routes("IRPR", _article("IRPR old release from August", "/investors/news/old", "2026-08-01")),
                 lambda v: {"row": ir_row(v, "status", "allowedAction", "rawCount"), "status": v.get("status")},
                 mode="pinned", decision="FINDING: an approved IR page with an empty window has allowedAction registry_review_required in Python and fetch_configured_source in the Worker",
                 expect_py={"row": {"status": "EMPTY_RESULT", "allowedAction": "registry_review_required", "rawCount": 0}, "status": "NOT_FOUND"},
                 expect_ts={"row": {"status": "EMPTY_RESULT", "allowedAction": "fetch_configured_source", "rawCount": 0}, "status": "NOT_FOUND"}),
    # Registry metadata on the status row: Python copies it from IR-page warnings only (so a clean approved fetch has none).
    ir_news_case("ir_page.approved_fetch_status_row_registry_metadata_differs", ["BR-NWS-101"], "IROKH", ir_routes("IROKH", _OKH_PAGE),
                 lambda v: ir_row(v, "registryVersion", "canonicalUrl", "approvalStatus"),
                 mode="pinned", decision="FINDING: the Worker status row always carries registryVersion/canonicalUrl/approvalStatus; Python only when an IR-page warning exists",
                 expect_py={"registryVersion": None, "canonicalUrl": None, "approvalStatus": None},
                 expect_ts={"registryVersion": "2026-10-15.0900", "canonicalUrl": "https://ir.irokh.example/investors/news/", "approvalStatus": "approved"}),
    # get_company_press_releases with an approved, fresh IR page: decision grade without any SEC exhibit (cards 101, 105).
    ir_news_case("ir_page.press_release_from_approved_page_is_decision_grade", ["BR-NWS-101", "BR-NWS-105"], "IRPR",
                 ir_routes("IRPR", _article("IRPR reports quarterly earnings", "/investors/news/q3", "2026-10-12")),
                 lambda v: {"status": v.get("status"), "coverageStatus": v.get("coverageStatus"), "decisionGrade": v.get("decisionGrade"),
                            "failureMode": v.get("failureMode"), "action": v["coverage"]["recommendedNextAction"],
                            "evidence": [{k: e.get(k) for k in ("title", "url", "adapter", "basis")} for e in v.get("irPageEvidence") or []]},
                 tool="press", args=["IRPR", 90, 20, ["company_ir_page"]],
                 expect={"status": "APPROVED_IR_PAGE_FOUND", "coverageStatus": "APPROVED_IR_PAGE_RESOLVED", "decisionGrade": True, "failureMode": None,
                         "action": "USE_OFFICIAL_EVIDENCE",
                         "evidence": [{"title": "IRPR reports quarterly earnings", "url": "https://ir.irpr.example/investors/news/q3",
                                       "adapter": "html_article", "basis": "approved_company_ir_page_registry"}]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# company_ir source: BR-NWS-103, 107 (P0: what "company_ir" means), 145, 165, 167 (status precedence), 170, 183 (discovery)
# Python serves company_ir from the Yahoo news feed (items keep their yahoo_finance_news type; the source is marked used and,
# having no company_ir/press_release items, reads EMPTY_RESULT). Until 2.10.0 the Worker discovered the company website's RSS/Atom
# feed; since 2.10.0 (fetch allow-list) it reads only approved company IR registry entries, so with none for ACME every scenario
# below is NOT_REGISTERED and fetches nothing from the company site (the routes stay to show they are no longer used).
# Every case is pinned: the Python values are what the same request returns there today.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

ACME_SITE = "https://www.acme.example"
ACME_WEB_INFO = {**ACME_INFO, "website": ACME_SITE}
RSS_DECISION = ("FINDING: company_ir means Yahoo-feed passthrough in Python; in the Worker it is the approved company IR registry only "
                "(2.10.0: website RSS/Atom autodiscovery removed with the fetch allow-list), so with no approved entry it is NOT_REGISTERED")
_ROOT_WITH_FEED = '<html><head><link rel="alternate" type="application/rss+xml" href="/rss/news.xml"></head><body>Acme</body></html>'
_ACME_FEED = rss([{"title": "Acme official RSS release", "link": f"{ACME_SITE}/news/1", "pubDate": "Tue, 13 Oct 2026 10:00:00 GMT",
                   "description": "Acme announces results"}], title="Acme News")
_PY_EMPTY = {"row": "EMPTY_RESULT", "collection": "NOT_FOUND", "warnings": []}
_TS_NOT_REGISTERED = {"row": "NOT_REGISTERED", "collection": "PROVIDER_ERROR", "warnings": []}


def ir_rss_view(v: dict) -> dict:
    row = (v.get("sourceStatus") or {}).get("company_ir") or {}
    return {"row": row.get("status"), "collection": v.get("status"), "warnings": codes(v)}


def _ir_rss_case(name: str, ids: list[str], routes: dict, expect_py: dict, expect_ts: dict, info: dict | None = None, news: list | None = None,
                 select=ir_rss_view) -> Case:
    return tool_case(name, ids, "news", ["ACME", 10, 14, ["company_ir"]], {"info": info if info is not None else ACME_WEB_INFO,
                                                                         "news": news or [], "routes": routes},
                     select=select, mode="pinned", decision=RSS_DECISION, expect_py=expect_py, expect_ts=expect_ts)


CASES += [
    # Cards 103/107: with Yahoo news available, Python returns it under the company_ir source; the Worker returns the feed's own item.
    tool_case("company_ir.passthrough_in_python_vs_rss_discovery_in_worker", ["BR-NWS-103", "BR-NWS-107"], "news",
              ["ACME", 10, 14, ["company_ir"]],
              {"info": ACME_WEB_INFO, "news": [yahoo_row("Acme wins big order", "2026-10-14T09:00:00Z")],
               "routes": {f"{ACME_SITE}/": {"body": _ROOT_WITH_FEED}, f"{ACME_SITE}/rss/news.xml": {"body": _ACME_FEED}}},
              select=lambda v: {"sourcesUsed": v["meta"]["sourcesUsed"], "row": (v["sourceStatus"].get("company_ir") or {}).get("status"),
                                "items": [(it["title"], it["sourceType"], it["evidenceClass"], it["decisionUse"]) for it in v["items"]]},
              mode="pinned", decision=RSS_DECISION,
              expect_py={"sourcesUsed": ["company_ir"], "row": "EMPTY_RESULT",
                         "items": [("Acme wins big order", "yahoo_finance_news", "CONTEXTUAL_NEWS", "CONTEXT_ONLY")]},
              expect_ts={"sourcesUsed": [], "row": "NOT_REGISTERED", "items": []}),
    # Card 170: probe order, rel=alternate link discovery, same-domain acceptance (HIGH confidence). Diagnostics exist only in the Worker.
    tool_case("company_ir.discovery_finds_alternate_link_feed_on_the_company_domain", ["BR-NWS-170", "BR-NWS-145"], "news",
              ["ACME", 10, 14, ["company_ir"]],
              {"info": ACME_WEB_INFO, "news": [],
               "routes": {f"{ACME_SITE}/": {"body": _ROOT_WITH_FEED}, f"{ACME_SITE}/rss/news.xml": {"body": _ACME_FEED}}},
              select=lambda v: {k: (v["sourceStatus"].get("company_ir") or {}).get(k)
                                for k in ("status", "rssDiscovery", "discoveredFeedCount", "discoveryPageProbeCount", "discoveryProbeBudget",
                                          "acceptedFeedCount", "acceptedFeeds", "discoveryProbeBudgetExhausted")}
              | {"item": [(it["discoveryMethod"], it["feedUrl"], it["confidence"], it["tickerRelevance"], it["publishedAt"][:10])
                          for it in v["items"]] if v["items"] and "discoveryMethod" in v["items"][0] else []},
              mode="pinned", decision=RSS_DECISION,
              expect_py={"status": "EMPTY_RESULT", "rssDiscovery": None, "discoveredFeedCount": None, "discoveryPageProbeCount": None,
                         "discoveryProbeBudget": None, "acceptedFeedCount": None, "acceptedFeeds": None, "discoveryProbeBudgetExhausted": None, "item": []},
              expect_ts={"status": "NOT_REGISTERED", "rssDiscovery": None, "discoveredFeedCount": None, "discoveryPageProbeCount": None,
                         "discoveryProbeBudget": None, "acceptedFeedCount": None, "acceptedFeeds": None,
                         "discoveryProbeBudgetExhausted": None, "item": []}),
    # Cards 183/165: no feed on any candidate host -> the 24-page probe budget runs out before all paths are tried;
    # FEED_NOT_FOUND is also raised and the status takes the earlier-checked DISCOVERY_BUDGET_EXHAUSTED.
    _ir_rss_case("company_ir.no_feed_anywhere_exhausts_the_probe_budget", ["BR-NWS-183", "BR-NWS-165", "BR-NWS-170"], {},
                 _PY_EMPTY,
                 _TS_NOT_REGISTERED),
    _ir_rss_case("company_ir.no_company_website_gives_website_not_available", ["BR-NWS-145", "BR-NWS-165"], {},
                 _PY_EMPTY, _TS_NOT_REGISTERED,
                 info=ACME_INFO),
    # Card 145: two linked external feeds that do not mention the company are rejected -> feed_not_accepted, source not used.
    _ir_rss_case("company_ir.external_feeds_not_matching_the_company_are_rejected", ["BR-NWS-145", "BR-NWS-228", "BR-NWS-165"],
                 {f"{ACME_SITE}/": {"body": '<html><body><a href="https://feeds.example-wire.com/a.xml">RSS</a>'
                                             '<a href="https://feeds.example-wire.com/b.xml">More</a></body></html>'},
                  "https://feeds.example-wire.com/a.xml": {"body": rss([{"title": "Other firm", "link": "https://x.example/1", "pubDate": "Tue, 13 Oct 2026 10:00:00 GMT"}], "Business Wire Top Stories")},
                  "https://feeds.example-wire.com/b.xml": {"body": rss([{"title": "Another firm", "link": "https://x.example/2", "pubDate": "Tue, 13 Oct 2026 10:00:00 GMT"}], "Business Wire Top Stories")}},
                 _PY_EMPTY, _TS_NOT_REGISTERED),
    # Card 167: discovery found a feed link but the feed itself could not be downloaded -> PROVIDER_ERROR (RSS unavailable).
    _ir_rss_case("company_ir.feed_that_fails_to_download_is_provider_error", ["BR-NWS-167", "BR-NWS-165"],
                 {f"{ACME_SITE}/": {"body": _ROOT_WITH_FEED}, f"{ACME_SITE}/rss/news.xml": {"status": 503, "body": "down"}},
                 _PY_EMPTY, _TS_NOT_REGISTERED),
    _ir_rss_case("company_ir.feed_that_is_not_xml_is_parse_error", ["BR-NWS-165", "BR-NWS-167"],
                 {f"{ACME_SITE}/": {"body": _ROOT_WITH_FEED}, f"{ACME_SITE}/rss/news.xml": {"body": "<html>not a feed</html>"}},
                 _PY_EMPTY, _TS_NOT_REGISTERED),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# GlobeNewswire (newswire) source: BR-NWS-182 (feed order, early stop), 154 (used / failed determination)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

GNW_ALL = "https://www.globenewswire.com/RssFeed/*"
_GNW_PUBLIC = rss([gnw_item("Acme public companies story one", "Wed, 14 Oct 2026 10:00:00 +0000", "Nasdaq:ACME"),
                   gnw_item("Acme public companies story two", "Tue, 13 Oct 2026 10:00:00 +0000", "Nasdaq:ACME"),
                   gnw_item("Unrelated issuer story", "Tue, 13 Oct 2026 11:00:00 +0000", "Nasdaq:OTHR")])
_GNW_EARNINGS = rss([gnw_item(f"Acme earnings story {c}", f"{day}, 1{n} Oct 2026 09:00:00 +0000", "Nasdaq:ACME")
                     for n, (c, day) in enumerate((("A", "Mon"), ("B", "Tue"), ("C", "Wed"), ("D", "Thu")), start=1)])

CASES += [
    # Card 182: max 3 -> both public_companies matches then the first earnings match in feed order; later feeds are not read
    # (they have no route here, so reading one would raise SOURCE_UNAVAILABLE).
    tool_case("newswire.collection_stops_at_max_results_in_feed_order", ["BR-NWS-182"], "timeline",
              ["ACME", "", "", ["newswire"], 3, False],
              {"info": ACME_INFO, "routes": {GNW_FEEDS[0][1]: {"body": _GNW_PUBLIC}, GNW_FEEDS[1][1]: {"body": rss([])},
                                             GNW_FEEDS[2][1]: {"body": _GNW_EARNINGS}}},
              select=lambda v: {"titles": [e["title"] for e in v["timeline"]], "warnings": codes(v), "used": v["meta"]["sourcesUsed"],
                                "feeds": None},
              expect={"titles": ["Acme earnings story A", "Acme public companies story two", "Acme public companies story one"],
                      "warnings": [], "used": ["newswire"], "feeds": None}),
    tool_case("newswire.items_carry_feed_source_stock_categories_and_high_confidence", ["BR-NWS-182"], "news",
              ["ACME", 10, 14, ["newswire"]],
              {"info": ACME_INFO, "routes": {GNW_FEEDS[0][1]: {"body": _GNW_PUBLIC}, GNW_ALL: {"body": rss([])}}},
              select=lambda v: [{k: it.get(k) for k in ("title", "feedSource", "stockCategories", "confidence", "sourceType", "originalSource", "tickerRelevance")}
                                for it in v["items"]],
              expect=[{"title": "Acme public companies story one", "feedSource": "public_companies", "stockCategories": ["Nasdaq:ACME"],
                       "confidence": "HIGH", "sourceType": "newswire", "originalSource": "GlobeNewswire", "tickerRelevance": "HIGH"},
                      {"title": "Acme public companies story two", "feedSource": "public_companies", "stockCategories": ["Nasdaq:ACME"],
                       "confidence": "HIGH", "sourceType": "newswire", "originalSource": "GlobeNewswire", "tickerRelevance": "HIGH"}]),
    # Card 154: 9 of 10 feeds parse with nothing matching and one fails -> the source counts as failed, not "nothing found".
    tool_case("newswire.one_failing_feed_out_of_ten_marks_the_source_provider_error", ["BR-NWS-154", "BR-NWS-159"], "news",
              ["ACME", 10, 14, ["newswire"]],
              {"info": ACME_INFO, "routes": {GNW_ALL: {"body": rss([gnw_item("Other issuer", "Wed, 14 Oct 2026 10:00:00 +0000", "Nasdaq:OTHR")])},
                                             GNW_FEEDS[9][1]: {"status": 500, "body": "down"}}},
              select=lambda v: {"row": v["sourceStatus"]["newswire"], "status": v.get("status"), "sourcesUsed": v["meta"]["sourcesUsed"],
                                "coverage": coverage_view(v), "codes": codes(v)},
              expect={"row": {"status": "PROVIDER_ERROR", "rawCount": 0, "filteredCount": 0}, "status": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": [],
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["newswire", "PROVIDER_ERROR"]], "skipped": [],
                                   "truncated": [], "action": "CHECK_OFFICIAL_RELEASES"},
                      "codes": ["SOURCE_UNAVAILABLE"]}),
    tool_case("newswire.all_feeds_parsed_with_nothing_matching_is_empty_result", ["BR-NWS-154"], "news",
              ["ACME", 10, 14, ["newswire"]],
              {"info": ACME_INFO, "routes": {GNW_ALL: {"body": rss([gnw_item("Other issuer", "Wed, 14 Oct 2026 10:00:00 +0000", "Nasdaq:OTHR")])}}},
              select=lambda v: {"row": v["sourceStatus"]["newswire"], "status": v.get("status"), "sourcesUsed": v["meta"]["sourcesUsed"], "codes": codes(v)},
              expect={"row": {"status": "EMPTY_RESULT", "rawCount": 0, "filteredCount": 0}, "status": "NOT_FOUND", "sourcesUsed": ["newswire"], "codes": []}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Explicit sources that select nothing: BR-NWS-151, 152 (and the batch result shape, BR-NWS-143)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _no_source_view(v: dict) -> dict:
    return {"status": v.get("status"), "sourceCoverage": v.get("sourceCoverage"), "state": v["coverage"]["state"],
            "action": v["coverage"]["recommendedNextAction"], "warnings": codes(v), "attempted": v["attemptedSources"],
            "requested": v["requestedSources"], "defaultApplied": v["defaultSourcesApplied"]}


_NO_SOURCE_EXPECT = {"status": "SOURCE_LIMITED_NOT_FOUND", "sourceCoverage": "PARTIAL", "state": "PARTIAL", "action": "CHECK_OFFICIAL_RELEASES",
                     "attempted": [], "requested": ["bloomberg"], "defaultApplied": False}
_BLOOMBERG_ARGS = {"news": ["ACME", 5, 14, ["bloomberg"]], "search": ["ACME", "earnings", "", "", ["bloomberg"], 5],
                   "press": ["ACME", 90, 20, ["bloomberg"]], "timeline": ["ACME", "", "", ["bloomberg"], 50, False],
                   "verify": ["ACME", "acquisition", "", "", ["bloomberg"]]}
CASES += [
    tool_case(f"sources.explicit_unsupported_list_reads_as_source_limited_in_{tool}", ["BR-NWS-151", "BR-NWS-152"] if tool == "news" else ["BR-NWS-152"],
              tool, args, {"info": ACME_INFO}, select=_no_source_view,
              expect={**_NO_SOURCE_EXPECT, "warnings": ["SOURCE_UNSUPPORTED"] + (["NO_OFFICIAL_RELEASE_SOURCE"] if tool == "press" else [])})
    for tool, args in _BLOOMBERG_ARGS.items()
]

# Card 143: a batch. The Worker adds __batchMeta; Python returns only the per-ticker results. A symbol that fails Python's ticker
# format check is an error entry there, while the Worker (which only rejects blanks) runs it like any other.
CASES += [
    tool_case("batch.per_ticker_results_and_batch_meta_differ", ["BR-NWS-143"], "news",
              [["AAPL", "MSFT"], 5, 14, []], {"info": {}},
              select=lambda v: {"keys": sorted(v), "errors": sorted(k for k, x in v.items() if isinstance(x, dict) and x.get("error")),
                                "meta": v.get("__batchMeta")},
              mode="pinned", decision="FINDING: batch get_company_news adds __batchMeta (partialSuccess/successCount/errorCount) in the Worker only",
              expect_py={"keys": ["AAPL", "MSFT"], "errors": [], "meta": None},
              expect_ts={"keys": ["AAPL", "MSFT", "__batchMeta"], "errors": [], "meta": {"partialSuccess": False, "successCount": 2, "errorCount": 0}}),
    tool_case("batch.symbol_failing_python_ticker_validation_is_an_error_entry_only_in_python", ["BR-NWS-143"], "news",
              [["AAPL", "BAD TICKER!"], 5, 14, []], {"info": {}},
              select=lambda v: {"keys": sorted(v), "errors": sorted(k for k, x in v.items() if isinstance(x, dict) and x.get("error")),
                                "meta": v.get("__batchMeta")},
              mode="pinned", decision="FINDING: batch news validates each ticker's format in Python (error entry) but not in the Worker",
              expect_py={"keys": ["AAPL", "BAD TICKER!"], "errors": ["BAD TICKER!"], "meta": None},
              expect_ts={"keys": ["AAPL", "BAD TICKER!", "__batchMeta"], "errors": [], "meta": {"partialSuccess": False, "successCount": 2, "errorCount": 0}}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Analyst valuation text extraction (news text -> price targets, firms, multiples): BR-NWS-110, 111, 112, 122, 131, 135, 137, 141, 162
# yfmcp/capital_structure.py <-> worker/src/capital-structure.ts; both exported, so called directly.
# ═══════════════════════════════════════════════════════════════════════════════════════════════

def _avm_case(name: str, ids: list[str], titles: list[str], select, changes: list | None = None, issuer_names: list | None = None,
              ticker: str = "TEST", **kw) -> Case:
    items = [{"title": t, "summary": "", "url": f"https://news.example/{n}", "publishedAt": f"2026-10-{n + 1:02d}T10:00:00Z", "source": "Wire"}
             for n, t in enumerate(titles)]
    changes = changes or []
    args = [ticker, items, changes, issuer_names]
    return Case(name=name, ids=ids,
                py=lambda: cs.analyst_valuation_methods(ticker, copy.deepcopy(items), copy.deepcopy(changes), copy.deepcopy(issuer_names)),
                ts={"js": "return m.capitalStructure.analystValuationMethods(args[0], args[1], args[2], args[3]);", "args": args},
                select=select, **kw)


def _targets(v: dict) -> list[tuple]:
    return [(e["firm"], e["priceTarget"]) for e in v["evidence"]]


def _pt(target, prior=None, currency=None) -> dict:
    return {"target": target, "prior": prior, "currency": currency}


CASES += [
    # Cards 110/111: the four target patterns in fixed precedence; thousands separators are removed; a percent is never a target.
    _avm_case("valuation_text.price_target_patterns_and_precedence", ["BR-NWS-110", "BR-NWS-111"],
              ["Needham raises its price target from $80 to $95", "Cantor Fitzgerald PT to 120p from 100p", "Cantor Fitzgerald reiterates $122 target",
               "Jefferies sets price target to $1,250", "Needham says price target implies 25% upside", "Danske lifts price target to SEK 92"],
              _targets,
              expect=[("Needham", _pt(95, 80, "USD")), ("Cantor Fitzgerald", _pt(120, 100, "GBp")), ("Cantor Fitzgerald", _pt(122, None, "USD")),
                      ("Jefferies", _pt(1250, None, "USD")), ("Danske", _pt(92, None, "SEK"))]),
    # Card 131: currency tokens; the lookup is exact-case, so a lower-case 'usd' (matched by the case-insensitive regex) maps to null.
    _avm_case("valuation_text.price_target_currency_codes", ["BR-NWS-131", "BR-NWS-111"],
              [f"Needham sets price target to {tok}" for tok in ("$95", "US$95", "GBP 4", "GBX 120", "120p", "45 kr", "NT$900", "EUR 30", "usd 95")],
              lambda v: [(e["priceTarget"]["target"], e["priceTarget"]["currency"]) for e in v["evidence"]],
              expect=[(95, "USD"), (95, "USD"), (4, "GBP"), (120, "GBp"), (120, "GBp"), (45, "SEK"), (900, "TWD"), (30, "EUR"), (95, None)]),
    # Card 112: the firm is the earliest-named broker (ties go to the longer name); matching is case-sensitive.
    _avm_case("valuation_text.firm_attribution_earliest_name_longest_on_tie_case_sensitive", ["BR-NWS-112"],
              ["Morgan Stanley and Cantor Fitzgerald lift price target to $90", "Bernstein SocGen cuts price target to $50",
               "benchmark raises price target to $10", "Piper Sandler and Needham see price target of $70"],
              _targets, changes=[{"firm": "Cantor Fitzgerald", "ptTo": 90, "ptFrom": 80}],
              expect=[("Morgan Stanley", _pt(90, None, "USD")), ("Bernstein SocGen", _pt(50, None, "USD")), ("Piper Sandler", _pt(70, None, "USD"))]),
    # Cards 122/141/162: firms with targets and no method; the same firm can be listed twice (news and rating changes).
    _avm_case("valuation_text.method_not_disclosed_lists_news_and_rating_change_firms", ["BR-NWS-141", "BR-NWS-162", "BR-NWS-122"],
              ["Jefferies PT $50", "Needham reiterates $95 target"],
              lambda v: {"status": v["status"], "basis": v["basis"], "decisionUse": v["decisionUse"], "counts": v["methodCounts"],
                         "notDisclosed": [(m["firm"], m["source"], m["priceTarget"]) for m in v["methodNotDisclosed"]]},
              changes=[{"firm": "Baird", "ptTo": 45, "ptFrom": 0}, {"firm": "Needham", "ptTo": 95, "ptFrom": 0}],
              expect={"status": "METHOD_NOT_DISCLOSED", "basis": "NEWS_TEXT_EXTRACTION", "decisionUse": "CONTEXT_ONLY", "counts": {},
                      "notDisclosed": [("Jefferies", "news", _pt(50, None, "USD")), ("Needham", "news", _pt(95, None, "USD")),
                                       ("Baird", "rating_changes", _pt(45, None, None)), ("Needham", "rating_changes", _pt(95, None, None))]}),
    _avm_case("valuation_text.rating_change_alone_gives_method_not_disclosed", ["BR-NWS-162"], [],
              lambda v: {"status": v["status"], "evidence": len(v["evidence"]), "notDisclosed": [(m["firm"], m["source"], m["priceTarget"]) for m in v["methodNotDisclosed"]]},
              changes=[{"firm": "Needham", "ptTo": 95, "ptFrom": 80}],
              expect={"status": "METHOD_NOT_DISCLOSED", "evidence": 0, "notDisclosed": [("Needham", "rating_changes", _pt(95, 80, None))]}),
    _avm_case("valuation_text.nothing_found", ["BR-NWS-162"], ["Needham shares rise"], lambda v: {"status": v["status"], "scanned": v["itemsScanned"]},
              expect={"status": "NOT_FOUND", "scanned": 1}),
    # Card 137: multiples (multiple-first, metric-first), EV flag, periods; growth wording is not a multiple.
    _avm_case("valuation_text.multiples_metric_ev_flag_and_periods", ["BR-NWS-137"],
              ["Target based on 41x 2H27 EV/EBITDA", "Valuation at 25 times FY 26E P/E on next twelve months", "revenue grew 3x faster than peers",
               "EBITDA multiple of about 12x for 2028", "Cantor Fitzgerald uses a SOTP with peer multiple cross-check",
               "Target at 18x next twelve months EV/sales"],
              lambda v: [(e["methods"]) for e in v["evidence"]],
              expect=[[{"method": "multiple", "multiple": 41, "metric": "EBITDA", "enterpriseValue": True, "periods": ["2H27"]}],
                      [{"method": "multiple", "multiple": 25, "metric": "earnings", "enterpriseValue": False, "periods": ["FY26E"]}],   # the window ends at the metric, so the trailing period is not read
                      [{"method": "multiple", "multiple": 12, "metric": "EBITDA", "enterpriseValue": False, "periods": ["2028"]}],
                      [{"method": "sum_of_the_parts"}, {"method": "peer_comparison"}],
                      [{"method": "multiple", "multiple": 18, "metric": "sales", "enterpriseValue": True, "periods": ["NTM"]}]]),
    # Card 135: subject aliases (ticker root >= 3 letters, full name, name without legal suffixes).
    Case(name="valuation_text.subject_aliases", ids=["BR-NWS-135"],
         py=lambda: [cs.subject_aliases("ASTS", ["AST SpaceMobile, Inc."]), cs.subject_aliases("GE.L", []), cs.subject_aliases("BRK.B", ["XYZ Holdings Group"]),
                     cs.subject_aliases("ZZ", ["Group"])],
         ts={"js": "return [m.capitalStructure.subjectAliases('ASTS', ['AST SpaceMobile, Inc.']), m.capitalStructure.subjectAliases('GE.L', []),"
                   " m.capitalStructure.subjectAliases('BRK.B', ['XYZ Holdings Group']), m.capitalStructure.subjectAliases('ZZ', ['Group'])];"},
         expect=[["ast spacemobile", "ast spacemobile inc", "asts"], [], ["brk", "xyz", "xyz holdings group"], ["group"]]),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Expanded institutional ownership: BR-NWS-150 (provider state machine), 177 and 180 (it reuses the news Finnhub market policy)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

from pathlib import Path

from yfmcp.tools import provider_gaps

FAKE_ALPHA = {"ALPHA_VANTAGE_API_KEY": "fake-alpha-key"}
FINNHUB_OWNERSHIP = "https://finnhub.io/api/v1/stock/ownership*"
ALPHA_QUERY = "https://www.alphavantage.co/query*"


def ownership_view(v: dict) -> dict:
    data = v.get("data") if isinstance(v.get("data"), dict) else v
    return {"status": data.get("status"), "source": data.get("source"), "action": data.get("recommendedNextAction"),
            "attempts": [[a.get("provider"), a.get("status"), a.get("attempted"), a.get("reasonCode"), a.get("validationStatus")]
                         for a in data.get("providerAttempts") or []],
            "holders": [h.get("holderName") for h in data.get("holders") or []], "returned": data.get("returnedCount"),
            "providerRows": data.get("providerRowCount"), "truncated": data.get("truncated"), "dataDate": data.get("dataDate"),
            "freshness": data.get("freshnessStatus"), "decisionGrade": data.get("decisionGrade"), "aggregate": data.get("aggregate"),
            "alphaAllowed": data.get("alphaFallbackAllowed")}


def ownership_case(name: str, ids: list[str], ticker: str, *, allow: bool = False, max_holders: int = 50, env: dict | None = None,
                   routes: dict | None = None, policy_missing: bool = False, **kw) -> Case:
    scn = {"env": env or {}, "routes": routes or {}, "info": {}, "news": []}

    def py():
        with _python_world(scn), contextlib.ExitStack() as stack:
            if policy_missing:
                stack.enter_context(patch.object(provider_gaps, "_SOURCE_POLICY_PATH", Path("/nonexistent/news-source-capabilities.json")))
            return json.loads(asyncio.run(provider_gaps.get_expanded_institutional_ownership(ticker, allow, max_holders)))

    fx = {"now": NOW, "env": env or {}, "fn": "getExpandedInstitutionalOwnership"}
    kw.setdefault("select", ownership_view)
    return Case(name=name, ids=ids, py=py, ts={"js": _TOOL_JS, "args": [ticker, allow, max_holders], "fx": fx, "isolate": True},
                routes=_routes_for(scn), **kw)


_OWN_ROWS = [{"name": "Vanguard Group", "share": 900, "change": 10, "filingDate": "2026-06-30"},
             {"name": "BlackRock", "share": 800, "change": -5, "filingDate": "2026-06-30"},
             {"name": "State Street", "share": 400, "change": 0, "filingDate": "2026-03-31"}]
_NOT_ELIGIBLE_OWN = {"status": "NOT_ELIGIBLE", "source": None, "action": "USE_YAHOO_OR_OFFICIAL_FILINGS",
                     "attempts": [["finnhub", "NOT_ELIGIBLE", False, "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT", None]]}
CASES += [
    # Card 150: a Finnhub answer for another symbol is discarded (PROVIDER_IDENTITY_MISMATCH); with the scarce fallback allowed
    # Alpha Vantage is tried, and an OK-but-empty Alpha answer is NOT_FOUND.
    ownership_case("ownership.finnhub_symbol_mismatch_falls_back_to_alpha_and_empty_alpha_is_not_found", ["BR-NWS-150"], "AAPL", allow=True,
                   env={**FAKE_FINNHUB, **FAKE_ALPHA},
                   routes={FINNHUB_OWNERSHIP: {"json": {"symbol": "MSFT", "ownership": _OWN_ROWS}}, ALPHA_QUERY: {"json": {"symbol": "AAPL", "data": []}}},
                   select=lambda v: {k: ownership_view(v)[k] for k in ("status", "source", "action", "attempts", "alphaAllowed")},
                   expect={"status": "NOT_FOUND", "source": None, "action": "VERIFY_TICKER_AND_DATE", "alphaAllowed": True,
                           "attempts": [["finnhub", "OK", True, None, "PROVIDER_IDENTITY_MISMATCH"], ["alpha_vantage", "OK", True, None, None]]}),
    ownership_case("ownership.finnhub_success_reports_truncation_date_and_next_action", ["BR-NWS-150"], "AAPL", max_holders=2, env=FAKE_FINNHUB,
                   routes={FINNHUB_OWNERSHIP: {"json": {"symbol": "AAPL", "ownership": _OWN_ROWS}}},
                   expect={"status": "OK", "source": "finnhub", "action": "VERIFY_SEC_13F_FOR_MATERIAL_USE",
                           "attempts": [["finnhub", "OK", True, None, None]], "holders": ["Vanguard Group", "BlackRock"], "returned": 2,
                           "providerRows": 3, "truncated": True, "dataDate": "2026-06-30", "freshness": "DATED", "decisionGrade": False,
                           "aggregate": {}, "alphaAllowed": None}),
    ownership_case("ownership.empty_finnhub_without_fallback_points_to_sec_13f", ["BR-NWS-150"], "AAPL", env=FAKE_FINNHUB,
                   routes={FINNHUB_OWNERSHIP: {"json": {"symbol": "AAPL", "ownership": []}}},
                   select=lambda v: {k: ownership_view(v)[k] for k in ("status", "action", "attempts", "alphaAllowed")},
                   expect={"status": "NOT_FOUND", "action": "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK",
                           "attempts": [["finnhub", "OK", True, None, None]], "alphaAllowed": False}),
    # Cards 177/180: the news Finnhub market policy gates this tool too; IQE.L (.L suffix) never reaches Finnhub.
    ownership_case("ownership.ineligible_suffix_never_reaches_finnhub", ["BR-NWS-177", "BR-NWS-180"], "IQE.L",
                   select=lambda v: {k: ownership_view(v)[k] for k in ("status", "source", "action", "attempts")}, expect={k: _NOT_ELIGIBLE_OWN[k] for k in ("status", "source", "action", "attempts")}),
    # Card 180: Python reads the policy file at call time and fails open on any read error (IQE.L is then sent to Finnhub, which has
    # no key here); the Worker's copy is compiled in.
    ownership_case("ownership.unreadable_policy_fails_open_in_python", ["BR-NWS-180", "BR-NWS-177"], "IQE.L", policy_missing=True,
                   select=lambda v: {k: ownership_view(v)[k] for k in ("status", "action", "attempts")},
                   mode="pinned", decision="FINDING: Python fails open (calls Finnhub) when the news-source policy file is unreadable; the Worker's bundled policy cannot fail",
                   expect_py={"status": "SOURCE_UNCONFIGURED", "action": "CONFIGURE_PROVIDER", "attempts": [["finnhub", "SOURCE_UNCONFIGURED", False, None, None]]},
                   expect_ts={"status": "NOT_ELIGIBLE", "action": "USE_YAHOO_OR_OFFICIAL_FILINGS",
                              "attempts": [["finnhub", "NOT_ELIGIBLE", False, "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT", None]]}),
]


# ═══════════════════════════════════════════════════════════════════════════════════════════════
# Further coverage for cards 161 (coverage with a healthy source beside a limited one), 170 (direct feed path), 182 (XML guard)
# ═══════════════════════════════════════════════════════════════════════════════════════════════

CASES += [
    # Card 161: Finnhub rate-limited beside a healthy Yahoo source: partial coverage, retry advice, and the Yahoo item is still returned.
    tool_case("news.coverage.rate_limited_source_beside_ok_source_is_partial_with_retry_action", ["BR-NWS-161", "BR-NWS-176"], "news",
              ["ACME", 10, 14, ["yahoo_finance_news", "finnhub"]],
              {"env": FAKE_FINNHUB, "info": ACME_INFO, "news": [yahoo_row("Acme wins supply contract", "2026-10-10T13:00:00Z")],
               "routes": {FINNHUB_NEWS: {"status": 429, "body": "{}"}}},
              select=lambda v: {"status": v.get("status"), "coverage": coverage_view(v), "titles": [it["title"] for it in v["items"]],
                                "rows": {s: v["sourceStatus"][s]["status"] for s in ("yahoo_finance_news", "finnhub")}},
              expect={"status": "PARTIAL",
                      "coverage": {"sourceCoverage": "PARTIAL", "state": "PARTIAL", "failed": [["finnhub", "RATE_LIMITED"]], "skipped": [],
                                   "truncated": [], "action": "RETRY_RETRYABLE_SOURCES"},
                      "titles": ["Acme wins supply contract"], "rows": {"yahoo_finance_news": "OK", "finnhub": "RATE_LIMITED"}}),
    # Card 170: a probed page that is itself an RSS document on the company domain is a direct feed (discoveryMethod direct_feed_path).
    tool_case("company_ir.discovery_accepts_a_probed_path_that_is_itself_a_feed", ["BR-NWS-170"], "news",
              ["ACME", 10, 14, ["company_ir"]],
              {"info": ACME_WEB_INFO, "news": [], "routes": {f"{ACME_SITE}/rss.cfm": {"body": _ACME_FEED}}},
              select=lambda v: {"row": (v["sourceStatus"].get("company_ir") or {}).get("status"),
                                "items": [(it.get("discoveryMethod"), it.get("feedUrl"), it.get("confidence"), it.get("sourceType")) for it in v["items"]]},
              mode="pinned", decision=RSS_DECISION,
              expect_py={"row": "EMPTY_RESULT", "items": []},
              expect_ts={"row": "NOT_REGISTERED", "items": []}),
    # Card 182: Python refuses any feed containing a DOCTYPE; the Worker refuses only <!ENTITY and checks the RSS structure instead.
    tool_case("newswire.doctype_in_a_feed_is_refused_only_by_python", ["BR-NWS-182", "BR-NWS-154"], "news",
              ["ACME", 10, 14, ["newswire"]],
              {"info": ACME_INFO,
               "routes": {GNW_FEEDS[0][1]: {"body": "<!DOCTYPE rss>" + _GNW_PUBLIC}, GNW_ALL: {"body": rss([])}}},
              select=lambda v: {"titles": [it["title"] for it in v["items"]], "row": v["sourceStatus"]["newswire"]["status"], "codes": codes(v),
                                "sourcesUsed": v["meta"]["sourcesUsed"]},
              mode="pinned", decision="FINDING: Python rejects a feed with a DOCTYPE (parse error); the Worker rejects only <!ENTITY, so the same feed yields items there",
              expect_py={"titles": [], "row": "PROVIDER_ERROR", "codes": ["SOURCE_UNAVAILABLE"], "sourcesUsed": []},
              expect_ts={"titles": ["Acme public companies story one", "Acme public companies story two"], "row": "OK", "codes": [], "sourcesUsed": ["newswire"]}),
]
