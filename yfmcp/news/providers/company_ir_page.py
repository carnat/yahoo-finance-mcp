"""company_ir_page provider: approved company IR page registry entries, the page fetch and the
JSON / HTML adapters.

Adapter contract (unchanged by the move): each ``_collect_*_events`` keeps its current signature and
return value. The return value is the items plus the per-source status/diagnostics; warnings are
appended to the caller's ``warnings`` list. There is no provider registry yet (2.15.1 is a pure
restructure; the registry and the off switch are part 2).
"""

import asyncio
import datetime
import html as _html_module
import json
import re as _re
import urllib.parse as _urlparse
import urllib.request as _urlrequest

from yfmcp import url_policy as _url_policy
from yfmcp.data import DATA_DIR
from yfmcp.util import _to_iso_utc
from yfmcp.parsing.html import _strip_html_tags
from yfmcp.news.shared import (
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _short_text,
    _within_date_window,
)


_COMPANY_IR_PAGE_REGISTRY_PATH = DATA_DIR / "company-ir-page-registry.json"


_COMPANY_IR_PAGE_MAX_BYTES = 750 * 1024


def _load_company_ir_page_registry() -> dict:
    try:
        return json.loads(_COMPANY_IR_PAGE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"schemaVersion": "unknown", "registryVersion": "unknown", "sources": []}


def _company_ir_page_entry(ticker: str) -> dict | None:
    ticker_u = str(ticker or "").upper()
    registry = _load_company_ir_page_registry()
    for entry in registry.get("sources") or []:
        if isinstance(entry, dict) and str(entry.get("ticker") or "").upper() == ticker_u:
            return dict(entry, registryVersion=registry.get("registryVersion"), registrySchemaVersion=registry.get("schemaVersion"))
    return None


def _host_without_www(host: str) -> str:
    return str(host or "").lower().removeprefix("www.")


def _company_ir_page_url_allowed(entry: dict, raw_url: str) -> bool:
    try:
        parsed = _urlparse.urlparse(str(raw_url or ""))
    except Exception:
        return False
    if parsed.scheme.lower() != "https":
        return False
    host = _host_without_www(parsed.hostname or "")
    allowed_hosts = {_host_without_www(h) for h in entry.get("allowedHosts") or []}
    prefixes = [str(p) for p in entry.get("allowedPathPrefixes") or [] if str(p)]
    return host in allowed_hosts and any(parsed.path.startswith(prefix) for prefix in prefixes)


def _company_ir_page_registry_fresh(entry: dict) -> bool:
    raw = entry.get("revalidateAfter")
    if not raw:
        return True
    try:
        deadline = datetime.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=datetime.timezone.utc)
        return deadline >= datetime.datetime.now(datetime.timezone.utc)
    except Exception:
        return False


def _company_ir_page_item(
    ticker: str,
    entry: dict,
    title: str,
    summary: str,
    raw_url: str | None,
    published_at: str | None,
    retrieved_at: str,
    final_url: str,
) -> dict | None:
    url = _urlparse.urljoin(final_url, raw_url or final_url)
    if not _company_ir_page_url_allowed(entry, url):
        return None
    ticker_u = ticker.upper()
    # The issuer is the registry issuerName and nothing else: an entry without one gives null, as in the Worker (2.19.0, B-G17).
    issuer = entry.get("issuerName")
    duplicate_group_id = _make_duplicate_group_id(ticker_u, title, published_at, issuer, url)
    return {
        "title": title,
        "source": "company_ir_page",
        "originalSource": issuer,
        "sourceType": "company_ir_page",
        "provider": "company_ir_page_registry",
        "discoveredVia": "approved_company_ir_page_registry",
        "adapter": entry.get("adapter"),
        "canonicalUrl": entry.get("canonicalUrl"),
        "registryVersion": entry.get("registryVersion"),
        "approvalStatus": "approved",
        "publishedAt": published_at,
        "retrievedAt": retrieved_at,
        "url": url,
        "issuer": issuer,
        "tickers": [ticker_u],
        "eventType": _event_type_from_keywords(f"{title} {summary}"),
        "summary": _short_text(summary or title, 240),
        "evidenceText": _short_text(summary or title, 180),
        "confidence": "HIGH",
        "tickerRelevance": "HIGH",
        "duplicateGroupId": duplicate_group_id,
    }


def _company_ir_page_json_records(payload: object) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    candidates = [
        payload.get("items"),
        payload.get("releases"),
        payload.get("pressReleases"),
        payload.get("news"),
        payload.get("data"),
    ]
    feed = payload.get("feed")
    if isinstance(feed, dict):
        candidates.append(feed.get("items"))
    data = payload.get("data")
    if isinstance(data, dict):
        candidates.extend(data.values())
    for candidate in candidates:
        if isinstance(candidate, list):
            records = [r for r in candidate if isinstance(r, dict)]
            if records:
                return records
    return []


def _parse_company_ir_page_json(ticker: str, entry: dict, text: str, retrieved_at: str, final_url: str) -> list[dict]:
    try:
        payload = json.loads(text)
    except Exception:
        return []
    items: list[dict] = []
    for raw in _company_ir_page_json_records(payload):
        title = str(raw.get("title") or raw.get("headline") or raw.get("name") or "").strip()
        if not title:
            continue
        summary = str(raw.get("summary") or raw.get("description") or raw.get("excerpt") or raw.get("content_text") or raw.get("content") or "").strip()
        raw_url = str(raw.get("url") or raw.get("link") or raw.get("external_url") or raw.get("permalink") or "").strip() or None
        published_at = _to_iso_utc(raw.get("publishedAt") or raw.get("published_at") or raw.get("datePublished") or raw.get("date_published") or raw.get("date") or raw.get("pubDate"))
        item = _company_ir_page_item(ticker, entry, title, summary, raw_url, published_at, retrieved_at, final_url)
        if item:
            items.append(item)
    return items


def _html_attr(attrs: str, name: str) -> str:
    match = _re.search(rf'\b{_re.escape(name)}\s*=\s*["\']([^"\']+)["\']', attrs or "", flags=_re.I)
    return _html_module.unescape(match.group(1)) if match else ""


def _looks_like_feed(text: str) -> bool:
    return bool(_re.search(r"<rss\b", text or "", flags=_re.I) or _re.search(r"<feed\b", text or "", flags=_re.I))


def _decode_xml_text(value: str) -> str:
    """CDATA, then entities decoded once with html.unescape (RNEWS3 decision 5). The Worker's decodeXmlText decodes
    `&amp;` last, so a double-escaped `&amp;lt;` reads as the text `&lt;` in both runtimes (R-news D)."""
    value = _re.sub(r"<!\[CDATA\[([\s\S]*?)\]\]>", r"\1", value or "", flags=_re.I)
    return _html_module.unescape(value)


def _extract_xml_tag(xml: str, tag_name: str) -> str:
    match = _re.search(rf"<{_re.escape(tag_name)}(?:\s[^>]*)?>([\s\S]*?)</{_re.escape(tag_name)}>", xml or "", flags=_re.I)
    return _decode_xml_text(match.group(1)).strip() if match else ""


def _parse_company_ir_feed_items(xml: str) -> list[tuple[str, str]]:
    # Same guard as the GlobeNewswire feed (2.17.0): a DOCTYPE or ENTITY declaration is refused.
    if _re.search(r"<!doctype|<!entity", xml or "", flags=_re.I):
        raise ValueError("unsupported XML declaration in company IR RSS/Atom")
    if not _looks_like_feed(xml):
        raise ValueError("invalid company IR RSS/Atom XML")
    item_xmls: list[tuple[str, str]] = []
    for m in _re.finditer(r"<item\b[^>]*>([\s\S]*?)</item>", xml, flags=_re.I):
        item_xmls.append(("rss", m.group(1)))
    for m in _re.finditer(r"<entry\b[^>]*>([\s\S]*?)</entry>", xml, flags=_re.I):
        item_xmls.append(("atom", m.group(1)))
    return item_xmls


def _feed_attr(attrs: str, name: str) -> str:
    match = _re.search(rf"{_re.escape(name)}\s*=\s*(\"([^\"]*)\"|'([^']*)'|([^\s\"'>]+))", attrs or "", flags=_re.I)
    if not match:
        return ""
    for group in (match.group(2), match.group(3), match.group(4)):
        if group is not None:
            return _decode_xml_text(group.strip())
    return ""


def _atom_entry_link(entry_xml: str) -> str:
    for m in _re.finditer(r"<link\b([^>]*)/?>", entry_xml or "", flags=_re.I):
        rel = _feed_attr(m.group(1), "rel").lower()
        href = _feed_attr(m.group(1), "href")
        if href and (not rel or rel == "alternate"):
            return href
    return _extract_xml_tag(entry_xml, "link")


def _parse_company_ir_page_feed(ticker: str, entry: dict, xml: str, retrieved_at: str, final_url: str) -> list[dict]:
    items: list[dict] = []
    for kind, feed_item in _parse_company_ir_feed_items(xml):
        title = _extract_xml_tag(feed_item, "title")
        if not title:
            continue
        if kind == "atom":
            summary_raw = _extract_xml_tag(feed_item, "summary") or _extract_xml_tag(feed_item, "content")
            raw_link = _atom_entry_link(feed_item)
            published_raw = _extract_xml_tag(feed_item, "published") or _extract_xml_tag(feed_item, "updated")
        else:
            summary_raw = _extract_xml_tag(feed_item, "description") or _extract_xml_tag(feed_item, "content:encoded")
            raw_link = _extract_xml_tag(feed_item, "link") or _extract_xml_tag(feed_item, "guid")
            published_raw = _extract_xml_tag(feed_item, "pubDate")
        item = _company_ir_page_item(
            ticker, entry, title, _strip_html_tags(summary_raw), raw_link or None, _to_iso_utc(published_raw), retrieved_at, final_url,
        )
        if item:
            items.append(item)
    return items


def _parse_company_ir_page_html(ticker: str, entry: dict, html_text: str, retrieved_at: str, final_url: str) -> list[dict]:
    blocks = _re.findall(r"<article\b[^>]*>[\s\S]*?</article>", html_text or "", flags=_re.I)
    if not blocks:
        blocks = [
            m.group(0)
            for m in _re.finditer(r"<li\b[^>]*>[\s\S]*?</li>", html_text or "", flags=_re.I)
            if _re.search(r"press release|news release|earnings|results|announces|reports", _strip_html_tags(m.group(0)), flags=_re.I)
        ]
    items: list[dict] = []
    for block in blocks[:50]:
        heading = _re.search(r"<h[1-4]\b[^>]*>([\s\S]*?)</h[1-4]>", block, flags=_re.I)
        anchor = _re.search(r"<a\b([^>]*)>([\s\S]*?)</a>", block, flags=_re.I)
        title = _strip_html_tags((heading.group(1) if heading else anchor.group(2) if anchor else "") or "").strip()
        if len(title) < 6:
            continue
        href = _html_attr(anchor.group(1), "href") if anchor else ""
        time_match = _re.search(r"<time\b([^>]*)>([\s\S]*?)</time>", block, flags=_re.I)
        datetime_raw = ""
        if time_match:
            datetime_raw = _html_attr(time_match.group(1), "datetime") or _strip_html_tags(time_match.group(2))
        item = _company_ir_page_item(
            ticker,
            entry,
            title,
            _strip_html_tags(block),
            href or None,
            _to_iso_utc(datetime_raw),
            retrieved_at,
            final_url,
        )
        if item:
            items.append(item)
    return items


async def _fetch_company_ir_page(entry: dict) -> tuple[str, str, str]:
    canonical_url = str(entry.get("canonicalUrl") or "")
    if not _company_ir_page_url_allowed(entry, canonical_url):
        raise ValueError("registry canonical URL is outside its allow-list")
    if str(entry.get("status") or "") != "approved":
        # Only approved entries are fetched. The Worker reaches the same refusal because the fetch allow-list
        # holds the hosts of approved entries only.
        raise _url_policy.UrlPolicyError("URL host not allowed")

    def _fetch() -> tuple[str, str, str]:
        req = _urlrequest.Request(
            canonical_url,
            headers={
                "User-Agent": "yahoo-finance-mcp/ir-page-registry",
                "Accept": "application/json, application/rss+xml, application/atom+xml, application/xml, text/xml, text/html;q=0.9",
            },
        )
        with _url_policy.urlopen(req, timeout=15) as resp:
            final_url = resp.geturl()
            if not _company_ir_page_url_allowed(entry, final_url):
                raise ValueError("redirect target is outside registry allow-list")
            content_type = str(resp.headers.get("content-type") or "").lower()
            # A structured entry is approved for json / xml / rss / atom / text only (2.20.0, F9).
            if entry.get("adapter") == "structured" and content_type and not _re.search(r"(json|xml|rss|atom|text/plain)", content_type, flags=_re.I):
                raise ValueError(f"unexpected structured content type: {content_type}")
            # An html_article entry is approved for HTML only (2.19.0, B-G17): any other declared content type fails validation.
            if entry.get("adapter") == "html_article" and content_type and not _re.search(r"(html|text/plain)", content_type, flags=_re.I):
                raise ValueError(f"unexpected HTML content type: {content_type}")
            data = resp.read(_COMPANY_IR_PAGE_MAX_BYTES + 1)
            if len(data) > _COMPANY_IR_PAGE_MAX_BYTES:
                raise ValueError("response exceeded size limit")
            return data.decode("utf-8", errors="replace"), final_url, content_type

    return await asyncio.to_thread(_fetch)


class _CompanyIrPageResult(tuple):
    """``(items, warnings, used)`` that also carries the source diagnostics (never serialized)."""

    diagnostics: dict

    def __new__(cls, triple: tuple, diagnostics: dict) -> "_CompanyIrPageResult":
        result = super().__new__(cls, triple)
        result.diagnostics = diagnostics
        return result


async def _collect_company_ir_page_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
) -> "_CompanyIrPageResult":
    """The ``(items, warnings, used)`` triple plus ``.diagnostics`` (2.19.0): the Worker's per-source diagnostics, i.e. the
    registry fields the status row carries and the state of the entry."""
    entry = _company_ir_page_entry(ticker)
    if not entry:
        registry = _load_company_ir_page_registry()
        return _CompanyIrPageResult(([], [], False), {
            "registryVersion": registry.get("registryVersion"),
            "registrySchemaVersion": registry.get("schemaVersion"),
            "status": "NOT_REGISTERED",
            "approvalStatus": "not_registered",
            "allowedAction": "registry_review_required",
        })
    status = str(entry.get("status") or "")
    warning_base = {
        "canonicalUrl": entry.get("canonicalUrl"),
        "registryVersion": entry.get("registryVersion"),
        "approvalStatus": status,
    }
    diagnostics: dict = {
        "registryVersion": entry.get("registryVersion"),
        "registrySchemaVersion": entry.get("registrySchemaVersion"),
        "status": "EMPTY_RESULT",
        "approvalStatus": status,
        "adapter": entry.get("adapter"),
        "canonicalUrl": entry.get("canonicalUrl"),
        "allowedHosts": entry.get("allowedHosts"),
        "allowedPathPrefixes": entry.get("allowedPathPrefixes"),
        "revalidateAfter": entry.get("revalidateAfter"),
    }
    if status == "disabled":
        warning = {**warning_base, "code": "COMPANY_IR_PAGE_DISABLED", "message": "Company IR page registry entry is disabled.", "severity": "info"}
        return _CompanyIrPageResult(([], [warning], False), {**diagnostics, "status": "DISABLED", "allowedAction": "registry_review_required"})
    if status == "candidate":
        candidate = {
            "ticker": entry.get("ticker"),
            "issuerName": entry.get("issuerName"),
            "canonicalUrl": entry.get("canonicalUrl"),
            "adapter": entry.get("adapter"),
            "candidateReason": entry.get("candidateReason"),
            "discoveredAt": entry.get("discoveredAt"),
            "decisionGrade": False,
        }
        warning = {**warning_base, "code": "COMPANY_IR_PAGE_CANDIDATE_AVAILABLE", "message": "Company IR page candidate is available for review but was not fetched.", "severity": "info", "candidate": candidate}
        return _CompanyIrPageResult(([], [warning], False), {**diagnostics, "status": "CANDIDATE_AVAILABLE", "allowedAction": "review_and_promote", "candidates": [candidate]})
    if not _company_ir_page_registry_fresh(entry):
        warning = {**warning_base, "code": "COMPANY_IR_PAGE_REVALIDATION_DUE", "message": "Approved company IR page is past its revalidation date; skipped until reviewed.", "severity": "warning"}
        return _CompanyIrPageResult(([], [warning], False), {**diagnostics, "status": "REVALIDATION_DUE", "allowedAction": "registry_review_required"})
    try:
        text, final_url, content_type = await _fetch_company_ir_page(entry)
        adapter = str(entry.get("adapter") or "")
        # An html_article entry is approved for HTML only: a feed body fails validation (2.19.0, call #17).
        if adapter == "html_article" and _looks_like_feed(text):
            raise ValueError("unexpected feed body for html_article adapter")
        if _looks_like_feed(text):
            items = _parse_company_ir_page_feed(ticker, entry, text, retrieved_at, final_url)
        elif adapter == "structured":
            items = _parse_company_ir_page_json(ticker, entry, text, retrieved_at, final_url)
        else:
            items = _parse_company_ir_page_html(ticker, entry, text, retrieved_at, final_url)
        items = [
            item for item in items
            if _within_date_window(item.get("publishedAt"), start_date=start_date, end_date=end_date, lookback_days=lookback_days)
        ][:max_results]
        return _CompanyIrPageResult((items, [], True), {
            **diagnostics,
            "status": "APPROVED_SOURCE_OK" if items else "EMPTY_RESULT",
            "allowedAction": "fetch_configured_source",
            "finalUrl": final_url,
            "rawCount": len(items),
            "filteredCount": len(items),
        })
    except Exception as exc:
        warning = {**warning_base, "code": "COMPANY_IR_PAGE_VALIDATION_FAILED", "message": f"Approved company IR page validation failed: {exc}", "severity": "warning"}
        return _CompanyIrPageResult(([], [warning], False), {**diagnostics, "status": "SOURCE_VALIDATION_FAILED", "allowedAction": "registry_review_required"})
