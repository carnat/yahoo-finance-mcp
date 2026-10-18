"""GlobeNewswire provider: RSS feed fetch with the size guard, category matching and
_collect_globenewswire_events.

Adapter contract (unchanged by the move): each ``_collect_*_events`` keeps its current signature and
return value. The return value is the items plus the per-source status/diagnostics; warnings are
appended to the caller's ``warnings`` list. There is no provider registry yet (2.15.1 is a pure
restructure; the registry and the off switch are part 2).
"""

import asyncio
import html as _html_module
import re as _re
import urllib.request as _urlrequest
import xml.etree.ElementTree as _ET

from yfmcp import url_policy as _url_policy
from yfmcp.cache import _tool_cache
from yfmcp.cache import TTL_NEWS
from yfmcp.util import _parse_rss_date
from yfmcp.clients.market_providers import _PROVIDER_USER_AGENT
from yfmcp.news.shared import (
    _event_type_from_keywords,
    _make_duplicate_group_id,
    _short_text,
    _within_date_window,
)


_GLOBENEWSWIRE_RSS_FEEDS = (
    (
        "public_companies",
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/"
        "GlobeNewswire%20-%20News%20about%20Public%20Companies",
    ),
    (
        "press_releases",
        "https://www.globenewswire.com/RssFeed/subjectcode/72-Press%20Releases/"
        "feedTitle/GlobeNewswire%20-%20Press%20Releases",
    ),
    (
        "earnings",
        "https://www.globenewswire.com/RssFeed/subjectcode/"
        "13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/"
        "GlobeNewswire%20-%20Earnings%20Releases%20And%20Operating%20Results",
    ),
    (
        "stock_market_news",
        "https://www.globenewswire.com/RssFeed/subjectcode/39-Stock%20Market%20News/"
        "feedTitle/GlobeNewswire%20-%20Stock%20Market%20News",
    ),
    (
        "technology",
        "https://www.globenewswire.com/RssFeed/industry/9000-Technology/feedTitle/"
        "GlobeNewswire%20-%20Industry%20News%20on%20Technology",
    ),
    (
        "semiconductors",
        "https://www.globenewswire.com/RssFeed/industry/9576-Semiconductors/feedTitle/"
        "GlobeNewswire%20-%20Industry%20News%20on%20Semiconductors",
    ),
    (
        "telecommunications",
        "https://www.globenewswire.com/RssFeed/industry/6000-Telecommunications/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications",
    ),
    (
        "mobile_telecommunications",
        "https://www.globenewswire.com/RssFeed/industry/6575-Mobile%20Telecommunications/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Mobile%20Telecommunications",
    ),
    (
        "telecommunications_equipment",
        "https://www.globenewswire.com/RssFeed/industry/9578-Telecommunications%20Equipment/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Telecommunications%20Equipment",
    ),
    (
        "electronic_equipment",
        "https://www.globenewswire.com/RssFeed/industry/2737-Electronic%20Equipment/"
        "feedTitle/GlobeNewswire%20-%20Industry%20News%20on%20Electronic%20Equipment",
    ),
)


_GLOBENEWSWIRE_RSS_URL = _GLOBENEWSWIRE_RSS_FEEDS[0][1]


_GLOBENEWSWIRE_MAX_BYTES = 2 * 1024 * 1024


_GLOBENEWSWIRE_BLOCKED_XML_MARKERS = ("<!doctype", "<!entity")


_GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/stock"


_GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN = "https://www.globenewswire.com/rss/ISIN"


def _globenewswire_xml_is_safe(xml_content: str) -> bool:
    lowered = xml_content.lower()
    return not any(marker in lowered for marker in _GLOBENEWSWIRE_BLOCKED_XML_MARKERS)


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":", 1)[-1]


def _xml_child_texts(node: _ET.Element, local_name: str) -> list[str]:
    values: list[str] = []
    for child in list(node):
        if _xml_local_name(str(child.tag)) == local_name:
            text = "".join(child.itertext()).strip()
            if text:
                values.append(text)
    return values


def _xml_first_child_text(node: _ET.Element, local_name: str) -> str:
    values = _xml_child_texts(node, local_name)
    return values[0] if values else ""


def _globenewswire_category_values(rss_item: _ET.Element, domain: str) -> list[str]:
    values: list[str] = []
    domain_l = domain.lower()
    for child in list(rss_item):
        if _xml_local_name(str(child.tag)) != "category":
            continue
        if str(child.attrib.get("domain") or "").lower() != domain_l:
            continue
        text = "".join(child.itertext()).strip()
        for part in text.split(","):
            value = part.strip()
            if value:
                values.append(value)
    return values


def _globenewswire_stock_category_matches(ticker: str, stock_categories: list[str]) -> bool:
    ticker_u = ticker.upper()
    for category in stock_categories:
        symbol = category.rsplit(":", 1)[-1].strip().upper()
        if symbol == ticker_u:
            return True
    return False


def _globenewswire_plain_text(value: str) -> str:
    text = _re.sub(r"<[^>]+>", " ", value or "")
    text = _html_module.unescape(text)
    return _re.sub(r"\s+", " ", text).strip()


async def _collect_globenewswire_events(
    ticker: str,
    *,
    retrieved_at: str,
    max_results: int,
    start_date: str = "",
    end_date: str = "",
    lookback_days: int | None = None,
    filter_low_relevance: bool = True,
) -> tuple[list[dict], list[dict], bool]:
    """Fetch GlobeNewswire RSS feeds and return exact stock-category matches.

    Items are labelled with ``source="newswire"``, ``sourceType="newswire"``,
    ``originalSource="GlobeNewswire"``, ``provider="globenewswire"``, and
    ``discoveredVia="globenewswire_rss"``. Feeds are cached independently for
    :data:`TTL_NEWS` seconds. Ticker relevance is established only from
    GlobeNewswire RSS stock-category metadata such as ``Nasdaq:NVDA``.
    """
    warnings: list[dict] = []
    items: list[dict] = []
    ticker_u = ticker.upper()
    seen_keys: set[str] = set()
    parsed_any_feed = False

    for feed_name, feed_url in _GLOBENEWSWIRE_RSS_FEEDS:
        cache_key = f"gnw_rss:{feed_name}"
        xml_content: str | None = None
        cached_entry = _tool_cache.get(cache_key)
        if cached_entry is not None:
            xml_content = cached_entry[0]

        if xml_content is None:
            loop = asyncio.get_event_loop()

            def _fetch_rss() -> str:
                req = _urlrequest.Request(
                    feed_url,
                    headers={"User-Agent": _PROVIDER_USER_AGENT},
                )
                with _url_policy.urlopen(req, timeout=20) as resp:  # noqa: S310
                    raw = resp.read(_GLOBENEWSWIRE_MAX_BYTES + 1)
                    if len(raw) > _GLOBENEWSWIRE_MAX_BYTES:
                        raise ValueError("GlobeNewswire RSS response exceeded size limit")
                    return raw.decode("utf-8", errors="replace")

            try:
                xml_content = await loop.run_in_executor(None, _url_policy.in_context(_fetch_rss))
                _tool_cache.set(cache_key, xml_content, TTL_NEWS)
            except Exception as exc:
                warnings.append({
                    "code": "SOURCE_UNAVAILABLE",
                    "message": f"GlobeNewswire RSS feed '{feed_name}' unavailable: {exc}",
                    "severity": "warning",
                })
                continue

        try:
            if not _globenewswire_xml_is_safe(xml_content):
                raise ValueError("unsupported XML declaration in GlobeNewswire RSS")
            root = _ET.fromstring(xml_content)  # noqa: S314 (trusted source)
            channel = root.find("channel") if _xml_local_name(str(root.tag)) != "channel" else root
            rss_items = channel.findall("item") if channel is not None else []
            parsed_any_feed = True
        except Exception as exc:
            warnings.append({
                "code": "SOURCE_UNAVAILABLE",
                "message": f"GlobeNewswire RSS feed '{feed_name}' parse error: {exc}",
                "severity": "warning",
            })
            continue

        for rss_item in rss_items:
            stock_categories = _globenewswire_category_values(
                rss_item,
                _GLOBENEWSWIRE_STOCK_CATEGORY_DOMAIN,
            )
            if not _globenewswire_stock_category_matches(ticker_u, stock_categories):
                continue
            relevance = "HIGH"

            title = _xml_first_child_text(rss_item, "title").strip()
            description_raw = _xml_first_child_text(rss_item, "description").strip()
            description = _globenewswire_plain_text(description_raw)
            link = _xml_first_child_text(rss_item, "link").strip() or None
            guid = _xml_first_child_text(rss_item, "guid").strip() or None
            published_at = _parse_rss_date(_xml_first_child_text(rss_item, "pubDate").strip())
            isin_values = _globenewswire_category_values(rss_item, _GLOBENEWSWIRE_ISIN_CATEGORY_DOMAIN)
            keywords = _xml_child_texts(rss_item, "keyword")
            subject = _xml_first_child_text(rss_item, "subject").strip() or None
            language = _xml_first_child_text(rss_item, "language").strip() or None
            issuer = _xml_first_child_text(rss_item, "contributor").strip() or None
            globenewswire_id = _xml_first_child_text(rss_item, "identifier").strip() or None

            if not _within_date_window(
                published_at,
                start_date=start_date,
                end_date=end_date,
                lookback_days=lookback_days,
            ):
                continue

            if not published_at:
                warnings.append({
                    "code": "PUBLISHED_AT_UNAVAILABLE",
                    "message": "Published timestamp unavailable for GlobeNewswire item.",
                    "severity": "warning",
                })

            duplicate_group_id = _make_duplicate_group_id(
                ticker_u,
                title,
                published_at,
                issuer,
                link or guid,
            )
            if duplicate_group_id is None:
                warnings.append({
                    "code": "DEDUPE_WEAK_KEY",
                    "message": "Weak dedupe key for at least one GlobeNewswire item.",
                    "severity": "warning",
                })

            dedupe_key = duplicate_group_id or f"{feed_name}:{link or guid}:{title}:{published_at}"
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            item = {
                "title": title,
                "source": "newswire",
                "originalSource": "GlobeNewswire",
                "sourceType": "newswire",
                "provider": "globenewswire",
                "discoveredVia": "globenewswire_rss",
                "publishedAt": published_at,
                "retrievedAt": retrieved_at,
                "url": link or guid,
                "issuer": issuer,
                "tickers": [ticker_u],
                "eventType": _event_type_from_keywords(f"{title} {description} {subject or ''}"),
                "summary": _short_text(description or title, 240),
                "evidenceText": _short_text(description or title, 180),
                "confidence": "HIGH" if (relevance == "HIGH" and (link or guid)) else "MEDIUM",
                "tickerRelevance": relevance,
                "duplicateGroupId": duplicate_group_id,
                "stockCategories": stock_categories,
                "feedSource": feed_name,
            }
            if isin_values:
                item["isin"] = isin_values[0]
            if subject:
                item["subject"] = subject
            if keywords:
                item["keywords"] = keywords
            if language:
                item["language"] = language
            if globenewswire_id:
                item["globenewswireId"] = globenewswire_id
            items.append(item)
            if len(items) >= max_results:
                return items, warnings, True

    if warnings and not items:
        return items, warnings, False
    return items, warnings, parsed_any_feed
