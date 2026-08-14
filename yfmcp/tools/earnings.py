"""Earnings and transcript MCP tools.

Extracted from server.py as a focused domain module. A few older cross-domain
handlers still live in server.py, so this module lazily resolves them at call
time instead of pulling more code into this refactor.
"""

import asyncio
import datetime
import hashlib
import json
import os
import re as _re
import urllib.error as _urlerror
import urllib.parse as _urlparse
import urllib.request as _urlrequest
from typing import Annotated

import pandas as pd
from pydantic import Field

from yfmcp.app import yfinance_server
from yfmcp.schemas import _TOOL_OUTPUT_SCHEMAS
from yfmcp.envelope import ErrorCode, _wrap_envelope_v2
from yfmcp.validation import _validate_ticker, _sanitize_sec_html
from yfmcp.cache import TTL_EDGAR, _tool_cache
from yfmcp.util import (
    _utc_now_iso,
    _to_iso_utc,
    _filter_paragraphs_by_topics,
    _safe_json_loads,
    _compact_excerpt,
)
from yfmcp.clients.edgar import (
    _edgar_build_filing_urls,
    _edgar_cik_from_accession,
    _edgar_get_html,
    _edgar_list_exhibits_from_index,
    _get_submissions_for_ticker,
)
from yfmcp.clients.market_providers import fetch_alpha_vantage_json
from yfmcp.clients.yahoo_transcripts import (
    fetch_yahoo_quartr_transcript,
    parse_yahoo_transcript_source_url,
    validate_yahoo_transcript_payload_identity,
)
from yfmcp.parsing.html import _strip_html_tags


def _server_attr(name: str):
    import server as _server  # local import avoids module-load circularity
    return getattr(_server, name)


_QUARTER_WORDS = {
    "first": "1", "1st": "1",
    "second": "2", "2nd": "2",
    "third": "3", "3rd": "3",
    "fourth": "4", "4th": "4",
}
_GUIDANCE_CONTEXT_RE = _re.compile(
    r"\b(?:guidance|outlook|expects?|expected|forecast|projects?|target|range)\b",
    flags=_re.IGNORECASE,
)
_REPORTED_CONTEXT_RE = _re.compile(
    r"\b(?:reported|was|were|total(?:ed|led)|generated|delivered|achieved)\b",
    flags=_re.IGNORECASE,
)


def _extract_earnings_period_from_text(text: str) -> dict[str, str | None]:
    """Return an issuer fiscal period explicitly stated in earnings-release text.

    Never infer a fiscal quarter from an SEC filing date.  This deliberately
    returns an unresolved period when a release does not state one.
    """
    normalized = _re.sub(r"\s+", " ", text or " ").strip()
    patterns = (
        _re.compile(
            r"\b(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter"
            r"(?:\s+and\s+(?:full\s+)?fiscal\s+year)?(?:\s+of)?\s+(?:fiscal\s+)?(20\d{2})\b",
            flags=_re.IGNORECASE,
        ),
        _re.compile(r"\bQ([1-4])\s*(?:of\s*)?(?:FY|fiscal\s+year)\s*(20\d{2})\b", flags=_re.IGNORECASE),
        _re.compile(
            r"\bfiscal\s+(20\d{2})\b.{0,80}?\b(first|1st|second|2nd|third|3rd|fourth|4th)\s+quarter\b",
            flags=_re.IGNORECASE,
        ),
    )
    matches = [(index, match) for index, pattern in enumerate(patterns) if (match := pattern.search(normalized))]
    if matches:
        # Prefer the first explicit period in the release. Comparative prior-year
        # figures appear later in the results bullets (AEHR is a concrete case).
        index, match = min(matches, key=lambda item: item[1].start())
        if index == 2:
            year, quarter_word = match.group(1), match.group(2).lower()
            quarter = _QUARTER_WORDS[quarter_word]
        elif index == 1:
            quarter, year = match.group(1), match.group(2)
        else:
            quarter_word, year = match.group(1).lower(), match.group(2)
            quarter = _QUARTER_WORDS[quarter_word]
        return {
            "period": f"FY{year} Q{quarter}",
            "periodStatus": "EX99_TEXT_RESOLVED",
            "periodEvidence": _compact_excerpt(match.group(0), max_len=220),
        }
    return {
        "period": None,
        "periodStatus": "UNRESOLVED",
        "periodEvidence": None,
    }


async def _resolve_earnings_period_from_source(source: dict) -> dict[str, str | None]:
    source_url = str(source.get("url") or "")
    if not source_url:
        return _extract_earnings_period_from_text("")
    if source_url.startswith("https://www.sec.gov/Archives/"):
        html = await _edgar_get_html(source_url, max_bytes=500_000)
    else:
        html = _fetch_public_html(source_url, max_bytes=500_000)
    return _extract_earnings_period_from_text(_strip_html_tags(_sanitize_sec_html(html or "")))


def _extract_reported_text_metric(
    text: str,
    label_pattern: str,
    value_pattern: str,
) -> tuple[float | None, str | None, str | None]:
    """Extract only explicitly reported prose metrics, never guidance wording.

    Free-text release prose cannot be decision-grade without a structured
    period-matched table or XBRL context.  This helper is intentionally strict:
    a sentence needs both an actual-result verb and a value near the requested
    metric label, and guidance/outlook language always wins as an exclusion.
    """
    for sentence in _re.split(r"(?<=[.!?])\s+", _re.sub(r"\s+", " ", text or "")):
        if not _re.search(label_pattern, sentence, flags=_re.IGNORECASE):
            continue
        if _GUIDANCE_CONTEXT_RE.search(sentence) or not _REPORTED_CONTEXT_RE.search(sentence):
            continue
        match = _re.search(
            rf"(?:{label_pattern})\D{{0,100}}?{value_pattern}",
            sentence,
            flags=_re.IGNORECASE,
        )
        if not match:
            continue
        raw = match.group(1)
        value = _scale_number_from_text(raw)
        if value is not None:
            return value, raw, _compact_excerpt(sentence, max_len=220)
    return None, None, None


def _is_paywalled_url(url: str) -> bool:
    host = (_urlparse.urlparse(url).hostname or "").lower()
    blocked = {
        "seekingalpha.com",
        "www.seekingalpha.com",
        "wsj.com",
        "www.wsj.com",
        "bloomberg.com",
        "www.bloomberg.com",
    }
    return host in blocked


def _classify_earnings_source_url(url: str) -> tuple[str | None, str | None]:
    if not isinstance(url, str) or not url.strip():
        return None, "source_url must be a non-empty string"
    parsed = _urlparse.urlparse(url.strip())
    if parsed.scheme != "https":
        return None, "source_url must use https"
    if _is_paywalled_url(url):
        return None, "source_url appears paywalled and is not allowed"
    if url.startswith("https://www.sec.gov/Archives/"):
        return "sec_8k", None
    return "company_ir", None


def _fetch_public_html(url: str, max_bytes: int = 3_000_000) -> str | None:
    req = _urlrequest.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; yahoo-finance-mcp-earnings/1.0)"},
        method="GET",
    )
    try:
        with _urlrequest.urlopen(req, timeout=60) as resp:
            data = resp.read(max_bytes)
            return data.decode("utf-8", errors="ignore")
    except (_urlerror.URLError, ValueError):
        return None


def _scale_number_from_text(raw: str) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    if not s:
        return None
    m = _re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not m:
        return None
    n = float(m.group(0))
    low = s.lower()
    if "billion" in low or low.endswith("b") or " bn" in low:
        n *= 1_000_000_000
    elif "million" in low or low.endswith("m"):
        n *= 1_000_000
    elif "thousand" in low or low.endswith("k"):
        n *= 1_000
    return n


def _first_sentence_for_topic(text: str, topic: str) -> str | None:
    topic_l = topic.lower()
    for sent in _re.split(r"(?<=[.!?])\s+", text):
        if topic_l in sent.lower():
            return _compact_excerpt(sent, max_len=220)
    return None


async def _resolve_ex991_url(accession_number: str, cik: int | None) -> str | None:
    """Resolve the EX-99.1 exhibit URL from an 8-K filing index.

    Returns the full SEC URL for the EX-99.1 document, or None if not found.
    """
    if not accession_number or cik is None:
        return None
    index_url, _ = _edgar_build_filing_urls(cik, accession_number, None)
    exhibits = await _edgar_list_exhibits_from_index(index_url)
    for ex in exhibits:
        doc_type = str(ex.get("type") or "").upper()
        if doc_type in ("EX-99.1", "EX-99", "99.1"):
            doc_name = ex.get("document")
            if doc_name:
                accession_nodash = accession_number.replace("-", "")
                return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{doc_name}"
    return None


async def _resolve_latest_earnings_sec_source(ticker: str) -> dict | None:
    raw = await _server_attr("list_sec_company_filings")(ticker=ticker, filing_type="8-K", limit=10)
    payload = _safe_json_loads(raw)
    filings = payload.get("filings") if isinstance(payload.get("filings"), list) else []
    try:
        issuer_cik = int(str(payload.get("cik") or "").lstrip("0"))
    except (TypeError, ValueError):
        issuer_cik = None
    if not filings:
        return None
    for filing in filings:
        if not isinstance(filing, dict):
            continue
        doc_url = str(filing.get("documentUrl") or "")
        if not doc_url.startswith("https://www.sec.gov/Archives/"):
            continue
        accn = filing.get("accessionNumber")
        # Try to resolve EX-99.1 exhibit URL (contains the actual press release)
        ex991_url = None
        if accn:
            # The accession prefix identifies the filing agent, not necessarily
            # the issuer. SEC archive paths must use the issuer CIK returned by
            # the submission response (AEHR is a concrete counterexample).
            cik = issuer_cik
            if cik:
                ex991_url = await _resolve_ex991_url(accn, cik)
        source_url = ex991_url or doc_url
        source_type = "sec_8k_ex991" if ex991_url else "sec_8k"
        return {
            "sourceType": source_type,
            "url": source_url,
            "primaryDocumentUrl": doc_url,
            "filingDate": filing.get("filingDate"),
            "acceptedAt": filing.get("acceptedAt"),
            "accessionNumber": accn,
            "confidence": "HIGH",
        }
    return None


async def _resolve_latest_earnings_release(ticker: str) -> dict:
    sec = await _resolve_latest_earnings_sec_source(ticker)
    if sec:
        reporting_ts = _to_iso_utc(sec.get("acceptedAt")) or _to_iso_utc(sec.get("filingDate"))
        return {
            "ticker": ticker.upper(),
            "eventType": "earnings_release",
            "period": "latest",
            "periodStatus": "UNRESOLVED",
            "periodEvidence": None,
            "reportedAt": reporting_ts,
            "sources": [sec],
            "confidence": "HIGH",
            "warnings": [],
        }

    yahoo_url = f"https://finance.yahoo.com/quote/{ticker.upper()}/analysis"
    cal_raw = await _server_attr("get_calendar")(ticker=ticker.upper())
    cal = _safe_json_loads(cal_raw)
    earnings_dates = (((cal.get("calendar") or {}).get("earnings") or {}).get("earningsDate") or [])
    published = earnings_dates[0] if isinstance(earnings_dates, list) and earnings_dates else None
    if published:
        return {
            "ticker": ticker.upper(),
            "eventType": "earnings_release",
            "period": "latest",
            "periodStatus": "UNRESOLVED",
            "periodEvidence": None,
            "reportedAt": _to_iso_utc(published),
            "sources": [
                {
                    "sourceType": "yahoo_estimate",
                    "url": yahoo_url,
                    "publishedAt": _to_iso_utc(published),
                    "retrievedAt": _utc_now_iso(),
                    "confidence": "MEDIUM",
                }
            ],
            "confidence": "MEDIUM",
            "warnings": [{"code": "SEC_8K_NOT_FOUND", "message": "SEC 8-K earnings release source not found"}],
        }

    return {
        "ticker": ticker.upper(),
        "eventType": "earnings_release",
        "period": "latest",
        "periodStatus": "UNRESOLVED",
        "periodEvidence": None,
        "reportedAt": None,
        "sources": [],
        "confidence": "NOT_FOUND",
        "warnings": [],
    }


@yfinance_server.tool(
    name="get_latest_earnings_release",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_latest_earnings_release"],
    description="Find the latest public earnings release evidence. Fiscal period is returned only when explicit release text resolves it; otherwise it remains unresolved.",
)
async def get_latest_earnings_release(ticker: str, period: str = "latest") -> str:
    _ = period  # reserved for future explicit periods
    err = _validate_ticker(ticker)
    if err:
        return _wrap_envelope_v2("get_latest_earnings_release", None, error=err, error_code=ErrorCode.INPUT_VALIDATION_ERROR)
    data = await _resolve_latest_earnings_release(ticker)
    sources = data.get("sources") if isinstance(data.get("sources"), list) else []
    source = sources[0] if sources and isinstance(sources[0], dict) else None
    if source and str(source.get("sourceType") or "") != "yahoo_estimate":
        data.update(await _resolve_earnings_period_from_source(source))
    return _wrap_envelope_v2("get_latest_earnings_release", data)


@yfinance_server.tool(
    name="index_earnings_release",
    output_schema=_TOOL_OUTPUT_SCHEMAS["index_earnings_release"],
    description="Build a compact section/table index for the latest public earnings release to support deterministic extraction.",
)
async def index_earnings_release(ticker: str, period: str = "latest", source_url: str | None = None) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _wrap_envelope_v2("index_earnings_release", None, error=err, error_code=ErrorCode.INPUT_VALIDATION_ERROR)
    source_type = None
    source_meta: dict = {}
    if source_url:
        source_type, source_err = _classify_earnings_source_url(source_url)
        if source_err:
            return _wrap_envelope_v2("index_earnings_release", None, error=source_err, error_code=ErrorCode.INPUT_VALIDATION_ERROR)
        source_meta = {"sourceType": source_type, "url": source_url}
    else:
        latest = await _resolve_latest_earnings_release(ticker)
        sources = latest.get("sources") if isinstance(latest.get("sources"), list) else []
        src = sources[0] if sources and isinstance(sources[0], dict) else {}
        source_type = str(src.get("sourceType") or "")
        source_url = str(src.get("url") or "")
        source_meta = src

    if not source_url:
        data = {
            "ticker": ticker.upper(),
            "period": period,
            "source": {"sourceType": source_type or "unknown", "url": None},
            "index": {"sections": [], "tables": [], "keywordMap": {}},
        }
        return _wrap_envelope_v2("index_earnings_release", data, warnings=[{"code": "SOURCE_NOT_FOUND", "message": "No public earnings release source found"}])

    cache_id = str(source_meta.get("accessionNumber") or source_url)
    cache_key = f"earnidx:{ticker.upper()}:{cache_id}"
    cached = _tool_cache.get(cache_key)
    if cached is not None:
        return cached[0]

    html = await _edgar_get_html(source_url, max_bytes=5_000_000) if source_url.startswith(
        "https://www.sec.gov/Archives/"
    ) else _fetch_public_html(source_url)
    if not html:
        return _wrap_envelope_v2("index_earnings_release", None, error=f"Failed to fetch source: {source_url}", error_code=ErrorCode.PROVIDER_ERROR)

    idx = _server_attr("_build_filing_index_from_html")(_sanitize_sec_html(html))
    period_info = _extract_earnings_period_from_text(_strip_html_tags(_sanitize_sec_html(html)))
    out_data = {
        "ticker": ticker.upper(),
        "period": period_info["period"] or period,
        "periodStatus": period_info["periodStatus"],
        "periodEvidence": period_info["periodEvidence"],
        "source": {
            "sourceType": source_type or source_meta.get("sourceType") or "company_ir",
            "url": source_url,
            "publishedAt": source_meta.get("publishedAt"),
            "retrievedAt": _utc_now_iso(),
            "filingDate": source_meta.get("filingDate"),
            "acceptedAt": source_meta.get("acceptedAt"),
            "accessionNumber": source_meta.get("accessionNumber"),
        },
        "index": idx,
    }
    result = _wrap_envelope_v2("index_earnings_release", out_data, meta_extra={"cacheKey": cache_key, "cacheTtlHours": 24})
    _tool_cache.set(cache_key, result, TTL_EDGAR)
    return result


@yfinance_server.tool(
    name="extract_earnings_metrics",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_earnings_metrics"],
    description="Extract reported earnings metrics from public earnings sources. EX-99 prose or unscoped iXBRL values remain non-decision-grade until their period is structurally matched.",
)
async def extract_earnings_metrics(
    ticker: str,
    period: str = "latest",
    source_preference: list[str] | None = None,
) -> str:
    _ = source_preference or ["sec_8k", "company_ir", "10-q", "yahoo"]
    release = await _resolve_latest_earnings_release(ticker)
    default_metric = lambda unit: {  # noqa: E731
        "value": None,
        "unit": unit,
        "confidence": "NOT_DISCLOSED",
        "evidence": None,
    }
    metrics: dict = {
        "revenue": default_metric("USD"),
        "epsDiluted": default_metric("USD/share"),
        "grossMargin": {
            "valueRatio": None,
            "valuePct": None,
            "rawValue": None,
            "confidence": "NOT_DISCLOSED",
            "evidence": None,
        },
        "operatingIncome": default_metric("USD"),
        "freeCashFlow": default_metric("USD"),
        "capex": default_metric("USD"),
    }
    evidence_items: list[dict] = []
    warnings: list[dict] = []
    sources = release.get("sources") if isinstance(release.get("sources"), list) else []
    src = sources[0] if sources and isinstance(sources[0], dict) else {}
    src_url = str(src.get("url") or "")
    src_type = str(src.get("sourceType") or "yahoo")
    src_published = _to_iso_utc(src.get("filingDate") or src.get("publishedAt"))
    retrieved_at = _utc_now_iso()
    period_info = _extract_earnings_period_from_text("")

    if src_url and src_url.startswith("https://www.sec.gov/Archives/"):
        html = await _edgar_get_html(src_url, max_bytes=5_000_000)
        text = _strip_html_tags(_sanitize_sec_html(html or ""))
        period_info = _extract_earnings_period_from_text(text)
        revenue_val, revenue_raw, revenue_ex = _extract_reported_text_metric(
            text, r"net sales|revenue(?:s)?", r"\$\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)",
        )
        eps_val, eps_raw, eps_ex = _extract_reported_text_metric(
            text, r"diluted (?:earnings per share|eps)|eps \(diluted\)", r"\$\s*([0-9]+(?:\.[0-9]+)?)",
        )
        gm_val, gm_raw, gm_ex = _extract_reported_text_metric(
            text, r"gross margin", r"([0-9]{1,2}(?:\.[0-9]+)?)\s*%",
        )
        op_val, op_raw, op_ex = _extract_reported_text_metric(
            text, r"operating income", r"\$\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)",
        )
        fcf_val, fcf_raw, fcf_ex = _extract_reported_text_metric(
            text, r"free cash flow", r"\$\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)",
        )
        capex_val, capex_raw, capex_ex = _extract_reported_text_metric(
            text, r"capital expenditures|capex", r"\$\s*([0-9][0-9,.\s]*(?:billion|million|thousand|bn|m|k)?)",
        )

        def _ev(excerpt: str | None) -> dict | None:
            if not excerpt:
                return None
            return {
                "url": src_url,
                "sourceType": src_type,
                "publishedAt": src_published,
                "retrievedAt": retrieved_at,
                "excerpt": excerpt,
            }

        if revenue_val is not None:
            metrics["revenue"] = {
                "value": revenue_val,
                "unit": "USD",
                "rawValue": revenue_raw,
                "confidence": "LOW",
                "extractionMethod": "EX99_TEXT_CONTEXT",
                "periodMatch": bool(period_info["period"]),
                "evidence": _ev(revenue_ex),
            }
        if eps_val is not None:
            metrics["epsDiluted"] = {
                "value": eps_val,
                "unit": "USD/share",
                "rawValue": f"${eps_raw}" if eps_raw else None,
                "confidence": "LOW",
                "extractionMethod": "EX99_TEXT_CONTEXT",
                "periodMatch": bool(period_info["period"]),
                "evidence": _ev(eps_ex),
            }
        if gm_val is not None:
            gm_pct = float(gm_val)
            metrics["grossMargin"] = {
                "valueRatio": round(gm_pct / 100.0, 6),
                "valuePct": gm_pct,
                "rawValue": f"{gm_raw}%" if gm_raw and "%" not in gm_raw else gm_raw,
                "confidence": "LOW",
                "extractionMethod": "EX99_TEXT_CONTEXT",
                "periodMatch": bool(period_info["period"]),
                "evidence": _ev(gm_ex),
            }
        if op_val is not None:
            metrics["operatingIncome"] = {
                "value": op_val,
                "unit": "USD",
                "rawValue": op_raw,
                "confidence": "LOW",
                "extractionMethod": "EX99_TEXT_CONTEXT",
                "periodMatch": bool(period_info["period"]),
                "evidence": _ev(op_ex),
            }
        if fcf_val is not None:
            metrics["freeCashFlow"] = {
                "value": fcf_val,
                "unit": "USD",
                "rawValue": fcf_raw,
                "confidence": "LOW",
                "extractionMethod": "EX99_TEXT_CONTEXT",
                "periodMatch": bool(period_info["period"]),
                "evidence": _ev(fcf_ex),
            }
        if capex_val is not None:
            metrics["capex"] = {
                "value": capex_val,
                "unit": "USD",
                "rawValue": capex_raw,
                "confidence": "LOW",
                "extractionMethod": "EX99_TEXT_CONTEXT",
                "periodMatch": bool(period_info["period"]),
                "evidence": _ev(capex_ex),
            }
        for key in ("revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex"):
            ev = metrics[key].get("evidence") if isinstance(metrics[key], dict) else None
            conf = str(metrics[key].get("confidence") or "")
            if conf in {"HIGH", "LOW"} and isinstance(ev, dict):
                evidence_items.append(ev)
        if any(str((metrics[k] or {}).get("confidence")) == "LOW" for k in metrics):
            warnings.append({
                "code": "TEXT_METRIC_VERIFY_REQUIRED",
                "message": "EX-99 prose extraction is contextual but not decision-grade without a structured period-matched table or XBRL fact.",
            })
    else:
        warnings.append({"code": "PUBLIC_RELEASE_NOT_FOUND", "message": "No SEC 8-K earnings release source available"})

    overall_conf = "NOT_DISCLOSED"
    if any(str((metrics[k] or {}).get("confidence")) == "HIGH" for k in ("revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex")):
        overall_conf = "HIGH"
    elif any(str((metrics[k] or {}).get("confidence")) == "LOW" for k in ("revenue", "epsDiluted", "grossMargin", "operatingIncome", "freeCashFlow", "capex")):
        overall_conf = "LOW"
    elif release.get("confidence") in {"MEDIUM", "LOW"}:
        overall_conf = str(release.get("confidence"))

    return _wrap_envelope_v2("extract_earnings_metrics", {
        "ticker": ticker.upper(),
        "eventType": "earnings_release",
        "period": period_info["period"] or release.get("period") or period,
        "periodStatus": period_info["periodStatus"],
        "periodEvidence": period_info["periodEvidence"],
        "reportedAt": release.get("reportedAt"),
        "source": src_type if src_type else "yahoo",
        "metrics": metrics,
        "evidence": evidence_items,
        "confidence": overall_conf,
    }, warnings=warnings)
@yfinance_server.tool(
    name="extract_guidance",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_guidance"],
    description="Extract company-provided guidance ranges from the resolved official SEC earnings exhibit, including 'between X and Y' wording.",
)
async def extract_guidance(ticker: str, period: str = "latest") -> str:
    release = await _resolve_latest_earnings_release(ticker)
    base = {
        "revenue": {"status": "NOT_DISCLOSED", "low": None, "high": None, "midpoint": None, "unit": "USD", "evidence": []},
        "grossMargin": {"status": "NOT_DISCLOSED", "lowPct": None, "highPct": None, "midpointPct": None, "evidence": []},
        "eps": {"status": "NOT_DISCLOSED", "low": None, "high": None, "midpoint": None, "unit": "USD/share", "evidence": []},
    }
    sources = release.get("sources") if isinstance(release.get("sources"), list) else []
    src = sources[0] if sources and isinstance(sources[0], dict) else {}
    src_url = str(src.get("url") or "")
    if not src_url.startswith("https://www.sec.gov/Archives/"):
        return _wrap_envelope_v2("extract_guidance", {"ticker": ticker.upper(), "period": release.get("period") or period, "guidance": base, "confidence": "NOT_DISCLOSED"})

    html = await _edgar_get_html(src_url, max_bytes=5_000_000)
    text = _strip_html_tags(_sanitize_sec_html(html or ""))
    patterns = {
        "revenue": _re.search(r"(?:expects|guidance|outlook)[^.\n]{0,120}revenue[^$]{0,25}\$?\s*([0-9.,]+(?:\s*(?:billion|million|thousand|bn|m|k))?)\s*(?:to|and|[-–—])\s*\$?\s*([0-9.,]+(?:\s*(?:billion|million|thousand|bn|m|k))?)", text, flags=_re.IGNORECASE),
        "grossMargin": _re.search(r"gross margin[^0-9]{0,20}([0-9]{1,2}(?:\.[0-9]+)?)\s*%\s*(?:to|and|[-–—])\s*([0-9]{1,2}(?:\.[0-9]+)?)\s*%", text, flags=_re.IGNORECASE),
        "eps": _re.search(r"(?:expects|guidance|outlook)[^.\n]{0,120}(?:eps|earnings per share)[^$]{0,25}\$?\s*([0-9]+(?:\.[0-9]+)?)\s*(?:to|and|[-–—])\s*\$?\s*([0-9]+(?:\.[0-9]+)?)", text, flags=_re.IGNORECASE),
    }
    if patterns["revenue"]:
        lo = _scale_number_from_text(patterns["revenue"].group(1))
        hi = _scale_number_from_text(patterns["revenue"].group(2))
        if lo is not None and hi is not None:
            base["revenue"] = {
                "status": "FOUND",
                "low": lo,
                "high": hi,
                "midpoint": (lo + hi) / 2.0,
                "unit": "USD",
                "evidence": [{"url": src_url, "sourceType": src.get("sourceType", "sec_8k"), "publishedAt": _to_iso_utc(src.get("filingDate")), "retrievedAt": _utc_now_iso(), "excerpt": _compact_excerpt(patterns["revenue"].group(0))}],
            }
    if patterns["grossMargin"]:
        lo = float(patterns["grossMargin"].group(1))
        hi = float(patterns["grossMargin"].group(2))
        base["grossMargin"] = {
            "status": "FOUND",
            "lowPct": lo,
            "highPct": hi,
            "midpointPct": (lo + hi) / 2.0,
            "evidence": [{"url": src_url, "sourceType": src.get("sourceType", "sec_8k"), "publishedAt": _to_iso_utc(src.get("filingDate")), "retrievedAt": _utc_now_iso(), "excerpt": _compact_excerpt(patterns["grossMargin"].group(0))}],
        }
    if patterns["eps"]:
        lo = float(patterns["eps"].group(1))
        hi = float(patterns["eps"].group(2))
        base["eps"] = {
            "status": "FOUND",
            "low": lo,
            "high": hi,
            "midpoint": (lo + hi) / 2.0,
            "unit": "USD/share",
            "evidence": [{"url": src_url, "sourceType": src.get("sourceType", "sec_8k"), "publishedAt": _to_iso_utc(src.get("filingDate")), "retrievedAt": _utc_now_iso(), "excerpt": _compact_excerpt(patterns["eps"].group(0))}],
        }
    found = any(base[k]["status"] == "FOUND" for k in ("revenue", "grossMargin", "eps"))
    return _wrap_envelope_v2("extract_guidance", {
        "ticker": ticker.upper(),
        "period": release.get("period") or period,
        "guidance": base,
        "confidence": "HIGH" if found else "NOT_DISCLOSED",
    })


@yfinance_server.tool(
    name="extract_management_commentary",
    output_schema=_TOOL_OUTPUT_SCHEMAS["extract_management_commentary"],
    description="Extract neutral, topic-specific management commentary snippets from public earnings release sources.",
)
async def extract_management_commentary(ticker: str, period: str = "latest", topics: list[str] | None = None) -> str:
    topic_list = [str(t).strip() for t in (topics or []) if str(t).strip()]
    release = await _resolve_latest_earnings_release(ticker)
    sources = release.get("sources") if isinstance(release.get("sources"), list) else []
    src = sources[0] if sources and isinstance(sources[0], dict) else {}
    src_url = str(src.get("url") or "")
    text = ""
    if src_url.startswith("https://www.sec.gov/Archives/"):
        html = await _edgar_get_html(src_url, max_bytes=5_000_000)
        text = _strip_html_tags(_sanitize_sec_html(html or ""))
    elif src_url:
        text = _strip_html_tags(_sanitize_sec_html(_fetch_public_html(src_url) or ""))
    out_topics: list[dict] = []
    for topic in topic_list:
        excerpt = _first_sentence_for_topic(text, topic) if text else None
        if excerpt:
            out_topics.append({
                "topic": topic,
                "status": "FOUND",
                "summary": excerpt,
                "evidence": [{
                    "sourceType": src.get("sourceType", "company_ir"),
                    "url": src_url,
                    "publishedAt": _to_iso_utc(src.get("filingDate") or src.get("publishedAt")),
                    "retrievedAt": _utc_now_iso(),
                    "excerpt": excerpt[:240],
                }],
                "confidence": "MEDIUM" if src.get("sourceType") != "sec_8k" else "HIGH",
            })
        else:
            out_topics.append({
                "topic": topic,
                "status": "NOT_FOUND",
                "summary": "",
                "evidence": [],
                "confidence": "LOW",
            })
    return _wrap_envelope_v2("extract_management_commentary", {"ticker": ticker.upper(), "period": release.get("period") or period, "topics": out_topics})


@yfinance_server.tool(
    name="compare_earnings_actual_vs_estimate",
    output_schema=_TOOL_OUTPUT_SCHEMAS["compare_earnings_actual_vs_estimate"],
    description="Compare official-release actuals with Yahoo's historical estimate row. The official fiscal label remains period/reportedPeriod; estimatePeriod and reportedDate identify the Yahoo row. Read periodAlignmentStatus before using cross-source revenue comparisons. Returns epsDelta and omits percentage surprise for near-zero estimates.",
)
async def compare_earnings_actual_vs_estimate(ticker: str, period: str = "latest") -> str:
    raw_metrics = _safe_json_loads(await extract_earnings_metrics(ticker=ticker, period=period))
    # extract_earnings_metrics now returns Envelope V2; unwrap data
    metrics = raw_metrics.get("data") if isinstance(raw_metrics.get("data"), dict) else raw_metrics
    ea = _safe_json_loads(await _server_attr("get_earnings_analysis")(ticker=ticker))

    def _num(v: object) -> float | None:
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        try:
            return float(v)
        except Exception:
            return None

    def _row_period(row: dict | None) -> str | None:
        if not row:
            return None
        for key in ("reportedPeriod", "period", "quarter", "index"):
            val = row.get(key)
            if val is not None and str(val).strip():
                return str(val)
        return None

    def _row_date(row: dict | None) -> str | None:
        if not row:
            return None
        for key in ("reportedDate", "quarter", "date", "earningsDate", "index"):
            val = row.get(key)
            if val is None:
                continue
            if isinstance(val, (int, float)):
                ts = float(val)
                if ts > 1_000_000_000_000:
                    ts /= 1000.0
                try:
                    return datetime.datetime.fromtimestamp(ts, tz=datetime.UTC).date().isoformat()
                except Exception:
                    continue
            parsed = pd.to_datetime(val, utc=True, errors="coerce")
            if not pd.isna(parsed):
                return parsed.date().isoformat()
            text = str(val).strip()
            if text:
                return text
        return None

    hist = ea.get("earningsHistory") if isinstance(ea.get("earningsHistory"), list) else []
    reported_rows = [row for row in hist if isinstance(row, dict) and _num(row.get("epsActual")) is not None]

    def _sort_key(row: dict) -> tuple[int, str]:
        date_text = _row_date(row)
        parsed = pd.to_datetime(date_text, utc=True, errors="coerce") if date_text else pd.NaT
        if pd.isna(parsed):
            return (0, "")
        return (1, parsed.isoformat())

    selected = sorted(reported_rows, key=_sort_key, reverse=True)[0] if reported_rows else None
    source_period = str(metrics.get("period") or "").strip()
    if not source_period or source_period.lower() == "latest":
        source_period = None
    estimate_period = _row_period(selected)
    reported_date = _row_date(selected)
    if source_period and estimate_period:
        if source_period == estimate_period:
            period_alignment_status = "MATCHED"
        elif _re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:T.*)?", estimate_period):
            period_alignment_status = "UNVERIFIED"
        else:
            period_alignment_status = "MISMATCH"
    else:
        period_alignment_status = "INCOMPLETE"

    actual_rev = (((metrics.get("metrics") or {}).get("revenue") or {}).get("value")
                  if isinstance((metrics.get("metrics") or {}).get("revenue"), dict) else None)
    actual_eps = _num(selected.get("epsActual")) if selected else None
    if actual_eps is None:
        actual_eps = (((metrics.get("metrics") or {}).get("epsDiluted") or {}).get("value")
                      if isinstance((metrics.get("metrics") or {}).get("epsDiluted"), dict) else None)
    eps_est = _num(selected.get("epsEstimate")) if selected else None

    revenue_est = None
    rev_est_arr = ea.get("revenueEstimate") if isinstance(ea.get("revenueEstimate"), list) else []
    for row in rev_est_arr:
        if not isinstance(row, dict):
            continue
        row_period = str(row.get("period") or row.get("reportedPeriod") or "")
        row_date = _row_date(row)
        if (
            (estimate_period and row_period == estimate_period)
            or (reported_date and row_date == reported_date)
        ):
            revenue_est = _num(row.get("avg"))
            break

    warnings: list[dict] = []
    result = {
        "ticker": ticker.upper(),
        "period": source_period or estimate_period or period,
        "reportedPeriod": source_period or estimate_period,
        "reportedDate": reported_date,
        "releasePublishedAt": metrics.get("reportedAt"),
        "estimatePeriod": estimate_period,
        "periodAlignmentStatus": period_alignment_status,
        "actual": {
            "revenue": {
                **(((metrics.get("metrics") or {}).get("revenue") or {}) if isinstance((metrics.get("metrics") or {}).get("revenue"), dict) else {}),
                "value": actual_rev,
                "unit": "USD",
                "period": source_period,
                "decisionGrade": bool((((metrics.get("metrics") or {}).get("revenue") or {}).get("decisionGrade")) if isinstance((metrics.get("metrics") or {}).get("revenue"), dict) else False),
            },
            "eps": {"value": actual_eps, "unit": "USD/share", "source": "yahoo", "period": estimate_period or reported_date, "decisionGrade": False},
        },
        "estimate": {
            "revenue": {"value": revenue_est, "unit": "USD", "source": "yahoo", "period": estimate_period or reported_date, "decisionGrade": False},
            "eps": {"value": eps_est, "unit": "USD/share", "source": "yahoo", "period": estimate_period or reported_date, "decisionGrade": False},
        },
        "surprise": {
            "revenueSurprisePct": None,
            "epsSurprisePct": None,
        },
        "confidence": "NOT_DISCLOSED",
    }

    if selected is None:
        warnings.append({
            "code": "NO_REPORTED_QUARTER",
            "message": "Yahoo earningsHistory did not include a quarter with non-null actual EPS.",
        })
        return _wrap_envelope_v2("compare_earnings_actual_vs_estimate", result, warnings=warnings)

    if source_period and period_alignment_status != "MATCHED":
        warnings.append({
            "code": "PERIOD_ALIGNMENT_MISMATCH" if period_alignment_status == "MISMATCH" else "PERIOD_ALIGNMENT_UNVERIFIED",
            "message": (
                "The official-release period differs from the selected Yahoo estimate period; cross-source revenue surprise is omitted."
                if period_alignment_status == "MISMATCH"
                else "Yahoo identifies the selected estimate row by date rather than the official fiscal-period label; cross-source revenue surprise is omitted unless the periods can be matched."
            ),
        })

    if period_alignment_status == "MATCHED" and actual_rev is not None and revenue_est not in (None, 0):
        try:
            result["surprise"]["revenueSurprisePct"] = round(((float(actual_rev) - float(revenue_est)) / abs(float(revenue_est))) * 100, 2)
        except Exception:
            result["surprise"]["revenueSurprisePct"] = None
    elif actual_rev is not None and period_alignment_status == "MATCHED":
        warnings.append({
            "code": "REVENUE_ESTIMATE_UNAVAILABLE",
            "message": "No Yahoo revenue estimate was available for the selected reported quarter.",
        })

    if actual_eps is None or eps_est in (None, 0):
        warnings.append({
            "code": "EPS_ESTIMATE_UNAVAILABLE",
            "message": "No Yahoo EPS estimate was available for the selected reported quarter.",
        })
        return _wrap_envelope_v2("compare_earnings_actual_vs_estimate", result, warnings=warnings)

    eps_delta = float(actual_eps) - float(eps_est)
    result["surprise"]["epsDelta"] = round(eps_delta, 4)
    if abs(float(eps_est)) < 0.02:
        warnings.append({
            "code": "EPS_NEAR_ZERO_ESTIMATE_BASE",
            "message": "EPS percentage surprise is omitted because the absolute estimate is below $0.02 per share; use epsDelta instead.",
        })
        result["confidence"] = "MEDIUM"
        return _wrap_envelope_v2("compare_earnings_actual_vs_estimate", result, warnings=warnings)

    try:
        result["surprise"]["epsSurprisePct"] = round((eps_delta / abs(float(eps_est))) * 100, 2)
    except Exception:
        return _wrap_envelope_v2("compare_earnings_actual_vs_estimate", result, warnings=warnings)
    result["confidence"] = "HIGH" if result["surprise"]["revenueSurprisePct"] is not None else "MEDIUM"
    return _wrap_envelope_v2("compare_earnings_actual_vs_estimate", result, warnings=warnings)


# ---------------------------------------------------------------------------
# Phase 5B: Earnings Call Transcript & SEC Exhibit Tools
# ---------------------------------------------------------------------------


@yfinance_server.tool(
    name="list_sec_filing_exhibits",
    output_schema=_TOOL_OUTPUT_SCHEMAS["list_sec_filing_exhibits"],
    description="List all exhibits/documents attached to a specific SEC filing by accession number.",
)
async def list_sec_filing_exhibits(ticker: str, accessionNumber: str) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _wrap_envelope_v2("list_sec_filing_exhibits", None, error=err, error_code=ErrorCode.INPUT_VALIDATION_ERROR)
    if not accessionNumber or not accessionNumber.strip():
        return _wrap_envelope_v2("list_sec_filing_exhibits", None, error="accessionNumber is required.", error_code=ErrorCode.INPUT_VALIDATION_ERROR)

    cik = _edgar_cik_from_accession(accessionNumber)
    if not cik:
        # Fall back to ticker-based CIK resolution
        cik_padded, _ = await _get_submissions_for_ticker(ticker)
        cik = int(cik_padded) if cik_padded else None
    if not cik:
        return _wrap_envelope_v2("list_sec_filing_exhibits", None, error=f"Could not resolve CIK for ticker '{ticker}'.", error_code=ErrorCode.TICKER_NOT_FOUND)

    index_url, _ = _edgar_build_filing_urls(cik, accessionNumber, None)
    exhibits = await _edgar_list_exhibits_from_index(index_url)
    return _wrap_envelope_v2("list_sec_filing_exhibits", {
        "ticker": ticker.upper(),
        "accessionNumber": accessionNumber,
        "indexUrl": index_url,
        "exhibits": exhibits,
    })


@yfinance_server.tool(
    name="get_sec_filing_exhibit_content",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_sec_filing_exhibit_content"],
    description="Fetch and return the text content of a specific exhibit from an SEC filing. Supports topic-based paragraph filtering to reduce token usage.",
)
async def get_sec_filing_exhibit_content(ticker: str, accessionNumber: str, fileName: str, topics: list[str] | None = None) -> str:
    err = _validate_ticker(ticker)
    if err:
        return _wrap_envelope_v2("get_sec_filing_exhibit_content", None, error=err, error_code=ErrorCode.INPUT_VALIDATION_ERROR)
    if not accessionNumber or not accessionNumber.strip():
        return _wrap_envelope_v2("get_sec_filing_exhibit_content", None, error="accessionNumber is required.", error_code=ErrorCode.INPUT_VALIDATION_ERROR)
    if not fileName or not fileName.strip():
        return _wrap_envelope_v2("get_sec_filing_exhibit_content", None, error="fileName is required.", error_code=ErrorCode.INPUT_VALIDATION_ERROR)

    cik = _edgar_cik_from_accession(accessionNumber)
    if not cik:
        cik_padded, _ = await _get_submissions_for_ticker(ticker)
        cik = int(cik_padded) if cik_padded else None
    if not cik:
        return _wrap_envelope_v2("get_sec_filing_exhibit_content", None, error=f"Could not resolve CIK for ticker '{ticker}'.", error_code=ErrorCode.TICKER_NOT_FOUND)

    accession_nodash = accessionNumber.replace("-", "")
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/{fileName}"
    html = await _edgar_get_html(doc_url, max_bytes=5_000_000)
    if not html:
        return _wrap_envelope_v2("get_sec_filing_exhibit_content", None, error=f"Could not fetch exhibit '{fileName}'.", error_code="FETCH_ERROR")

    clean_text = _strip_html_tags(_sanitize_sec_html(html))

    warnings: list[dict] = []
    if topics:
        filtered = _filter_paragraphs_by_topics(clean_text, topics)
        if not filtered:
            warnings.append({"code": "NO_TOPIC_MATCHES", "message": f"No paragraphs matched the provided topics: {topics}"})
        return _wrap_envelope_v2("get_sec_filing_exhibit_content", {
            "ticker": ticker.upper(),
            "accessionNumber": accessionNumber,
            "fileName": fileName,
            "documentUrl": doc_url,
            "filteredByTopics": topics,
            "matchedParagraphs": filtered,
            "totalTextLength": len(clean_text),
        }, warnings=warnings)

    # No topics: return full text truncated at safe threshold
    max_chars = 50_000
    truncated = len(clean_text) > max_chars
    if truncated:
        warnings.append({"code": "TEXT_TRUNCATED", "message": f"Text truncated from {len(clean_text)} to {max_chars} characters."})
    return _wrap_envelope_v2("get_sec_filing_exhibit_content", {
        "ticker": ticker.upper(),
        "accessionNumber": accessionNumber,
        "fileName": fileName,
        "documentUrl": doc_url,
        "filteredByTopics": None,
        "text": clean_text[:max_chars],
        "totalTextLength": len(clean_text),
        "truncated": truncated,
    }, warnings=warnings)


@yfinance_server.tool(
    name="parse_public_transcript",
    output_schema=_TOOL_OUTPUT_SCHEMAS["parse_public_transcript"],
    description="Parse a public transcript from an https URL or caller-supplied raw_text, with optional topic filtering.",
)
async def parse_public_transcript(url: str = "", topics: list[str] | None = None, raw_text: str | None = None) -> str:
    # If raw_text is provided, parse it directly (bypass URL fetching)
    if raw_text and str(raw_text).strip():
        clean_text = _strip_html_tags(_sanitize_sec_html(str(raw_text)))
        warnings: list[dict] = []
        if topics:
            filtered = _filter_paragraphs_by_topics(clean_text, topics)
            if not filtered:
                warnings.append({"code": "NO_TOPIC_MATCHES", "message": f"No paragraphs matched the provided topics: {topics}"})
            return _wrap_envelope_v2("parse_public_transcript", {
                "url": None,
                "source": "raw_text",
                "filteredByTopics": topics,
                "matchedParagraphs": filtered,
                "totalTextLength": len(clean_text),
            }, warnings=warnings)
        max_chars = 50_000
        truncated = len(clean_text) > max_chars
        if truncated:
            warnings.append({"code": "TEXT_TRUNCATED", "message": f"Text truncated from {len(clean_text)} to {max_chars} characters."})
        return _wrap_envelope_v2("parse_public_transcript", {
            "url": None,
            "source": "raw_text",
            "filteredByTopics": None,
            "text": clean_text[:max_chars],
            "totalTextLength": len(clean_text),
            "truncated": truncated,
        }, warnings=warnings)

    if not url or not url.startswith("https://"):
        return _wrap_envelope_v2("parse_public_transcript", None, error="A valid https:// URL or raw_text is required.", error_code=ErrorCode.INPUT_VALIDATION_ERROR)

    loop = asyncio.get_event_loop()

    def _fetch_page() -> tuple[str | None, str | None, int | None]:
        """Fetch page and return (html, error_detail, http_status)."""
        req = _urlrequest.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"},
        )
        try:
            with _urlrequest.urlopen(req, timeout=30) as resp:  # noqa: S310
                status_code = resp.getcode()
                raw = resp.read(5_000_000)
            try:
                html = raw.decode("utf-8")
            except UnicodeDecodeError:
                html = raw.decode("latin-1", errors="replace")
            return html, None, status_code
        except _urlerror.HTTPError as e:
            return None, f"HTTP {e.code}: {e.reason}", e.code
        except _urlerror.URLError as e:
            return None, f"URL error: {e.reason}", None
        except TimeoutError:
            return None, "Connection timed out after 30s", None
        except Exception as exc:
            return None, f"Fetch error: {type(exc).__name__}: {exc}", None

    html, error_detail, http_status = await loop.run_in_executor(None, _fetch_page)
    if not html:
        diagnostics: dict = {"url": url}
        if http_status is not None:
            diagnostics["httpStatus"] = http_status
        if error_detail:
            diagnostics["detail"] = error_detail
        # Classify the failure
        error_code = "FETCH_ERROR"
        if http_status == 403:
            error_code = "FETCH_FORBIDDEN"
            diagnostics["hint"] = "Site may block automated requests. Try raw_text parameter instead."
        elif http_status == 429:
            error_code = "FETCH_RATE_LIMITED"
            diagnostics["hint"] = "Rate limited by host. Wait and retry, or use raw_text parameter."
        elif http_status and http_status >= 500:
            error_code = "FETCH_SERVER_ERROR"
        return _wrap_envelope_v2("parse_public_transcript", None,
            error=error_detail or f"Could not fetch URL: {url}",
            error_code=error_code,
            meta_extra={"diagnostics": diagnostics})

    # Detect paywall/robots block
    clean_text = _strip_html_tags(_sanitize_sec_html(html))
    warnings: list[dict] = []
    lower_text = clean_text[:2000].lower()
    if any(kw in lower_text for kw in ("access denied", "robot", "captcha", "subscribe to read", "paywall")):
        warnings.append({
            "code": "POSSIBLE_PAYWALL_OR_BLOCK",
            "message": "Page content suggests a paywall, CAPTCHA, or robot block. Parsed content may be incomplete.",
            "severity": "warning",
        })

    if topics:
        filtered = _filter_paragraphs_by_topics(clean_text, topics)
        if not filtered:
            warnings.append({"code": "NO_TOPIC_MATCHES", "message": f"No paragraphs matched the provided topics: {topics}"})
        return _wrap_envelope_v2("parse_public_transcript", {
            "url": url,
            "source": "public_url",
            "filteredByTopics": topics,
            "matchedParagraphs": filtered,
            "totalTextLength": len(clean_text),
        }, warnings=warnings)

    max_chars = 50_000
    truncated = len(clean_text) > max_chars
    if truncated:
        warnings.append({"code": "TEXT_TRUNCATED", "message": f"Text truncated from {len(clean_text)} to {max_chars} characters."})
    return _wrap_envelope_v2("parse_public_transcript", {
        "url": url,
        "source": "public_url",
        "filteredByTopics": None,
        "text": clean_text[:max_chars],
        "totalTextLength": len(clean_text),
        "truncated": truncated,
    }, warnings=warnings)


def _transcript_attempt(source_type: str, status: str, **extra: object) -> dict:
    attempt = {"sourceType": source_type, "status": status}
    attempt.update({k: v for k, v in extra.items() if v is not None})
    return attempt


_SEC_TRANSCRIPT_MARKER_RE = _re.compile(
    r"\b(?:earnings|conference)\s+call\b|\bprepared\s+remarks\b|"
    r"\bquestion(?:s)?[- ]and[- ]answer(?:s)?\b|\bopen\s+the\s+call\s+(?:for|to)\s+questions\b",
    flags=_re.IGNORECASE,
)
_SEC_TRANSCRIPT_SPEAKER_RE = _re.compile(
    r"\b(Operator|[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,3})"
    r"(?:\s*[-–—]\s*(?:CEO|CFO|COO|President|Analyst|Investor Relations))?\s*:",
)
_SEC_PRESENTATION_RE = _re.compile(
    r"\b(?:investor presentation|presentation slides?|slide\s+\d+)\b",
    flags=_re.IGNORECASE,
)


def _validate_sec_transcript_text(text: str) -> tuple[bool, dict[str, object]]:
    """Require call markers and repeated speaker turns before certifying an exhibit."""
    normalized = _re.sub(r"\s+", " ", text or "").strip()
    speaker_turns = [match.group(1) for match in _SEC_TRANSCRIPT_SPEAKER_RE.finditer(normalized)]
    unique_speakers = {speaker.casefold() for speaker in speaker_turns}
    has_call_marker = bool(_SEC_TRANSCRIPT_MARKER_RE.search(normalized))
    looks_like_presentation = bool(_SEC_PRESENTATION_RE.search(normalized))
    valid = (
        len(normalized) >= 300
        and has_call_marker
        and len(speaker_turns) >= 3
        and len(unique_speakers) >= 2
        and not (looks_like_presentation and len(speaker_turns) < 5)
    )
    return valid, {
        "textLength": len(normalized),
        "callMarkerFound": has_call_marker,
        "speakerTurnCount": len(speaker_turns),
        "distinctSpeakerCount": len(unique_speakers),
        "presentationMarkersFound": looks_like_presentation,
    }


def _is_sec_transcript_candidate(exhibit: dict) -> bool:
    exhibit_type = _re.sub(r"\s+", "", str(exhibit.get("type") or "").upper())
    description = str(exhibit.get("description") or "").upper()
    return bool(
        _re.match(r"^EX-99\.0?[23]\b", exhibit_type)
        or any(keyword in description for keyword in ("TRANSCRIPT", "CONFERENCE CALL", "PROCEEDINGS", "EARNINGS CALL"))
    )


def _metadata_epoch_iso(value: object) -> str | None:
    try:
        return datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _normalize_yahoo_quartr_payload(
    ticker: str,
    payload: dict,
    *,
    source_url: str | None,
    event_id: str | None,
    topics: list[str] | None,
    paragraph_limit: int,
    paragraph_cursor: int,
    cache_status: str,
    fetched_at: str | None,
) -> dict | None:
    content = payload.get("transcriptContent")
    metadata = payload.get("transcriptMetadata")
    if not isinstance(content, dict):
        return None
    metadata = metadata if isinstance(metadata, dict) else {}
    transcript = content.get("transcript")
    if not isinstance(transcript, dict):
        return None

    speaker_rows = content.get("speaker_mapping") or content.get("speakerMapping") or []
    speaker_map: dict[str, dict[str, object]] = {}
    speakers: list[dict[str, object]] = []
    if isinstance(speaker_rows, list):
        for row in speaker_rows:
            if not isinstance(row, dict):
                continue
            speaker_id = str(row.get("speaker"))
            speaker_data = row.get("speaker_data") or row.get("speakerData") or {}
            speaker_data = speaker_data if isinstance(speaker_data, dict) else {}
            compact = {
                "speakerId": row.get("speaker"),
                "name": speaker_data.get("name"),
                "role": speaker_data.get("role"),
                "company": speaker_data.get("company"),
            }
            speaker_map[speaker_id] = compact
            speakers.append(compact)

    paragraph_rows = transcript.get("paragraphs")
    normalized_paragraphs: list[dict[str, object]] = []
    if isinstance(paragraph_rows, list):
        for index, row in enumerate(paragraph_rows):
            if not isinstance(row, dict):
                continue
            text = str(row.get("text") or "").strip()
            if not text and isinstance(row.get("sentences"), list):
                text = " ".join(
                    str(sentence.get("text") or "").strip()
                    for sentence in row["sentences"]
                    if isinstance(sentence, dict) and str(sentence.get("text") or "").strip()
                )
            if not text:
                continue
            speaker = speaker_map.get(str(row.get("speaker")), {})
            normalized_paragraphs.append({
                "index": index,
                "speaker": speaker.get("name") or ("Operator" if str(row.get("speaker")) == "operator" else None),
                "role": speaker.get("role"),
                "company": speaker.get("company"),
                "start": row.get("start"),
                "end": row.get("end"),
                "text": text,
            })
    if not normalized_paragraphs:
        full_text = str(transcript.get("text") or "").strip()
        normalized_paragraphs = [
            {"index": index, "speaker": None, "role": None, "company": None, "start": None, "end": None, "text": para}
            for index, para in enumerate(_re.split(r"\n\s*\n", full_text))
            if para.strip()
        ]
    if not normalized_paragraphs:
        return None

    topic_list = [str(topic).strip() for topic in (topics or []) if str(topic).strip()]
    selected: list[dict[str, object]] = []
    for paragraph in normalized_paragraphs:
        if not topic_list:
            selected.append(paragraph)
            continue
        searchable = f"{paragraph.get('speaker') or ''} {paragraph['text']}"
        matched = [topic for topic in topic_list if _re.search(_re.escape(topic), searchable, flags=_re.IGNORECASE)]
        if matched:
            selected.append({**paragraph, "matchedTopics": matched})

    offset = min(paragraph_cursor, len(selected))
    page = selected[offset:offset + paragraph_limit]
    next_cursor = str(offset + len(page)) if offset + len(page) < len(selected) else None
    resolved_event_id = str(metadata.get("eventId") or content.get("event_id") or content.get("eventId") or event_id or "") or None
    fiscal_period = str(metadata.get("fiscalPeriod") or "") or None
    try:
        fiscal_year = int(metadata.get("fiscalYear")) if metadata.get("fiscalYear") is not None else None
    except (TypeError, ValueError):
        fiscal_year = None
    resolved_source_url = source_url
    if not resolved_source_url and resolved_event_id and fiscal_period and fiscal_year and _re.fullmatch(r"Q[1-4]", fiscal_period, flags=_re.IGNORECASE):
        ticker_u = ticker.upper()
        resolved_source_url = (
            f"https://finance.yahoo.com/quote/{_urlparse.quote(ticker_u, safe='.^=-')}/earnings/"
            f"{_urlparse.quote(ticker_u, safe='.^=-')}-{fiscal_period.upper()}-{fiscal_year}"
            f"-earnings_call-{resolved_event_id}.html"
        )
    raw_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    warnings: list[dict] = []
    if topic_list and not selected:
        warnings.append({"code": "NO_TOPIC_MATCHES", "message": f"No transcript paragraphs matched: {topic_list}"})
    return {
        "ticker": ticker.upper(),
        "status": "OK",
        "sourceType": "yahoo_quartr",
        "provider": "yahoo_finance_quartr",
        "evidenceClass": "CONTEXTUAL_TRANSCRIPT",
        "decisionGrade": False,
        "sourceUrl": resolved_source_url,
        "eventId": resolved_event_id,
        "title": metadata.get("title"),
        "fiscalPeriod": fiscal_period,
        "fiscalYear": fiscal_year,
        "eventDate": _metadata_epoch_iso(metadata.get("date")),
        "updatedAt": _metadata_epoch_iso(metadata.get("updated")),
        "retrievedAt": fetched_at,
        "cacheStatus": cache_status,
        "contentSha256": hashlib.sha256(raw_json.encode("utf-8")).hexdigest(),
        "speakerCount": transcript.get("number_of_speakers") or transcript.get("numberOfSpeakers") or len(speakers),
        "speakers": speakers,
        "filteredByTopics": topic_list or None,
        "paragraphs": None if topic_list else page,
        "matchedParagraphs": page if topic_list else [],
        "content": None,
        "pagination": {
            "cursor": str(offset),
            "nextCursor": next_cursor,
            "limit": paragraph_limit,
            "returned": len(page),
            "matchingParagraphs": len(selected),
            "totalParagraphs": len(normalized_paragraphs),
            "hasMore": next_cursor is not None,
        },
        "warnings": warnings,
    }


async def _attempt_yahoo_quartr_transcript(
    ticker: str,
    *,
    fiscal_quarter: str | None,
    topics: list[str] | None,
    event_id: str | None,
    source_url: str | None,
    paragraph_limit: int,
    paragraph_cursor: int,
) -> tuple[dict | None, dict]:
    result = await fetch_yahoo_quartr_transcript(
        ticker,
        event_id=event_id,
        source_url=source_url,
        fiscal_quarter=fiscal_quarter,
    )
    attempt_extras: dict[str, object] = {
        "url": result.source_url,
        "eventId": result.event_id,
        "fiscalQuarter": fiscal_quarter,
        "cacheStatus": result.cache_status,
        "attempted": result.provider_attempted,
    }
    if result.http_status is not None:
        attempt_extras["httpStatus"] = result.http_status
    if result.message:
        attempt_extras["message"] = result.message
    if result.retry_after:
        attempt_extras["retryAfter"] = result.retry_after
    if result.status != "OK" or not result.payload:
        return None, _transcript_attempt("yahoo_quartr", result.status, **attempt_extras)
    expected_quarter = fiscal_quarter
    if not expected_quarter and result.source_url:
        source_identity, _ = parse_yahoo_transcript_source_url(ticker, result.source_url)
        if source_identity:
            expected_quarter = str(source_identity.get("fiscalQuarter") or "") or None
    identity_error = validate_yahoo_transcript_payload_identity(
        result.payload,
        str(result.event_id or event_id or ""),
        expected_quarter,
    )
    if identity_error:
        return None, _transcript_attempt(
            "yahoo_quartr",
            "YAHOO_METADATA_MISMATCH",
            **attempt_extras,
            reasonCode="YAHOO_METADATA_MISMATCH",
            reason=identity_error,
        )
    normalized = _normalize_yahoo_quartr_payload(
        ticker,
        result.payload,
        source_url=result.source_url,
        event_id=result.event_id,
        topics=topics,
        paragraph_limit=paragraph_limit,
        paragraph_cursor=paragraph_cursor,
        cache_status=result.cache_status,
        fetched_at=result.fetched_at,
    )
    if not normalized:
        return None, _transcript_attempt(
            "yahoo_quartr", "PROVIDER_ERROR", **attempt_extras,
            reasonCode="TRANSCRIPT_SHAPE_INVALID",
            reason="Yahoo returned transcript data without usable paragraphs.",
        )
    return normalized, _transcript_attempt("yahoo_quartr", "SUCCESS", **attempt_extras)


def _next_transcript_fallback(attempted_sources: list[dict]) -> dict | None:
    statuses = {str(a.get("sourceType")): str(a.get("status")) for a in attempted_sources}
    if statuses.get("company_ir") in {None, "SKIPPED"}:
        return {
            "sourceType": "company_ir",
            "action": "Provide or discover a company IR earnings-call/transcript URL, then call parse_public_transcript.",
        }
    if statuses.get("public_transcript_url") in {None, "SKIPPED"}:
        return {
            "sourceType": "public_transcript_url",
            "action": "Call parse_public_transcript with a verified public transcript URL.",
        }
    if statuses.get("yahoo_quartr") in {None, "SKIPPED"}:
        return {
            "sourceType": "yahoo_quartr",
            "action": "Retry with a validated Yahoo earnings-call source_url or event_id.",
        }
    if statuses.get("alpha_vantage") in {None, "SKIPPED"}:
        return {
            "sourceType": "alpha_vantage",
            "action": "Configure ALPHA_VANTAGE_API_KEY and retry when a fiscal quarter is known.",
        }
    return None


def _alpha_vantage_quarter(period: str) -> str | None:
    text = str(period or "").strip()
    m = _re.search(r"(\d{4})\s*Q([1-4])", text, flags=_re.IGNORECASE)
    if m:
        return f"{m.group(1)}Q{m.group(2)}"
    return None


async def _resolve_transcript_fiscal_quarter(
    fiscal_quarter: str | None,
    official_release_source: dict | None,
) -> dict[str, str | None]:
    explicit = _alpha_vantage_quarter(fiscal_quarter or "")
    if explicit:
        return {
            "fiscalQuarter": explicit,
            "fiscalQuarterStatus": "EXPLICIT",
            "periodEvidence": None,
        }
    if official_release_source:
        period_info = await _resolve_earnings_period_from_source(official_release_source)
        resolved = _alpha_vantage_quarter(str(period_info.get("period") or ""))
        if resolved:
            return {
                "fiscalQuarter": resolved,
                "fiscalQuarterStatus": "OFFICIAL_RELEASE_RESOLVED",
                "periodEvidence": period_info.get("periodEvidence"),
            }
    return {
        "fiscalQuarter": None,
        "fiscalQuarterStatus": "UNRESOLVED",
        "periodEvidence": None,
    }


async def _attempt_alpha_vantage_transcript(
    ticker: str,
    fiscal_quarter: str | None,
    official_release_source: dict | None,
    topics: list[str] | None,
) -> tuple[dict | None, dict, dict[str, str | None]]:
    resolution = await _resolve_transcript_fiscal_quarter(fiscal_quarter, official_release_source)
    quarter = resolution.get("fiscalQuarter")
    if not quarter:
        return None, _transcript_attempt(
            "alpha_vantage",
            "NOT_ELIGIBLE",
            reasonCode="FISCAL_QUARTER_UNRESOLVED",
            reason=(
                "Alpha Vantage requires an issuer fiscal quarter. Provide fiscal_quarter "
                "as YYYYQn or retry after an official earnings release is available."
            ),
        ), resolution
    payload, attempt = await _fetch_alpha_vantage_transcript(ticker, quarter, topics)
    return payload, attempt, resolution


async def _fetch_alpha_vantage_transcript(ticker: str, quarter: str, topics: list[str] | None = None) -> tuple[dict | None, dict]:
    result = await fetch_alpha_vantage_json(
        "EARNINGS_CALL_TRANSCRIPT",
        {"symbol": ticker.upper(), "quarter": quarter},
        ttl_seconds=30 * 24 * 60 * 60,
    )
    public_url = result.public_url
    rate_limit = {
        "provider": "alpha_vantage",
        "used": result.provider_attempted,
        "note": "Alpha Vantage free-tier rate limits may apply.",
    }
    if result.status != "OK" or not isinstance(result.payload, dict):
        extras: dict[str, object] = {
            "url": public_url,
            "quarter": quarter,
            "rateLimit": rate_limit,
            "cacheStatus": result.cache_status,
        }
        if result.http_status is not None:
            extras["httpStatus"] = result.http_status
        if result.message:
            extras["message"] = result.message
        if result.retry_after:
            extras["retryAfter"] = result.retry_after
        if result.status == "SOURCE_UNCONFIGURED":
            extras.update({
                "reasonCode": "ALPHA_VANTAGE_API_KEY_MISSING",
                "reason": "ALPHA_VANTAGE_API_KEY not configured.",
            })
        return None, _transcript_attempt(
            "alpha_vantage",
            result.status,
            **extras,
        )
    payload = result.payload
    rows = payload.get("transcript") if isinstance(payload.get("transcript"), list) else []
    paragraphs: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        speaker = str(row.get("speaker") or row.get("speaker_name") or "").strip()
        content = str(row.get("content") or row.get("text") or "").strip()
        if content:
            paragraphs.append(f"{speaker}: {content}" if speaker else content)
    if not paragraphs:
        return None, _transcript_attempt(
            "alpha_vantage", "NOT_FOUND", url=public_url, quarter=quarter,
            rateLimit=rate_limit, cacheStatus=result.cache_status,
        )

    clean_text = "\n\n".join(paragraphs)
    max_chars = 50_000
    warnings: list[dict] = []
    if topics:
        matched = _filter_paragraphs_by_topics(clean_text, topics)
        return {
            "sourceType": "alpha_vantage",
            "status": "OK",
            "evidenceClass": "CONTEXTUAL_TRANSCRIPT",
            "decisionGrade": False,
            "filteredByTopics": topics,
            "matchedParagraphs": matched,
            "content": None,
            "totalTextLength": len(clean_text),
            "truncated": False,
            "warnings": warnings if matched else [{"code": "NO_TOPIC_MATCHES", "message": f"No paragraphs matched the provided topics: {topics}"}],
        }, _transcript_attempt("alpha_vantage", "SUCCESS", url=public_url, quarter=quarter, rateLimit=rate_limit, cacheStatus=result.cache_status)
    truncated = len(clean_text) > max_chars
    if truncated:
        warnings.append({"code": "TEXT_TRUNCATED", "message": f"Text truncated from {len(clean_text)} to {max_chars} characters."})
    return {
        "sourceType": "alpha_vantage",
        "status": "OK",
        "evidenceClass": "CONTEXTUAL_TRANSCRIPT",
        "decisionGrade": False,
        "filteredByTopics": None,
        "content": clean_text[:max_chars],
        "totalTextLength": len(clean_text),
        "truncated": truncated,
        "warnings": warnings,
    }, _transcript_attempt("alpha_vantage", "SUCCESS", url=public_url, quarter=quarter, rateLimit=rate_limit, cacheStatus=result.cache_status)


@yfinance_server.tool(
    name="get_earnings_call_transcript",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_earnings_call_transcript"],
    description="Retrieve an earnings-call transcript from a content-validated SEC exhibit, then Yahoo/Quartr, then optional Alpha Vantage. Yahoo results are structured and paragraph-paginated for LLM use and include the human-readable sourceUrl. Provide source_url or event_id to target a known Yahoo call. Yahoo and Alpha transcripts are contextual and never decision-grade; filing dates are never treated as fiscal periods.",
)
async def get_earnings_call_transcript(
    ticker: str,
    period: str = "latest",
    topics: list[str] | None = None,
    fiscal_quarter: Annotated[
        str | None,
        Field(
            pattern=r"^\d{4}Q[1-4]$",
            description=(
                "Optional issuer fiscal quarter for Alpha Vantage fallback, e.g. 2026Q4. "
                "Do not derive this from the filing or publication date."
            ),
        ),
    ] = None,
    event_id: Annotated[
        str | None,
        Field(pattern=r"^\d+$", description="Optional Yahoo earnings-call event ID from a validated Yahoo transcript URL."),
    ] = None,
    source_url: Annotated[
        str | None,
        Field(description="Optional https://finance.yahoo.com/quote/{ticker}/earnings/...-earnings_call-{eventId}.html URL."),
    ] = None,
    paragraph_limit: Annotated[
        int,
        Field(ge=1, le=50, description="Maximum Yahoo transcript paragraphs to return per call."),
    ] = 20,
    paragraph_cursor: Annotated[
        str | None,
        Field(pattern=r"^\d+$", description="Opaque nextCursor from a previous Yahoo transcript response."),
    ] = None,
) -> str:
    normalized_fiscal_quarter = str(fiscal_quarter or "").strip().upper() or None
    if normalized_fiscal_quarter and not _re.fullmatch(r"\d{4}Q[1-4]", normalized_fiscal_quarter):
        return _wrap_envelope_v2(
            "get_earnings_call_transcript",
            None,
            error="fiscal_quarter must use issuer fiscal-quarter format YYYYQ1 through YYYYQ4.",
            error_code=ErrorCode.INPUT_VALIDATION_ERROR,
        )
    normalized_event_id = str(event_id or "").strip() or None
    if normalized_event_id and not normalized_event_id.isdigit():
        return _wrap_envelope_v2(
            "get_earnings_call_transcript", None,
            error="event_id must contain digits only.",
            error_code=ErrorCode.INPUT_VALIDATION_ERROR,
        )
    normalized_source_url = str(source_url or "").strip() or None
    source_identity: dict | None = None
    if normalized_source_url:
        source_identity, source_error = parse_yahoo_transcript_source_url(ticker, normalized_source_url)
        if source_error:
            return _wrap_envelope_v2(
                "get_earnings_call_transcript", None,
                error=source_error,
                error_code=ErrorCode.INPUT_VALIDATION_ERROR,
            )
        if normalized_event_id and normalized_event_id != source_identity.get("eventId"):
            return _wrap_envelope_v2(
                "get_earnings_call_transcript", None,
                error="event_id does not match source_url.",
                error_code=ErrorCode.INPUT_VALIDATION_ERROR,
            )
        source_quarter = str(source_identity.get("fiscalQuarter") or "") or None
        if normalized_fiscal_quarter and source_quarter != normalized_fiscal_quarter:
            return _wrap_envelope_v2(
                "get_earnings_call_transcript", None,
                error="fiscal_quarter does not match source_url.",
                error_code=ErrorCode.INPUT_VALIDATION_ERROR,
            )
        normalized_event_id = normalized_event_id or str(source_identity.get("eventId") or "") or None
        normalized_fiscal_quarter = normalized_fiscal_quarter or source_quarter
        normalized_source_url = str(source_identity.get("sourceUrl") or normalized_source_url)
    try:
        cursor_offset = int(str(paragraph_cursor or "0"))
    except ValueError:
        return _wrap_envelope_v2(
            "get_earnings_call_transcript", None,
            error="paragraph_cursor must be a non-negative integer cursor.",
            error_code=ErrorCode.INPUT_VALIDATION_ERROR,
        )
    if cursor_offset < 0 or not 1 <= int(paragraph_limit) <= 50:
        return _wrap_envelope_v2(
            "get_earnings_call_transcript", None,
            error="paragraph_limit must be between 1 and 50 and paragraph_cursor must be non-negative.",
            error_code=ErrorCode.INPUT_VALIDATION_ERROR,
        )
    err = _validate_ticker(ticker)
    if err:
        return _wrap_envelope_v2("get_earnings_call_transcript", None, error=err, error_code=ErrorCode.INPUT_VALIDATION_ERROR)

    attempted_sources: list[dict] = []
    explicit_yahoo = bool(normalized_event_id or normalized_source_url)
    if explicit_yahoo:
        yahoo_payload, yahoo_attempt = await _attempt_yahoo_quartr_transcript(
            ticker,
            fiscal_quarter=normalized_fiscal_quarter,
            topics=topics,
            event_id=normalized_event_id,
            source_url=normalized_source_url,
            paragraph_limit=int(paragraph_limit),
            paragraph_cursor=cursor_offset,
        )
        attempted_sources.append(yahoo_attempt)
        if yahoo_payload:
            yahoo_payload.update({
                "period": period,
                "fiscalQuarter": normalized_fiscal_quarter,
                "fiscalQuarterStatus": "EXPLICIT_SOURCE",
                "attemptedSources": attempted_sources,
                "nextRecommendedFallback": None,
            })
            warnings = yahoo_payload.pop("warnings", [])
            return _wrap_envelope_v2("get_earnings_call_transcript", yahoo_payload, warnings=warnings)

    sec_fallback: dict[str, object]
    official_release_source: dict | None = None
    sec_source = await _resolve_latest_earnings_sec_source(ticker)
    if not sec_source:
        attempted_sources.append(_transcript_attempt("sec_8k_exhibit", "NOT_FOUND"))
        sec_fallback = {
            "ticker": ticker.upper(),
            "period": period,
            "status": "SEC_8K_NOT_FOUND",
            "message": "No recent SEC 8-K filing found for this ticker.",
            "content": None,
        }
    else:
        accession = str(sec_source.get("accessionNumber") or "")
        official_release_source = sec_source if sec_source.get("sourceType") == "sec_8k_ex991" else None
        cik = _edgar_cik_from_accession(accession)
        if not cik:
            cik_padded, _ = await _get_submissions_for_ticker(ticker)
            cik = int(cik_padded) if cik_padded else None
        if not cik:
            attempted_sources.append(_transcript_attempt(
                "sec_8k_exhibit", "FAILED", accessionNumber=accession, reason="CIK resolution failed."
            ))
            sec_fallback = {
                "ticker": ticker.upper(), "period": period, "status": "CIK_RESOLUTION_FAILED",
                "message": "Could not resolve CIK for ticker.", "content": None,
            }
        else:
            index_url, _ = _edgar_build_filing_urls(cik, accession, None)
            exhibits = await _edgar_list_exhibits_from_index(index_url)
            candidates = [exhibit for exhibit in exhibits if _is_sec_transcript_candidate(exhibit)]
            candidate_diagnostics: list[dict[str, object]] = []
            validated: tuple[dict, str, str, dict[str, object]] | None = None
            for exhibit in candidates[:4]:
                file_name = str(exhibit.get("document") or "")
                if not file_name:
                    candidate_diagnostics.append({
                        "type": exhibit.get("type"), "document": None, "status": "REJECTED",
                        "reasonCode": "MISSING_DOCUMENT_NAME",
                    })
                    continue
                doc_url = (
                    f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/{file_name}"
                )
                html = await _edgar_get_html(doc_url, max_bytes=5_000_000)
                if not html:
                    candidate_diagnostics.append({
                        "type": exhibit.get("type"), "document": file_name, "url": doc_url,
                        "status": "FETCH_ERROR",
                    })
                    continue
                clean_text = _strip_html_tags(_sanitize_sec_html(html))
                is_valid, validation = _validate_sec_transcript_text(clean_text)
                candidate_diagnostics.append({
                    "type": exhibit.get("type"), "document": file_name, "url": doc_url,
                    "status": "VALIDATED" if is_valid else "REJECTED",
                    "validation": validation,
                })
                if is_valid:
                    validated = (exhibit, doc_url, clean_text, validation)
                    break

            if validated:
                transcript_exhibit, doc_url, clean_text, validation = validated
                attempted_sources.append(_transcript_attempt(
                    "sec_8k_exhibit", "SUCCESS", url=doc_url, accessionNumber=accession,
                    filingDate=sec_source.get("filingDate"), validation=validation,
                ))
                warnings: list[dict] = []
                common = {
                    "ticker": ticker.upper(), "period": period, "status": "OK",
                    "sourceType": "sec_8k_exhibit", "evidenceClass": "SEC_FILING", "decisionGrade": True,
                    "sourceUrl": doc_url, "accessionNumber": accession,
                    "filingDate": sec_source.get("filingDate"), "exhibitType": transcript_exhibit.get("type"),
                    "documentUrl": doc_url, "totalTextLength": len(clean_text),
                    "attemptedSources": attempted_sources, "nextRecommendedFallback": None,
                }
                if topics:
                    filtered = _filter_paragraphs_by_topics(clean_text, topics)
                    if not filtered:
                        warnings.append({"code": "NO_TOPIC_MATCHES", "message": f"No paragraphs matched the provided topics: {topics}"})
                    return _wrap_envelope_v2("get_earnings_call_transcript", {
                        **common, "filteredByTopics": topics, "matchedParagraphs": filtered, "content": None,
                    }, warnings=warnings)
                max_chars = 50_000
                truncated = len(clean_text) > max_chars
                if truncated:
                    warnings.append({"code": "TEXT_TRUNCATED", "message": f"Text truncated from {len(clean_text)} to {max_chars} characters."})
                return _wrap_envelope_v2("get_earnings_call_transcript", {
                    **common, "filteredByTopics": None, "content": clean_text[:max_chars], "truncated": truncated,
                }, warnings=warnings)

            attempted_sources.append(_transcript_attempt(
                "sec_8k_exhibit", "NOT_FOUND", url=index_url, accessionNumber=accession,
                filingDate=sec_source.get("filingDate"), exhibitsSearched=len(exhibits),
                candidatesChecked=len(candidate_diagnostics), candidateDiagnostics=candidate_diagnostics,
            ))
            sec_fallback = {
                "ticker": ticker.upper(), "period": period, "status": "SEC_EXHIBIT_NOT_FOUND",
                "accessionNumber": accession, "filingDate": sec_source.get("filingDate"),
                "availableExhibits": [
                    {"type": ex.get("type"), "description": ex.get("description"), "document": ex.get("document")}
                    for ex in exhibits
                ],
                "candidateDiagnostics": candidate_diagnostics,
                "message": "8-K filing found but no content-validated transcript exhibit was detected.",
                "content": None,
            }

    attempted_sources.append(_transcript_attempt(
        "company_ir", "SKIPPED", reason="No approved company IR transcript source was configured."
    ))
    if not explicit_yahoo:
        yahoo_payload, yahoo_attempt = await _attempt_yahoo_quartr_transcript(
            ticker,
            fiscal_quarter=normalized_fiscal_quarter,
            topics=topics,
            event_id=None,
            source_url=None,
            paragraph_limit=int(paragraph_limit),
            paragraph_cursor=cursor_offset,
        )
        attempted_sources.append(yahoo_attempt)
        if yahoo_payload:
            metadata_quarter = None
            if yahoo_payload.get("fiscalYear") and yahoo_payload.get("fiscalPeriod"):
                metadata_quarter = f"{yahoo_payload['fiscalYear']}{str(yahoo_payload['fiscalPeriod']).upper()}"
            yahoo_payload.update({
                "period": period,
                "fiscalQuarter": normalized_fiscal_quarter or metadata_quarter,
                "fiscalQuarterStatus": "EXPLICIT" if normalized_fiscal_quarter else "YAHOO_METADATA_RESOLVED",
                "attemptedSources": attempted_sources,
                "nextRecommendedFallback": None,
            })
            warnings = yahoo_payload.pop("warnings", [])
            return _wrap_envelope_v2("get_earnings_call_transcript", yahoo_payload, warnings=warnings)

    attempted_sources.append(_transcript_attempt(
        "public_transcript_url", "SKIPPED", reason="No non-Yahoo public transcript URL was provided to parse_public_transcript."
    ))
    alpha_payload, alpha_attempt, quarter_resolution = await _attempt_alpha_vantage_transcript(
        ticker, normalized_fiscal_quarter, official_release_source, topics
    )
    attempted_sources.append(alpha_attempt)
    if alpha_payload:
        alpha_payload.update({
            "ticker": ticker.upper(), "period": period, **quarter_resolution,
            "attemptedSources": attempted_sources, "nextRecommendedFallback": None,
        })
        warnings = alpha_payload.pop("warnings", [])
        return _wrap_envelope_v2("get_earnings_call_transcript", alpha_payload, warnings=warnings)

    sec_fallback.update({
        **quarter_resolution,
        "attemptedSources": attempted_sources,
        "nextRecommendedFallback": _next_transcript_fallback(attempted_sources),
    })
    return _wrap_envelope_v2("get_earnings_call_transcript", sec_fallback)
