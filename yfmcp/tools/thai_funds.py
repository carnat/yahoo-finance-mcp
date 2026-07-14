"""Bounded Thai SEC Open Data fund workflows.

This module intentionally exposes three different workflows instead of an
unbounded fund overview: share-class NAV, dated factsheet evidence, and
project-scoped dividend history.  It uses only the documented SEC Open Data
JSON endpoints and never follows AMC/PDF links.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from typing import Any
from urllib import error as _urlerror
from urllib import parse as _urlparse
from urllib import request as _urlrequest
from zoneinfo import ZoneInfo

from yfmcp.app import yfinance_server
from yfmcp.envelope import ErrorCode, _mcp_failure, _mcp_success
from yfmcp.schemas import _TOOL_OUTPUT_SCHEMAS


_SOURCE = "sec_thailand_open_data"
_EVIDENCE_CLASS = "OFFICIAL_REGULATORY_DATA"
_BASE_URL = "https://api.sec.or.th/v2/fund"
_TIMEZONE = "Asia/Bangkok"
_REQUEST_TIMEOUT_SECONDS = 12
_MAX_PAGE_SIZE = 100
_DEFAULT_NAV_LOOKBACK_DAYS = 45
_MAX_NAV_LOOKBACK_DAYS = 90
_FACTSHEET_SECTIONS = frozenset({"statistics", "top_holdings", "urls"})
_DEFAULT_FACTSHEET_SECTIONS = ("statistics", "top_holdings", "urls")


class SecThailandProviderError(Exception):
    """A sanitized, caller-actionable failure from the Thailand SEC provider."""

    def __init__(
        self,
        code: str,
        message: str,
        recovery_action: str,
        diagnostics: dict[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.recovery_action = recovery_action
        self.diagnostics = diagnostics


@dataclass(frozen=True)
class FundIdentity:
    fund_class_name: str
    proj_id: str
    unique_id: str | None
    company_name_th: str | None
    company_name_en: str | None
    project_name_th: str | None
    project_name_en: str | None

    @classmethod
    def from_profile(cls, row: dict[str, Any]) -> "FundIdentity":
        return cls(
            fund_class_name=str(row.get("fund_class_name") or ""),
            proj_id=str(row.get("proj_id") or ""),
            unique_id=_optional_str(row.get("unique_id")),
            company_name_th=_optional_str(row.get("comp_name_th")),
            company_name_en=_optional_str(row.get("comp_name_en")),
            project_name_th=_optional_str(row.get("proj_name_th")),
            project_name_en=_optional_str(row.get("proj_name_en")),
        )

    def compact(self) -> dict[str, str | None]:
        return {
            "fundClassName": self.fund_class_name,
            "projId": self.proj_id,
            "uniqueId": self.unique_id,
            "companyNameTh": self.company_name_th,
            "companyNameEn": self.company_name_en,
            "projectNameTh": self.project_name_th,
            "projectNameEn": self.project_name_en,
        }


@dataclass(frozen=True)
class Resolution:
    status: str
    identity: FundIdentity | None = None
    candidates: tuple[dict[str, str | None], ...] = ()
    message: str | None = None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bangkok_today() -> date:
    return datetime.now(ZoneInfo(_TIMEZONE)).date()


def _parse_iso_date(value: str, field: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc


def _safe_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _base_payload(status: str) -> dict[str, object]:
    return {
        "status": status,
        "source": _SOURCE,
        "evidenceClass": _EVIDENCE_CLASS,
        "decisionGrade": False,
    }


def _recovery(action: str, detail: str) -> dict[str, str]:
    return {"action": action, "detail": detail}


def _request_json_sync(path: str, params: dict[str, object]) -> dict[str, Any]:
    api_key = os.environ.get("SEC_OPEN_DATA_API_KEY", "").strip()
    if not api_key:
        raise SecThailandProviderError(
            "SOURCE_UNCONFIGURED",
            "SEC Open Data is not configured for this runtime.",
            "CONFIGURE_SEC_OPEN_DATA_API_KEY",
        )

    request_params = dict(params)
    if request_params.get("next_cursor") is None:
        request_params["next_cursor"] = ""
    query = {
        key: str(value).lower() if isinstance(value, bool) else str(value)
        for key, value in request_params.items()
        if value is not None and (str(value) != "" or key == "next_cursor")
    }
    url = f"{_BASE_URL}{path}?{_urlparse.urlencode(query)}" if query else f"{_BASE_URL}{path}"
    request = _urlrequest.Request(
        url,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Ocp-Apim-Subscription-Key": api_key,
        },
        method="GET",
    )
    response_status: int | None = None
    content_type: str | None = None
    try:
        with _urlrequest.urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
            response_status = getattr(response, "status", None)
            headers = getattr(response, "headers", None)
            content_type = headers.get("Content-Type") if headers is not None else None
            raw = response.read()
    except _urlerror.HTTPError as exc:
        if exc.code in {401, 403}:
            raise SecThailandProviderError(
                "AUTH_ERROR",
                "SEC Open Data rejected the configured subscription key.",
                "VERIFY_SEC_OPEN_DATA_API_KEY",
            ) from exc
        if exc.code == 429:
            raise SecThailandProviderError(
                "RATE_LIMIT",
                "SEC Open Data rate limited this request.",
                "RETRY_LATER",
            ) from exc
        raise SecThailandProviderError(
            "PROVIDER_ERROR",
            f"SEC Open Data returned HTTP {exc.code}.",
            "RETRY_LATER",
        ) from exc
    except (TimeoutError, _urlerror.URLError) as exc:
        raise SecThailandProviderError(
            "PROVIDER_TIMEOUT",
            "SEC Open Data did not respond before the request timeout.",
            "RETRY_LATER",
        ) from exc

    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SecThailandProviderError(
            "PROVIDER_ERROR",
            "SEC Open Data returned an invalid JSON response.",
            "RETRY_LATER",
            diagnostics={
                "httpStatus": response_status,
                "contentType": content_type,
                "bodyBytes": len(raw),
                "bodySha256": hashlib.sha256(raw).hexdigest(),
            },
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise SecThailandProviderError(
            "PROVIDER_ERROR",
            "SEC Open Data returned an unexpected response shape.",
            "RETRY_LATER",
        )
    return payload


async def _request_json(path: str, params: dict[str, object]) -> dict[str, Any]:
    return await asyncio.to_thread(_request_json_sync, path, params)


async def _resolve_fund(
    fund_class_name: str,
    proj_id: str | None,
    project_info: str | None,
) -> Resolution:
    target_class = fund_class_name.strip()
    project_lookup = proj_id or project_info
    params: dict[str, object] = {
        "fund_class_name": target_class,
        "fund_status": "Registered",
        "next_cursor": "",
        "page_size": _MAX_PAGE_SIZE,
    }
    if project_lookup:
        params["project_info"] = project_lookup
    payload = await _request_json("/general-info/profiles", params)
    records = [row for row in payload["items"] if isinstance(row, dict)]
    if not any(str(row.get("fund_class_name") or "") == target_class for row in records) and not payload.get("next_cursor"):
        payload = await _request_json("/general-info/profiles", {**params, "fund_status": "IPO"})
        records = [row for row in payload["items"] if isinstance(row, dict)]
    exact = [
        FundIdentity.from_profile(row)
        for row in records
        if str(row.get("fund_class_name") or "") == target_class
        and (not proj_id or str(row.get("proj_id") or "") == proj_id)
    ]

    if proj_id and not exact:
        class_matches = [
            FundIdentity.from_profile(row)
            for row in records
            if str(row.get("fund_class_name") or "") == target_class
        ]
        return Resolution(
            "FUND_IDENTITY_MISMATCH",
            candidates=tuple(item.compact() for item in class_matches[:10]),
            message="The supplied proj_id does not match the requested fund_class_name.",
        )
    if not exact:
        return Resolution(
            "FUND_NOT_FOUND",
            message="No exact active share class matched fund_class_name.",
        )

    has_more = bool(payload.get("next_cursor"))
    if len(exact) != 1 or has_more:
        return Resolution(
            "AMBIGUOUS_SHARE_CLASS",
            candidates=tuple(item.compact() for item in exact[:10]),
            message="Provide proj_id to select an exact share class; no automatic class selection was made.",
        )
    return Resolution("OK", identity=exact[0])


def _resolution_response(
    tool: str,
    fund_class_name: str,
    proj_id: str | None,
    project_info: str | None,
    resolution: Resolution,
) -> str:
    payload = _base_payload(resolution.status)
    payload.update({
        "scope": "SHARE_CLASS",
        "requestedIdentity": {"fundClassName": fund_class_name, "projId": proj_id, "projectInfo": project_info},
        "identity": resolution.identity.compact() if resolution.identity else None,
        "candidates": list(resolution.candidates),
        "recovery": _recovery(
            "PROVIDE_PROJ_ID" if resolution.status == "AMBIGUOUS_SHARE_CLASS" else "CHECK_FUND_CLASS_NAME",
            resolution.message or "Resolve an exact Thai SEC share class before requesting fund data.",
        ),
    })
    return _mcp_success(tool, payload, source=_SOURCE)


def _provider_failure(tool: str, error: SecThailandProviderError) -> str:
    meta_extra: dict[str, object] = {
        "recoveryAction": error.recovery_action,
        "evidenceClass": _EVIDENCE_CLASS,
        "decisionGrade": False,
    }
    if error.diagnostics is not None:
        meta_extra["diagnostics"] = error.diagnostics
    return _mcp_failure(
        tool,
        error.code,
        error.args[0],
        source=_SOURCE,
        meta_extra=meta_extra,
    )


async def _resolve_or_response(
    tool: str,
    fund_class_name: str,
    proj_id: str | None,
    project_info: str | None,
) -> tuple[FundIdentity | None, str | None]:
    if not isinstance(fund_class_name, str) or not fund_class_name.strip():
        return None, _mcp_failure(
            tool,
            ErrorCode.INPUT_VALIDATION_ERROR,
            "fund_class_name is required.",
            source=_SOURCE,
        )
    normalized_project = _optional_str(proj_id)
    normalized_project_info = _optional_str(project_info)
    try:
        resolution = await _resolve_fund(fund_class_name, normalized_project, normalized_project_info)
    except SecThailandProviderError as error:
        return None, _provider_failure(tool, error)
    if resolution.status != "OK" or resolution.identity is None:
        return None, _resolution_response(tool, fund_class_name.strip(), normalized_project, normalized_project_info, resolution)
    return resolution.identity, None


def _freshness(data_date: str | None, requested_as_of: date) -> dict[str, object]:
    parsed = _safe_date(data_date)
    return {
        "asOfDate": requested_as_of.isoformat(),
        "dataDate": data_date,
        "calendarDaysFromAsOf": (requested_as_of - parsed).days if parsed else None,
        "timezone": _TIMEZONE,
    }


@yfinance_server.tool(
    name="get_thai_fund_nav",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_thai_fund_nav"],
    description="""Return the latest Thai SEC daily NAV in a bounded Bangkok-time window for one exact fund share class.

Use when a caller needs official NAV/pricing evidence. fund_class_name is exact;
provide proj_id whenever the class name can be ambiguous, or project_info to
narrow the documented SEC profile search by official name or abbreviation. The default window is
45 calendar days ending on as_of_date (or Bangkok today), capped at 90 days.
NAV_NOT_FOUND_IN_WINDOW does not mean the fund has no NAV outside that window.
""",
)
async def get_thai_fund_nav(
    fund_class_name: str,
    proj_id: str | None = None,
    as_of_date: str | None = None,
    lookback_days: int = _DEFAULT_NAV_LOOKBACK_DAYS,
    project_info: str | None = None,
) -> str:
    """Get one exact share class's latest official NAV within a bounded date window."""
    identity, response = await _resolve_or_response("get_thai_fund_nav", fund_class_name, proj_id, project_info)
    if response:
        return response
    assert identity is not None
    try:
        end_date = _parse_iso_date(as_of_date, "as_of_date") if as_of_date else _bangkok_today()
    except ValueError as error:
        return _mcp_failure("get_thai_fund_nav", ErrorCode.INPUT_VALIDATION_ERROR, str(error), source=_SOURCE)
    if isinstance(lookback_days, bool) or not isinstance(lookback_days, int) or not 1 <= lookback_days <= _MAX_NAV_LOOKBACK_DAYS:
        return _mcp_failure(
            "get_thai_fund_nav",
            ErrorCode.INPUT_VALIDATION_ERROR,
            "lookback_days must be an integer from 1 through 90.",
            source=_SOURCE,
        )
    start_date = end_date - timedelta(days=lookback_days - 1)
    try:
        payload = await _request_json("/daily-info/nav", {
            "proj_id": identity.proj_id,
            "fund_class_name": identity.fund_class_name,
            "start_nav_date": start_date.isoformat(),
            "end_nav_date": end_date.isoformat(),
            "page_size": _MAX_PAGE_SIZE,
        })
    except SecThailandProviderError as error:
        return _provider_failure("get_thai_fund_nav", error)

    rows = [row for row in payload["items"] if isinstance(row, dict) and _safe_date(row.get("nav_date"))]
    latest = max(rows, key=lambda row: str(row.get("nav_date"))) if rows else None
    base = _base_payload("OK" if latest else "NAV_NOT_FOUND_IN_WINDOW")
    base.update({
        "scope": "SHARE_CLASS",
        "identity": identity.compact(),
        "requestedWindow": {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "lookbackCalendarDays": lookback_days,
            "timezone": _TIMEZONE,
        },
        "dataDate": _optional_str(latest.get("nav_date")) if latest else None,
        "freshness": _freshness(_optional_str(latest.get("nav_date")) if latest else None, end_date),
        "nav": ({
            "navDate": latest.get("nav_date"),
            "netAsset": latest.get("net_asset"),
            "lastValue": latest.get("last_val"),
            "sellPrice": latest.get("sell_price"),
            "buyPrice": latest.get("buy_price"),
            "sellSwapPrice": latest.get("sell_swap_price"),
            "buySwapPrice": latest.get("buy_swap_price"),
            "lastUpdatedAt": latest.get("last_upd_date"),
        } if latest else None),
        "nextCursor": _optional_str(payload.get("next_cursor")),
        "hasMore": bool(payload.get("next_cursor")),
        "recovery": _recovery(
            "EXPAND_WINDOW_UP_TO_90_DAYS" if not latest else "NONE",
            "No NAV was returned inside this requested window; try a later as_of_date or a wider window." if not latest else "Latest returned NAV selected by nav_date, not provider row order.",
        ),
    })
    return _mcp_success("get_thai_fund_nav", base, source=_SOURCE, data_date=base["dataDate"] if isinstance(base["dataDate"], str) else None)


def _factsheet_section_error(section: str, error: SecThailandProviderError) -> dict[str, object]:
    return {
        "status": error.code,
        "scope": "SHARE_CLASS" if section != "top_holdings" else "PROJECT",
        "asOfDate": None,
        "data": None,
        "recovery": _recovery(error.recovery_action, error.args[0]),
    }


async def _factsheet_statistics(identity: FundIdentity) -> dict[str, object]:
    try:
        payload = await _request_json("/factsheet/statistics", {
            "proj_id": identity.proj_id,
            "fund_class_name": identity.fund_class_name,
            "latest": True,
            "page_size": 1,
        })
    except SecThailandProviderError as error:
        return _factsheet_section_error("statistics", error)
    row = next((item for item in payload["items"] if isinstance(item, dict)), None)
    if row is None:
        return {
            "status": "EMPTY_RESULT", "scope": "SHARE_CLASS", "asOfDate": None, "data": None,
            "recovery": _recovery("CHECK_LATER", "No dated statistics record is currently available for this share class."),
        }
    fields = (
        "portfolio_turnover_ratio", "recovering_period", "portfolio_duration_period", "maximum_drawdown",
        "sharpe_ratio", "beta", "alpha", "fx_hedging", "tracking_error", "yield_to_maturity",
    )
    return {
        "status": "OK",
        "scope": "SHARE_CLASS",
        "asOfDate": row.get("end_date") or row.get("start_date"),
        "period": {"startDate": row.get("start_date"), "endDate": row.get("end_date"), "prospectusType": row.get("prospectus_type")},
        "data": {key: row.get(key) for key in fields},
        "recovery": _recovery("NONE", "Dated factsheet statistics returned."),
    }


async def _factsheet_top_holdings(identity: FundIdentity) -> dict[str, object]:
    try:
        payload = await _request_json("/factsheet/top5-holdings", {
            "proj_id": identity.proj_id,
            "latest": True,
            "page_size": 5,
        })
    except SecThailandProviderError as error:
        return _factsheet_section_error("top_holdings", error)
    rows = [row for row in payload["items"] if isinstance(row, dict)]
    as_of = next((row.get("end_date") or row.get("start_date") for row in rows if row.get("end_date") or row.get("start_date")), None)
    return {
        "status": "OK" if rows else "EMPTY_RESULT",
        "scope": "PROJECT",
        "asOfDate": as_of,
        "data": [{
            "assetName": row.get("asset_name"), "assetRatio": row.get("asset_ratio"),
            "assetSequence": row.get("asset_seq"), "startDate": row.get("start_date"),
            "endDate": row.get("end_date"), "prospectusType": row.get("prospectus_type"),
            "lastUpdatedAt": row.get("last_upd_date"),
        } for row in rows],
        "recovery": _recovery(
            "CHECK_LATER" if not rows else "NONE",
            "Top holdings are dated project-level factsheet evidence, not current share-class holdings.",
        ),
    }


async def _factsheet_urls(identity: FundIdentity) -> dict[str, object]:
    try:
        payload = await _request_json("/factsheet/urls", {
            "proj_id": identity.proj_id,
            "fund_class_name": identity.fund_class_name,
            "page_size": _MAX_PAGE_SIZE,
        })
    except SecThailandProviderError as error:
        return _factsheet_section_error("urls", error)
    rows = [row for row in payload["items"] if isinstance(row, dict)]
    as_of = next((row.get("as_of_date") for row in rows if row.get("as_of_date")), None)
    return {
        "status": "OK" if rows else "EMPTY_RESULT",
        "scope": "SHARE_CLASS",
        "asOfDate": as_of,
        "data": [{
            "prospectusType": row.get("prospectus_type"), "amcUrlFactsheet": row.get("amc_url_factsheet"),
            "pdfFactsheet": row.get("pdf_factsheet"), "asOfDate": row.get("as_of_date"),
            "lastUpdatedAt": row.get("last_upd_date"),
        } for row in rows],
        "recovery": _recovery(
            "CHECK_LATER" if not rows else "NONE",
            "URLs are returned as official references only; this tool does not fetch or parse PDFs.",
        ),
    }


@yfinance_server.tool(
    name="get_thai_fund_factsheet",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_thai_fund_factsheet"],
    description="""Return dated official Thai SEC factsheet evidence for one exact share class.

Choose sections from statistics, top_holdings, and urls (all by default). Each
section preserves its own asOfDate and scope. Top holdings are project scoped;
statistics and URLs are share-class scoped. Partial section failures are kept
alongside successful sections. Use project_info to narrow the documented SEC
profile search by official project name or abbreviation. URLs are references only: no PDF is fetched.
""",
)
async def get_thai_fund_factsheet(
    fund_class_name: str,
    proj_id: str | None = None,
    sections: list[str] | None = None,
    project_info: str | None = None,
) -> str:
    """Get independent, dated factsheet sections without treating them as live holdings."""
    identity, response = await _resolve_or_response("get_thai_fund_factsheet", fund_class_name, proj_id, project_info)
    if response:
        return response
    assert identity is not None
    selected = list(_DEFAULT_FACTSHEET_SECTIONS) if sections is None else [str(section) for section in sections]
    unknown = sorted(set(selected) - _FACTSHEET_SECTIONS)
    if not selected or unknown:
        valid = ", ".join(sorted(_FACTSHEET_SECTIONS))
        return _mcp_failure(
            "get_thai_fund_factsheet",
            ErrorCode.INPUT_VALIDATION_ERROR,
            f"sections must be a non-empty subset of: {valid}.",
            source=_SOURCE,
        )

    loaders = {
        "statistics": _factsheet_statistics,
        "top_holdings": _factsheet_top_holdings,
        "urls": _factsheet_urls,
    }
    results = await asyncio.gather(*(loaders[section](identity) for section in selected))
    section_data = dict(zip(selected, results, strict=True))
    section_statuses = {name: section["status"] for name, section in section_data.items()}
    failures = [name for name, status in section_statuses.items() if status not in {"OK", "EMPTY_RESULT"}]
    payload = _base_payload("PARTIAL" if failures else "OK")
    payload.update({
        "scope": "MIXED",
        "identity": identity.compact(),
        "sections": section_data,
        "sectionStatus": section_statuses,
        "partialSuccess": bool(failures),
        "recovery": _recovery(
            "RETRY_FAILED_SECTIONS" if failures else "NONE",
            "Retry only the listed failed factsheet sections; successful dated evidence remains valid." if failures else "All requested factsheet sections completed.",
        ),
    })
    return _mcp_success("get_thai_fund_factsheet", payload, source=_SOURCE)


@yfinance_server.tool(
    name="get_thai_fund_dividend_history",
    output_schema=_TOOL_OUTPUT_SCHEMAS["get_thai_fund_dividend_history"],
    description="""Return one page of official Thai SEC mutual-fund dividend history.

The requested fund_class_name is resolved exactly, but payout history is
project scoped by the SEC endpoint. Each row retains class_abbr_name and the
response exposes nextCursor/hasMore instead of claiming complete history.
project_info may narrow the documented SEC profile search by official project
name or abbreviation.
""",
)
async def get_thai_fund_dividend_history(
    fund_class_name: str,
    proj_id: str | None = None,
    max_results: int = _MAX_PAGE_SIZE,
    next_cursor: str | None = None,
    project_info: str | None = None,
) -> str:
    """Get one sorted, project-scoped page of dividend history without class inference."""
    identity, response = await _resolve_or_response("get_thai_fund_dividend_history", fund_class_name, proj_id, project_info)
    if response:
        return response
    assert identity is not None
    if isinstance(max_results, bool) or not isinstance(max_results, int) or not 1 <= max_results <= _MAX_PAGE_SIZE:
        return _mcp_failure(
            "get_thai_fund_dividend_history",
            ErrorCode.INPUT_VALIDATION_ERROR,
            "max_results must be an integer from 1 through 100.",
            source=_SOURCE,
        )
    try:
        payload = await _request_json("/daily-info/dividend-history", {
            "proj_id": identity.proj_id,
            "page_size": max_results,
            "next_cursor": _optional_str(next_cursor),
        })
    except SecThailandProviderError as error:
        return _provider_failure("get_thai_fund_dividend_history", error)

    rows = [row for row in payload["items"] if isinstance(row, dict)]
    rows.sort(key=lambda row: str(row.get("dividend_date") or ""), reverse=True)
    dividends = [{
        "projId": row.get("proj_id"), "uniqueId": row.get("unique_id"),
        "classAbbrName": row.get("class_abbr_name"), "bookCloseDate": row.get("book_close_date"),
        "dividendDate": row.get("dividend_date"), "dividendValue": row.get("dividend_value"),
        "lastUpdatedAt": row.get("last_upd_date"),
    } for row in rows]
    next_value = _optional_str(payload.get("next_cursor"))
    base = _base_payload("OK" if dividends else "EMPTY_RESULT")
    base.update({
        "scope": "PROJECT",
        "identity": identity.compact(),
        "dataDate": max((item["dividendDate"] for item in dividends if isinstance(item["dividendDate"], str)), default=None),
        "dividends": dividends,
        "nextCursor": next_value,
        "hasMore": bool(next_value),
        "recovery": _recovery(
            "FETCH_NEXT_PAGE" if next_value else "NONE",
            "Dividend history is project scoped and this response covers only the returned page.",
        ),
    })
    return _mcp_success(
        "get_thai_fund_dividend_history",
        base,
        source=_SOURCE,
        data_date=base["dataDate"] if isinstance(base["dataDate"], str) else None,
    )
