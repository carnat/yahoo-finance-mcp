"""Explicit provider-gap workflows for scarce or entitlement-sensitive data."""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import re as _re
import urllib.error as _urlerror
import urllib.parse as _urlparse
import urllib.request as _urlrequest
from typing import Annotated, Any

import yfinance as yf
from pydantic import Field

from yfmcp import capital_structure as _cs
from yfmcp import url_policy as _url_policy
from yfmcp.data import DATA_DIR
from yfmcp.app import yfinance_server
from yfmcp.clients.market_providers import (
    _PROVIDER_USER_AGENT,
    ProviderJsonResult,
    fetch_alpha_vantage_json,
    fetch_finnhub_json,
)
from yfmcp.envelope import ErrorCode, _mcp_failure, _mcp_success
from yfmcp.schemas import _TOOL_OUTPUT_SCHEMAS
from yfmcp.util import _clamp_int
from yfmcp.validation import _validate_ticker


_ALPHA_HISTORICAL_PUT_CALL_TTL = 365 * 24 * 60 * 60
_OWNERSHIP_TTL = 7 * 24 * 60 * 60
_ALPHA_CAPACITY = "SCARCE_SHARED_QUOTA"
_FINNHUB_CAPACITY = "ENTITLEMENT_DEPENDENT"
_SOURCE_POLICY_PATH = DATA_DIR / "news-source-capabilities.json"


def _number(value: object) -> float | int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _attempt(provider: str, result: ProviderJsonResult) -> dict[str, object]:
    attempt: dict[str, object] = {
        "provider": provider,
        "status": result.status,
        "attempted": result.provider_attempted,
        "cacheStatus": result.cache_status,
        "url": result.public_url,
    }
    if result.http_status is not None:
        attempt["httpStatus"] = result.http_status
    if result.message:
        attempt["message"] = result.message
    if result.retry_after:
        attempt["retryAfter"] = result.retry_after
    return attempt


def _recovery_for(status: str, *, alpha_allowed: bool = False) -> str:
    if status == "SOURCE_UNCONFIGURED":
        return "CONFIGURE_PROVIDER"
    if status == "AUTH_ERROR":
        return "CHECK_PROVIDER_CREDENTIALS"
    if status == "ENTITLEMENT_REQUIRED":
        return "USE_ALPHA_VANTAGE_EXPLICITLY" if alpha_allowed else "CHECK_PROVIDER_ENTITLEMENT"
    if status == "RATE_LIMIT":
        return "RETRY_AFTER_PROVIDER_RESET"
    if status == "TIMEOUT":
        return "RETRY_PROVIDER"
    if status == "NOT_ELIGIBLE":
        return "USE_YAHOO_OR_OFFICIAL_FILINGS"
    if status == "NOT_FOUND":
        return "VERIFY_TICKER_AND_DATE"
    return "RETRY_OR_USE_PRIMARY_PROVIDER"


def _provider_failure_payload(
    ticker: str,
    status: str,
    attempts: list[dict[str, object]],
    *,
    action: str,
    capacity_class: str | None = None,
) -> dict[str, object]:
    return {
        "ticker": ticker,
        "status": status,
        "source": None,
        "providerGapFilled": False,
        "capacityClass": capacity_class,
        "evidenceClass": "CONTEXTUAL_PROVIDER_DATA",
        "decisionGrade": False,
        "providerAttempts": attempts,
        "recommendedNextAction": action,
    }


def _validate_historical_date(value: str) -> datetime.date:
    try:
        parsed = datetime.date.fromisoformat(str(value))
    except ValueError as error:
        raise ValueError("date must use YYYY-MM-DD format.") from error
    if parsed <= datetime.date(2008, 1, 1):
        raise ValueError("date must be later than 2008-01-01.")
    if parsed > datetime.datetime.now(datetime.timezone.utc).date():
        raise ValueError("date must not be in the future.")
    return parsed


@yfinance_server.tool(
    name="get_historical_put_call_ratio",
    description="""Return Alpha Vantage's historical put/call ratio for one ticker and one explicit date.

Use this only when the user needs a historical dated ratio. For a current
options snapshot, call summarize_options_flow instead; omitting date is not
allowed because it would spend scarce Alpha quota on data Yahoo already
provides. A successful response is contextual, never decision-grade.
""",
)
async def get_historical_put_call_ratio(
    ticker: str,
    date: Annotated[str, Field(description="Historical trading date in YYYY-MM-DD; required and later than 2008-01-01.")],
) -> str:
    tool = "get_historical_put_call_ratio"
    error = _validate_ticker(ticker)
    if error:
        return _mcp_failure(tool, ErrorCode.INPUT_VALIDATION_ERROR, error)
    try:
        requested_date = _validate_historical_date(date)
    except ValueError as error:
        return _mcp_failure(
            tool,
            ErrorCode.INPUT_VALIDATION_ERROR,
            f"{error} Use summarize_options_flow for a current Yahoo options snapshot.",
        )

    ticker_u = ticker.upper()
    result = await fetch_alpha_vantage_json(
        "HISTORICAL_PUT_CALL_RATIO",
        {"symbol": ticker_u, "date": requested_date.isoformat()},
        ttl_seconds=_ALPHA_HISTORICAL_PUT_CALL_TTL,
    )
    attempt = _attempt("alpha_vantage", result)
    if result.status != "OK" or not isinstance(result.payload, dict):
        payload = _provider_failure_payload(
            ticker_u,
            result.status,
            [attempt],
            action=_recovery_for(result.status),
            capacity_class=_ALPHA_CAPACITY,
        )
        payload["dataDate"] = requested_date.isoformat()
        return _mcp_success(tool, payload, source="alpha_vantage", data_date=requested_date.isoformat())

    response_symbol = str(result.payload.get("symbol") or ticker_u).upper()
    response_date = str(result.payload.get("date") or requested_date.isoformat())
    if response_symbol != ticker_u or response_date != requested_date.isoformat():
        attempt["validationStatus"] = "PROVIDER_IDENTITY_MISMATCH"
        payload = _provider_failure_payload(
            ticker_u,
            "PROVIDER_IDENTITY_MISMATCH",
            [attempt],
            action="RETRY_OR_VERIFY_PROVIDER_RESPONSE",
            capacity_class=_ALPHA_CAPACITY,
        )
        payload["dataDate"] = requested_date.isoformat()
        return _mcp_success(tool, payload, source="alpha_vantage", data_date=requested_date.isoformat())

    full_chain = _number(result.payload.get("put_call_ratio_full_chain"))
    expiration_rows: list[dict[str, object]] = []
    raw_rows = result.payload.get("put_call_ratio_by_expiration")
    if isinstance(raw_rows, list):
        for row in raw_rows:
            if not isinstance(row, dict):
                continue
            expiry = str(row.get("date") or "").strip()
            ratio = _number(row.get("value"))
            if expiry and ratio is not None:
                expiration_rows.append({"expirationDate": expiry, "putCallRatio": ratio})
    if full_chain is None and not expiration_rows:
        payload = _provider_failure_payload(
            ticker_u,
            "PROVIDER_ERROR",
            [attempt],
            action="RETRY_OR_USE_PRIMARY_PROVIDER",
            capacity_class=_ALPHA_CAPACITY,
        )
        payload["dataDate"] = requested_date.isoformat()
        return _mcp_success(tool, payload, source="alpha_vantage", data_date=requested_date.isoformat())

    payload = {
        "ticker": ticker_u,
        "status": "OK",
        "source": "alpha_vantage",
        "providerGapFilled": True,
        "capacityClass": _ALPHA_CAPACITY,
        "dataDate": response_date,
        "freshnessStatus": "HISTORICAL_OBSERVATION",
        "putCallRatioFullChain": full_chain,
        "byExpiration": expiration_rows,
        "expirationCount": len(expiration_rows),
        "evidenceClass": "CONTEXTUAL_OPTIONS_DATA",
        "decisionGrade": False,
        "cacheStatus": result.cache_status,
        "fetchedAt": result.fetched_at,
        "providerAttempts": [attempt],
        "recommendedNextAction": "NONE",
    }
    return _mcp_success(
        tool,
        payload,
        source="alpha_vantage",
        data_date=str(payload["dataDate"]),
        cache_hit=result.cache_status != "MISS",
    )


def _finnhub_eligible(ticker: str) -> tuple[bool, str | None]:
    try:
        policy = json.loads(_SOURCE_POLICY_PATH.read_text(encoding="utf-8"))["providers"]["finnhub"]
    except (OSError, KeyError, TypeError, ValueError):
        # Fail closed (R-news Batch C, decision 14) with the bundled policy's reasonCode, as the Worker's compiled-in policy gives.
        return False, "MARKET_NOT_ENABLED_BY_DEPLOYED_ENTITLEMENT"
    ticker_u = ticker.upper()
    ineligible = ticker_u in {str(value).upper() for value in policy.get("ineligibleTickers") or []} or any(
        ticker_u.endswith(str(suffix).upper()) for suffix in policy.get("ineligibleTickerSuffixes") or []
    )
    return (not ineligible, str(policy.get("reasonCode") or "FINNHUB_MARKET_NOT_ELIGIBLE") if ineligible else None)


def _ownership_rows(payload: dict[str, object], provider: str) -> list[dict[str, object]]:
    candidates: object = None
    for key in ("holdings", "ownership", "data"):
        if isinstance(payload.get(key), list):
            candidates = payload[key]
            break
    rows: list[dict[str, object]] = []
    if not isinstance(candidates, list):
        return rows
    for row in candidates:
        if not isinstance(row, dict):
            continue
        if provider == "alpha_vantage":
            normalized = {
                "holderName": row.get("holder_name"),
                "sharesHeld": _number(row.get("shares_held")),
                "sharesChanged": _number(row.get("shares_changed")),
                "sharesChangedPct": _number(row.get("shares_changed_percentage")),
                "changeType": row.get("change_type"),
                "reportDate": row.get("last_reported"),
            }
        else:
            normalized = {
                "holderName": row.get("name") or row.get("holder") or row.get("holderName"),
                "sharesHeld": _number(row.get("share") or row.get("shares") or row.get("sharesHeld")),
                "sharesChanged": _number(row.get("change") or row.get("sharesChanged")),
                "sharesChangedPct": _number(row.get("changePercent") or row.get("sharesChangedPct")),
                "reportDate": row.get("filingDate") or row.get("reportDate") or row.get("date"),
            }
        compact = {
            key: value
            for key, value in normalized.items()
            if value is not None and value != ""
        }
        if compact:
            rows.append(compact)
    return rows


def _ownership_data_date(payload: dict[str, object], rows: list[dict[str, object]]) -> str | None:
    direct = payload.get("date") or payload.get("last_reported") or payload.get("lastReported")
    if direct:
        return str(direct)[:10]
    dates = sorted(
        {str(row.get("reportDate"))[:10] for row in rows if row.get("reportDate")},
        reverse=True,
    )
    return dates[0] if dates else None


@yfinance_server.tool(
    name="get_expanded_institutional_ownership",
    description="""Return a deeper institutional-holder list than Yahoo's ordinary top-holder view.

Use get_ownership_holders first for normal holder questions. This action tries
eligible Finnhub coverage first. It may use scarce Alpha Vantage quota only
when allow_scarce_fallback=true; provider failures never trigger Alpha
silently. All results are contextual and should be verified against SEC 13F
filings for material ownership decisions.
""",
)
async def get_expanded_institutional_ownership(
    ticker: str,
    allow_scarce_fallback: Annotated[
        bool,
        Field(description="Explicitly permit one Alpha Vantage call if Finnhub is unavailable, ineligible, or returns no usable holders."),
    ] = False,
    max_holders: Annotated[int, Field(ge=1, le=100, description="Maximum compact holder rows returned; default 50, cap 100.")] = 50,
) -> str:
    tool = "get_expanded_institutional_ownership"
    error = _validate_ticker(ticker)
    if error:
        return _mcp_failure(tool, ErrorCode.INPUT_VALIDATION_ERROR, error)
    if isinstance(max_holders, bool) or not isinstance(max_holders, int) or not 1 <= max_holders <= 100:
        return _mcp_failure(tool, ErrorCode.INPUT_VALIDATION_ERROR, "max_holders must be an integer from 1 through 100.")

    ticker_u = ticker.upper()
    attempts: list[dict[str, object]] = []
    provider = "finnhub"
    eligible, reason_code = _finnhub_eligible(ticker_u)
    if eligible:
        result = await fetch_finnhub_json(
            "/stock/ownership",
            {"symbol": ticker_u, "limit": max_holders},
            ttl_seconds=_OWNERSHIP_TTL,
        )
        attempts.append(_attempt("finnhub", result))
    else:
        result = ProviderJsonResult(None, "NOT_ELIGIBLE", "", False, "MISS", message=reason_code)
        attempts.append({
            "provider": "finnhub",
            "status": "NOT_ELIGIBLE",
            "attempted": False,
            "reasonCode": reason_code,
        })

    rows = _ownership_rows(result.payload, "finnhub") if isinstance(result.payload, dict) else []
    if isinstance(result.payload, dict):
        response_symbol = str(result.payload.get("symbol") or ticker_u).upper()
        if response_symbol != ticker_u:
            attempts[-1]["validationStatus"] = "PROVIDER_IDENTITY_MISMATCH"
            result = ProviderJsonResult(
                None,
                "PROVIDER_IDENTITY_MISMATCH",
                result.public_url,
                result.provider_attempted,
                result.cache_status,
                fetched_at=result.fetched_at,
                message="Provider response symbol did not match the requested ticker.",
            )
            rows = []
    if not rows and allow_scarce_fallback:
        provider = "alpha_vantage"
        result = await fetch_alpha_vantage_json(
            "INSTITUTIONAL_HOLDINGS",
            {"symbol": ticker_u},
            ttl_seconds=_OWNERSHIP_TTL,
        )
        attempts.append(_attempt("alpha_vantage", result))
        rows = _ownership_rows(result.payload, "alpha_vantage") if isinstance(result.payload, dict) else []
        if isinstance(result.payload, dict):
            response_symbol = str(result.payload.get("symbol") or ticker_u).upper()
            if response_symbol != ticker_u:
                attempts[-1]["validationStatus"] = "PROVIDER_IDENTITY_MISMATCH"
                result = ProviderJsonResult(
                    None,
                    "PROVIDER_IDENTITY_MISMATCH",
                    result.public_url,
                    result.provider_attempted,
                    result.cache_status,
                    fetched_at=result.fetched_at,
                    message="Provider response symbol did not match the requested ticker.",
                )
                rows = []

    capacity_class = _ALPHA_CAPACITY if provider == "alpha_vantage" else _FINNHUB_CAPACITY
    if result.status != "OK" or not isinstance(result.payload, dict) or not rows:
        status = result.status if result.status != "OK" else "NOT_FOUND"
        action = (
            "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK"
            if status == "NOT_FOUND" and not allow_scarce_fallback
            else _recovery_for(status, alpha_allowed=not allow_scarce_fallback)
        )
        payload = _provider_failure_payload(
            ticker_u,
            status,
            attempts,
            action=action,
            capacity_class=capacity_class,
        )
        payload["alphaFallbackAllowed"] = allow_scarce_fallback
        return _mcp_success(tool, payload, source=provider)

    total_count = len(rows)
    returned = rows[:max_holders]
    data_date = _ownership_data_date(result.payload, returned)
    aggregate: dict[str, object] = {}
    if provider == "alpha_vantage":
        for source_key, output_key in (
            ("total_institutional_holders", "totalInstitutionalHolders"),
            ("total_institutional_shares", "totalInstitutionalShares"),
            ("total_institutional_ownership_percentage", "totalInstitutionalOwnershipPct"),
            ("holders_with_increased_holdings", "holdersIncreased"),
            ("holders_with_decreased_holdings", "holdersDecreased"),
            ("holders_with_unchanged_holdings", "holdersUnchanged"),
            ("shares_with_increased_holdings", "sharesIncreased"),
            ("shares_with_decreased_holdings", "sharesDecreased"),
            ("shares_with_unchanged_holdings", "sharesUnchanged"),
        ):
            value = _number(result.payload.get(source_key))
            if value is not None:
                aggregate[output_key] = value
    payload = {
        "ticker": ticker_u,
        "status": "OK",
        "source": provider,
        "providerGapFilled": True,
        "capacityClass": capacity_class,
        "scope": "PROVIDER_AGGREGATED_INSTITUTIONAL_OWNERSHIP",
        "dataDate": data_date,
        "freshnessStatus": "DATED" if data_date else "DATE_NOT_DISCLOSED",
        "holders": returned,
        "returnedCount": len(returned),
        "providerRowCount": total_count,
        "truncated": total_count > len(returned),
        "aggregate": aggregate,
        "evidenceClass": "CONTEXTUAL_OWNERSHIP_DATA",
        "decisionGrade": False,
        "cacheStatus": result.cache_status,
        "fetchedAt": result.fetched_at,
        "providerAttempts": attempts,
        "recommendedNextAction": "VERIFY_SEC_13F_FOR_MATERIAL_USE",
    }
    return _mcp_success(
        tool,
        payload,
        source=provider,
        data_date=data_date,
        cache_hit=result.cache_status != "MISS",
    )


_COMPANIES_HOUSE_API = "https://api.company-information.service.gov.uk"
_COMPANIES_HOUSE_CATEGORIES = [
    "accounts", "capital", "mortgage", "confirmation-statement", "resolution", "incorporation", "officers",
    "persons-with-significant-control", "address", "annotation", "change-of-name", "miscellaneous",
]


async def _companies_house_get(path: str, key: str) -> tuple[int, dict | None]:
    import base64

    token = base64.b64encode(f"{key}:".encode()).decode()

    def fetch() -> tuple[int, dict | None]:
        req = _urlrequest.Request(
            f"{_COMPANIES_HOUSE_API}{path}",
            headers={"Authorization": f"Basic {token}", "Accept": "application/json", "User-Agent": _PROVIDER_USER_AGENT},
        )
        try:
            with _url_policy.urlopen(req, timeout=15) as resp:  # noqa: S310
                return resp.status, json.loads(resp.read().decode("utf-8", errors="replace"))
        except _urlerror.HTTPError as exc:
            return exc.code, None

    return await asyncio.get_running_loop().run_in_executor(None, _url_policy.in_context(fetch))


def _companies_house_error(status: int, step: str) -> str:
    if status == 429:
        return json.dumps({"error": True, "message": f"Companies House {step}: HTTP 429 rate limited (600 requests per 5 minutes)."})
    if status in (401, 403):
        return json.dumps({"error": True, "code": "PROVIDER_ERROR", "status": "AUTH_ERROR", "message": f"Companies House {step}: HTTP {status}; check COMPANIES_HOUSE_API_KEY."})
    return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": f"Companies House {step}: HTTP {status}."})


def _charge_rows(payload: dict) -> list[dict]:
    out = []
    for c in payload.get("items") if isinstance(payload.get("items"), list) else []:
        classification = c.get("classification") if isinstance(c.get("classification"), dict) else {}
        particulars = c.get("particulars") if isinstance(c.get("particulars"), dict) else {}
        persons = c.get("persons_entitled") if isinstance(c.get("persons_entitled"), list) else []
        out.append({
            "chargeCode": c.get("charge_code"),
            "status": c.get("status"),
            "createdOn": c.get("created_on"),
            "deliveredOn": c.get("delivered_on"),
            "satisfiedOn": c.get("satisfied_on"),
            "classification": classification.get("description"),
            "personsEntitled": [str(p.get("name") or "") for p in persons if isinstance(p, dict) and p.get("name")],
            "particulars": particulars["description"][:400] if isinstance(particulars.get("description"), str) else None,
        })
    return out


async def _issuer_name(ticker: str) -> str | None:
    try:
        info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
    except Exception:
        return None
    info = info if isinstance(info, dict) else {}
    return str(info.get("longName") or info.get("shortName") or "").strip() or None


@yfinance_server.tool(
    name="get_uk_company_filings",
    description="UK Companies House filing history for a UK-registered issuer (e.g. IQE.L): accounts, share allotments (SH01), charges (MR01, secured lending) and resolutions, with document links; include_charges adds the charge register. Resolves by company_number, company_name, or the ticker's issuer name. Official statutory filings; RNS market announcements are not held here. Needs the COMPANIES_HOUSE_API_KEY secret.",
    after="reconcile_metric_sources",  # listing is resolved in registration order: extract_analyst_valuation_methods then settles after this anchor, ahead of this tool
)
async def get_uk_company_filings(
    ticker: str | None = None,
    company_number: str | None = None,
    company_name: str | None = None,
    category: str | None = None,
    limit: int = 25,
    include_charges: bool = False,
) -> str:
    key = (os.environ.get("COMPANIES_HOUSE_API_KEY") or "").strip()
    base = {"source": "companies_house", "sourceType": "uk_companies_house"}
    if not key:
        return json.dumps({
            **base, "ticker": ticker, "status": "SOURCE_UNCONFIGURED", "code": "SOURCE_UNCONFIGURED",
            "message": "Companies House is not configured; set the COMPANIES_HOUSE_API_KEY secret (a free key from the Companies House developer hub).",
            "filings": [],
        })
    cat = category.strip().lower() if category else None
    if cat and cat not in _COMPANIES_HOUSE_CATEGORIES:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": f"category must be one of: {', '.join(_COMPANIES_HOUSE_CATEGORIES)}."})
    size = _clamp_int(limit, 25, 1, 100)
    warnings: list[dict] = []
    number = company_number.strip().upper() if company_number else None
    if number and _re.fullmatch(r"[0-9]{1,8}", number):
        number = number.zfill(8)
    if number and not _re.fullmatch(r"[A-Z0-9]{8}", number):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "company_number must be an 8-character Companies House number, e.g. 01234567 or SC123456."})
    matched_by = "company_number"
    company: dict | None = None
    try:
        if not number:
            name = (company_name or "").strip() or None
            matched_by = "company_name"
            if not name and ticker:
                name = await _issuer_name(ticker)
                matched_by = "ticker_issuer_name"
                if not _re.search(r"\.(?:L|IL)$", ticker, _re.IGNORECASE):
                    warnings.append({"code": "NON_UK_LISTING", "message": f"{ticker} is not a London listing; the match is by issuer name only.", "severity": "info"})
            if not name:
                return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "Provide company_number, company_name, or a ticker whose issuer name can be resolved."})
            status, body = await _companies_house_get(f"/search/companies?q={_urlparse.quote(name, safe='')}&items_per_page=20", key)
            if body is None:
                return _companies_house_error(status, "company search")
            match = _cs.pick_companies_house_match(name, body)
            if match is None:
                items = body.get("items") if isinstance(body.get("items"), list) else []
                return json.dumps({
                    **base, "ticker": ticker, "status": "COMPANY_NOT_MATCHED", "searchedName": name,
                    "candidates": [{"companyNumber": i.get("company_number"), "title": i.get("title"), "companyStatus": i.get("company_status"), "addressSnippet": i.get("address_snippet")} for i in items[:5]],
                    "message": "No registered company name matched the issuer exactly; pass company_number from the candidates.",
                    "filings": [], "warnings": warnings,
                })
            number = str(match.get("company_number") or "")
            company = {"companyNumber": number, "name": match.get("title"), "status": match.get("company_status")}
        if company is None:
            status, body = await _companies_house_get(f"/company/{number}", key)
            if status == 404:
                return json.dumps({**base, "ticker": ticker, "status": "COMPANY_NOT_FOUND", "companyNumber": number, "filings": []})
            if body is None:
                return _companies_house_error(status, "company profile")
            company = {"companyNumber": number, "name": body.get("company_name"), "status": body.get("company_status")}
        status, history = await _companies_house_get(f"/company/{number}/filing-history?items_per_page={size}{f'&category={cat}' if cat else ''}", key)
        if history is None:
            return _companies_house_error(status, "filing history")
        total = history.get("total_count")
        out: dict = {
            **base, "ticker": ticker, "status": "OK", "decisionUse": "USE_OFFICIAL_EVIDENCE",
            "company": {**company, "matchedBy": matched_by},
            "category": cat,
            "filings": _cs.companies_house_filings(str(number), history),
            "totalFilings": total if isinstance(total, (int, float)) and not isinstance(total, bool) else None,
            "filingHistoryUrl": f"https://find-and-update.company-information.service.gov.uk/company/{number}/filing-history",
            "notes": [
                "Companies House holds statutory filings: accounts, share allotments (SH01), charges (MR01), resolutions and confirmation statements.",
                "Market announcements (RNS) such as results, loan-note terms and trading updates are not filed here; read them on the company's investor site.",
                "Documents are PDFs; documentContentUrl needs the same API key, viewerUrl opens the public viewer.",
            ],
            "warnings": warnings,
        }
        if include_charges:
            status, charges = await _companies_house_get(f"/company/{number}/charges", key)
            if charges is not None:
                out["charges"] = _charge_rows(charges)
            elif status == 404:
                out["charges"] = []
            else:
                warnings.append({"code": "CHARGES_UNAVAILABLE", "message": f"Companies House charges: HTTP {status}.", "severity": "warning"})
        return json.dumps(out)
    except Exception as exc:
        message = str(exc)
        if "timed out" in message.lower():
            return json.dumps({"error": True, "message": "Companies House request timed out."})
        return json.dumps({"error": True, "code": "PROVIDER_ERROR", "message": f"Companies House request failed: {message}"})
