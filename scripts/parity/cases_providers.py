"""Characterization parity cases for the provider rules BR-PRV-001, 004-011 (Phase 5 unit 5 coverage).

BR-PRV-002 / BR-PRV-003 (provider eligibility) are covered by the `prv.eligibility.*` block of cases_pricing_evidence_tools.py.

Every case runs the real tool code in both runtimes against routed fake upstream responses (no network):
  - expanded institutional ownership: provider_gaps.get_expanded_institutional_ownership / getExpandedInstitutionalOwnership
  - historical put/call ratio:         provider_gaps.get_historical_put_call_ratio / getHistoricalPutCallRatio
  - Companies House filings:           server.get_uk_company_filings / getUkCompanyFilings
The cases pin today's output. Where the runtimes differ the case is `pinned` with `expect_py` / `expect_ts` and a
`FINDING` decision note; nothing here is a repair.

Harness limits noticed while writing these (not production findings): the Python stub raises HTTPError with empty headers,
so a `Retry-After` response header cannot reach `retryAfter` in Python; the cases leave retryAfter out. Both runtimes
get fresh provider caches per case (Python: the process cache is cleared; Worker: `isolate`).
"""

from __future__ import annotations

import contextlib
import json
import os
from unittest.mock import patch

from scripts.parity.harness import Case

import server  # noqa: F401,E402  (registers the tools the way the existing suites do)
from yfmcp.cache import _tool_cache  # noqa: E402
from yfmcp.tools import provider_gaps  # noqa: E402

CASES: list[Case] = []

AV_KEY, FH_KEY, CH_KEY = "avkey123", "fhkey456", "chkey789"
_KEY_VARS = ("ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY", "FINNHUB_API_KEY", "FINNHUB_TOKEN", "COMPANIES_HOUSE_API_KEY")


# ───────────────────────────── plumbing ─────────────────────────────

@contextlib.contextmanager
def _python_env(env: dict):
    """Only the given provider keys are set; the Python provider cache starts empty."""
    with patch.dict(os.environ, {}, clear=False):
        for var in _KEY_VARS:
            os.environ.pop(var, None)
        os.environ.update(env)
        _tool_cache._store.clear()
        yield


def _worker_js(env: dict, body: str) -> dict:
    return {"js": f"m.response.setWorkerEnv({json.dumps(env)}); {body}", "isolate": True}


def _data(v: dict) -> dict:
    """The payload of a Python envelope, or the Worker's bare JSON."""
    return v["data"] if isinstance(v.get("data"), dict) else v


def _error(v: dict):
    err = v.get("error")
    if isinstance(err, dict):
        return {"error": True, "code": err.get("code"), "message": err.get("message")}
    if v.get("error") is True:  # Companies House returns bare error objects
        return {k: v[k] for k in ("error", "code", "status", "message") if k in v}
    return None


_PAYLOAD_KEYS = ("status", "source", "providerGapFilled", "capacityClass", "decisionGrade", "evidenceClass", "alphaFallbackAllowed",
                 "dataDate", "freshnessStatus", "scope", "returnedCount", "providerRowCount", "truncated", "expirationCount",
                 "putCallRatioFullChain", "byExpiration", "aggregate", "cacheStatus", "recommendedNextAction")


def _attempt_view(a: dict, *, detail: bool, msg: bool = True) -> dict:
    keys = ("provider", "status", "attempted", "validationStatus", "reasonCode") + (("httpStatus", "cacheStatus", "url") if detail else ())
    out = {k: a[k] for k in keys if k in a}
    if detail and msg and "message" in a:
        out["message"] = a["message"]
    return out


def _view(*, holders: bool | int = False, detail: bool = False, msg: bool = True, drop: tuple = ()):
    """Compact comparable view of a provider-gap payload (fetchedAt and clock fields never compared)."""
    def view(v: dict):
        err = _error(v)
        if err:
            return err
        d = _data(v)
        out = {k: d[k] for k in _PAYLOAD_KEYS if k in d and k not in drop}
        if holders is not False and "holders" in d:
            out["holders"] = d["holders"] if holders is True else d["holders"][:holders]
        out["attempts"] = [_attempt_view(a, detail=detail, msg=msg) for a in d.get("providerAttempts", [])]
        return out
    return view


def _brief(v: dict):
    """status, next action and the (provider, status, attempted, httpStatus) of every attempt."""
    err = _error(v)
    if err:
        return err
    d = _data(v)
    return [d["status"], d["recommendedNextAction"],
            [[a["provider"], a["status"], a["attempted"], a.get("httpStatus")] for a in d["providerAttempts"]]]


def _mm(v: dict):
    """status, source, next action and (provider, status, validationStatus) of every attempt."""
    err = _error(v)
    if err:
        return err
    d = _data(v)
    return [d["status"], d["source"], d["recommendedNextAction"],
            [[a["provider"], a["status"], a.get("validationStatus")] for a in d["providerAttempts"]]]


def _code(v: dict):
    err = _error(v)
    return err["code"] if err else _data(v).get("status")


def _add(name, ids, py, ts, select, routes=None, expect=None, expect_py=None, expect_ts=None, finding=None, note=""):
    kw = {}
    if finding:
        kw.update(mode="pinned", decision="FINDING: " + finding, expect_py=expect_py, expect_ts=expect_ts)
    elif expect is not None:
        kw["expect"] = expect
    CASES.append(Case(name=name, ids=ids, py=py, ts=ts, select=select, routes=routes, note=note, **kw))


# ───────────────────────────── ownership ─────────────────────────────

FH_OWN = "https://finnhub.io/api/v1/stock/ownership?symbol={sym}&limit={limit}"
AV_OWN = "https://www.alphavantage.co/query?function=INSTITUTIONAL_HOLDINGS&symbol={sym}&apikey=" + AV_KEY


def _own(name, ids, ticker="AAPL", *, env=None, routes=None, allow=False, max_holders=50, calls=1, select=None, **kw):
    env = {"FINNHUB_API_KEY": FH_KEY} if env is None else env
    tickers = ticker if isinstance(ticker, list) else [ticker] * calls

    async def py():
        with _python_env(env):
            out = [json.loads(await provider_gaps.get_expanded_institutional_ownership(t, allow, max_holders)) for t in tickers]
        return out[-1] if len(out) == 1 else out

    body = (f"const out = []; for (const t of {json.dumps(tickers)}) out.push(JSON.parse(await m.yahooFinance.getExpandedInstitutionalOwnership("
            f"t, {json.dumps(allow)}, {json.dumps(max_holders)}))); return out.length === 1 ? out[0] : out;")
    _add(name, ids, py, _worker_js(env, body), select or _view(), routes, **kw)


def _fh(rows, symbol="AAPL", key="ownership", **extra):
    return {"json": {"symbol": symbol, key: rows, **extra}}


FH_ROWS = [
    {"name": "Vanguard Group", "share": 1_000_000, "change": 5_000, "filingDate": "2025-03-31"},
    {"name": "BlackRock", "share": "900,000", "change": -2_500, "filingDate": "2025-06-30"},
    {"name": "State Street", "share": 400_000, "change": 100, "filingDate": "2025-03-31"},
]


def _av_rows(n):
    return [{"holder_name": f"Holder {i:03d}", "shares_held": str(1000 - i), "shares_changed": str(i), "shares_changed_percentage": "1.5",
             "change_type": "INCREASE", "last_reported": f"2025-0{1 + i % 6}-15"} for i in range(n)]


# BR-PRV-001: Finnhub first, Alpha only on explicit opt-in and only when Finnhub gave no usable rows.
_own("prv.ownership.finnhub_rows_are_used_and_alpha_is_never_called", ["BR-PRV-001"],
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh(FH_ROWS)}, select=_view(holders=True, detail=True, drop=("cacheStatus",)),
     expect={"status": "OK", "source": "finnhub", "providerGapFilled": True, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
             "evidenceClass": "CONTEXTUAL_OWNERSHIP_DATA", "dataDate": "2025-06-30", "freshnessStatus": "DATED",
             "scope": "PROVIDER_AGGREGATED_INSTITUTIONAL_OWNERSHIP", "returnedCount": 3, "providerRowCount": 3, "truncated": False,
             "aggregate": {}, "recommendedNextAction": "VERIFY_SEC_13F_FOR_MATERIAL_USE",
             "holders": [{"holderName": "Vanguard Group", "sharesHeld": 1000000, "sharesChanged": 5000, "reportDate": "2025-03-31"},
                         {"holderName": "BlackRock", "sharesHeld": 900000, "sharesChanged": -2500, "reportDate": "2025-06-30"},
                         {"holderName": "State Street", "sharesHeld": 400000, "sharesChanged": 100, "reportDate": "2025-03-31"}],
             "attempts": [{"provider": "finnhub", "status": "OK", "attempted": True, "cacheStatus": "MISS",
                           "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50"}]})

_own("prv.ownership.no_holders_without_opt_in_is_not_found_and_alpha_is_not_called", ["BR-PRV-001", "BR-PRV-007"],
     env={"FINNHUB_API_KEY": FH_KEY, "ALPHA_VANTAGE_API_KEY": AV_KEY}, routes={FH_OWN.format(sym="AAPL", limit=50): _fh([])},
     expect={"status": "NOT_FOUND", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
             "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False,
             "recommendedNextAction": "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK",
             "attempts": [{"provider": "finnhub", "status": "OK", "attempted": True}]})

_own("prv.ownership.opt_in_falls_back_to_alpha_and_truncates_to_max_holders", ["BR-PRV-001", "BR-PRV-009"], allow=True,
     env={"FINNHUB_API_KEY": FH_KEY, "ALPHA_VANTAGE_API_KEY": AV_KEY},
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh([]),
             AV_OWN.format(sym="AAPL"): {"json": {"symbol": "AAPL", "holdings": _av_rows(120), "total_institutional_holders": "4,512",
                                                  "total_institutional_shares": "9000000000", "total_institutional_ownership_percentage": "61.25",
                                                  "holders_with_increased_holdings": "2000", "holders_with_decreased_holdings": "1800",
                                                  "holders_with_unchanged_holdings": "712", "shares_with_increased_holdings": "5",
                                                  "shares_with_decreased_holdings": "6", "shares_with_unchanged_holdings": "bad"}}},
     select=_view(holders=2, drop=("cacheStatus",)),
     expect={"status": "OK", "source": "alpha_vantage", "providerGapFilled": True, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
             "evidenceClass": "CONTEXTUAL_OWNERSHIP_DATA", "dataDate": "2025-06-15", "freshnessStatus": "DATED",
             "scope": "PROVIDER_AGGREGATED_INSTITUTIONAL_OWNERSHIP", "returnedCount": 50, "providerRowCount": 120, "truncated": True,
             "aggregate": {"totalInstitutionalHolders": 4512, "totalInstitutionalShares": 9000000000, "totalInstitutionalOwnershipPct": 61.25,
                           "holdersIncreased": 2000, "holdersDecreased": 1800, "holdersUnchanged": 712, "sharesIncreased": 5, "sharesDecreased": 6},
             "recommendedNextAction": "VERIFY_SEC_13F_FOR_MATERIAL_USE",
             "holders": [{"holderName": "Holder 000", "sharesHeld": 1000, "sharesChanged": 0, "sharesChangedPct": 1.5, "changeType": "INCREASE", "reportDate": "2025-01-15"},
                         {"holderName": "Holder 001", "sharesHeld": 999, "sharesChanged": 1, "sharesChangedPct": 1.5, "changeType": "INCREASE", "reportDate": "2025-02-15"}],
             "attempts": [{"provider": "finnhub", "status": "OK", "attempted": True}, {"provider": "alpha_vantage", "status": "OK", "attempted": True}]})

_own("prv.ownership.finnhub_rows_are_cut_to_max_holders", ["BR-PRV-009"], max_holders=2,
     routes={FH_OWN.format(sym="AAPL", limit=2): _fh(FH_ROWS)}, select=_view(holders=True, drop=("cacheStatus",)),
     expect={"status": "OK", "source": "finnhub", "providerGapFilled": True, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
             "evidenceClass": "CONTEXTUAL_OWNERSHIP_DATA", "dataDate": "2025-06-30", "freshnessStatus": "DATED",
             "scope": "PROVIDER_AGGREGATED_INSTITUTIONAL_OWNERSHIP", "returnedCount": 2, "providerRowCount": 3, "truncated": True,
             "aggregate": {}, "recommendedNextAction": "VERIFY_SEC_13F_FOR_MATERIAL_USE",
             "holders": [{"holderName": "Vanguard Group", "sharesHeld": 1000000, "sharesChanged": 5000, "reportDate": "2025-03-31"},
                         {"holderName": "BlackRock", "sharesHeld": 900000, "sharesChanged": -2500, "reportDate": "2025-06-30"}],
             "attempts": [{"provider": "finnhub", "status": "OK", "attempted": True}]})


_own("prv.ownership.payload_date_beats_row_dates", ["BR-PRV-001", "BR-PRV-009"], allow=True, env={"ALPHA_VANTAGE_API_KEY": AV_KEY},
     routes={AV_OWN.format(sym="AAPL"): {"json": {"symbol": "AAPL", "date": "2025-09-30T00:00:00Z", "holdings": _av_rows(2)}}},
     select=lambda v: [_data(v)[k] for k in ("dataDate", "freshnessStatus", "returnedCount")], expect=["2025-09-30", "DATED", 2])
_own("prv.ownership.no_date_anywhere_is_date_not_disclosed", ["BR-PRV-009"],
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh([{"name": "Vanguard Group", "share": 5}])}, select=lambda v: [_data(v)[k] for k in ("dataDate", "freshnessStatus", "holders")],
     expect=[None, "DATE_NOT_DISCLOSED", [{"holderName": "Vanguard Group", "sharesHeld": 5}]])
_own("prv.ownership.finnhub_identity_mismatch_drops_rows", ["BR-PRV-001", "BR-PRV-006"],
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh(FH_ROWS, symbol="MSFT")}, select=_mm,
     expect=["PROVIDER_IDENTITY_MISMATCH", None, "RETRY_OR_USE_PRIMARY_PROVIDER", [["finnhub", "OK", "PROVIDER_IDENTITY_MISMATCH"]]])
_own("prv.ownership.finnhub_identity_mismatch_with_opt_in_uses_alpha", ["BR-PRV-001", "BR-PRV-006"], allow=True,
     env={"FINNHUB_API_KEY": FH_KEY, "ALPHA_VANTAGE_API_KEY": AV_KEY},
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh(FH_ROWS, symbol="MSFT"), AV_OWN.format(sym="AAPL"): {"json": {"symbol": "AAPL", "holdings": _av_rows(1)}}},
     select=_mm, expect=["OK", "alpha_vantage", "VERIFY_SEC_13F_FOR_MATERIAL_USE",
                         [["finnhub", "OK", "PROVIDER_IDENTITY_MISMATCH"], ["alpha_vantage", "OK", None]]])
_own("prv.ownership.alpha_identity_mismatch", ["BR-PRV-001", "BR-PRV-006"], allow=True,
     env={"FINNHUB_API_KEY": FH_KEY, "ALPHA_VANTAGE_API_KEY": AV_KEY},
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh([]), AV_OWN.format(sym="AAPL"): {"json": {"symbol": "MSFT", "holdings": _av_rows(3)}}},
     select=_mm, expect=["PROVIDER_IDENTITY_MISMATCH", None, "RETRY_OR_USE_PRIMARY_PROVIDER",
                         [["finnhub", "OK", None], ["alpha_vantage", "OK", "PROVIDER_IDENTITY_MISMATCH"]]])
_own("prv.ownership.symbol_comparison_is_case_insensitive_and_absent_symbol_is_accepted", ["BR-PRV-006"], ticker=["AAPL", "MSFT"],
     routes={FH_OWN.format(sym="AAPL", limit=50): {"json": {"symbol": "aapl", "ownership": FH_ROWS[:1]}},
             FH_OWN.format(sym="MSFT", limit=50): {"json": {"ownership": FH_ROWS[:1]}}},
     select=lambda v: [_data(x)["status"] for x in v], expect=["OK", "OK"])
_own("prv.ownership.empty_holdings_list_shadows_later_keys", ["BR-PRV-001"],
     routes={FH_OWN.format(sym="AAPL", limit=50): {"json": {"symbol": "AAPL", "holdings": [], "ownership": FH_ROWS, "data": FH_ROWS}}},
     select=_mm, expect=["NOT_FOUND", None, "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK", [["finnhub", "OK", None]]])
_own("prv.ownership.data_key_is_read_when_it_is_the_first_list", ["BR-PRV-001"],
     routes={FH_OWN.format(sym="AAPL", limit=50): {"json": {"symbol": "AAPL", "holdings": "x", "data": FH_ROWS[:2]}}},
     select=lambda v: [_data(v)[k] for k in ("status", "providerRowCount", "returnedCount")], expect=["OK", 2, 2])
_own("prv.ownership.zero_valued_finnhub_fields", ["BR-PRV-001"],
     routes={FH_OWN.format(sym="AAPL", limit=50): _fh([{"name": "Zero Holder", "share": 0, "shares": 7, "change": 0, "changePercent": 0, "filingDate": "2025-03-31"}])},
     select=lambda v: _view(holders=True)(v)["holders"],
     finding="Finnhub row fields equal to 0: Python's `or` chain skips a 0 (share 0 falls through to shares=7; change 0 and changePercent 0 are dropped); the Worker's `??` keeps the 0",
     expect_py=[{"holderName": "Zero Holder", "sharesHeld": 7, "reportDate": "2025-03-31"}],
     expect_ts=[{"holderName": "Zero Holder", "sharesHeld": 0, "sharesChanged": 0, "sharesChangedPct": 0, "reportDate": "2025-03-31"}])
_own("prv.ownership.max_holders_zero_is_rejected", ["BR-PRV-001"], max_holders=0, select=_view(),
     expect={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "max_holders must be an integer from 1 through 100."})


# BR-PRV-005 / 007 / 011: status mapping and next action per status (Finnhub endpoint errors, JSON error bodies as Finnhub sends them)
FH50 = FH_OWN.format(sym="AAPL", limit=50)
NO_ACCESS = "You don't have access to this resource."


def _err(status, text):
    return {"status": status, "json": {"error": text}}


for _n, _status, _text, _ids, _exp, _act in [
    ("403_no_access_is_entitlement_required", 403, NO_ACCESS, ["BR-PRV-005", "BR-PRV-007", "BR-PRV-011"], "ENTITLEMENT_REQUIRED", "USE_ALPHA_VANTAGE_EXPLICITLY"),
    ("403_invalid_token_is_auth_error", 403, "Invalid token", ["BR-PRV-005", "BR-PRV-011"], "AUTH_ERROR", "CHECK_PROVIDER_CREDENTIALS"),
    ("403_unrecognised_message_is_auth_error", 403, "denied", ["BR-PRV-005"], "AUTH_ERROR", "CHECK_PROVIDER_CREDENTIALS"),
    ("429_is_rate_limit", 429, "API limit reached", ["BR-PRV-005", "BR-PRV-011"], "RATE_LIMIT", "RETRY_AFTER_PROVIDER_RESET"),
    ("401_is_auth_error", 401, "nope", ["BR-PRV-005"], "AUTH_ERROR", "CHECK_PROVIDER_CREDENTIALS"),
    # Without the scarce-fallback opt-in a NOT_FOUND points at SEC 13F or the opt-in, not at VERIFY_TICKER_AND_DATE (see the opt-in case below).
    ("404_is_not_found", 404, "no such symbol", ["BR-PRV-005", "BR-PRV-011"], "NOT_FOUND", "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK"),
    ("500_is_provider_error", 500, "boom", ["BR-PRV-005", "BR-PRV-011"], "PROVIDER_ERROR", "RETRY_OR_USE_PRIMARY_PROVIDER"),
]:
    _own(f"prv.status.finnhub_{_n}", _ids, routes={FH50: _err(_status, _text)}, select=_brief,
         expect=[_exp, _act, [["finnhub", _exp, True, _status]]])
_own("prv.status.finnhub_404_with_opt_in_and_no_alpha_key_reports_the_alpha_attempt", ["BR-PRV-011"], allow=True, routes={FH50: _err(404, "no such symbol")}, select=_brief,
     expect=["SOURCE_UNCONFIGURED", "CONFIGURE_PROVIDER", [["finnhub", "NOT_FOUND", True, 404], ["alpha_vantage", "SOURCE_UNCONFIGURED", False, None]]])
_own("prv.status.finnhub_404_then_alpha_404_with_opt_in_is_verify_ticker_and_date", ["BR-PRV-011"], allow=True,
     env={"FINNHUB_API_KEY": FH_KEY, "ALPHA_VANTAGE_API_KEY": AV_KEY}, routes={FH50: _err(404, "no such symbol"), AV_OWN.format(sym="AAPL"): _err(404, "gone")}, select=_brief,
     expect=["NOT_FOUND", "VERIFY_TICKER_AND_DATE", [["finnhub", "NOT_FOUND", True, 404], ["alpha_vantage", "NOT_FOUND", True, 404]]])
_own("prv.status.missing_finnhub_key_is_source_unconfigured_and_nothing_is_called", ["BR-PRV-005", "BR-PRV-011"], env={}, select=_brief,
     expect=["SOURCE_UNCONFIGURED", "CONFIGURE_PROVIDER", [["finnhub", "SOURCE_UNCONFIGURED", False, None]]])
_own("prv.status.secret_in_error_body_is_redacted", ["BR-PRV-005"], routes={FH50: _err(500, f"upstream echoed {FH_KEY} in the error")},
     select=lambda v: {"status": _data(v)["status"],
                       "redacted": [("REDACTED" in json.dumps(a["message"])) and (FH_KEY not in json.dumps(a["message"])) for a in _data(v)["providerAttempts"]]},
     expect={"status": "PROVIDER_ERROR", "redacted": [True]})
_own("prv.status.entitlement_with_opt_in_and_alpha_unconfigured", ["BR-PRV-007", "BR-PRV-011"], allow=True,
     routes={FH50: _err(403, NO_ACCESS)}, select=_brief,
     expect=["SOURCE_UNCONFIGURED", "CONFIGURE_PROVIDER", [["finnhub", "ENTITLEMENT_REQUIRED", True, 403], ["alpha_vantage", "SOURCE_UNCONFIGURED", False, None]]])
_own("prv.status.entitlement_with_opt_in_when_alpha_also_refuses", ["BR-PRV-007", "BR-PRV-011"], allow=True,
     env={"FINNHUB_API_KEY": FH_KEY, "ALPHA_VANTAGE_API_KEY": AV_KEY},
     routes={FH50: _err(403, NO_ACCESS),
             AV_OWN.format(sym="AAPL"): {"json": {"Information": "This is a premium endpoint. Please subscribe to a premium plan."}}},
     select=_brief,
     expect=["ENTITLEMENT_REQUIRED", "CHECK_PROVIDER_ENTITLEMENT", [["finnhub", "ENTITLEMENT_REQUIRED", True, 403], ["alpha_vantage", "ENTITLEMENT_REQUIRED", True, None]]])
_own("prv.status.not_eligible_next_action", ["BR-PRV-007", "BR-PRV-011"], ticker="VOD.L", select=_brief,
     expect=["NOT_ELIGIBLE", "USE_YAHOO_OR_OFFICIAL_FILINGS", [["finnhub", "NOT_ELIGIBLE", False, None]]])
_own("prv.status.finnhub_non_json_200_body", ["BR-PRV-005"], routes={FH50: {"body": "<html>"}}, select=_brief,
     expect=["PROVIDER_ERROR", "RETRY_OR_USE_PRIMARY_PROVIDER", [["finnhub", "PROVIDER_ERROR", True, None]]])

# FINDINGs: Python reads the error body only when it is JSON; the Worker uses the raw body text.
_own("prv.status.finnhub_json_error_body_message", ["BR-PRV-005"], routes={FH50: _err(403, NO_ACCESS)}, select=_view(detail=True),
     finding="Finnhub JSON error body: Python reports the parsed error text as the attempt message, the Worker the raw JSON text",
     expect_py={"status": "ENTITLEMENT_REQUIRED", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
                "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False, "recommendedNextAction": "USE_ALPHA_VANTAGE_EXPLICITLY",
                "attempts": [{"provider": "finnhub", "status": "ENTITLEMENT_REQUIRED", "attempted": True, "httpStatus": 403, "cacheStatus": "MISS",
                              "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50", "message": NO_ACCESS}]},
     expect_ts={"status": "ENTITLEMENT_REQUIRED", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
                "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False, "recommendedNextAction": "USE_ALPHA_VANTAGE_EXPLICITLY",
                "attempts": [{"provider": "finnhub", "status": "ENTITLEMENT_REQUIRED", "attempted": True, "httpStatus": 403, "cacheStatus": "MISS",
                              "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50", "message": {"error": NO_ACCESS}}]})
_own("prv.status.finnhub_plain_text_403_no_access", ["BR-PRV-005", "BR-PRV-007"], routes={FH50: {"status": 403, "body": NO_ACCESS}}, select=_view(detail=True),
     finding="Plain-text HTTP error body: Python replaces the body with the HTTP reason phrase before mapping the status, so the card's 403 'access to this resource' is AUTH_ERROR "
             "(CHECK_PROVIDER_CREDENTIALS) in Python and ENTITLEMENT_REQUIRED (USE_ALPHA_VANTAGE_EXPLICITLY) in the Worker",
     expect_py={"status": "AUTH_ERROR", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
                "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False, "recommendedNextAction": "CHECK_PROVIDER_CREDENTIALS",
                "attempts": [{"provider": "finnhub", "status": "AUTH_ERROR", "attempted": True, "httpStatus": 403, "cacheStatus": "MISS",
                              "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50", "message": "HTTP Error 403: stubbed"}]},
     expect_ts={"status": "ENTITLEMENT_REQUIRED", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
                "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False, "recommendedNextAction": "USE_ALPHA_VANTAGE_EXPLICITLY",
                "attempts": [{"provider": "finnhub", "status": "ENTITLEMENT_REQUIRED", "attempted": True, "httpStatus": 403, "cacheStatus": "MISS",
                              "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50", "message": NO_ACCESS}]})
_own("prv.status.finnhub_non_object_payload", ["BR-PRV-005"], routes={FH50: {"json": []}}, select=_view(detail=True),
     finding="Finnhub 200 with a non-object JSON body: Python reports status OK on the attempt and NOT_FOUND overall; the Worker reports PROVIDER_ERROR with a message",
     expect_py={"status": "NOT_FOUND", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
                "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False, "recommendedNextAction": "VERIFY_SEC_13F_OR_ENABLE_SCARCE_FALLBACK",
                "attempts": [{"provider": "finnhub", "status": "OK", "attempted": True, "cacheStatus": "MISS",
                              "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50"}]},
     expect_ts={"status": "PROVIDER_ERROR", "source": None, "providerGapFilled": False, "capacityClass": "ENTITLEMENT_DEPENDENT", "decisionGrade": False,
                "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "alphaFallbackAllowed": False, "recommendedNextAction": "RETRY_OR_USE_PRIMARY_PROVIDER",
                "attempts": [{"provider": "finnhub", "status": "PROVIDER_ERROR", "attempted": True, "cacheStatus": "MISS",
                              "url": "https://finnhub.io/api/v1/stock/ownership?symbol=AAPL&limit=50", "message": "Finnhub returned a non-object payload."}]})


def _repeat(v):
    return [{"status": _data(x)["status"], "action": _data(x)["recommendedNextAction"],
             "attempt": [_data(x)["providerAttempts"][0]["attempted"], _data(x)["providerAttempts"][0]["cacheStatus"]]} for x in v]


_own("prv.status.repeat_call_after_entitlement_refusal", ["BR-PRV-005"], ticker=["AAPL", "MSFT"],
     routes={"https://finnhub.io/api/v1/stock/ownership?*": _err(403, NO_ACCESS)}, select=_repeat,
     finding="Entitlement refusals: the Worker remembers a Finnhub 403 per endpoint (6 h) and answers the next symbol without a request (attempted false, HIT_PROCESS); Python asks again",
     expect_py=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "attempt": [True, "MISS"]},
                {"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "attempt": [True, "MISS"]}],
     expect_ts=[{"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "attempt": [True, "MISS"]},
                {"status": "ENTITLEMENT_REQUIRED", "action": "USE_ALPHA_VANTAGE_EXPLICITLY", "attempt": [False, "HIT_PROCESS"]}])


# ───────────────────────────── historical put/call ratio ─────────────────────────────

AV_PC = "https://www.alphavantage.co/query?function=HISTORICAL_PUT_CALL_RATIO&symbol={sym}&date={date}&apikey=" + AV_KEY
PC = AV_PC.format(sym="AAPL", date="2024-03-15")


def _pc(name, ids, ticker="AAPL", date="2024-03-15", *, env=None, routes=None, calls=1, select=None, **kw):
    env = {"ALPHA_VANTAGE_API_KEY": AV_KEY} if env is None else env

    async def py():
        with _python_env(env):
            out = [json.loads(await provider_gaps.get_historical_put_call_ratio(ticker, date)) for _ in range(calls)]
        return out[-1] if calls == 1 else out

    body = (f"const out = []; for (let i = 0; i < {calls}; i++) out.push(JSON.parse(await m.yahooFinance.getHistoricalPutCallRatio("
            f"{json.dumps(ticker)}, {json.dumps(date)}))); return out.length === 1 ? out[0] : out;")
    _add(name, ids, py, _worker_js(env, body), select or _view(), routes, **kw)


PC_OK = {"symbol": "AAPL", "date": "2024-03-15", "put_call_ratio_full_chain": "0.85",
         "put_call_ratio_by_expiration": [{"date": "2024-03-22", "value": "0.91"}, {"date": "2024-04-19", "value": "1,234.5"}, {"date": "2024-05-17", "value": 0}]}

# BR-PRV-004 / 006: response validation
_pc("prv.putcall.matching_reply_is_ok_contextual_and_not_decision_grade", ["BR-PRV-004"], routes={PC: {"json": PC_OK}},
    select=_view(detail=True, drop=("cacheStatus",)),
    expect={"status": "OK", "source": "alpha_vantage", "providerGapFilled": True, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
            "evidenceClass": "CONTEXTUAL_OPTIONS_DATA", "dataDate": "2024-03-15", "freshnessStatus": "HISTORICAL_OBSERVATION", "expirationCount": 3,
            "putCallRatioFullChain": 0.85, "recommendedNextAction": "NONE",
            "byExpiration": [{"expirationDate": "2024-03-22", "putCallRatio": 0.91}, {"expirationDate": "2024-04-19", "putCallRatio": 1234.5},
                             {"expirationDate": "2024-05-17", "putCallRatio": 0}],
            "attempts": [{"provider": "alpha_vantage", "status": "OK", "attempted": True, "cacheStatus": "MISS",
                          "url": "https://www.alphavantage.co/query?function=HISTORICAL_PUT_CALL_RATIO&symbol=AAPL&date=2024-03-15&apikey=REDACTED"}]})
_pc("prv.putcall.unusable_expiration_rows_are_dropped", ["BR-PRV-004"],
    routes={PC: {"json": {"symbol": "AAPL", "date": "2024-03-15", "put_call_ratio_full_chain": "n/a",
                          "put_call_ratio_by_expiration": ["x", {"date": "2024-03-22", "value": "0.5"}, {"date": "", "value": "0.6"}, {"date": "2024-04-19"},
                                                           {"date": "2024-05-17", "value": "abc"}]}}},
    select=_view(drop=("cacheStatus",)),
    expect={"status": "OK", "source": "alpha_vantage", "providerGapFilled": True, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
            "evidenceClass": "CONTEXTUAL_OPTIONS_DATA", "dataDate": "2024-03-15", "freshnessStatus": "HISTORICAL_OBSERVATION", "expirationCount": 1,
            "putCallRatioFullChain": None, "recommendedNextAction": "NONE", "byExpiration": [{"expirationDate": "2024-03-22", "putCallRatio": 0.5}],
            "attempts": [{"provider": "alpha_vantage", "status": "OK", "attempted": True}]})
_pc("prv.putcall.date_mismatch_is_identity_mismatch", ["BR-PRV-004", "BR-PRV-006"], date="2024-03-15",
    routes={PC: {"json": {**PC_OK, "date": "2024-03-14"}}}, select=_view(detail=True),
    expect={"status": "PROVIDER_IDENTITY_MISMATCH", "source": None, "providerGapFilled": False, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
            "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "dataDate": "2024-03-15", "recommendedNextAction": "RETRY_OR_VERIFY_PROVIDER_RESPONSE",
            "attempts": [{"provider": "alpha_vantage", "status": "OK", "attempted": True, "validationStatus": "PROVIDER_IDENTITY_MISMATCH", "cacheStatus": "MISS",
                          "url": "https://www.alphavantage.co/query?function=HISTORICAL_PUT_CALL_RATIO&symbol=AAPL&date=2024-03-15&apikey=REDACTED"}]})
_pc("prv.putcall.symbol_mismatch_is_identity_mismatch", ["BR-PRV-006"], routes={PC: {"json": {**PC_OK, "symbol": "MSFT"}}}, select=_view(),
    expect={"status": "PROVIDER_IDENTITY_MISMATCH", "source": None, "providerGapFilled": False, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
            "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "dataDate": "2024-03-15", "recommendedNextAction": "RETRY_OR_VERIFY_PROVIDER_RESPONSE",
            "attempts": [{"provider": "alpha_vantage", "status": "OK", "attempted": True, "validationStatus": "PROVIDER_IDENTITY_MISMATCH"}]})
_pc("prv.putcall.reply_without_symbol_and_date_is_accepted_and_ticker_is_upper_cased", ["BR-PRV-006", "BR-PRV-004"], ticker="aapl",
    routes={PC: {"json": {"put_call_ratio_full_chain": 1}}}, select=_view(drop=("cacheStatus",)),
    expect={"status": "OK", "source": "alpha_vantage", "providerGapFilled": True, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
            "evidenceClass": "CONTEXTUAL_OPTIONS_DATA", "dataDate": "2024-03-15", "freshnessStatus": "HISTORICAL_OBSERVATION", "expirationCount": 0,
            "putCallRatioFullChain": 1, "recommendedNextAction": "NONE", "byExpiration": [], "attempts": [{"provider": "alpha_vantage", "status": "OK", "attempted": True}]})
_pc("prv.putcall.matching_reply_without_ratio_or_rows_is_provider_error", ["BR-PRV-004", "BR-PRV-007"],
    routes={PC: {"json": {"symbol": "AAPL", "date": "2024-03-15", "put_call_ratio_by_expiration": []}}}, select=_view(),
    expect={"status": "PROVIDER_ERROR", "source": None, "providerGapFilled": False, "capacityClass": "SCARCE_SHARED_QUOTA", "decisionGrade": False,
            "evidenceClass": "CONTEXTUAL_PROVIDER_DATA", "dataDate": "2024-03-15", "recommendedNextAction": "RETRY_OR_USE_PRIMARY_PROVIDER",
            "attempts": [{"provider": "alpha_vantage", "status": "OK", "attempted": True}]})
_pc("prv.putcall.second_identical_request_is_served_from_the_process_cache", ["BR-PRV-004"], calls=2, routes={PC: {"json": PC_OK}},
    select=lambda v: [[_data(x)["status"], _data(x)["cacheStatus"], _data(x)["providerAttempts"][0]["attempted"], _data(x)["providerAttempts"][0]["cacheStatus"]] for x in v],
    expect=[["OK", "MISS", True, "MISS"], ["OK", "HIT_PROCESS", False, "HIT_PROCESS"]])

# BR-PRV-004 / BR-PRV-010: request validation
for _n, _d, _ids in [
    ("floor_date_itself_is_rejected", "2008-01-01", ["BR-PRV-004", "BR-PRV-010"]),
    ("date_before_floor_is_rejected", "2007-12-31", ["BR-PRV-010"]),
    ("future_date_is_rejected", "2999-01-01", ["BR-PRV-004", "BR-PRV-010"]),
    ("impossible_calendar_date_is_rejected", "2024-02-30", ["BR-PRV-010"]),
    ("unpadded_date_is_rejected", "2024-3-5", ["BR-PRV-010"]),
    ("empty_date_is_rejected", "", ["BR-PRV-004"]),
    ("non_date_text_is_rejected", "yesterday", ["BR-PRV-010"]),
]:
    _pc(f"prv.putcall.{_n}", _ids, date=_d, env={}, select=_code, expect="INPUT_VALIDATION_ERROR")
_pc("prv.putcall.empty_ticker_is_rejected", ["BR-PRV-004"], ticker="", env={}, select=_code, expect="INPUT_VALIDATION_ERROR")
_pc("prv.putcall.day_after_the_floor_is_accepted", ["BR-PRV-010"], date="2008-01-02", routes={AV_PC.format(sym="AAPL", date="2008-01-02"): {"json": {"put_call_ratio_full_chain": "0.7"}}},
    select=lambda v: [_data(v)["status"], _data(v)["dataDate"]], expect=["OK", "2008-01-02"])

_pc("prv.putcall.date_before_floor_message", ["BR-PRV-010"], date="2007-12-31", env={}, select=lambda v: _error(v),
    finding="Date window error text: Python 'date must be later than 2008-01-01. Use summarize_options_flow ...' for a floor miss; the Worker one combined text 'date must be a valid YYYY-MM-DD after 2008-01-01 and not in the future. ...' for every date fault",
    expect_py={"error": True, "code": "INPUT_VALIDATION_ERROR",
               "message": "date must be later than 2008-01-01. Use summarize_options_flow for a current Yahoo options snapshot."},
    expect_ts={"error": True, "code": "INPUT_VALIDATION_ERROR",
               "message": "date must be a valid YYYY-MM-DD after 2008-01-01 and not in the future. Use summarize_options_flow for a current Yahoo options snapshot."})
_pc("prv.putcall.compact_iso_date", ["BR-PRV-004", "BR-PRV-010"], date="20240315", routes={PC: {"json": PC_OK}},
    select=lambda v: _error(v) or [_data(v)["status"], _data(v)["dataDate"]],
    finding="Date format: Python (3.11 date.fromisoformat) accepts '20240315' as 2024-03-15 and calls Alpha; the Worker requires dashes and rejects it with INPUT_VALIDATION_ERROR",
    expect_py=["OK", "2024-03-15"],
    expect_ts={"error": True, "code": "INPUT_VALIDATION_ERROR",
               "message": "date must be a valid YYYY-MM-DD after 2008-01-01 and not in the future. Use summarize_options_flow for a current Yahoo options snapshot."})
_pc("prv.putcall.blank_string_ratio", ["BR-PRV-004"],
    routes={PC: {"json": {"symbol": "AAPL", "date": "2024-03-15", "put_call_ratio_full_chain": "", "put_call_ratio_by_expiration": [{"date": "2024-03-22", "value": " "}]}}},
    select=lambda v: [_data(v)["status"], _data(v).get("putCallRatioFullChain"), _data(v).get("byExpiration")],
    finding="Blank numeric strings: Python treats '' / ' ' as no number (full chain null; row dropped; empty result is PROVIDER_ERROR); the Worker's Number('') is 0, so it reports status OK with ratio 0 and a 0 row",
    expect_py=["PROVIDER_ERROR", None, None],
    expect_ts=["OK", 0, [{"expirationDate": "2024-03-22", "putCallRatio": 0}]])

# BR-PRV-005 / 007 / 011: Alpha Vantage failure mapping through the put/call tool
for _n, _route, _ids, _exp, _act, _http in [
    ("note_with_rate_limit_text_is_rate_limit", {"json": {"Note": "Thank you for using Alpha Vantage! Our standard API rate limit is 25 requests per day."}},
     ["BR-PRV-005", "BR-PRV-011"], "RATE_LIMIT", "RETRY_AFTER_PROVIDER_RESET", None),
    ("information_with_premium_text_is_entitlement_required", {"json": {"Information": "This is a premium endpoint. Subscribe to a premium plan."}},
     ["BR-PRV-005", "BR-PRV-007", "BR-PRV-011"], "ENTITLEMENT_REQUIRED", "CHECK_PROVIDER_ENTITLEMENT", None),
    ("error_message_with_api_key_text_is_auth_error", {"json": {"Error Message": "the parameter apikey is invalid or missing."}},
     ["BR-PRV-005", "BR-PRV-011"], "AUTH_ERROR", "CHECK_PROVIDER_CREDENTIALS", None),
    ("error_message_without_known_text_is_provider_error", {"json": {"Error Message": "Invalid API call."}},
     ["BR-PRV-005", "BR-PRV-011"], "PROVIDER_ERROR", "RETRY_OR_USE_PRIMARY_PROVIDER", None),
    ("http_404_is_not_found", {"status": 404, "json": {"message": "gone"}}, ["BR-PRV-005", "BR-PRV-011"], "NOT_FOUND", "VERIFY_TICKER_AND_DATE", 404),
    ("http_429_is_rate_limit", {"status": 429, "json": {"message": "slow"}}, ["BR-PRV-005", "BR-PRV-011"], "RATE_LIMIT", "RETRY_AFTER_PROVIDER_RESET", 429),
    ("http_500_is_provider_error", {"status": 500, "json": {"message": "boom"}}, ["BR-PRV-005"], "PROVIDER_ERROR", "RETRY_OR_USE_PRIMARY_PROVIDER", 500),
    ("non_object_payload_is_provider_error", {"json": ["x"]}, ["BR-PRV-005"], "PROVIDER_ERROR", "RETRY_OR_USE_PRIMARY_PROVIDER", None),
]:
    _pc(f"prv.status.alpha_{_n}", _ids, routes={PC: _route}, select=_brief, expect=[_exp, _act, [["alpha_vantage", _exp, True, _http]]])
_pc("prv.status.alpha_missing_key_is_source_unconfigured_and_nothing_is_called", ["BR-PRV-005", "BR-PRV-011"], env={}, select=_brief,
    expect=["SOURCE_UNCONFIGURED", "CONFIGURE_PROVIDER", [["alpha_vantage", "SOURCE_UNCONFIGURED", False, None]]])
_pc("prv.status.alpha_secret_in_note_is_redacted", ["BR-PRV-005"], routes={PC: {"json": {"Note": f"rate limit for key {AV_KEY} reached"}}},
    select=lambda v: {"status": _data(v)["status"],
                      "message": _data(v)["providerAttempts"][0]["message"], "url": _data(v)["providerAttempts"][0]["url"]},
    expect={"status": "RATE_LIMIT", "message": "rate limit for key REDACTED reached",
            "url": "https://www.alphavantage.co/query?function=HISTORICAL_PUT_CALL_RATIO&symbol=AAPL&date=2024-03-15&apikey=REDACTED"})


# ───────────────────────────── Companies House (BR-PRV-008) ─────────────────────────────

CH = "https://api.company-information.service.gov.uk"
CH_HISTORY_URL = "https://find-and-update.company-information.service.gov.uk/company/{n}/filing-history"
CH_FILINGS = {"total_count": 41, "items": [
    {"transaction_id": "MzA1", "category": "accounts", "date": "2025-06-30", "description": "accounts-with-accounts-type-full", "type": "AA", "pages": 52,
     "links": {"document_metadata": "https://document-api.company-information.service.gov.uk/document/abc"}},
    {"category": "mortgage", "date": "2025-02-01", "description": "mortgage-create-with-deed", "type": "MR01"},
]}


def _ch(name, ids, *, ticker=None, number=None, company_name=None, category=None, limit=25, charges=False, env=None, routes=None, select=None, **kw):
    env = {"COMPANIES_HOUSE_API_KEY": CH_KEY} if env is None else env

    async def py():
        with _python_env(env):
            return json.loads(await server.get_uk_company_filings(ticker, number, company_name, category, limit, charges))

    body = ("return JSON.parse(await m.yahooFinance.getUkCompanyFilings("
            f"{json.dumps(ticker)}, {json.dumps(number)}, {json.dumps(company_name)}, {json.dumps(category)}, {json.dumps(limit)}, {json.dumps(charges)}));")
    _add(name, ids, py, _worker_js(env, body), select or _ch_view, routes, **kw)


def _ch_view(v: dict):
    out = {k: v[k] for k in ("error", "code", "status", "message", "searchedName", "companyNumber", "company", "category", "totalFilings",
                             "filingHistoryUrl", "candidates", "warnings", "charges", "ticker") if k in v}
    if "filings" in v:
        out["filings"] = v["filings"]
    return out


CH_COMPANY = {"company_name": "IQE PLC", "company_status": "active"}
CH_UNREGISTERED = "company_number must be an 8-character Companies House number, e.g. 01234567 or SC123456."


def _ch_ok(number, *, matched="company_number", name="IQE PLC", filings=None, cat=None, warnings=()):
    return {"status": "OK", "ticker": None, "company": {"companyNumber": number, "name": name, "status": "active", "matchedBy": matched}, "category": cat,
            "totalFilings": 41, "filingHistoryUrl": CH_HISTORY_URL.format(n=number), "warnings": list(warnings),
            "filings": filings if filings is not None else [
                {"date": "2025-06-30", "type": "AA", "category": "accounts", "categoryLabel": "Accounts", "description": "accounts with accounts type full",
                 "descriptionValues": {}, "pages": 52, "documentMetadataUrl": "https://document-api.company-information.service.gov.uk/document/abc",
                 "documentContentUrl": "https://document-api.company-information.service.gov.uk/document/abc/content",
                 "viewerUrl": CH_HISTORY_URL.format(n=number) + "/MzA1/document?format=pdf&download=0"},
                {"date": "2025-02-01", "type": "MR01", "category": "mortgage", "categoryLabel": "Charges (secured lending)", "description": "mortgage create with deed",
                 "descriptionValues": {}, "pages": None, "documentMetadataUrl": None, "documentContentUrl": None, "viewerUrl": None}]}


_ch("prv.ch.missing_key_is_source_unconfigured_before_input_is_validated", ["BR-PRV-008"], number="SC12", env={}, select=lambda v: _ch_view(v),
    expect={"status": "SOURCE_UNCONFIGURED", "code": "SOURCE_UNCONFIGURED", "ticker": None,
            "message": "Companies House is not configured; set the COMPANIES_HOUSE_API_KEY secret (a free key from the Companies House developer hub).", "filings": []})
_ch("prv.ch.blank_key_is_source_unconfigured", ["BR-PRV-008"], number="01234567", env={"COMPANIES_HOUSE_API_KEY": "   "}, select=lambda v: [v["status"], v["filings"]],
    expect=["SOURCE_UNCONFIGURED", []])
_ch("prv.ch.short_all_digit_number_is_zero_padded_and_accepted", ["BR-PRV-008"], number="1234567",
    routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page=25": {"json": CH_FILINGS}},
    expect=_ch_ok("01234567"))
_ch("prv.ch.number_is_trimmed_and_upper_cased", ["BR-PRV-008"], number="  sc123456 ",
    routes={f"{CH}/company/SC123456": {"json": CH_COMPANY}, f"{CH}/company/SC123456/filing-history?items_per_page=25": {"json": {"items": [], "total_count": 0}}},
    select=lambda v: [v["status"], v["company"]["companyNumber"], v["filings"], v["totalFilings"]], expect=["OK", "SC123456", [], 0])
_ch("prv.ch.eight_digit_number_is_not_changed", ["BR-PRV-008"], number="12345678",
    routes={f"{CH}/company/12345678": {"json": CH_COMPANY}, f"{CH}/company/12345678/filing-history?items_per_page=25": {"json": {"items": []}}},
    select=lambda v: [v["status"], v["company"]["companyNumber"], v["totalFilings"]], expect=["OK", "12345678", None])
for _n, _num in [("letters_and_digits_shorter_than_eight_are_rejected", "SC12"), ("nine_digits_are_rejected", "123456789"), ("punctuation_is_rejected", "0123-456"),
                 ("blank_number_falls_through_to_the_name_check", None)]:
    if _num is None:
        _ch(f"prv.ch.{_n}", ["BR-PRV-008"], number="   ", select=_ch_view,
            expect={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "Provide company_number, company_name, or a ticker whose issuer name can be resolved."})
    else:
        _ch(f"prv.ch.{_n}", ["BR-PRV-008"], number=_num, select=_ch_view,
            expect={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": CH_UNREGISTERED})
_ch("prv.ch.no_number_name_or_resolvable_ticker_is_rejected", ["BR-PRV-008"], select=_ch_view,
    expect={"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "Provide company_number, company_name, or a ticker whose issuer name can be resolved."})
_ch("prv.ch.unknown_category_is_rejected", ["BR-PRV-008"], number="01234567", category="nonsense", select=_ch_view,
    expect={"error": True, "code": "INPUT_VALIDATION_ERROR",
            "message": "category must be one of: accounts, capital, mortgage, confirmation-statement, resolution, incorporation, officers, "
                       "persons-with-significant-control, address, annotation, change-of-name, miscellaneous."})
_ch("prv.ch.category_is_trimmed_lower_cased_and_sent", ["BR-PRV-008"], number="01234567", category=" Mortgage ",
    routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page=25&category=mortgage": {"json": CH_FILINGS}},
    select=lambda v: [v["status"], v["category"], len(v["filings"])], expect=["OK", "mortgage", 2])
for _n, _lim, _size in [("limit_above_100_is_clamped", 500, 100), ("limit_below_1_is_clamped", -5, 1)]:
    _ch(f"prv.ch.{_n}", ["BR-PRV-008"], number="01234567", limit=_lim,
        routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page={_size}": {"json": CH_FILINGS}},
        select=lambda v: v["status"], expect="OK")
_ch("prv.ch.company_name_search_matches_exactly_and_skips_the_profile_call", ["BR-PRV-008"], company_name="IQE plc",
    routes={f"{CH}/search/companies?q=IQE%20plc&items_per_page=20": {"json": {"items": [
        {"company_number": "09999999", "title": "IQE HOLDINGS LIMITED", "company_status": "dissolved"}, {"company_number": "02262866", "title": "IQE PLC", "company_status": "active"}]}},
        f"{CH}/company/02262866/filing-history?items_per_page=25": {"json": {"items": [], "total_count": 3}}},
    select=lambda v: [v["status"], v["company"], v["totalFilings"]],
    expect=["OK", {"companyNumber": "02262866", "name": "IQE PLC", "status": "active", "matchedBy": "company_name"}, 3])
_ch("prv.ch.company_name_without_an_exact_match_lists_candidates", ["BR-PRV-008"], company_name="Nothing Matching",
    routes={f"{CH}/search/companies?q=Nothing%20Matching&items_per_page=20": {"json": {"items": [
        {"company_number": "1", "title": "SOMETHING ELSE LTD", "company_status": "active", "address_snippet": "1 High St"}]}}},
    expect={"status": "COMPANY_NOT_MATCHED", "ticker": None, "searchedName": "Nothing Matching", "warnings": [],
            "message": "No registered company name matched the issuer exactly; pass company_number from the candidates.",
            "candidates": [{"companyNumber": "1", "title": "SOMETHING ELSE LTD", "companyStatus": "active", "addressSnippet": "1 High St"}], "filings": []})
_ch("prv.ch.unknown_company_number_is_company_not_found", ["BR-PRV-008"], number="1234567", routes={f"{CH}/company/01234567": {"status": 404, "body": "{}"}},
    expect={"status": "COMPANY_NOT_FOUND", "ticker": None, "companyNumber": "01234567", "filings": []})
for _n, _http, _exp in [
    ("401_is_auth_error", 401, {"error": True, "code": "PROVIDER_ERROR", "status": "AUTH_ERROR", "message": "Companies House company profile: HTTP 401; check COMPANIES_HOUSE_API_KEY."}),
    ("403_is_auth_error", 403, {"error": True, "code": "PROVIDER_ERROR", "status": "AUTH_ERROR", "message": "Companies House company profile: HTTP 403; check COMPANIES_HOUSE_API_KEY."}),
    ("429_is_a_rate_limit_message_without_a_code", 429, {"error": True, "message": "Companies House company profile: HTTP 429 rate limited (600 requests per 5 minutes)."}),
    ("500_is_a_provider_error", 500, {"error": True, "code": "PROVIDER_ERROR", "message": "Companies House company profile: HTTP 500."}),
]:
    _ch(f"prv.ch.profile_{_n}", ["BR-PRV-005", "BR-PRV-008"], number="01234567", routes={f"{CH}/company/01234567": {"status": _http, "body": "x"}}, expect=_exp)
_ch("prv.ch.filing_history_failure_names_its_step", ["BR-PRV-008"], number="01234567",
    routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page=25": {"status": 500, "body": "x"}},
    expect={"error": True, "code": "PROVIDER_ERROR", "message": "Companies House filing history: HTTP 500."})
_ch("prv.ch.company_search_failure_names_its_step", ["BR-PRV-008"], company_name="IQE",
    routes={f"{CH}/search/companies?q=IQE&items_per_page=20": {"status": 401, "body": "x"}},
    expect={"error": True, "code": "PROVIDER_ERROR", "status": "AUTH_ERROR", "message": "Companies House company search: HTTP 401; check COMPANIES_HOUSE_API_KEY."})
_CHARGES = {"items": [{"charge_code": "0226286600001", "status": "outstanding", "created_on": "2024-01-02", "delivered_on": "2024-01-10",
                       "classification": {"description": "A registered charge"}, "particulars": {"description": "x" * 450},
                       "persons_entitled": [{"name": "Bank A"}, {"name": ""}, {"other": 1}]}]}
_ch("prv.ch.include_charges_adds_the_charge_rows", ["BR-PRV-008"], number="01234567", charges=True,
    routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page=25": {"json": CH_FILINGS},
            f"{CH}/company/01234567/charges": {"json": _CHARGES}},
    select=lambda v: v["charges"],
    expect=[{"chargeCode": "0226286600001", "status": "outstanding", "createdOn": "2024-01-02", "deliveredOn": "2024-01-10", "satisfiedOn": None,
             "classification": "A registered charge", "personsEntitled": ["Bank A"], "particulars": "x" * 400}])
_ch("prv.ch.charges_404_is_an_empty_register_and_other_failures_are_a_warning", ["BR-PRV-008"], number="01234567", charges=True,
    routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page=25": {"json": CH_FILINGS},
            f"{CH}/company/01234567/charges": {"status": 404, "body": "{}"}},
    select=lambda v: [v["charges"], v["warnings"]], expect=[[], []])
_ch("prv.ch.charges_failure_is_a_warning", ["BR-PRV-008"], number="01234567", charges=True,
    routes={f"{CH}/company/01234567": {"json": CH_COMPANY}, f"{CH}/company/01234567/filing-history?items_per_page=25": {"json": CH_FILINGS},
            f"{CH}/company/01234567/charges": {"status": 500, "body": "x"}},
    select=lambda v: [("charges" in v), v["warnings"]],
    expect=[False, [{"code": "CHARGES_UNAVAILABLE", "message": "Companies House charges: HTTP 500.", "severity": "warning"}]])
