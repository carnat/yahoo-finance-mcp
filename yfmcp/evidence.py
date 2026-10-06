"""Evidence composition primitives, shared with worker/src/evidence.ts.

Parity is tested in scripts/test_evidence.py. Pure: no I/O and no clock.

MCP supplies reproducible facts and mechanical transformations; valuation
method, multiple, scenario weights, price target, G2, opportunity and action
belong to the caller. Every payload built here carries AUTHORITY_BOUNDARY,
and missing provider evidence stays visibly missing: nothing is interpolated,
extrapolated or derived and called consensus.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import re
from decimal import Decimal
from typing import Any

from yfmcp.fiscal_calendar import company_fiscal_year_end, fiscal_year_of_period_end

EVIDENCE_CUT_SCHEMA = "yfmcp.evidence-cut/1"
CONSENSUS_OBSERVATION_SCHEMA = "yfmcp.consensus-observation/1"
CANONICALIZATION = "yfmcp-canonical-json/1: sorted keys, no whitespace, ECMAScript number formatting, UTF-8"

AUTHORITY_BOUNDARY: dict[str, Any] = {
    "decisionUse": "EVIDENCE_ONLY",
    "selectedMethod": None,
    "selectedMultiple": None,
    "scenarioWeights": None,
    "priceTarget": None,
    "g2": None,
    "opportunity": None,
    "action": None,
}


# Actions whose payloads are evidence only; each result they return carries AUTHORITY_BOUNDARY (2.5.8).
EVIDENCE_ONLY_ACTIONS = frozenset({
    "build_valuation_evidence_pack",
    "extract_funding_capex_schedule",
    "extract_operating_driver_ledger",
    "get_consensus_forecast_curve",
    "get_eps_revisions",
    "get_evidence_cut",
    "get_evidence_quality",
    "get_guidance_history",
    "get_historical_valuation_context",
    "get_share_count_scenarios",
    "list_evidence_cuts",
    "reconcile_metric_sources",
})


def with_authority_boundary(action: str, raw: object) -> object:
    """An evidence-only action's result object with AUTHORITY_BOUNDARY.

    A terminal status such as PERIOD_NOT_FOUND or NO_SEC_REGISTRANT then has the same shape as a full
    result. Errors ({"error": true}) become failure envelopes with no data and are left alone, as are other
    tools and non-object results.
    """
    if action not in EVIDENCE_ONLY_ACTIONS or not isinstance(raw, str):
        return raw
    try:
        parsed = json.loads(raw)
    except ValueError:
        return raw
    if not isinstance(parsed, dict) or parsed.get("error") is True or isinstance(parsed.get("ok"), bool):
        return raw
    if all(k in parsed and parsed[k] == v for k, v in AUTHORITY_BOUNDARY.items()):
        return raw
    return json.dumps({**parsed, **AUTHORITY_BOUNDARY})


# ── Canonical JSON ───────────────────────────────────────────────────────────

def _es_number(value: float) -> str:
    """Number::toString from ECMAScript, so both runtimes hash the same bytes."""
    if value == 0:
        return "0"
    if value < 0:
        return "-" + _es_number(-value)
    sign, digits, exponent = Decimal(repr(value)).as_tuple()
    ds = "".join(str(d) for d in digits).rstrip("0") or "0"
    exponent += len(digits) - len(ds)
    k = len(ds)
    n = exponent + k
    if k <= n <= 21:
        return ds + "0" * (n - k)
    if 0 < n <= 21:
        return ds[:n] + "." + ds[n:]
    if -6 < n <= 0:
        return "0." + "0" * (-n) + ds
    e = n - 1
    exp = ("+" if e >= 0 else "-") + str(abs(e))
    return ds + "e" + exp if k == 1 else ds[0] + "." + ds[1:] + "e" + exp


def canonical_json(value: Any) -> str:
    """Canonical JSON: the bytes an evidence cut's SHA-256 is taken over."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return _es_number(float(value)) if abs(value) >= 2**53 else str(value)
    if isinstance(value, float):
        return _es_number(value) if math.isfinite(value) else "null"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(canonical_json(v) for v in value) + "]"
    if isinstance(value, dict):
        keys = sorted(str(k) for k in value.keys())
        return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + canonical_json(value[k]) for k in keys) + "}"
    return "null"


# ── Evidence cut identity ────────────────────────────────────────────────────

_CUT_ID_RE = re.compile(r"^ec1_([A-Z0-9.^=-]{1,20})_(\d{8}T\d{6}Z)_([0-9a-f]{64})$")


def compact_timestamp(iso: str) -> str:
    """2026-09-26T08:02:09.101Z -> 20260926T080209Z"""
    return re.sub(r"[-:]", "", re.sub(r"\.\d+", "", iso, count=1))


def evidence_cut_id(ticker: str, cutoff_iso: str, sha256: str) -> str:
    return f"ec1_{ticker.upper()}_{compact_timestamp(cutoff_iso)}_{sha256}"


def evidence_cut_key(ticker: str, compact_ts: str, sha256: str) -> str:
    return f"evidence-cuts/{ticker.upper()}/{compact_ts}/{sha256}.json"


def parse_evidence_cut_id(cut_id: str) -> dict | None:
    m = _CUT_ID_RE.match(cut_id.strip())
    if not m:
        return None
    return {"ticker": m.group(1), "timestamp": m.group(2), "sha256": m.group(3), "key": evidence_cut_key(m.group(1), m.group(2), m.group(3))}


def consensus_observation_key(ticker: str, iso_date: str) -> str:
    return f"consensus-history/{ticker.upper()}/{iso_date[:10]}.json"


# ── Numbers ──────────────────────────────────────────────────────────────────

def _num(value: Any) -> float | int | None:
    v = value.get("raw") if isinstance(value, dict) else value
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return v if math.isfinite(v) else None
    if isinstance(v, str) and v.strip():
        try:
            f = float(v)
        except ValueError:
            return None
        return f if math.isfinite(f) else None
    return None


def _round(value: float, digits: int) -> float:
    f = 10 ** digits
    return math.floor(value * f + 0.5) / f


def _str(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


# ── Consensus provider inputs ────────────────────────────────────────────────

CONSENSUS_METRICS = ["eps", "revenue"]
METRICS_NOT_COVERED = ["ebitda", "ebit", "freeCashFlow", "capex", "grossMargin", "ebitdaMargin"]


def _estimate(block: Any, currency: str | None, currency_basis: str) -> dict:
    b = block if isinstance(block, dict) else {}
    count = _num(b.get("numberOfAnalysts") if b.get("numberOfAnalysts") is not None else b.get("analystCount"))
    empty = count == 0
    return {
        "mean": None if empty else _num(b.get("avg") if b.get("avg") is not None else b.get("mean")),
        "high": None if empty else _num(b.get("high")),
        "low": None if empty else _num(b.get("low")),
        "analystCount": count,
        "currency": currency,
        "currencyBasis": currency_basis,
    }


def _first(block: dict, *keys: str) -> Any:
    for k in keys:
        if block.get(k) is not None:
            return block.get(k)
    return None


def yahoo_consensus_input(
    trend: list,
    *,
    retrieved_at: str | None,
    status: str | None = None,
    message: str | None = None,
    financial_currency: str | None = None,
    fiscal_year_ends: dict[str, str] | None = None,
    fiscal_year_end_basis: str | None = None,
) -> dict:
    """Yahoo earningsTrend rows (0y, +1y) as provider periods.

    Values may be {raw, fmt} objects or numbers. Yahoo states the fiscal period
    end (`endDate`); when a caller cannot supply it, `fiscal_year_ends` maps the
    period label to an end date it derived, and the basis says so.
    """
    periods = []
    for raw in trend if isinstance(trend, list) else []:
        t = raw if isinstance(raw, dict) else {}
        label = _str(t.get("period"))
        if label not in ("0y", "+1y"):
            continue
        stated = _str(t.get("endDate"))
        fiscal_year_end = stated or (fiscal_year_ends or {}).get(label)
        basis = "PROVIDER_STATED" if stated else (fiscal_year_end_basis or "DERIVED") if fiscal_year_end else "NOT_STATED"
        ee = t.get("earningsEstimate") or {}
        re_ = t.get("revenueEstimate") or {}
        eps_currency = _str(ee.get("earningsCurrency"))
        rev_currency = _str(re_.get("revenueCurrency"))
        fallback = _str(financial_currency)
        trend_block = t.get("epsTrend")
        rev_block = t.get("epsRevisions")
        periods.append({
            "providerPeriodLabel": label,
            "fiscalYearEnd": fiscal_year_end,
            "fiscalYearEndBasis": basis,
            "eps": _estimate(ee, eps_currency or fallback, "PROVIDER_STATED" if eps_currency else "FINANCIAL_CURRENCY" if fallback else "NOT_STATED"),
            "revenue": _estimate(re_, rev_currency or fallback, "PROVIDER_STATED" if rev_currency else "FINANCIAL_CURRENCY" if fallback else "NOT_STATED"),
            "epsTrend": {
                "current": _num(trend_block.get("current")),
                "d7": _num(trend_block.get("7daysAgo")),
                "d30": _num(trend_block.get("30daysAgo")),
                "d60": _num(trend_block.get("60daysAgo")),
                "d90": _num(trend_block.get("90daysAgo")),
            } if isinstance(trend_block, dict) else None,
            "epsRevisions": {
                "up7d": _num(_first(rev_block, "upLast7days", "upLast7Days")),
                "down7d": _num(_first(rev_block, "downLast7days", "downLast7Days")),
                "up30d": _num(_first(rev_block, "upLast30days", "upLast30Days")),
                "down30d": _num(_first(rev_block, "downLast30days", "downLast30Days")),
            } if isinstance(rev_block, dict) else None,
        })
    return {
        "provider": "yahoo_finance",
        "status": status or ("OK" if periods else "NO_DATA"),
        "retrievedAt": retrieved_at,
        "providerTimestamp": None,
        "message": message,
        "periods": periods,
    }


def alpha_vantage_consensus_input(payload: Any, *, retrieved_at: str | None, status: str | None = None, message: str | None = None) -> dict:
    """Alpha Vantage EARNINGS_ESTIMATES fiscal-year rows as provider periods."""
    rows = (payload.get("estimates") if isinstance(payload, dict) else None) or []
    periods = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict) or _str(row.get("horizon")) != "fiscal year":
            continue
        fiscal_year_end = _str(row.get("date"))
        if not fiscal_year_end:
            continue
        periods.append({
            "providerPeriodLabel": "fiscal year",
            "fiscalYearEnd": fiscal_year_end,
            "fiscalYearEndBasis": "PROVIDER_STATED",
            "eps": _estimate({"avg": row.get("eps_estimate_average"), "high": row.get("eps_estimate_high"), "low": row.get("eps_estimate_low"),
                              "numberOfAnalysts": row.get("eps_estimate_analyst_count")}, None, "NOT_STATED"),
            "revenue": _estimate({"avg": row.get("revenue_estimate_average"), "high": row.get("revenue_estimate_high"), "low": row.get("revenue_estimate_low"),
                                  "numberOfAnalysts": row.get("revenue_estimate_analyst_count")}, None, "NOT_STATED"),
            "epsTrend": {
                "current": _num(row.get("eps_estimate_average")),
                "d7": _num(row.get("eps_estimate_average_7_days_ago")),
                "d30": _num(row.get("eps_estimate_average_30_days_ago")),
                "d60": _num(row.get("eps_estimate_average_60_days_ago")),
                "d90": _num(row.get("eps_estimate_average_90_days_ago")),
            },
            "epsRevisions": {
                "up7d": _num(row.get("eps_estimate_revision_up_trailing_7_days")),
                "down7d": _num(row.get("eps_estimate_revision_down_trailing_7_days")),
                "up30d": _num(row.get("eps_estimate_revision_up_trailing_30_days")),
                "down30d": _num(row.get("eps_estimate_revision_down_trailing_30_days")),
            },
        })
    periods.sort(key=lambda p: str(p["fiscalYearEnd"]))
    return {
        "provider": "alpha_vantage",
        "status": status or ("OK" if periods else "NO_DATA"),
        "retrievedAt": retrieved_at,
        "providerTimestamp": None,
        "message": message,
        "periods": periods,
    }


# ── Fiscal period identity ───────────────────────────────────────────────────

def _day(iso_date: str) -> int:
    return _dt.date.fromisoformat(iso_date[:10]).toordinal()


def resolve_fy0(inputs: list[dict], as_of_date: str) -> dict | None:
    """FY0 is Yahoo's current fiscal year when stated, else the first provider fiscal year ending on or after the as-of date."""
    for inp in inputs:
        for p in inp["periods"]:
            if p["providerPeriodLabel"] == "0y" and p["fiscalYearEnd"]:
                return {"fiscalYearEnd": p["fiscalYearEnd"], "basis": f"{inp['provider']}:0y"}
    today = _day(as_of_date)
    ends = sorted(p["fiscalYearEnd"] for inp in inputs for p in inp["periods"] if p["fiscalYearEnd"] and _day(p["fiscalYearEnd"]) >= today)
    return {"fiscalYearEnd": ends[0], "basis": "earliest_provider_fiscal_year_on_or_after_as_of"} if ends else None


def fiscal_year_offset(fy0_end: str, fiscal_year_end: str) -> int:
    """Years after FY0, from fiscal year ends (52/53-week years stay within a few days of a whole year)."""
    return math.floor((_day(fiscal_year_end) - _day(fy0_end)) / 365.25 + 0.5)


# ── Consensus curve ──────────────────────────────────────────────────────────

DEFAULT_CONSENSUS_POLICY = {"horizonYears": 5, "minAnalystCount": 3, "conflictTolerancePct": 10, "epsAbsoluteTolerance": 0.02}


# How far back a split can explain a provider EPS gap (2.5.23, F-002): stale rows outlive a split by months.
SPLIT_LOOKBACK_DAYS = 400
# A provider EPS ratio within this fraction of a split's ratio is read as an unadjusted row, not a conflict.
SPLIT_RATIO_TOLERANCE = 0.15


def _js(value: Any) -> str:
    """A value as a JS template literal prints it: ECMAScript numbers, null for None."""
    if value is None:
        return "null"
    if _is_number(value):
        return _es_number(float(value))
    return str(value)


def split_text(ratio: float) -> str:
    """"4-for-1" for a forward split, "1-for-10" for a reverse one."""
    return f"{_js(_round(ratio, 4))}-for-1" if ratio >= 1 else f"1-for-{_js(_round(1 / ratio, 4))}"


def splits_within(history: dict | None, as_of: str, days: int) -> list[dict]:
    """Splits dated on or before as_of and within `days` of it, oldest first."""
    if not history or history.get("status") != "OK":
        return []
    end = _day(as_of[:10])
    out = []
    for s in history.get("splits") or []:
        ratio = s.get("ratio")
        date = s.get("date")
        if not (_is_number(ratio) and math.isfinite(ratio) and ratio > 0 and ratio != 1):
            continue
        if not (isinstance(date, str) and re.match(r"^\d{4}-\d{2}-\d{2}", date)):
            continue
        try:
            d = _day(date[:10])
        except ValueError:
            continue
        if d <= end and end - d <= days:
            out.append(s)
    return sorted(out, key=lambda s: s["date"])


def range_inconsistency(e: dict) -> str | None:
    """A provider row whose mean lies outside its own high-low range, or whose low is above its high, cannot be a
    consensus (2.5.23, F-001: Alpha Vantage ANET revenue averaged 667.6M against a 652.7M high)."""
    def tol(v: float) -> float:
        return 1e-9 * max(1, abs(v))
    mean, high, low = e["mean"], e["high"], e["low"]
    if high is not None and low is not None and low - high > tol(high):
        return "LOW_ABOVE_HIGH"
    if mean is None:
        return None
    if high is not None and mean - high > tol(high):
        return "MEAN_ABOVE_HIGH"
    if low is not None and low - mean > tol(low):
        return "MEAN_BELOW_LOW"
    return None


def _provider_entry(inp: dict, period: dict, metric: str, policy: dict) -> dict:
    e = period[metric]
    rng = e["high"] - e["low"] if e["high"] is not None and e["low"] is not None else None
    inconsistency = range_inconsistency(e)
    state = "PROVIDER_COVERED"
    if e["mean"] is None:
        state = "PROVIDER_NOT_COVERED"
    elif inconsistency:
        state = "PROVIDER_INCONSISTENT"
    elif e["analystCount"] is None or e["analystCount"] < policy["minAnalystCount"]:
        state = "INSUFFICIENT_ANALYST_COUNT"
    return {
        "provider": inp["provider"],
        "providerPeriodLabel": period["providerPeriodLabel"],
        "fiscalYearEnd": period["fiscalYearEnd"],
        "fiscalYearEndBasis": period["fiscalYearEndBasis"],
        "mean": e["mean"],
        "high": e["high"],
        "low": e["low"],
        "analystCount": e["analystCount"],
        "currency": e["currency"],
        "currencyBasis": e["currencyBasis"],
        "highLowRange": _round(rng, 6) if rng is not None else None,
        "highLowRangePctOfMean": _round(rng / abs(e["mean"]) * 100, 2) if rng is not None and e["mean"] not in (None, 0) else None,
        "providerTimestamp": inp["providerTimestamp"],
        "retrievedAt": inp["retrievedAt"],
        "state": state,
        **({"inconsistency": inconsistency} if inconsistency else {}),
    }


def _agreement(entries: list[dict], metric: str, policy: dict, splits: list[dict] | None = None) -> dict:
    splits = splits or []
    # An impossible row is not a second opinion (2.5.23, F-001): it is left out of the comparison and named.
    inconsistent = [e for e in entries if _is_number(e["mean"]) and e.get("state") == "PROVIDER_INCONSISTENT"]
    excluded = {"providersExcluded": [{"provider": e["provider"], "reason": e.get("inconsistency")} for e in inconsistent]} if inconsistent else {}
    valued = [e for e in entries if _is_number(e["mean"]) and e.get("state") != "PROVIDER_INCONSISTENT"]
    compared = [e["provider"] for e in valued]
    if not valued and inconsistent:
        return {"status": "PROVIDER_INCONSISTENT", "providersCompared": compared, "relativeDiffPct": None, "absoluteDiff": None, **excluded}
    if not valued:
        return {"status": "NO_PROVIDER", "providersCompared": compared, "relativeDiffPct": None, "absoluteDiff": None}
    if len(valued) == 1:
        return {"status": "SINGLE_PROVIDER", "providersCompared": compared, "relativeDiffPct": None, "absoluteDiff": None, **excluded}
    ends = [str(e["fiscalYearEnd"] or "") for e in valued]
    end_days = [_day(x) for x in ends if x]
    if len(end_days) == len(valued) and max(end_days) - min(end_days) > 10:
        return {"status": "PERIOD_IDENTITY_MISMATCH", "providersCompared": compared, "fiscalYearEnds": ends, "relativeDiffPct": None, "absoluteDiff": None, **excluded}
    currencies = list(dict.fromkeys(e["currency"] for e in valued if isinstance(e["currency"], str)))
    if len(currencies) > 1:
        return {"status": "CURRENCY_MISMATCH", "providersCompared": compared, "currencies": currencies, "relativeDiffPct": None, "absoluteDiff": None, **excluded}
    verified = len(currencies) == 1 and all(e["currency"] == currencies[0] for e in valued)
    currency_identity = "VERIFIED" if verified else "UNVERIFIED"
    # The same mean, high, low and analyst count wherever both publish them: one upstream feed carried twice, not
    # two checks (2.5.22, F-005: Yahoo and Alpha Vantage matched to the last published digit).
    if _identical_at_published_precision(valued):
        return {"status": "IDENTICAL", "providersCompared": compared, "relativeDiffPct": 0, "absoluteDiff": 0,
                "currencyIdentity": currency_identity, "independence": "NOT_INDEPENDENT", **excluded}
    means = [e["mean"] for e in valued]
    hi, lo = max(means), min(means)
    absolute = _round(hi - lo, 6)
    scale = max(abs(hi), abs(lo))
    relative = _round((hi - lo) / scale * 100, 2) if scale > 0 else 0
    within_absolute = metric == "eps" and absolute <= policy["epsAbsoluteTolerance"]
    status = "AGREED" if within_absolute or relative <= policy["conflictTolerancePct"] else "CONFLICT"
    if status == "CONFLICT" and metric == "eps":
        # An EPS gap that is a recent split's ratio is one provider not split-adjusted, not two views (2.5.23, F-002).
        explained = _split_explains(valued, splits)
        if explained:
            return {"status": "NOT_SPLIT_ADJUSTED", "providersCompared": compared, "relativeDiffPct": relative, "absoluteDiff": absolute,
                    "currencyIdentity": currency_identity, "independence": "UNVERIFIED",
                    "split": explained["split"], "providersNotAdjusted": explained["notAdjusted"], **excluded}
    return {
        "status": status,
        "providersCompared": compared,
        "relativeDiffPct": relative,
        "absoluteDiff": absolute,
        "currencyIdentity": currency_identity,
        "independence": "UNVERIFIED",
        **excluded,
    }


def _split_explains(valued: list[dict], splits: list[dict]) -> dict | None:
    """The recent split (or run of recent splits) whose ratio matches the gap between two same-sign EPS means, and the
    provider still on the pre-split basis: the larger magnitude after a forward split, the smaller after a reverse one."""
    if len(valued) != 2 or not splits:
        return None
    a, b = valued[0]["mean"], valued[1]["mean"]
    if a == 0 or b == 0 or (a > 0) != (b > 0):
        return None
    gap = max(abs(a), abs(b)) / min(abs(a), abs(b))
    candidates = [{"split": {"date": s["date"][:10], "ratio": s["ratio"]}, "ratio": s["ratio"]} for s in splits]
    if len(splits) > 1:
        product = 1
        for s in splits:
            product = product * s["ratio"]
        candidates.append({"split": {"date": splits[-1]["date"][:10], "ratio": _round(product, 6), "cumulativeOf": len(splits)}, "ratio": product})
    for c in candidates:
        factor = max(c["ratio"], 1 / c["ratio"])
        if factor < 1.5 or abs(gap / factor - 1) > SPLIT_RATIO_TOLERANCE:
            continue
        larger = valued[0] if abs(a) > abs(b) else valued[1]
        smaller = valued[1] if larger is valued[0] else valued[0]
        return {"split": c["split"], "notAdjusted": [str((larger if c["ratio"] > 1 else smaller)["provider"])]}
    return None


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _identical_at_published_precision(valued: list[dict]) -> bool:
    for field in ("mean", "high", "low", "analystCount"):
        values = [e[field] for e in valued if _is_number(e[field])]
        if len(values) >= 2 and len(set(values)) > 1:
            return False
    return all(_is_number(e["analystCount"]) for e in valued)


_COMPARED = {"AGREED", "CONFLICT", "IDENTICAL", "NOT_SPLIT_ADJUSTED"}


def _cross_check(inputs: list[dict], periods: list[dict]) -> tuple[dict, list[dict]]:
    """Whether the curve is a cross-check at all (2.5.22, F-003/F-005): which providers answered, which failed and
    why, and whether the ones that answered are independent. A failed provider or identical feeds is a warning,
    not a quiet single-source curve."""
    answered = [i["provider"] for i in inputs if i["status"] == "OK"]
    failed = [{"provider": i["provider"], "status": i["status"], "message": i["message"]}
              for i in inputs if i["status"] != "OK" and i["status"] != "NO_DATA"]
    compared = 0
    identical = 0
    for period in periods:
        for cell in period["metrics"].values():
            status = str(cell["agreement"]["status"])
            if status in _COMPARED:
                compared += 1
            if status == "IDENTICAL":
                identical += 1
    if not answered:
        status = "NO_PROVIDER"
    elif len(answered) == 1:
        status = "SINGLE_PROVIDER"
    elif compared == 0:
        status = "NOT_COMPARED"
    elif identical == compared:
        status = "NOT_INDEPENDENT"
    else:
        status = "CROSS_CHECKED"
    warnings: list[dict] = []
    for f in failed:
        quota = (" This server's Alpha Vantage key and its quota are its own, separate from any key used directly."
                 if f["provider"] == "alpha_vantage" else "")
        suffix = f": {f['message']}" if f["message"] else ""
        warnings.append({
            "code": "CROSS_CHECK_DEGRADED",
            "message": f"{f['provider']} returned {f['status']}{suffix}. The curve is not cross-checked against it.{quota}",
            "severity": "warning",
        })
    if status == "NOT_INDEPENDENT":
        warnings.append({
            "code": "PROVIDERS_NOT_INDEPENDENT",
            "message": "Every compared figure is identical at the providers' published precision: they carry one upstream feed, so their agreement is one source, not two.",
            "severity": "warning",
        })
    return ({"status": status, "providersAnswered": answered, "providersFailed": failed, "cellsCompared": compared, "cellsIdentical": identical},
            warnings)


def _metric_coverage(entries: list[dict], agreement_status: str) -> str:
    if not any(isinstance(e["mean"], (int, float)) for e in entries):
        return "PROVIDER_NOT_COVERED"
    if agreement_status == "PROVIDER_INCONSISTENT":
        return "PROVIDER_INCONSISTENT"
    if agreement_status in ("CONFLICT", "PERIOD_IDENTITY_MISMATCH", "CURRENCY_MISMATCH", "NOT_SPLIT_ADJUSTED"):
        return "PROVIDER_CONFLICT"
    if not any(e["state"] == "PROVIDER_COVERED" for e in entries):
        return "INSUFFICIENT_ANALYST_COUNT"
    return "PROVIDER_COVERED"


_INCONSISTENCY_TEXT = {
    "MEAN_ABOVE_HIGH": "its mean is above its own high",
    "MEAN_BELOW_LOW": "its mean is below its own low",
    "LOW_ABOVE_HIGH": "its low is above its own high",
}


def _cell_warnings(label: str, metric: str, cell: dict) -> list[dict]:
    """One warning per impossible provider row (F-001) and per EPS gap read as a missed split adjustment (F-002)."""
    out: list[dict] = []
    for e in cell["providers"]:
        if e["state"] != "PROVIDER_INCONSISTENT":
            continue
        fy_end = e["fiscalYearEnd"] if e["fiscalYearEnd"] is not None else "not stated"
        out.append({
            "code": "PROVIDER_ROW_INCONSISTENT",
            "message": f"{e['provider']} {label} {metric} (fiscal year ending {fy_end}): {_INCONSISTENCY_TEXT.get(str(e['inconsistency']), 'undefined')} "
                       f"(mean {_js(e['mean'])}, low {_js(e['low'])}, high {_js(e['high'])}). The row is shown as given and left out of the agreement check.",
            "severity": "warning",
        })
    agree = cell["agreement"]
    if agree["status"] == "NOT_SPLIT_ADJUSTED":
        split = agree["split"]
        out.append({
            "code": "PROVIDER_NOT_SPLIT_ADJUSTED",
            "message": f"{label} EPS: the providers differ by the ratio of the {split_text(split['ratio'])} split on {split['date']}; "
                       f"{', '.join(agree['providersNotAdjusted'])} appears not split-adjusted. Reported as NOT_SPLIT_ADJUSTED, not as a conflict between views.",
            "severity": "warning",
        })
    return out


def _unavailable_statistic() -> dict:
    return {"value": None, "state": "PROVIDER_NOT_COVERED"}


def build_consensus_curve(ticker: str, inputs: list[dict], as_of: str, policy: dict | None = None, naming: dict | None = None,
                          split_history: dict | None = None) -> dict:
    """FY0 to FY+horizon consensus, per provider, per fiscal period and metric.

    Periods no provider covers are listed as PROVIDER_NOT_COVERED rather than
    filled; no provider value is selected.
    """
    policy = dict(policy or DEFAULT_CONSENSUS_POLICY)
    # The ticker's split history (2.5.23); without it EPS gaps are not checked against splits.
    recent_splits = splits_within(split_history, as_of, SPLIT_LOOKBACK_DAYS)
    cell_warning_list: list[dict] = []
    # How the company names its fiscal years, from its annual reports (2.5.13); the period-end rule without it.
    year_naming = naming or {"offset": 0, "basis": "PERIOD_END_RULE", "periodEnd": None, "statedFiscalYear": None, "calendar": None}
    horizon = max(1, min(5, int(policy["horizonYears"])))
    fy0 = resolve_fy0(inputs, as_of)
    periods = []
    summary: dict[str, int] = {}
    for k in range(horizon + 1):
        label = "FY0" if k == 0 else f"FY+{k}"
        matching = [
            (inp, p) for inp in inputs for p in inp["periods"]
            if fy0 and p["fiscalYearEnd"] and fiscal_year_offset(fy0["fiscalYearEnd"], p["fiscalYearEnd"]) == k
        ]
        fiscal_year_ends = sorted({p["fiscalYearEnd"] for _, p in matching})
        metrics = {}
        for metric in CONSENSUS_METRICS:
            entries = [_provider_entry(inp, p, metric, policy) for inp, p in matching]
            agree = _agreement(entries, metric, policy, recent_splits)
            coverage = _metric_coverage(entries, str(agree["status"]))
            summary[coverage] = summary.get(coverage, 0) + 1
            metrics[metric] = {
                "coverage": coverage,
                "providers": entries,
                "agreement": agree,
                "dispersionStatistics": {"highLowRange": "PER_PROVIDER", "median": _unavailable_statistic(), "standardDeviation": _unavailable_statistic()},
            }
            cell_warning_list.extend(_cell_warnings(label, metric, metrics[metric]))
        periods.append({
            "label": label,
            # A 52/53-week year ending in early January is the prior year's (2.5.11); the company's own naming
            # (DG's year ending January 2026 is its fiscal 2025) shifts it (2.5.13).
            "fiscalYear": (fiscal_year_of_period_end(fy0["fiscalYearEnd"]) + year_naming["offset"] + k
                           if fy0 and fiscal_year_of_period_end(fy0["fiscalYearEnd"]) is not None else None),
            "fiscalYearEnds": fiscal_year_ends,
            # The company's own year end for that year, from its SEC fiscal calendar; fiscalYearEnds stays as the
            # providers state it (2.5.16: DG's 2027-01-31 from Yahoo is its Friday 2027-01-29).
            "companyFiscalYearEnd": company_fiscal_year_end(year_naming.get("calendar"), fiscal_year_ends[0] if fiscal_year_ends else None),
            "metrics": metrics,
        })
    check, check_warnings = _cross_check(inputs, periods)
    return {
        "ticker": ticker.upper(),
        "asOf": as_of,
        "crossCheck": check,
        "splitHistory": _split_history_summary(split_history, recent_splits),
        "fiscalYearBasis": fy0 or {"fiscalYearEnd": None, "basis": "NO_PROVIDER_FISCAL_YEAR"},
        "fiscalYearNaming": year_naming,
        "policy": {**policy, "horizonYears": horizon},
        "providers": [
            {
                "provider": i["provider"],
                "status": i["status"],
                "retrievedAt": i["retrievedAt"],
                "providerTimestamp": i["providerTimestamp"],
                "message": i["message"],
                "fiscalYearsReturned": [p["fiscalYearEnd"] for p in i["periods"] if p["fiscalYearEnd"]],
            }
            for i in inputs
        ],
        "periods": periods,
        "coverageSummary": summary,
        "metricsNotCoveredByAnyProvider": [{"metric": m, "coverage": "PROVIDER_NOT_COVERED"} for m in METRICS_NOT_COVERED],
        "notes": [
            "Each provider's figures are reported as given; no provider value is selected, blended or averaged.",
            "Periods and metrics no provider covers are PROVIDER_NOT_COVERED; nothing is interpolated or extended from long-term growth rates.",
            "Median and standard deviation are not published by the configured providers; the high-low range per provider is the only dispersion measure.",
            "Agreement between providers does not establish independence: they may redistribute the same underlying estimates. IDENTICAL marks figures equal to the last published digit, which is one feed carried twice.",
        ],
        "warnings": [*check_warnings, *cell_warning_list,
                     *_split_history_warnings(split_history, "EPS gaps between providers are not checked against splits.")],
        **AUTHORITY_BOUNDARY,
    }


def _split_history_summary(history: dict | None, recent: list[dict]) -> dict:
    return {
        "status": history["status"] if history else "NOT_REQUESTED",
        "lookbackDays": SPLIT_LOOKBACK_DAYS,
        "recentSplits": [{"date": s["date"][:10], "ratio": s["ratio"]} for s in recent],
        **({"message": history["message"]} if history and history.get("message") else {}),
    }


def _split_history_warnings(history: dict | None, consequence: str) -> list[dict]:
    if not history or history["status"] == "OK":
        return []
    suffix = f": {history['message']}" if history.get("message") else ""
    return [{
        "code": "SPLIT_HISTORY_UNAVAILABLE",
        "message": f"Split history returned {history['status']}{suffix}. {consequence}",
        "severity": "warning",
    }]


# ── EPS revision windows ─────────────────────────────────────────────────────

_WINDOWS = [("7d", "d7", 7), ("30d", "d30", 30), ("60d", "d60", 60), ("90d", "d90", 90)]


def _revision_provider(inp: dict, p: dict, label: str, as_of: str, splits: list[dict], warnings: list[dict]) -> dict:
    """Each window's change, unless it cannot mean anything (2.5.23): the row is impossible (F-001), or a split falls
    inside the window so the past mean may be on the pre-split share count (F-002: ANET 2.73 -> 0.73 at its 4-for-1).
    A day of margin covers providers that adjust the day after the split."""
    trend = p["epsTrend"]
    current = trend["current"] if trend else None
    inconsistency = range_inconsistency(p["eps"])
    if inconsistency:
        fy_end = p["fiscalYearEnd"] if p["fiscalYearEnd"] is not None else "not stated"
        warnings.append({
            "code": "PROVIDER_ROW_INCONSISTENT",
            "message": f"{inp['provider']} {label} eps (fiscal year ending {fy_end}): {_INCONSISTENCY_TEXT[inconsistency]} "
                       f"(mean {_js(p['eps']['mean'])}, low {_js(p['eps']['low'])}, high {_js(p['eps']['high'])}). No window change is computed from it.",
            "severity": "warning",
        })
    windows = {}
    not_reported = []
    for name, key, days in _WINDOWS:
        past = trend[key] if trend else None
        if past is None:
            not_reported.append(f"{name}.mean")
        in_window = splits_within({"status": "OK", "splits": splits}, as_of, days + 1)
        split = in_window[-1] if in_window else None
        if past is None or current is None:
            state = "NOT_REPORTED"
        elif inconsistency:
            state = "PROVIDER_INCONSISTENT"
        elif split:
            state = "SPLIT_IN_WINDOW"
        else:
            state = "COMPARED"
        compared = state == "COMPARED"
        windows[name] = {
            "mean": past,
            "change": _round(current - past, 6) if compared else None,
            "changePct": _round((current - past) / abs(past) * 100, 2) if compared and past != 0 else None,
            "state": state,
            **({"split": {"date": split["date"][:10], "ratio": split["ratio"]}} if state == "SPLIT_IN_WINDOW" and split else {}),
        }
    counts = p["epsRevisions"] or {"up7d": None, "down7d": None, "up30d": None, "down30d": None}
    for k, v in counts.items():
        if v is None:
            not_reported.append(f"revisionCounts.{k}")
    return {
        "provider": inp["provider"],
        "providerPeriodLabel": p["providerPeriodLabel"],
        "fiscalYearEnd": p["fiscalYearEnd"],
        "state": "PROVIDER_INCONSISTENT" if inconsistency else "PROVIDER_COVERED",
        **({"inconsistency": inconsistency} if inconsistency else {}),
        "current": current,
        "windows": windows,
        "revisionCounts": counts,
        "notReported": not_reported,
        "retrievedAt": inp["retrievedAt"],
        "providerTimestamp": inp["providerTimestamp"],
    }


def build_eps_revisions(ticker: str, inputs: list[dict], as_of: str, split_history: dict | None = None) -> dict:
    """EPS estimate windows (7/30/60/90 days) and revision counts as each provider reports them for FY0 and FY+1."""
    fy0 = resolve_fy0(inputs, as_of)
    window_splits = splits_within(split_history, as_of, 91)
    warnings: list[dict] = []
    periods = []
    for k in (0, 1):
        label = "FY0" if k == 0 else "FY+1"
        providers = [
            _revision_provider(inp, p, label, as_of, window_splits, warnings) for inp in inputs for p in inp["periods"]
            if fy0 and p["fiscalYearEnd"] and fiscal_year_offset(fy0["fiscalYearEnd"], p["fiscalYearEnd"]) == k and (p["epsTrend"] or p["epsRevisions"])
        ] if fy0 else []
        periods.append({"label": label, "coverage": "PROVIDER_COVERED" if providers else "PROVIDER_NOT_COVERED", "providers": providers})
    if window_splits:
        last = window_splits[-1]
        warnings.append({
            "code": "SPLIT_IN_WINDOW",
            "message": f"A {split_text(last['ratio'])} split on {last['date'][:10]} falls inside the revision windows; "
                       "windows spanning it compare pre- and post-split estimates and carry no change.",
            "severity": "warning",
        })
    warnings.extend(_split_history_warnings(split_history, "Revision windows are not checked against splits."))
    return {
        "ticker": ticker.upper(),
        "asOf": as_of,
        "fiscalYearBasis": fy0 or {"fiscalYearEnd": None, "basis": "NO_PROVIDER_FISCAL_YEAR"},
        "periods": periods,
        "revenueRevisions": {"coverage": "PROVIDER_NOT_COVERED", "note": "No configured provider publishes revenue estimate history."},
        "analystCountChanges": {"coverage": "PROVIDER_NOT_COVERED", "note": "No configured provider publishes analyst adds or drops."},
        "providers": [{"provider": i["provider"], "status": i["status"], "retrievedAt": i["retrievedAt"], "message": i["message"]} for i in inputs],
        "splitHistory": {**_split_history_summary(split_history, window_splits), "lookbackDays": 91},
        "warnings": warnings,
        **AUTHORITY_BOUNDARY,
    }


# ── Evidence quality ─────────────────────────────────────────────────────────

def days_between(from_iso: str, to_iso: str) -> int:
    return _day(to_iso) - _day(from_iso)


_PERIODIC_FORMS = {"10-K", "10-Q", "20-F", "40-F", "10-K/A", "10-Q/A", "20-F/A", "40-F/A"}
QUARTERLY_REPORT_STALE_DAYS = 135
# A year plus the four months a 20-F has after year end, plus a week of 52/53-week drift.
ANNUAL_REPORT_STALE_DAYS = 365 + 120 + 7
# Provider statuses that mean the request failed, as opposed to the provider having nothing (NO_DATA).
PROVIDER_FAILURE_STATUSES = {"PROVIDER_ERROR", "RATE_LIMIT", "PROVIDER_TIMEOUT", "ERROR", "TIMEOUT"}


def evidence_quality(*, ticker: str, as_of: str, quote: dict | None, filings: list[dict] | None, filings_status: str,
                     consensus: dict | None, storage_available: bool) -> dict:
    """Status, freshness and coverage per doctrine-relevant evidence family, from light inputs only.

    Families that need a full extraction report readiness, not results.
    """
    families: dict[str, Any] = {}
    blockers: list[dict] = []

    def block(family: str, code: str, message: str) -> None:
        blockers.append({"family": family, "code": code, "message": message})

    q = quote
    quote_age = days_between(q["priceTime"], as_of) if q and q.get("priceTime") else None
    # A quote request that failed is not a missing price: it is unavailable and worth a retry (2.5.15).
    quote_failed = (not q or q.get("price") is None) and str((q.get("status") if q else None) or "") in PROVIDER_FAILURE_STATUSES
    families["quote"] = {
        "state": ("STALE" if quote_age is not None and quote_age > 4 else "READY") if q and q.get("price") is not None else "UNAVAILABLE" if quote_failed else "MISSING",
        "price": q.get("price") if q else None,
        "currency": q.get("currency") if q else None,
        "priceTime": q.get("priceTime") if q else None,
        "ageDays": quote_age,
        "sourceStatus": q.get("status") if q else "NOT_RETRIEVED",
    }
    if quote_failed:
        blockers.append({"family": "quote", "code": "QUOTE_UNAVAILABLE", "message": f"The price request failed ({q.get('status')}); retry. Mechanical dilution at price cannot run without it.", "retryable": True})
    elif not q or q.get("price") is None:
        block("quote", "QUOTE_MISSING", "No current price; mechanical dilution at price cannot run.")

    rows = filings or []
    periodic_rows = sorted((f for f in rows if f["form"] in _PERIODIC_FORMS), key=lambda f: f["filingDate"], reverse=True)
    periodic = periodic_rows[0] if periodic_rows else None
    period_end = periodic["reportDate"] if periodic else None
    period_age = days_between(period_end, as_of) if period_end else None
    # A foreign private issuer files one periodic report a year (20-F within four months of year end, 40-F
    # likewise) and furnishes interim results on 6-K; its periodic report is stale only once the next one is
    # overdue (2.5.15: TSM, TSEM, NBIS were STALE at 273 days).
    annual_cadence = periodic is not None and re.match(r"(?:20-F|40-F)", periodic["form"]) is not None
    stale_after_days = ANNUAL_REPORT_STALE_DAYS if annual_cadence else QUARTERLY_REPORT_STALE_DAYS
    periodic_state = "UNAVAILABLE" if filings is None else "MISSING" if not periodic else "STALE" if period_age is not None and period_age > stale_after_days else "READY"
    latest_6k = sorted(f["filingDate"] for f in rows if f["form"] == "6-K")[-1:] if annual_cadence else None
    families["secPeriodicFiling"] = {
        "state": periodic_state,
        "form": periodic["form"] if periodic else None,
        "filingDate": periodic["filingDate"] if periodic else None,
        "periodEnd": period_end,
        "periodAgeDays": period_age,
        "cadence": None if periodic is None else "ANNUAL" if annual_cadence else "QUARTERLY",
        "staleAfterDays": None if periodic is None else stale_after_days,
        **({"latestInterim6k": latest_6k[0] if latest_6k else None} if annual_cadence else {}),
        "inlineXbrl": periodic["isInlineXBRL"] if periodic else None,
        "sourceStatus": filings_status,
    }
    if periodic_state != "READY":
        block("secPeriodicFiling", f"SEC_PERIODIC_{periodic_state}",
              f"Latest periodic filing covers {period_end}, {period_age} days ago." if periodic else "No SEC periodic filing found.")

    extractable = periodic_state in ("READY", "STALE")
    for family in ("capitalStructure", "dilution"):
        families[family] = {
            "state": ("PARTIAL" if periodic and periodic["isInlineXBRL"] is False else "READY_TO_EXTRACT") if extractable else "UNAVAILABLE",
            "evaluated": False,
            "basis": f"{periodic['form']} for {period_end}" if extractable and periodic else None,
            "note": "Preflight checks inputs only; the extraction runs in build_valuation_evidence_pack.",
        }

    earnings = sorted(
        (f for f in rows if f["form"] == "8-K" and "2.02" in [s.strip() for s in (f.get("items") or "").split(",")]),
        key=lambda f: f["filingDate"], reverse=True,
    )
    earnings8k = earnings[0] if earnings else None
    guidance_age = days_between(earnings8k["filingDate"], as_of) if earnings8k else None
    families["guidance"] = {
        "state": "UNAVAILABLE" if filings is None else "MISSING" if not earnings8k else "STALE" if guidance_age is not None and guidance_age > 120 else "READY_TO_EXTRACT",
        "latestEarningsRelease8k": earnings8k["filingDate"] if earnings8k else None,
        "ageDays": guidance_age,
        "evaluated": False,
    }

    recent8k = [f for f in rows if f["form"] == "8-K" and days_between(f["filingDate"], as_of) <= 90]
    latest = sorted(f["filingDate"] for f in recent8k)
    families["materialEvents"] = {
        "state": "UNAVAILABLE" if filings is None else "READY",
        "eightKCount90d": len(recent8k),
        "latest8k": latest[-1] if latest else None,
    }

    # No provider returned estimates because every request failed (Yahoo errored, Alpha Vantage rate-limited):
    # that says nothing about analyst coverage, so the family is UNAVAILABLE with one retryable blocker (2.5.15).
    provider_statuses = [{"provider": str(p.get("provider") if p.get("provider") is not None else ""), "status": str(p.get("status") if p.get("status") is not None else "")}
                         for p in ((consensus or {}).get("providers") or [])]
    consensus_failed = (len(provider_statuses) > 0
                        and not any(p["status"] == "OK" for p in provider_statuses)
                        and any(p["status"] in PROVIDER_FAILURE_STATUSES for p in provider_statuses))
    cells: dict[str, str] = {}
    for p in ((consensus or {}).get("periods") or [])[:2]:
        m = p.get("metrics") or {}
        for metric in CONSENSUS_METRICS:
            cells[f"{p['label']}.{metric}"] = (m.get(metric) or {}).get("coverage") or "PROVIDER_NOT_COVERED"
    covered = sum(1 for c in cells.values() if c == "PROVIDER_COVERED")
    families["consensus"] = {
        "state": "UNAVAILABLE" if not consensus or consensus_failed else "READY" if covered == len(cells) and covered > 0 else "PARTIAL" if covered > 0 else "MISSING",
        "cells": {} if consensus_failed else cells,
        "beyondFy1": "PROVIDER_NOT_COVERED",
        "providerStatuses": provider_statuses,
    }
    if consensus_failed:
        blockers.append({
            "family": "consensus",
            "code": "CONSENSUS_PROVIDER_ERROR",
            "message": f"No estimates were read: {', '.join(p['provider'] + ' ' + p['status'] for p in provider_statuses)}; retry. Analyst coverage is unknown, not absent.",
            "retryable": True,
        })
    else:
        for cell, coverage in cells.items():
            if coverage != "PROVIDER_COVERED":
                block("consensus", coverage, f"{cell} is {coverage}.")

    families["evidenceStorage"] = {"state": "READY" if storage_available else "UNAVAILABLE", "durable": storage_available}

    return {
        "ticker": ticker.upper(),
        "asOf": as_of,
        "families": families,
        "blockers": blockers,
        "readyFamilies": [k for k, v in families.items() if v["state"] in ("READY", "READY_TO_EXTRACT")],
        **AUTHORITY_BOUNDARY,
    }


# ── Evidence cut receipt ─────────────────────────────────────────────────────

def _warning_codes(warnings: list) -> list[str]:
    return [w["code"] for w in warnings if isinstance(w, dict) and isinstance(w.get("code"), str)]


def _provider_timestamps(data: Any) -> dict:
    d = data if isinstance(data, dict) else {}
    out = {}
    for key in ("priceTime", "filingDate", "periodEnd", "asOf", "acceptedAt", "reportDate"):
        if isinstance(d.get(key), str):
            out[key] = d[key]
    source = d.get("source") if isinstance(d.get("source"), dict) else {}
    for key in ("filingDate", "periodEnd", "accessionNumber", "form", "filingType"):
        if isinstance(source.get(key), str):
            out[f"source.{key}"] = source[key]
    return out


def build_receipt(*, ticker: str, evidence_cutoff: str, server_version: str, build_sha: str | None, runtime: str,
                  components: dict[str, dict], component_hashes: dict[str, str], components_sha256: str) -> dict:
    """The receipt embedded in an evidence cut."""
    names = sorted(components)
    failures = [n for n in names if components[n]["status"] != "OK"]
    return {
        "ticker": ticker.upper(),
        "evidenceCutoff": evidence_cutoff,
        "serverVersion": server_version,
        "buildSha": build_sha,
        "runtime": runtime,
        "hashAlgorithm": "sha256",
        "canonicalization": CANONICALIZATION,
        "componentsSha256": components_sha256,
        "components": [
            {
                "name": name,
                "sourceTool": components[name]["sourceTool"],
                "status": components[name]["status"],
                "sha256": component_hashes[name],
                "retrievedAt": components[name]["retrievedAt"],
                "providerTimestamps": _provider_timestamps(components[name]["data"]),
                "warningCodes": _warning_codes(components[name]["warnings"]),
                "failure": components[name]["error"],
            }
            for name in names
        ],
        "coverage": {
            "componentCount": len(names),
            "okCount": len(names) - len(failures),
            "failedOrLimited": failures,
            "state": "COMPLETE" if not failures else "FAILED" if len(failures) == len(names) else "PARTIAL",
            "scope": "COMPONENT_WRAPPER_STATUS",
            "evidenceCompleteness": "NOT_ASSERTED",
        },
    }


def evidence_cut_document(*, ticker: str, evidence_cutoff: str, components: dict, receipt: dict) -> dict:
    """The document an evidence cut stores.

    The authority boundary sits at top level, one record per component, and
    the receipt as `provenance`. Its SHA-256 over canonical_json(document) is
    the cut's identity.
    """
    return {
        "schema": EVIDENCE_CUT_SCHEMA,
        "ticker": ticker.upper(),
        "evidenceCutoff": evidence_cutoff,
        **AUTHORITY_BOUNDARY,
        **components,
        "provenance": receipt,
    }


def component_from_tool_text(source_tool: str, text: str | None, retrieved_at: str, error: Any = None) -> dict:
    """Classify a sub-tool's JSON text as a component record without altering its payload."""
    if error is not None or text is None:
        return {"status": "FAILED", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": None, "warnings": [],
                "error": {"code": "COMPONENT_FAILED", "message": str(error if error is not None else "no response")}}
    try:
        parsed = json.loads(text)
    except ValueError:
        return {"status": "FAILED", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": text, "warnings": [],
                "error": {"code": "NON_JSON_RESPONSE", "message": text[:300]}}
    return component_from_value(source_tool, parsed, retrieved_at)


_LIMITED_RE = re.compile(r"PARTIAL|INCOMPLETE|STALE|NOT_AVAILABLE|NOT_FOUND|UNAVAILABLE|UNSUPPORTED|NO_DATA|NOT_DISCLOSED|UNCONFIGURED|FAILED")


def component_from_value(source_tool: str, parsed: Any, retrieved_at: str) -> dict:
    value = parsed
    if isinstance(value, dict) and isinstance(value.get("ok"), bool) and "data" in value:
        if value["ok"] is not True:
            err = value.get("error") if isinstance(value.get("error"), dict) else {}
            return {"status": "FAILED", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": None, "warnings": [],
                    "error": {"code": str(err["code"] if err.get("code") is not None else "PROVIDER_ERROR"), "message": str(err["message"] if err.get("message") is not None else "")}}
        value = value["data"]
    obj = value if isinstance(value, dict) else {}
    warnings = obj.get("warnings") if isinstance(obj.get("warnings"), list) else []
    if obj.get("error") is True or (obj.get("ok") is False and obj.get("error")):
        err = obj["error"] if isinstance(obj.get("error"), dict) else obj
        return {"status": "FAILED", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": value, "warnings": warnings,
                "error": {"code": str(err["code"] if err.get("code") is not None else "PROVIDER_ERROR"), "message": str(err["message"] if err.get("message") is not None else "")}}
    limited = isinstance(obj.get("status"), str) and bool(_LIMITED_RE.search(obj["status"]))
    return {
        "status": "LIMITED" if limited else "OK",
        "sourceTool": source_tool,
        "retrievedAt": retrieved_at,
        "data": value,
        "warnings": warnings,
        "error": {"code": str(obj["status"]), "message": str(next((obj[k] for k in ("message", "reason") if obj.get(k) is not None), ""))} if limited else None,
    }
