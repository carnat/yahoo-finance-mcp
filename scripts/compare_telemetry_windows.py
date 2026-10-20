#!/usr/bin/env python3
"""Phase 4 exit comparison of two telemetry windows, synthetic traffic excluded (F6, D10).

Queries the Workers Analytics Engine SQL API (dataset `yfmcp_telemetry`, layout
in worker/src/telemetry.ts) once for a baseline window and once for a
comparison window. Rows with blob7 (`credentialTransport`) = 'header' are
excluded: every CI canary, preview canary and deploy audit authenticates with
an `Authorization: Bearer` header (scripts/live_smoke_utils.py), while the
owner's connector uses the path transport. Caveat: once Phase 1b personal keys
exist, a real user could use header transport; revisit the rule then.

Per action group (blob1 `domain`), weighted by `_sample_interval`:
- requests: SUM(_sample_interval);
- latencyMs p50 and p95 (double1): quantileExactWeighted;
- the error-plus-exception rate: outcomes (blob4) tool_error + rpc_error +
  exception over all requests of the group. `rejected` (a request refused
  before dispatch: auth, key limit, method, body size, invalid JSON-RPC, batch)
  is reported but is not an error. The outcome values are export_telemetry's
  OUTCOMES.

D10 bounds (analysis/yahoo-finance-mcp/PHASE4_SPEC.md §11a): only groups with
>= 30 requests in BOTH windows are judged. A judged group passes when its p95
and its p50 are each at most 1.2 x the baseline value and its error-plus-
exception rate is at most the baseline rate + 1 percentage point. Overall pass
needs at least one judged group and no failing one.

Exit codes: 0 overall pass, 1 overall fail, 2 missing secrets or bad
arguments, 3 no group judgeable (too little volume), 4 a query failed.

Environment, transport, auth and token hygiene are those of
scripts/export_telemetry.py (whose helpers are imported, not copied):
CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_TELEMETRY_TOKEN (Account Analytics: Read
is enough here). The token is sent only in the Authorization header and is
never printed or written. API references: see export_telemetry.py's docstring
(SQL API, quantileExactWeighted, sumIf, FORMAT JSON, toDateTime, sampling).

Usage:
  python scripts/compare_telemetry_windows.py \\
    --compare-start 2026-11-01T00:00Z --compare-end 2026-11-08T00:00Z [--out report.json]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from export_telemetry import (  # noqa: E402
    ACCOUNT_ID,
    DATASET,
    EXCLUDED_TRANSPORT,
    OUTCOMES,
    ExportError,
    Transport,
    _clean,
    _num,
    _safe,
    _where,
    run_sql,
    urllib_transport,
)

SCHEMA_VERSION = 1
ERROR_OUTCOMES = ("tool_error", "rpc_error", "exception")
MIN_REQUESTS = 30
LATENCY_MAX_INCREASE = 0.20  # +20 % for p50 and p95
ERROR_RATE_MAX_INCREASE_PP = 1.0  # +1 percentage point
EPSILON = 1e-9

DEFAULT_BASELINE = ("2026-10-17T00:00Z", "2026-10-24T00:00Z")

EXIT_PASS, EXIT_FAIL, EXIT_USAGE, EXIT_NOTHING_JUDGEABLE, EXIT_QUERY_FAILED = 0, 1, 2, 3, 4


def parse_utc(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def window_sql(dataset: str, start: datetime, end: datetime) -> str:
    outcome_cols = ",\n  ".join(f"sumIf(_sample_interval, blob4 = '{o}') AS outcome_{o}" for o in OUTCOMES)
    return f"""SELECT
  blob1 AS action_group,
  SUM(_sample_interval) AS requests,
  {outcome_cols},
  quantileExactWeighted(0.5)(double1, _sample_interval) AS latency_p50,
  quantileExactWeighted(0.95)(double1, _sample_interval) AS latency_p95
FROM {dataset}
WHERE {_where(start, end, exclude_header=True)}
GROUP BY action_group
ORDER BY action_group
FORMAT JSON"""


def window_metrics(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Per action group: requests, outcome counts, p50/p95 latencyMs, error-plus-exception rate."""
    groups: dict[str, dict[str, Any]] = {}
    for row in rows:
        group = _safe(row.get("action_group")) or "-"
        requests = _num(row.get("requests"))
        outcomes = {o: _num(row.get(f"outcome_{o}")) for o in OUTCOMES}
        if group in groups:  # two raw values that both map to "invalid": merge counts, drop percentiles
            entry = groups[group]
            entry["requests"] += requests
            for o in OUTCOMES:
                entry["outcomes"][o] += outcomes[o]
            entry["latencyMs"] = {"p50": None, "p95": None}
            continue
        groups[group] = {
            "requests": requests,
            "outcomes": outcomes,
            "latencyMs": {"p50": _num(row.get("latency_p50")), "p95": _num(row.get("latency_p95"))},
        }
    for entry in groups.values():
        errors = sum(entry["outcomes"][o] for o in ERROR_OUTCOMES)
        entry["errorRate"] = errors / entry["requests"] if entry["requests"] else None
    return groups


def _rounded(entry: dict[str, Any]) -> dict[str, Any]:
    rate = entry["errorRate"]
    return {
        "requests": _clean(entry["requests"]),
        "outcomes": {o: _clean(n) for o, n in entry["outcomes"].items()},
        "latencyMs": {q: (None if v is None else _clean(v)) for q, v in entry["latencyMs"].items()},
        "errorRatePct": None if rate is None else round(rate * 100, 4),
    }


def _latency_check(base: float | None, value: float | None) -> dict[str, Any]:
    if base is None or value is None:
        return {"baseline": base, "compare": value, "limit": None, "pass": False}
    limit = base * (1 + LATENCY_MAX_INCREASE)
    return {"baseline": _clean(base), "compare": _clean(value), "limit": _clean(limit),
            "pass": value <= limit + EPSILON}


def judge(baseline: dict[str, dict[str, Any]], compare: dict[str, dict[str, Any]]) -> dict[str, Any]:
    groups: dict[str, Any] = {}
    judged: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []
    for name in sorted(set(baseline) | set(compare)):
        base, comp = baseline.get(name), compare.get(name)
        entry: dict[str, Any] = {
            "baseline": _rounded(base) if base else None,
            "compare": _rounded(comp) if comp else None,
        }
        base_n = base["requests"] if base else 0.0
        comp_n = comp["requests"] if comp else 0.0
        if base_n < MIN_REQUESTS or comp_n < MIN_REQUESTS:
            entry.update(judged=False, **{"pass": None})
            entry["skipReason"] = f"fewer than {MIN_REQUESTS} requests in " + (
                "both windows" if base_n < MIN_REQUESTS and comp_n < MIN_REQUESTS
                else "the baseline window" if base_n < MIN_REQUESTS else "the comparison window")
            skipped.append(name)
        else:
            base_rate, comp_rate = base["errorRate"], comp["errorRate"]
            rate_limit = base_rate * 100 + ERROR_RATE_MAX_INCREASE_PP
            checks = {
                "latencyP95": _latency_check(base["latencyMs"]["p95"], comp["latencyMs"]["p95"]),
                "latencyP50": _latency_check(base["latencyMs"]["p50"], comp["latencyMs"]["p50"]),
                "errorRatePct": {"baseline": round(base_rate * 100, 4), "compare": round(comp_rate * 100, 4),
                                 "limit": round(rate_limit, 4), "pass": comp_rate * 100 <= rate_limit + EPSILON},
            }
            passed = all(c["pass"] for c in checks.values())
            entry.update(judged=True, checks=checks, **{"pass": passed})
            judged.append(name)
            if not passed:
                failed.append(name)
        groups[name] = entry
    overall = "no_judgeable_groups" if not judged else "fail" if failed else "pass"
    return {"groupsJudged": judged, "groupsSkippedLowVolume": skipped, "groupsFailed": failed,
            "overall": overall, "groups": groups}


def build_report(transport: Transport, account_id: str, token: str,
                 baseline: tuple[datetime, datetime], compare: tuple[datetime, datetime],
                 dataset: str = DATASET) -> dict[str, Any]:
    if not ACCOUNT_ID.match(account_id):
        raise ExportError("CLOUDFLARE_ACCOUNT_ID is not a 32-character hex account id")
    if not re.match(r"^[A-Za-z0-9_]{1,64}$", dataset):
        raise ExportError("dataset name must be [A-Za-z0-9_]")
    base_rows = run_sql(transport, account_id, token, window_sql(dataset, *baseline))
    comp_rows = run_sql(transport, account_id, token, window_sql(dataset, *compare))
    result = judge(window_metrics(base_rows), window_metrics(comp_rows))
    return {
        "schemaVersion": SCHEMA_VERSION,
        "dataset": dataset,
        "windows": {
            "baseline": {"from": _iso(baseline[0]), "to": _iso(baseline[1])},
            "compare": {"from": _iso(compare[0]), "to": _iso(compare[1])},
        },
        "excluded": {"credentialTransport": EXCLUDED_TRANSPORT},
        "groupBy": "domain",
        "bounds": {
            "minRequestsBothWindows": MIN_REQUESTS,
            "latencyP95MaxIncreasePct": round(LATENCY_MAX_INCREASE * 100, 4),
            "latencyP50MaxIncreasePct": round(LATENCY_MAX_INCREASE * 100, 4),
            "errorRateMaxIncreasePp": ERROR_RATE_MAX_INCREASE_PP,
            "errorOutcomes": list(ERROR_OUTCOMES),
        },
        **result,
    }


def main(argv: list[str] | None = None, transport: Transport = urllib_transport) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--baseline-start", default=DEFAULT_BASELINE[0], help="ISO 8601 UTC (default %(default)s)")
    parser.add_argument("--baseline-end", default=DEFAULT_BASELINE[1], help="ISO 8601 UTC (default %(default)s)")
    parser.add_argument("--compare-start", default=None, help="ISO 8601 UTC (default: compare end minus 7 days)")
    parser.add_argument("--compare-end", default=None, help="ISO 8601 UTC (default: start of the current UTC hour)")
    parser.add_argument("--dataset", default=DATASET)
    parser.add_argument("--out", default=None, help="write the JSON report here instead of stdout")
    try:
        args = parser.parse_args(argv)
    except SystemExit as exit_:
        return EXIT_USAGE if exit_.code else EXIT_PASS
    try:
        baseline = (parse_utc(args.baseline_start), parse_utc(args.baseline_end))
        compare_end = (parse_utc(args.compare_end) if args.compare_end
                       else datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0))
        compare_start = parse_utc(args.compare_start) if args.compare_start else compare_end - timedelta(days=7)
    except ValueError as error:
        print(f"bad window argument: {error}", file=sys.stderr)
        return EXIT_USAGE
    compare = (compare_start, compare_end)
    for label, (start, end) in (("baseline", baseline), ("compare", compare)):
        if start >= end:
            print(f"the {label} window must start before it ends", file=sys.stderr)
            return EXIT_USAGE

    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip()
    token = os.environ.get("CLOUDFLARE_TELEMETRY_TOKEN", "").strip()
    if not account_id or not token:
        print("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_TELEMETRY_TOKEN must be set.", file=sys.stderr)
        return EXIT_USAGE
    try:
        report = build_report(transport, account_id, token, baseline, compare, args.dataset)
    except ExportError as error:
        print(f"telemetry comparison failed: {error}", file=sys.stderr)
        return EXIT_QUERY_FAILED

    text = json.dumps(report, indent=2) + "\n"
    if args.out:
        Path(args.out).write_text(text, encoding="utf-8")
        print(f"wrote {args.out}: overall {report['overall']} "
              f"({len(report['groupsJudged'])} judged, {len(report['groupsSkippedLowVolume'])} skipped)")
    else:
        sys.stdout.write(text)
    return {"pass": EXIT_PASS, "fail": EXIT_FAIL}.get(report["overall"], EXIT_NOTHING_JUDGEABLE)


if __name__ == "__main__":
    sys.exit(main())
