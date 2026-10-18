#!/usr/bin/env python3
"""Daily export of the Worker's privacy-minimised telemetry (2.11.1, R4 Q10).

Reads the last 24 hours (ending at the start of the current UTC hour, or at
--end) from two Cloudflare APIs and writes aggregates only, never raw rows, to
telemetry/<end date>.json:

1. Workers Analytics Engine SQL API, dataset `yfmcp_telemetry` (one data point
   per /mcp request; layout in worker/src/telemetry.ts): hourly per action the
   request count, outcome counts, weighted p50/p95/p99 latencyMs, mean
   upstreamRequests and subrequestsUsed, and the sums of budgetRefusals,
   providerTimeouts and providerRetries; per action for the day the latency
   percentiles, the sizeBucket histogram and the error-code counts. Every
   count and sum is weighted by `_sample_interval`.
2. Workers Observability telemetry query API (`events` view, paged): invocation
   outcome counts (ok, exceededCpu, exceededResources, exception, ...). Each
   count is joined to the action through the Worker's dispatch-start log line
   {"t":"yfmcp","v":1,"action":...,"domain":...,"build":...}, which is parsed
   client-side, and the outcome comes from the invocation log with the same
   request id. Only action, domain, build and the outcome are read.
3. coverage.warnings (2.14.0): LOGS_ZERO_WITH_REQUESTS when Analytics Engine
   counted requests and Workers Logs, read successfully, counted 0 invocations.
   An ops signal: it is printed and the run still exits 0.

Environment: CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_TELEMETRY_TOKEN, a token with
Account Analytics: Read plus Workers Observability access (the API reference
below lists "Workers Observability Write" as the accepted permission for the
query endpoint). The token is sent only in the Authorization header and is
never printed or written.

API references (checked 2026-10-16):
- Analytics Engine SQL API (endpoint, POST of the SQL text, bearer token,
  Account Analytics: Read):
  https://developers.cloudflare.com/analytics/analytics-engine/sql-api/
- SQL aggregate functions (sumIf, quantileExactWeighted(q)(col, weight)):
  https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/aggregate-functions/
- SQL statements (FORMAT JSON -> {"meta", "data", "rows"}):
  https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/statements/
- Date and time functions (toStartOfInterval, toDateTime(<unix seconds>)):
  https://developers.cloudflare.com/analytics/analytics-engine/sql-reference/date-time-functions/
- Sampling and _sample_interval:
  https://developers.cloudflare.com/analytics/analytics-engine/sampling/
- Workers Observability telemetry query, checked 2026-10-17 (POST
  /accounts/{account_id}/workers/observability/telemetry/query):
  - timeframe is in epoch ms.
  - `dry` "executes the query without persisting the results".
  - view "events" returns result.events.events[], each with $metadata {id,
    requestId, type, message, ...}, `source` (a string or an object) and
    $workers {scriptName, outcome, ...}.
  - `limit` is at most 2000. `offset` is the $metadata.id of the last event.
  https://developers.cloudflare.com/api/resources/workers/subresources/observability/subresources/telemetry/methods/query/
- Workers Logs: logged objects have their fields extracted, while a logged
  string becomes a message. Invocation logs have
  $metadata.type "cf-worker-event". Retention is 3 days on Free, hence the
  daily export.
  https://developers.cloudflare.com/workers/observability/logs/workers-logs/
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

API_BASE = "https://api.cloudflare.com/client/v4"
DATASET = "yfmcp_telemetry"
SCRIPT_NAME = "yahoo-finance-mcp"
SCHEMA_VERSION = 1
OUTCOMES = ("ok", "tool_error", "rpc_error", "rejected", "exception")
SIZE_BUCKETS = ("<1K", "1-10K", "10-100K", "100K-1M", ">=1M")
# What a recorded string may look like; anything else is counted as "invalid".
SAFE_VALUE = re.compile(r"^[A-Za-z0-9_./:<>=+-]{0,96}$")
ACCOUNT_ID = re.compile(r"^[0-9a-f]{32}$")
HTTP_TIMEOUT_S = 60

# (url, headers, body) -> response body; replaced by canned responses in tests.
Transport = Callable[[str, dict[str, str], bytes], bytes]


class ExportError(RuntimeError):
    pass


def _safe(value: Any) -> str:
    text = "" if value is None else str(value)
    return text if SAFE_VALUE.match(text) else "invalid"


def _num(value: Any) -> float:
    # The SQL API returns 64-bit integers as JSON strings.
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if number == number else 0.0  # NaN -> 0


def _clean(number: float) -> float | int:
    rounded = round(number, 3)
    return int(rounded) if rounded == int(rounded) else rounded


def urllib_transport(url: str, headers: dict[str, str], body: bytes) -> bytes:
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        detail = ""
        try:
            payload = json.loads(error.read().decode("utf-8", "replace"))
            messages = [str(e.get("message", "")) for e in payload.get("errors", []) if isinstance(e, dict)]
            detail = "; ".join(m for m in messages if m)[:300]
        except Exception:  # noqa: BLE001 - the body is optional detail only
            pass
        raise ExportError(f"HTTP {error.code} from {url.split('/accounts/')[0]}/accounts/…{(': ' + detail) if detail else ''}") from None
    except urllib.error.URLError as error:
        raise ExportError(f"request failed: {error.reason}") from None


# ── Window ───────────────────────────────────────────────────────────────────

def export_window(end: datetime | None = None) -> tuple[datetime, datetime]:
    """The 24 hours ending at `end` (default: the start of the current UTC hour)."""
    if end is None:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    elif end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    end = end.astimezone(timezone.utc)
    return end - timedelta(hours=24), end


# ── Analytics Engine SQL ─────────────────────────────────────────────────────

def _where(start: datetime, end: datetime) -> str:
    return f"timestamp >= toDateTime({int(start.timestamp())}) AND timestamp < toDateTime({int(end.timestamp())})"


def hourly_sql(dataset: str, start: datetime, end: datetime) -> str:
    outcome_cols = ",\n  ".join(f"sumIf(_sample_interval, blob4 = '{o}') AS outcome_{o}" for o in OUTCOMES)
    return f"""SELECT
  toStartOfInterval(timestamp, INTERVAL '1' HOUR) AS hour,
  index1 AS action,
  blob1 AS domain,
  SUM(_sample_interval) AS requests,
  {outcome_cols},
  quantileExactWeighted(0.5)(double1, _sample_interval) AS latency_p50,
  quantileExactWeighted(0.95)(double1, _sample_interval) AS latency_p95,
  quantileExactWeighted(0.99)(double1, _sample_interval) AS latency_p99,
  SUM(_sample_interval * double4) AS upstream_requests_sum,
  SUM(_sample_interval * double5) AS subrequests_used_sum,
  SUM(_sample_interval * double6) AS budget_refusals,
  SUM(_sample_interval * double7) AS provider_timeouts,
  SUM(_sample_interval * double10) AS provider_retries
FROM {dataset}
WHERE {_where(start, end)}
GROUP BY hour, action, domain
ORDER BY hour, action
FORMAT JSON"""


def daily_latency_sql(dataset: str, start: datetime, end: datetime) -> str:
    return f"""SELECT
  index1 AS action,
  SUM(_sample_interval) AS requests,
  quantileExactWeighted(0.5)(double1, _sample_interval) AS latency_p50,
  quantileExactWeighted(0.95)(double1, _sample_interval) AS latency_p95,
  quantileExactWeighted(0.99)(double1, _sample_interval) AS latency_p99
FROM {dataset}
WHERE {_where(start, end)}
GROUP BY action
ORDER BY action
FORMAT JSON"""


def breakdown_sql(dataset: str, start: datetime, end: datetime) -> str:
    return f"""SELECT
  index1 AS action,
  blob4 AS outcome,
  blob5 AS error_code,
  blob6 AS size_bucket,
  SUM(_sample_interval) AS requests
FROM {dataset}
WHERE {_where(start, end)}
GROUP BY action, outcome, error_code, size_bucket
ORDER BY action
FORMAT JSON"""


def run_sql(transport: Transport, account_id: str, token: str, sql: str) -> list[dict[str, Any]]:
    url = f"{API_BASE}/accounts/{account_id}/analytics_engine/sql"
    raw = transport(url, {"Authorization": f"Bearer {token}", "Content-Type": "text/plain"}, sql.encode("utf-8"))
    try:
        payload = json.loads(raw)
    except ValueError:
        raise ExportError("Analytics Engine SQL API returned a non-JSON body") from None
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise ExportError("Analytics Engine SQL API response has no data array")
    return [row for row in rows if isinstance(row, dict)]


def _empty_action(domain: str) -> dict[str, Any]:
    return {
        "domain": domain,
        "requests": 0.0,
        "outcomes": {o: 0.0 for o in OUTCOMES},
        "latencyMs": None,
        "upstreamRequestsMean": None,
        "subrequestsUsedMean": None,
        "budgetRefusals": 0.0,
        "providerTimeouts": 0.0,
        "providerRetries": 0.0,
        "sizeBuckets": {b: 0.0 for b in SIZE_BUCKETS},
        "errorCodes": {},
        "hourly": [],
        "_upstream": 0.0,
        "_subrequests": 0.0,
    }


def aggregate_analytics(hourly: list[dict], daily: list[dict], breakdown: list[dict]) -> dict[str, Any]:
    actions: dict[str, dict[str, Any]] = {}
    for row in hourly:
        action = _safe(row.get("action"))
        entry = actions.setdefault(action, _empty_action(_safe(row.get("domain"))))
        requests = _num(row.get("requests"))
        upstream = _num(row.get("upstream_requests_sum"))
        subrequests = _num(row.get("subrequests_used_sum"))
        hour = {
            "hour": _safe(str(row.get("hour", "")).replace(" ", "T")),
            "requests": _clean(requests),
            "outcomes": {o: _clean(_num(row.get(f"outcome_{o}"))) for o in OUTCOMES},
            "latencyMs": {q: _clean(_num(row.get(f"latency_{q}"))) for q in ("p50", "p95", "p99")},
            "upstreamRequestsMean": _clean(upstream / requests) if requests else None,
            "subrequestsUsedMean": _clean(subrequests / requests) if requests else None,
            "budgetRefusals": _clean(_num(row.get("budget_refusals"))),
            "providerTimeouts": _clean(_num(row.get("provider_timeouts"))),
            "providerRetries": _clean(_num(row.get("provider_retries"))),
        }
        entry["hourly"].append(hour)
        entry["requests"] += requests
        entry["_upstream"] += upstream
        entry["_subrequests"] += subrequests
        for o in OUTCOMES:
            entry["outcomes"][o] += _num(row.get(f"outcome_{o}"))
        for key, column in (("budgetRefusals", "budget_refusals"), ("providerTimeouts", "provider_timeouts"),
                            ("providerRetries", "provider_retries")):
            entry[key] += _num(row.get(column))

    for row in daily:
        action = _safe(row.get("action"))
        if action in actions:
            actions[action]["latencyMs"] = {q: _clean(_num(row.get(f"latency_{q}"))) for q in ("p50", "p95", "p99")}

    for row in breakdown:
        action = _safe(row.get("action"))
        entry = actions.setdefault(action, _empty_action("-"))
        requests = _num(row.get("requests"))
        bucket = _safe(row.get("size_bucket"))
        entry["sizeBuckets"][bucket] = entry["sizeBuckets"].get(bucket, 0.0) + requests
        code = _safe(row.get("error_code"))
        if code:
            entry["errorCodes"][code] = entry["errorCodes"].get(code, 0.0) + requests

    totals = {
        "requests": 0.0,
        "outcomes": {o: 0.0 for o in OUTCOMES},
        "budgetRefusals": 0.0,
        "providerTimeouts": 0.0,
        "providerRetries": 0.0,
        "sizeBuckets": {b: 0.0 for b in SIZE_BUCKETS},
        "errorCodes": {},
    }
    for entry in actions.values():
        requests = entry["requests"]
        upstream, subrequests = entry.pop("_upstream"), entry.pop("_subrequests")
        entry["upstreamRequestsMean"] = _clean(upstream / requests) if requests else None
        entry["subrequestsUsedMean"] = _clean(subrequests / requests) if requests else None
        totals["requests"] += requests
        for o in OUTCOMES:
            totals["outcomes"][o] += entry["outcomes"][o]
        for key in ("budgetRefusals", "providerTimeouts", "providerRetries"):
            totals[key] += entry[key]
        for bucket, n in entry["sizeBuckets"].items():
            totals["sizeBuckets"][bucket] = totals["sizeBuckets"].get(bucket, 0.0) + n
        for code, n in entry["errorCodes"].items():
            totals["errorCodes"][code] = totals["errorCodes"].get(code, 0.0) + n
        entry["hourly"].sort(key=lambda h: h["hour"])

    return {"totals": _round_tree(totals), "actions": _round_tree(dict(sorted(actions.items())))}


def _round_tree(value: Any) -> Any:
    if isinstance(value, float):
        return _clean(value)
    if isinstance(value, dict):
        return {k: _round_tree(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_round_tree(v) for v in value]
    return value


# ── Workers Observability ────────────────────────────────────────────────────

#
# Why the `events` view and not a `calculations` count grouped by `action`
# (2.13.1). The 2.11.1 query returned `invocations: 0` on the first live export,
# while Analytics Engine counted 68 requests in the same window:
#
# - The Worker logged a JSON *string*: console.log(JSON.stringify({t: "yfmcp", ...})).
#   (2.14.0 logs the object itself; both forms are still read below.)
#   Workers Logs documents field extraction only for logged *objects*
#   (`console.log({user_id: 123})` gives `{user_id: 123}`, filterable by key). It
#   shows a logged string as `{message: "..."}`, and the API says an event's
#   `source` "may be a string or a structured object depending on how the log
#   was emitted". A filter on `t`, or a group-by on `action`, has no field to match.
# - The outcome is on the invocation log (`$metadata.type` "cf-worker-event", whose
#   `$workers` variant has `outcome: string`). It is not on the console-log event,
#   whose `$workers` variant has `outcome: optional string`.
#
# So the script lists the Worker's events, filtered by `$workers.scriptName`
# (both `$workers` variants carry it). It reads the dispatch line from each
# event's `source` (an object or a JSON string) or from `$metadata.message`, and
# takes the outcome from the invocation log with the same `$metadata.requestId`.
# This works whether or not Cloudflare extracts fields from a JSON string.

LOG_TAG = "yfmcp"
LOG_FIELDS = ("action", "domain", "build")
INVOCATION_EVENT_TYPE = "cf-worker-event"
EVENTS_PAGE_LIMIT = 2000  # the API maximum for the events view
# Allows 100,000 requests/day (the Free cap) at about 2 events each, with
# headroom. Past this the export fails loudly instead of reporting a partial count.
MAX_EVENT_PAGES = 200


def observability_query(script_name: str, start: datetime, end: datetime, offset: str | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "queryId": "yfmcp-telemetry-export",
        "timeframe": {"from": int(start.timestamp() * 1000), "to": int(end.timestamp() * 1000)},
        "view": "events",
        "dry": True,
        "limit": EVENTS_PAGE_LIMIT,
        "parameters": {
            "datasets": [],
            "filterCombination": "and",
            "filters": [
                {"key": "$workers.scriptName", "operation": "eq", "type": "string", "value": script_name},
            ],
        },
    }
    if offset is not None:
        body["offset"] = offset
        body["offsetDirection"] = "next"
    return body


def run_observability(transport: Transport, account_id: str, token: str, body: dict[str, Any]) -> dict[str, Any]:
    url = f"{API_BASE}/accounts/{account_id}/workers/observability/telemetry/query"
    raw = transport(url, {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json.dumps(body).encode("utf-8"))
    try:
        payload = json.loads(raw)
    except ValueError:
        raise ExportError("Workers Observability API returned a non-JSON body") from None
    if not isinstance(payload, dict) or payload.get("success") is False:
        raise ExportError("Workers Observability API reported an unsuccessful query")
    result = payload.get("result")
    return result if isinstance(result, dict) else {}


def fetch_events(transport: Transport, account_id: str, token: str, script_name: str,
                 start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Every event of the Worker in the window, following the `$metadata.id` cursor."""
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    offset: str | None = None
    for _ in range(MAX_EVENT_PAGES):
        result = run_observability(transport, account_id, token,
                                   observability_query(script_name, start, end, offset))
        block = result.get("events")
        if not isinstance(block, dict):
            raise ExportError("Workers Observability API response has no events object")
        page = [e for e in block.get("events") or [] if isinstance(e, dict)]
        fresh = 0
        for event in page:
            event_id = _event_metadata(event).get("id")
            key = event_id if isinstance(event_id, str) and event_id else f"#{len(events)}"
            if key in seen:
                continue
            seen.add(key)
            events.append(event)
            fresh += 1
        last_id = _event_metadata(page[-1]).get("id") if page else None
        if len(page) < EVENTS_PAGE_LIMIT or not fresh or not isinstance(last_id, str) or not last_id:
            return events
        offset = last_id
    raise ExportError(f"more than {MAX_EVENT_PAGES * EVENTS_PAGE_LIMIT} Workers Logs events in the window")


def _event_metadata(event: dict[str, Any]) -> dict[str, Any]:
    metadata = event.get("$metadata")
    return metadata if isinstance(metadata, dict) else {}


def _event_workers(event: dict[str, Any]) -> dict[str, Any]:
    workers = event.get("$workers")
    return workers if isinstance(workers, dict) else {}


def _as_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.lstrip().startswith("{"):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def dispatch_line(event: dict[str, Any]) -> dict[str, str] | None:
    """The action, domain and build of the Worker's dispatch log line, or None for any other event.

    The line can arrive as a structured `source`, as a JSON-string `source`, as
    `$metadata.message`, or with its fields extracted to the top level of the
    event. Only those three fields are read.
    """
    for candidate in (event.get("source"), _event_metadata(event).get("message"), event):
        line = _as_object(candidate)
        if line is not None and line.get("t") == LOG_TAG:
            return {field: _safe(line.get(field)) for field in LOG_FIELDS}
    return None


def _request_id(event: dict[str, Any]) -> str | None:
    for value in (_event_metadata(event).get("requestId"), _event_workers(event).get("requestId")):
        if isinstance(value, str) and value:
            return value
    return None


def aggregate_invocations(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Count each dispatch log line once, under the outcome of its invocation.

    The outcome comes from the invocation log (`cf-worker-event`) with the same
    request id. Failing that, it comes from the log event's own `$workers.outcome`,
    and otherwise it is "unknown".
    """
    outcome_by_request: dict[str, str] = {}
    lines: list[tuple[dict[str, str], str | None, Any]] = []
    for event in events:
        request_id = _request_id(event)
        line = dispatch_line(event)
        if line is not None:
            lines.append((line, request_id, _event_workers(event).get("outcome")))
        elif _event_metadata(event).get("type") == INVOCATION_EVENT_TYPE and request_id:
            outcome = _event_workers(event).get("outcome")
            if isinstance(outcome, str) and outcome:
                outcome_by_request[request_id] = outcome

    outcomes: dict[str, float] = {}
    actions: dict[str, dict[str, Any]] = {}
    builds: dict[str, float] = {}
    for line, request_id, own_outcome in lines:
        raw = outcome_by_request.get(request_id) if request_id else None
        if raw is None and isinstance(own_outcome, str) and own_outcome:
            raw = own_outcome
        outcome = _safe(raw) or "unknown"
        entry = actions.setdefault(line["action"], {"domain": line["domain"], "invocations": 0.0, "outcomes": {}})
        entry["invocations"] += 1
        entry["outcomes"][outcome] = entry["outcomes"].get(outcome, 0.0) + 1
        outcomes[outcome] = outcomes.get(outcome, 0.0) + 1
        builds[line["build"]] = builds.get(line["build"], 0.0) + 1
    return _round_tree({
        "invocations": sum(outcomes.values()),
        "outcomes": dict(sorted(outcomes.items())),
        "builds": dict(sorted(builds.items())),
        "actions": dict(sorted(actions.items())),
    })


# ── Coverage ─────────────────────────────────────────────────────────────────

LOGS_ZERO_WITH_REQUESTS = "LOGS_ZERO_WITH_REQUESTS"


def coverage_warnings(analytics: dict[str, Any], logs: dict[str, Any]) -> list[dict[str, str]]:
    """Ops signals about how far the two sources agree (2.14.0, OPS-TEL-2). They never fail the export.

    LOGS_ZERO_WITH_REQUESTS: Analytics Engine counted requests but Workers Logs, read successfully,
    counted no invocations (the 2.13.0 export read 0 this way).
    """
    warnings: list[dict[str, str]] = []
    requests = _num(analytics.get("totals", {}).get("requests"))
    if logs.get("available") and requests > 0 and _num(logs.get("invocations")) == 0:
        warnings.append({
            "code": LOGS_ZERO_WITH_REQUESTS,
            "message": f"Analytics Engine counted {_clean(requests)} requests but Workers Logs counted 0 invocations.",
        })
    return warnings


# ── Export ───────────────────────────────────────────────────────────────────

def build_export(
    transport: Transport,
    account_id: str,
    token: str,
    end: datetime | None = None,
    dataset: str = DATASET,
    script_name: str = SCRIPT_NAME,
) -> tuple[dict[str, Any], list[str]]:
    """The export document and the list of problems (empty when both sources were read)."""
    if not ACCOUNT_ID.match(account_id):
        raise ExportError("CLOUDFLARE_ACCOUNT_ID is not a 32-character hex account id")
    if not re.match(r"^[A-Za-z0-9_]{1,64}$", dataset):
        raise ExportError("dataset name must be [A-Za-z0-9_]")
    start, stop = export_window(end)
    hourly = run_sql(transport, account_id, token, hourly_sql(dataset, start, stop))
    daily = run_sql(transport, account_id, token, daily_latency_sql(dataset, start, stop))
    breakdown = run_sql(transport, account_id, token, breakdown_sql(dataset, start, stop))
    problems: list[str] = []
    try:
        logs: dict[str, Any] = {"available": True, **aggregate_invocations(
            fetch_events(transport, account_id, token, script_name, start, stop))}
    except ExportError as error:
        problems.append(f"workersLogs: {error}")
        logs = {"available": False, "error": str(error)}
    analytics = aggregate_analytics(hourly, daily, breakdown)
    document = {
        "schemaVersion": SCHEMA_VERSION,
        "date": stop.date().isoformat(),
        "window": {"from": start.isoformat().replace("+00:00", "Z"), "to": stop.isoformat().replace("+00:00", "Z")},
        "dataset": dataset,
        "scriptName": script_name,
        "analyticsEngine": analytics,
        "workersLogs": logs,
        "coverage": {"warnings": coverage_warnings(analytics, logs)},
    }
    return document, problems


def write_export(document: dict[str, Any], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{document['date']}.json"
    path.write_text(json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return path


def _parse_end(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def main(argv: list[str] | None = None, transport: Transport = urllib_transport) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out-dir", default="telemetry", help="directory for <date>.json (default: telemetry)")
    parser.add_argument("--end", type=_parse_end, default=None,
                        help="window end, ISO 8601 UTC (default: start of the current hour)")
    parser.add_argument("--dataset", default=DATASET)
    parser.add_argument("--script-name", default=SCRIPT_NAME)
    args = parser.parse_args(argv)

    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip()
    token = os.environ.get("CLOUDFLARE_TELEMETRY_TOKEN", "").strip()
    if not account_id or not token:
        print("CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_TELEMETRY_TOKEN must be set.", file=sys.stderr)
        return 2
    try:
        document, problems = build_export(transport, account_id, token, args.end, args.dataset, args.script_name)
    except ExportError as error:
        print(f"telemetry export failed: {error}", file=sys.stderr)
        return 1
    path = write_export(document, Path(args.out_dir))
    totals = document["analyticsEngine"]["totals"]
    print(f"wrote {path} ({totals['requests']} requests, "
          f"{document['workersLogs'].get('invocations', 'n/a')} logged invocations)")
    # Coverage warnings are reported, never a failure: the export itself succeeded.
    for warning in document["coverage"]["warnings"]:
        print(f"telemetry export coverage warning: {warning['code']}: {warning['message']}")
    for problem in problems:
        print(f"telemetry export incomplete: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
