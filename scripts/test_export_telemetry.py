#!/usr/bin/env python3
"""scripts/export_telemetry.py over canned Cloudflare API responses (2.11.1).

No network: a fake transport records each request and answers from canned
Analytics Engine SQL and Workers Observability payloads.

- Requests: the SQL API and telemetry query endpoints, bearer auth, the
  24-hour window, `_sample_interval` weighting, the dataset and log filters.
- Aggregation: per-action and total counts, outcome counts, weighted means,
  sums, the size-bucket histogram, error codes, hourly rows and day percentiles.
  Invocation outcomes are joined to the action from the dispatch log line
  (2.13.1: the `events` view in its documented shape, where the line is a JSON
  string in `source` or `$metadata.message` and the outcome comes from the
  `cf-worker-event` with the same request id), and pages follow the cursor.
- Output: telemetry/<date>.json with aggregates only, no raw rows, no token;
  unsafe strings are counted as "invalid".
- Failure modes: missing secrets, an SQL failure, a Workers Logs failure.
- excludingHeaderTransport (Phase 4 prep, F6): the same Analytics Engine
  aggregates from the three SQL queries re-run with `blob7 != 'header'`; every
  pre-existing key is unchanged.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import export_telemetry as et  # noqa: E402

ACCOUNT = "0123456789abcdef0123456789abcdef"
TOKEN = "cf-telemetry-token-SECRET-1b2c3d"
END = datetime(2026, 10, 16, 2, 0, tzinfo=timezone.utc)
START_S = int(datetime(2026, 10, 15, 2, 0, tzinfo=timezone.utc).timestamp())
END_S = int(END.timestamp())

PRICE = "tools/call:stock_pricing.get_historical_prices"

HOURLY = {
    "meta": [{"name": "hour", "type": "DateTime"}],
    "data": [
        {"hour": "2026-10-15 03:00:00", "action": PRICE, "domain": "stock_pricing", "requests": "10",
         "outcome_ok": "8", "outcome_tool_error": "2", "outcome_rpc_error": "0", "outcome_rejected": "0",
         "outcome_exception": "0", "latency_p50": 120, "latency_p95": 900, "latency_p99": 1500.5,
         "upstream_requests_sum": "20", "subrequests_used_sum": "30", "budget_refusals": "0",
         "provider_timeouts": "1", "provider_retries": "2"},
        {"hour": "2026-10-15 04:00:00", "action": PRICE, "domain": "stock_pricing", "requests": "30",
         "outcome_ok": "30", "outcome_tool_error": "0", "outcome_rpc_error": "0", "outcome_rejected": "0",
         "outcome_exception": "0", "latency_p50": 100, "latency_p95": 400, "latency_p99": 800,
         "upstream_requests_sum": "20", "subrequests_used_sum": "50", "budget_refusals": "3",
         "provider_timeouts": "0", "provider_retries": "0"},
        {"hour": "2026-10-15 03:00:00", "action": "rejected:auth", "domain": "-", "requests": "5",
         "outcome_ok": "0", "outcome_tool_error": "0", "outcome_rpc_error": "0", "outcome_rejected": "5",
         "outcome_exception": "0", "latency_p50": 0, "latency_p95": 1, "latency_p99": 1,
         "upstream_requests_sum": "0", "subrequests_used_sum": "0", "budget_refusals": "0",
         "provider_timeouts": "0", "provider_retries": "0"},
        # A string that is not a value the Worker writes is counted, never copied.
        {"hour": "2026-10-15 05:00:00", "action": "AAPL secret ticker'; DROP", "domain": "x y", "requests": "1",
         "outcome_ok": "1", "outcome_tool_error": "0", "outcome_rpc_error": "0", "outcome_rejected": "0",
         "outcome_exception": "0", "latency_p50": 1, "latency_p95": 1, "latency_p99": 1,
         "upstream_requests_sum": "0", "subrequests_used_sum": "0", "budget_refusals": "0",
         "provider_timeouts": "0", "provider_retries": "0"},
    ],
    "rows": 4,
}
DAILY = {"meta": [], "data": [
    {"action": PRICE, "requests": "40", "latency_p50": 105, "latency_p95": 700, "latency_p99": 1400},
    {"action": "rejected:auth", "requests": "5", "latency_p50": 0, "latency_p95": 1, "latency_p99": 1},
], "rows": 2}
BREAKDOWN = {"meta": [], "data": [
    {"action": PRICE, "outcome": "ok", "error_code": "", "size_bucket": "1-10K", "requests": "38"},
    {"action": PRICE, "outcome": "tool_error", "error_code": "PROVIDER_TIMEOUT", "size_bucket": "<1K", "requests": "1"},
    {"action": PRICE, "outcome": "tool_error", "error_code": "RATE_LIMIT", "size_bucket": "<1K", "requests": "1"},
    {"action": "rejected:auth", "outcome": "rejected", "error_code": "AUTH_REQUIRED", "size_bucket": "<1K", "requests": "5"},
], "rows": 4}
# Per keyId (2.18.0, blob12). The empty key id is a pre-2.18.0 row.
KEYS = {"meta": [], "data": [
    {"key_id": "owner", "outcome": "ok", "requests": "30"},
    {"key_id": "owner", "outcome": "tool_error", "requests": "2"},
    {"key_id": "e773fa26", "outcome": "ok", "requests": "8"},
    {"key_id": "e773fa26", "outcome": "rejected", "requests": "1"},
    {"key_id": "-", "outcome": "rejected", "requests": "4"},
    {"key_id": "", "outcome": "ok", "requests": "1"},
    {"key_id": "yfk_leaked-label", "outcome": "ok", "requests": "1"},
], "rows": 7}
# Workers Observability `events` view, in the documented response shape
# (result.events.events[] of {"$metadata", dataset, source, timestamp, "$workers"}).
# The Worker logs a JSON string, so `source` is a string. The outcome is on the
# invocation log ($metadata.type "cf-worker-event"), joined by $metadata.requestId.
TS = START_S * 1000 + 3_600_000


def log_event(n: int, request_id: str, line: object, *, as_message: bool = False, outcome: str | None = None) -> dict:
    """A console.log event. `line` is the logged value (a JSON string or a dict)."""
    workers = {"eventType": "fetch", "scriptName": "yahoo-finance-mcp", "requestId": request_id}
    if outcome is not None:
        workers["outcome"] = outcome
    event = {
        "$metadata": {"id": f"log-{n:05d}", "requestId": request_id, "type": "cf-worker-log", "level": "log",
                      "service": "yahoo-finance-mcp"},
        "dataset": "cloudflare-workers",
        "timestamp": TS + n,
        "$workers": workers,
    }
    if as_message:
        event["$metadata"]["message"] = line
    else:
        event["source"] = line
    return event


def invocation_event(n: int, request_id: str, outcome: str, url: str = "https://x/mcp/LEAKED") -> dict:
    return {
        "$metadata": {"id": f"inv-{n:05d}", "requestId": request_id, "type": "cf-worker-event",
                      "message": f"POST {url}", "service": "yahoo-finance-mcp", "url": url,
                      "trigger": "POST /mcp/LEAKED", "statusCode": 200},
        "dataset": "cloudflare-workers",
        "source": {"message": f"POST {url}"},
        "timestamp": TS + n,
        "$workers": {"eventType": "fetch", "scriptName": "yahoo-finance-mcp", "outcome": outcome,
                     "cpuTimeMs": 3, "wallTimeMs": 40, "requestId": request_id,
                     "event": {"request": {"url": url, "method": "POST"}}},
    }


def dispatch(action: str, domain: str, build: str = "0123456") -> str:
    return json.dumps({"t": "yfmcp", "v": 1, "action": action, "domain": domain, "build": build})


def _events() -> list[dict]:
    events: list[dict] = []
    for i in range(40):  # 39 ok, 1 killed for CPU
        rid = f"req-price-{i}"
        events.append(log_event(len(events), rid, dispatch(PRICE, "stock_pricing")))
        events.append(invocation_event(len(events), rid, "exceededCpu" if i == 0 else "ok"))
    # Two tools/list calls: one whose line arrives as a structured `source`, and one
    # whose line is only in $metadata.message (both shapes are documented).
    events.append(log_event(len(events), "req-list-0", json.loads(dispatch("tools/list", "-"))))
    events.append(invocation_event(len(events), "req-list-0", "ok"))
    events.append(log_event(len(events), "req-list-1", dispatch("tools/list", "-"), as_message=True))
    events.append(invocation_event(len(events), "req-list-1", "ok"))
    # Not a dispatch line: another console.log, and a GET / with no /mcp dispatch.
    events.append(log_event(len(events), "req-list-1", "AAPL some other message"))
    events.append(log_event(len(events), "req-other", json.dumps({"t": "other", "action": "x"})))
    events.append(invocation_event(len(events), "req-other", "ok", url="https://x/"))
    return events


def events_payload(events: list[dict]) -> dict:
    return {
        "success": True,
        "errors": [],
        "messages": [{"message": "Successful request"}],
        "result": {
            "run": {"id": "run-1", "accountId": ACCOUNT, "dry": True, "granularity": 0},
            "statistics": {"bytes_read": 1, "elapsed": 1, "rows_read": 1},
            "events": {"count": len(events), "events": events, "fields": [], "series": []},
        },
    }


OBSERVABILITY = events_payload(_events())

# What the three SQL queries return with `blob7 != 'header'` (F6): 6 of the 40
# price requests and every rejected request came from header-transport CI canaries.
HEADER_FILTER = "blob7 != 'header'"
HOURLY_NO_HEADER = {"meta": [], "data": [
    {"hour": "2026-10-15 04:00:00", "action": PRICE, "domain": "stock_pricing", "requests": "34",
     "outcome_ok": "33", "outcome_tool_error": "1", "outcome_rpc_error": "0", "outcome_rejected": "0",
     "outcome_exception": "0", "latency_p50": 90, "latency_p95": 350, "latency_p99": 700,
     "upstream_requests_sum": "34", "subrequests_used_sum": "68", "budget_refusals": "0",
     "provider_timeouts": "0", "provider_retries": "1"},
], "rows": 1}
DAILY_NO_HEADER = {"meta": [], "data": [
    {"action": PRICE, "requests": "34", "latency_p50": 90, "latency_p95": 350, "latency_p99": 700},
], "rows": 1}
BREAKDOWN_NO_HEADER = {"meta": [], "data": [
    {"action": PRICE, "outcome": "ok", "error_code": "", "size_bucket": "1-10K", "requests": "33"},
    {"action": PRICE, "outcome": "tool_error", "error_code": "RATE_LIMIT", "size_bucket": "<1K", "requests": "1"},
], "rows": 2}


class FakeTransport:
    def __init__(self, observability: object = OBSERVABILITY, fail_sql: bool = False, fail_logs: bool = False):
        self.calls: list[tuple[str, dict, bytes]] = []
        # A payload, or a callable from the request body to a payload (for paging).
        self.observability = observability
        self.fail_sql = fail_sql
        self.fail_logs = fail_logs

    def __call__(self, url: str, headers: dict, body: bytes) -> bytes:
        self.calls.append((url, headers, body))
        if url.endswith("/analytics_engine/sql"):
            if self.fail_sql:
                raise et.ExportError("HTTP 403 from https://api.cloudflare.com/client/v4/accounts/…")
            sql = body.decode()
            if HEADER_FILTER in sql:
                if "GROUP BY hour, action, domain" in sql:
                    return json.dumps(HOURLY_NO_HEADER).encode()
                if "GROUP BY action, outcome, error_code, size_bucket" in sql:
                    return json.dumps(BREAKDOWN_NO_HEADER).encode()
                if "GROUP BY action\n" in sql:
                    return json.dumps(DAILY_NO_HEADER).encode()
                raise AssertionError("unexpected filtered SQL")
            if "GROUP BY hour, action, domain" in sql:
                return json.dumps(HOURLY).encode()
            if "GROUP BY action, outcome, error_code, size_bucket" in sql:
                return json.dumps(BREAKDOWN).encode()
            if "GROUP BY key_id, outcome" in sql:
                return json.dumps(KEYS).encode()
            return json.dumps(DAILY).encode()
        if url.endswith("/workers/observability/telemetry/query"):
            if self.fail_logs:
                raise et.ExportError("HTTP 403 from https://api.cloudflare.com/client/v4/accounts/…")
            payload = self.observability(json.loads(body)) if callable(self.observability) else self.observability
            return json.dumps(payload).encode()
        raise AssertionError(f"unexpected URL {url}")


def _export(transport: FakeTransport | None = None) -> tuple[dict, list[str], FakeTransport]:
    transport = transport or FakeTransport()
    document, problems = et.build_export(transport, ACCOUNT, TOKEN, END)
    return document, problems, transport


class TestRequests(unittest.TestCase):
    def test_endpoints_auth_and_bodies(self):
        _, _, t = _export()
        urls = [c[0] for c in t.calls]
        sql_url = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/analytics_engine/sql"
        logs_url = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/workers/observability/telemetry/query"
        # Four SQL queries, Workers Logs, then the three header-excluded SQL queries (F6).
        self.assertEqual(urls, [sql_url, sql_url, sql_url, sql_url, logs_url, sql_url, sql_url, sql_url])
        for _, headers, _ in t.calls:
            self.assertEqual(headers["Authorization"], f"Bearer {TOKEN}")

    def test_sql_window_dataset_and_sample_weighting(self):
        _, _, t = _export()
        for _, _, body in t.calls[:4]:
            sql = body.decode()
            self.assertIn("FROM yfmcp_telemetry", sql)
            self.assertIn(f"timestamp >= toDateTime({START_S}) AND timestamp < toDateTime({END_S})", sql)
            self.assertIn("SUM(_sample_interval)", sql)
            self.assertIn("FORMAT JSON", sql)
            self.assertNotIn("count()", sql)  # counts are sample-weighted sums
            self.assertNotRegex(sql, r"\bblob(1[3-9]|20)\b")
        hourly = t.calls[0][2].decode()
        self.assertIn("toStartOfInterval(timestamp, INTERVAL '1' HOUR) AS hour", hourly)
        self.assertIn("quantileExactWeighted(0.95)(double1, _sample_interval)", hourly)
        self.assertIn("sumIf(_sample_interval, blob4 = 'tool_error')", hourly)
        for column in ("double4", "double5", "double6", "double7", "double10"):
            self.assertIn(f"SUM(_sample_interval * {column})", hourly)

    def test_observability_query_shape(self):
        _, _, t = _export()
        body = json.loads(t.calls[4][2])
        self.assertEqual(body["view"], "events")
        self.assertEqual(body["timeframe"], {"from": START_S * 1000, "to": END_S * 1000})  # epoch ms
        self.assertTrue(body["dry"])  # documented: executes, does not persist the results
        self.assertEqual(body["limit"], 2000)
        self.assertNotIn("offset", body)
        params = body["parameters"]
        # Only the script filter. The dispatch line is a JSON string, so `t`, `action`
        # and the rest are not filterable fields; they are parsed client-side.
        self.assertEqual(params["filters"], [
            {"key": "$workers.scriptName", "operation": "eq", "type": "string", "value": "yahoo-finance-mcp"}])
        self.assertNotIn("groupBys", params)
        self.assertNotIn("calculations", params)

    def test_events_are_paged_with_the_last_metadata_id(self):
        full: list[dict] = []
        while len(full) < et.EVENTS_PAGE_LIMIT:  # a full first page: 1000 requests
            rid = f"page1-{len(full)}"
            full.append(log_event(len(full), rid, dispatch(PRICE, "stock_pricing")))
            full.append(invocation_event(len(full), rid, "ok"))
        tail = [log_event(90_000, "req-tail", dispatch(PRICE, "stock_pricing")),
                invocation_event(90_001, "req-tail", "exceededResources")]
        bodies: list[dict] = []

        def pages(body: dict) -> dict:
            bodies.append(body)
            return events_payload(full if "offset" not in body else tail)

        doc, problems, t = _export(FakeTransport(observability=pages))
        self.assertEqual(problems, [])
        self.assertEqual(len(bodies), 2)
        self.assertEqual(bodies[1]["offset"], full[-1]["$metadata"]["id"])
        self.assertEqual(bodies[1]["offsetDirection"], "next")
        self.assertEqual(doc["workersLogs"]["outcomes"], {"exceededResources": 1, "ok": 1000})
        self.assertEqual(doc["workersLogs"]["invocations"], 1001)

    def test_runaway_paging_fails_loudly(self):
        def endless(body: dict) -> dict:
            n = int(body.get("offset", "e-0").split("-")[1])
            return events_payload([log_event(0, f"r{n + i}", "x") | {"$metadata": {"id": f"e-{n + i + 1}"}}
                                   for i in range(et.EVENTS_PAGE_LIMIT)])

        with mock.patch.object(et, "MAX_EVENT_PAGES", 3):
            doc, problems, t = _export(FakeTransport(observability=endless))
        self.assertFalse(doc["workersLogs"]["available"])
        self.assertEqual(len(t.calls), 4 + 3 + 3)
        self.assertTrue(problems and "more than 6000" in problems[0])


class TestAggregation(unittest.TestCase):
    def test_header_fields(self):
        doc, problems, _ = _export()
        self.assertEqual(problems, [])
        self.assertEqual(doc["schemaVersion"], 1)
        self.assertEqual(doc["date"], "2026-10-16")
        self.assertEqual(doc["window"], {"from": "2026-10-15T02:00:00Z", "to": "2026-10-16T02:00:00Z"})
        self.assertEqual(doc["dataset"], "yfmcp_telemetry")

    def test_per_action_aggregates(self):
        doc, _, _ = _export()
        price = doc["analyticsEngine"]["actions"][PRICE]
        self.assertEqual(price["domain"], "stock_pricing")
        self.assertEqual(price["requests"], 40)
        self.assertEqual(price["outcomes"], {"ok": 38, "tool_error": 2, "rpc_error": 0, "rejected": 0, "exception": 0})
        self.assertEqual(price["upstreamRequestsMean"], 1)       # (20 + 20) / 40
        self.assertEqual(price["subrequestsUsedMean"], 2)        # (30 + 50) / 40
        self.assertEqual((price["budgetRefusals"], price["providerTimeouts"], price["providerRetries"]), (3, 1, 2))
        self.assertEqual(price["latencyMs"], {"p50": 105, "p95": 700, "p99": 1400})
        self.assertEqual(price["sizeBuckets"], {"<1K": 2, "1-10K": 38, "10-100K": 0, "100K-1M": 0, ">=1M": 0})
        self.assertEqual(price["errorCodes"], {"PROVIDER_TIMEOUT": 1, "RATE_LIMIT": 1})
        self.assertEqual([h["hour"] for h in price["hourly"]], ["2026-10-15T03:00:00", "2026-10-15T04:00:00"])
        first = price["hourly"][0]
        self.assertEqual(first["latencyMs"], {"p50": 120, "p95": 900, "p99": 1500.5})
        self.assertEqual((first["upstreamRequestsMean"], first["subrequestsUsedMean"]), (2, 3))

    def test_totals(self):
        doc, _, _ = _export()
        totals = doc["analyticsEngine"]["totals"]
        self.assertEqual(totals["requests"], 46)
        self.assertEqual(totals["outcomes"], {"ok": 39, "tool_error": 2, "rpc_error": 0, "rejected": 5, "exception": 0})
        self.assertEqual(totals["errorCodes"], {"PROVIDER_TIMEOUT": 1, "RATE_LIMIT": 1, "AUTH_REQUIRED": 5})
        self.assertEqual(totals["sizeBuckets"]["<1K"], 7)
        self.assertEqual((totals["budgetRefusals"], totals["providerTimeouts"], totals["providerRetries"]), (3, 1, 2))

    def test_unsafe_strings_are_counted_not_copied(self):
        doc, _, _ = _export()
        actions = doc["analyticsEngine"]["actions"]
        self.assertIn("invalid", actions)
        self.assertEqual(actions["invalid"]["domain"], "invalid")
        self.assertNotIn("AAPL", json.dumps(doc))

    def test_requests_by_key_id(self):
        doc, _, t = _export()
        keys_sql = t.calls[3][2].decode()
        self.assertIn("blob12 AS key_id", keys_sql)
        self.assertIn("GROUP BY key_id, outcome", keys_sql)
        keys = doc["analyticsEngine"]["keys"]
        self.assertEqual(sorted(keys), ["-", "e773fa26", "invalid", "owner"])
        self.assertEqual(keys["owner"], {"requests": 32, "outcomes": {
            "ok": 30, "tool_error": 2, "rpc_error": 0, "rejected": 0, "exception": 0}})
        self.assertEqual(keys["e773fa26"]["requests"], 9)
        self.assertEqual(keys["e773fa26"]["outcomes"]["rejected"], 1)
        self.assertEqual(keys["-"]["requests"], 5)  # 4 unidentified + 1 pre-2.18.0 row
        self.assertEqual(keys["invalid"]["requests"], 1)
        self.assertNotIn("yfk_", json.dumps(doc))

    def test_invocations_joined_to_action(self):
        doc, _, _ = _export()
        logs = doc["workersLogs"]
        self.assertTrue(logs["available"])
        self.assertEqual(logs["invocations"], 42)
        self.assertEqual(logs["outcomes"], {"exceededCpu": 1, "ok": 41})
        self.assertEqual(logs["builds"], {"0123456": 42})
        self.assertEqual(logs["actions"][PRICE], {"domain": "stock_pricing", "invocations": 40,
                                                  "outcomes": {"ok": 39, "exceededCpu": 1}})
        self.assertEqual(logs["actions"]["tools/list"], {"domain": "-", "invocations": 2, "outcomes": {"ok": 2}})
        self.assertEqual(sorted(logs["actions"]), [PRICE, "tools/list"])  # other logs and GET / are not counted
        text = json.dumps(doc)
        for leaked in ("LEAKED", "AAPL", "req-", "log-0", "inv-0"):  # only action, domain, build, outcome
            self.assertNotIn(leaked, text)

    def _logs(self, events: list[dict]) -> dict:
        doc, problems, _ = _export(FakeTransport(observability=events_payload(events)))
        self.assertEqual(problems, [])
        return doc["workersLogs"]

    def test_outcome_falls_back_to_the_log_event_then_unknown(self):
        logs = self._logs([
            log_event(0, "r-own", dispatch(PRICE, "stock_pricing"), outcome="exception"),
            log_event(1, "r-none", dispatch(PRICE, "stock_pricing")),
            log_event(2, "", dispatch(PRICE, "stock_pricing")),  # no request id at all
        ])
        self.assertEqual(logs["outcomes"], {"exception": 1, "unknown": 2})
        self.assertEqual(logs["invocations"], 3)

    def test_invocation_log_outcome_wins_over_the_log_event(self):
        logs = self._logs([
            log_event(0, "r1", dispatch(PRICE, "stock_pricing"), outcome="ok"),
            invocation_event(1, "r1", "exceededCpu"),
        ])
        self.assertEqual(logs["outcomes"], {"exceededCpu": 1})

    def test_extracted_fields_at_top_level_are_read_too(self):
        # If Cloudflare extracts the JSON string's fields onto the event, still count it.
        event = invocation_event(0, "r1", "ok")
        extracted = {"$metadata": {"id": "x1", "requestId": "r1", "type": "cf-worker-log"},
                     "$workers": {"scriptName": "yahoo-finance-mcp"},
                     "t": "yfmcp", "v": 1, "action": "tools/list", "domain": "-", "build": "abc1234"}
        logs = self._logs([extracted, event])
        self.assertEqual(logs["actions"], {"tools/list": {"domain": "-", "invocations": 1, "outcomes": {"ok": 1}}})
        self.assertEqual(logs["builds"], {"abc1234": 1})

    def test_unsafe_log_values_are_counted_not_copied(self):
        logs = self._logs([log_event(0, "r1", json.dumps({"t": "yfmcp", "action": "AAPL secret'; x",
                                                           "domain": "d", "build": "b"})),
                           invocation_event(1, "r1", "weird outcome!")])
        self.assertEqual(logs["actions"], {"invalid": {"domain": "d", "invocations": 1, "outcomes": {"invalid": 1}}})

    def test_response_without_events_object_is_a_logs_failure(self):
        doc, problems, _ = _export(FakeTransport(observability={"success": True, "result": {"calculations": []}}))
        self.assertFalse(doc["workersLogs"]["available"])
        self.assertIn("no events object", problems[0])


class TestOutput(unittest.TestCase):
    def test_main_writes_one_aggregate_file_without_the_token(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_TELEMETRY_TOKEN": TOKEN}):
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                code = et.main(["--out-dir", tmp, "--end", "2026-10-16T02:00:00Z"], transport=FakeTransport())
            self.assertEqual(code, 0, err.getvalue())
            files = sorted(Path(tmp).iterdir())
            self.assertEqual([f.name for f in files], ["2026-10-16.json"])
            text = files[0].read_text(encoding="utf-8")
            doc = json.loads(text)
            self.assertNotIn(TOKEN, text + out.getvalue() + err.getvalue())
            self.assertNotIn(ACCOUNT, text)
            # Aggregates only: no raw API rows or SQL column names are copied.
            for raw_key in ('"data"', '"meta"', "upstream_requests_sum", "outcome_ok", "groups"):
                self.assertNotIn(raw_key, text)
            self.assertEqual(list(doc), ["schemaVersion", "date", "window", "dataset", "scriptName", "analyticsEngine",
                                         "workersLogs", "coverage", "excludingHeaderTransport"])
            self.assertEqual(doc["coverage"], {"warnings": []})
            self.assertNotIn("coverage warning", out.getvalue())

    def test_missing_secrets(self):
        with mock.patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": "", "CLOUDFLARE_TELEMETRY_TOKEN": ""}):
            err = io.StringIO()
            with redirect_stderr(err):
                code = et.main(["--out-dir", "/nonexistent-should-not-be-created"], transport=FakeTransport())
        self.assertEqual(code, 2)
        self.assertIn("must be set", err.getvalue())

    def test_sql_failure_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_TELEMETRY_TOKEN": TOKEN}):
            err = io.StringIO()
            with redirect_stderr(err), redirect_stdout(io.StringIO()):
                code = et.main(["--out-dir", tmp, "--end", "2026-10-16T02:00:00Z"], transport=FakeTransport(fail_sql=True))
            self.assertEqual(code, 1)
            self.assertEqual(list(Path(tmp).iterdir()), [])
            self.assertNotIn(TOKEN, err.getvalue())

    def test_logs_failure_keeps_analytics_and_fails_the_run(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_TELEMETRY_TOKEN": TOKEN}):
            with redirect_stderr(io.StringIO()), redirect_stdout(io.StringIO()):
                code = et.main(["--out-dir", tmp, "--end", "2026-10-16T02:00:00Z"], transport=FakeTransport(fail_logs=True))
            self.assertEqual(code, 1)
            doc = json.loads((Path(tmp) / "2026-10-16.json").read_text(encoding="utf-8"))
            self.assertEqual(doc["workersLogs"]["available"], False)
            self.assertEqual(doc["analyticsEngine"]["totals"]["requests"], 46)

    def test_bad_account_id_is_refused_before_any_request(self):
        t = FakeTransport()
        with self.assertRaises(et.ExportError):
            et.build_export(t, "../../evil", TOKEN, END)
        self.assertEqual(t.calls, [])

    def test_default_window_ends_at_the_current_hour(self):
        start, end = et.export_window()
        self.assertEqual((end.minute, end.second, end.microsecond), (0, 0, 0))
        self.assertEqual((end - start).total_seconds(), 86_400)

    def test_empty_day(self):
        empty = {"meta": [], "data": [], "rows": 0}

        def transport(url: str, headers: dict, body: bytes) -> bytes:
            if url.endswith("/sql"):
                return json.dumps(empty).encode()
            return json.dumps(events_payload([])).encode()

        doc, problems = et.build_export(transport, ACCOUNT, TOKEN, END)
        self.assertEqual(problems, [])
        self.assertEqual(doc["analyticsEngine"]["actions"], {})
        self.assertEqual(doc["analyticsEngine"]["totals"]["requests"], 0)
        self.assertEqual(doc["analyticsEngine"]["keys"], {})
        self.assertEqual(doc["workersLogs"]["invocations"], 0)


class TestExcludingHeaderTransport(unittest.TestCase):
    """F6 (Phase 4 prep): the daily export also carries the aggregates without header-transport (CI) rows."""

    def test_unfiltered_queries_are_unchanged_and_filtered_ones_add_only_the_header_filter(self):
        _, _, t = _export()
        sql = [body.decode() for url, _, body in t.calls if url.endswith("/sql")]
        self.assertEqual(len(sql), 7)
        for text in sql[:4]:
            self.assertNotIn("blob7", text)
        start, stop = et.export_window(END)
        self.assertEqual(sql[:3], [et.hourly_sql(et.DATASET, start, stop), et.daily_latency_sql(et.DATASET, start, stop),
                                   et.breakdown_sql(et.DATASET, start, stop)])
        window = f"timestamp >= toDateTime({START_S}) AND timestamp < toDateTime({END_S})"
        for unfiltered, filtered in zip(sql[:3], sql[4:]):
            self.assertIn(f"WHERE {window} AND blob7 != 'header'\n", filtered)
            self.assertEqual(filtered.replace(" AND blob7 != 'header'", ""), unfiltered)

    def test_filtered_view(self):
        doc, problems, _ = _export()
        self.assertEqual(problems, [])
        view = doc["excludingHeaderTransport"]
        self.assertEqual(list(view), ["excluded", "totals", "actions"])
        self.assertEqual(view["excluded"], {"credentialTransport": "header"})
        self.assertEqual(sorted(view["actions"]), [PRICE])
        price = view["actions"][PRICE]
        self.assertEqual(price["requests"], 34)
        self.assertEqual(price["outcomes"], {"ok": 33, "tool_error": 1, "rpc_error": 0, "rejected": 0, "exception": 0})
        self.assertEqual(price["latencyMs"], {"p50": 90, "p95": 350, "p99": 700})
        self.assertEqual((price["upstreamRequestsMean"], price["subrequestsUsedMean"]), (1, 2))
        self.assertEqual(price["errorCodes"], {"RATE_LIMIT": 1})
        self.assertEqual(view["totals"]["requests"], 34)
        self.assertEqual(view["totals"]["outcomes"]["rejected"], 0)
        # The existing, unfiltered aggregates are untouched.
        self.assertEqual(doc["analyticsEngine"]["totals"]["requests"], 46)
        self.assertEqual(doc["analyticsEngine"]["actions"][PRICE]["requests"], 40)

    def test_filtered_view_has_no_token_or_raw_rows(self):
        doc, _, _ = _export()
        text = json.dumps(doc["excludingHeaderTransport"])
        for leaked in (TOKEN, ACCOUNT, '"data"', "outcome_ok", "upstream_requests_sum"):
            self.assertNotIn(leaked, text)

    def test_filtered_sql_failure_fails_the_export(self):
        def transport(url: str, headers: dict, body: bytes) -> bytes:
            if HEADER_FILTER in body.decode("utf-8", "replace"):
                raise et.ExportError("HTTP 500 from https://api.cloudflare.com/client/v4/accounts/…")
            return FakeTransport()(url, headers, body)

        with self.assertRaises(et.ExportError) as ctx:
            et.build_export(transport, ACCOUNT, TOKEN, END)
        self.assertNotIn(TOKEN, str(ctx.exception))


class TestCoverage(unittest.TestCase):
    """OPS-TEL-2 (2.14.0): Analytics Engine requests with zero logged invocations is an ops signal, not a failure."""

    ZERO_LOGS = events_payload([invocation_event(0, "req-x", "ok")])

    def test_requests_with_zero_logged_invocations_warns(self):
        doc, problems, _ = _export(FakeTransport(observability=self.ZERO_LOGS))
        self.assertEqual(problems, [])
        self.assertEqual(doc["workersLogs"]["invocations"], 0)
        self.assertEqual(doc["coverage"]["warnings"], [{
            "code": "LOGS_ZERO_WITH_REQUESTS",
            "message": "Analytics Engine counted 46 requests but Workers Logs counted 0 invocations.",
        }])

    def test_main_reports_the_warning_and_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch.dict(os.environ, {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_TELEMETRY_TOKEN": TOKEN}):
            out, err = io.StringIO(), io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                code = et.main(["--out-dir", tmp, "--end", "2026-10-16T02:00:00Z"],
                               transport=FakeTransport(observability=self.ZERO_LOGS))
            self.assertEqual(code, 0, err.getvalue())
            self.assertIn("LOGS_ZERO_WITH_REQUESTS", out.getvalue())
            doc = json.loads((Path(tmp) / "2026-10-16.json").read_text(encoding="utf-8"))
            self.assertEqual([w["code"] for w in doc["coverage"]["warnings"]], ["LOGS_ZERO_WITH_REQUESTS"])

    def test_no_warning_when_logs_were_read(self):
        doc, _, _ = _export()
        self.assertGreater(doc["workersLogs"]["invocations"], 0)
        self.assertEqual(doc["coverage"]["warnings"], [])

    def test_no_warning_when_logs_could_not_be_read(self):
        """An unreadable Workers Logs is already a problem (exit 1), not a zero count."""
        doc, problems, _ = _export(FakeTransport(fail_logs=True))
        self.assertEqual(len(problems), 1)
        self.assertEqual(doc["coverage"]["warnings"], [])

    def test_no_warning_on_an_empty_day(self):
        empty = {"meta": [], "data": [], "rows": 0}

        def transport(url: str, headers: dict, body: bytes) -> bytes:
            if url.endswith("/sql"):
                return json.dumps(empty).encode()
            return json.dumps(events_payload([])).encode()

        doc, _ = et.build_export(transport, ACCOUNT, TOKEN, END)
        self.assertEqual(doc["coverage"]["warnings"], [])


if __name__ == "__main__":
    unittest.main()
