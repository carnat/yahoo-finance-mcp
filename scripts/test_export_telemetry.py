#!/usr/bin/env python3
"""scripts/export_telemetry.py over canned Cloudflare API responses (2.11.1).

No network: a fake transport records each request and answers from canned
Analytics Engine SQL and Workers Observability payloads.

- Requests: the SQL API and telemetry query endpoints, bearer auth, the
  24-hour window, `_sample_interval` weighting, the dataset and log filters.
- Aggregation: per-action and total counts, outcome counts, weighted means,
  sums, size-bucket histogram, error codes, hourly rows and day percentiles;
  invocation outcomes joined to the action from the dispatch log line.
- Output: telemetry/<date>.json with aggregates only, no raw rows, no token;
  unsafe strings are counted as "invalid".
- Failure modes: missing secrets, an SQL failure, a Workers Logs failure.
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
OBSERVABILITY = {
    "success": True,
    "errors": [],
    "messages": [{"message": "Successful request"}],
    "result": {
        "run": {"id": "run-1"},
        "calculations": [{
            "calculation": "count",
            "alias": "invocations",
            "aggregates": [
                {"count": 39, "interval": 1, "sampleInterval": 1, "value": 39, "groups": [
                    {"key": "action", "value": PRICE}, {"key": "domain", "value": "stock_pricing"},
                    {"key": "build", "value": "0123456"}, {"key": "$workers.outcome", "value": "ok"}]},
                {"count": 1, "interval": 1, "sampleInterval": 1, "value": 1, "groups": [
                    {"key": "action", "value": PRICE}, {"key": "domain", "value": "stock_pricing"},
                    {"key": "build", "value": "0123456"}, {"key": "$workers.outcome", "value": "exceededCpu"}]},
                {"count": 2, "interval": 1, "sampleInterval": 1, "value": 2, "groups": [
                    {"key": "action", "value": "tools/list"}, {"key": "domain", "value": "-"},
                    {"key": "build", "value": "0123456"}, {"key": "$workers.outcome", "value": "ok"},
                    {"key": "$workers.event.request.url", "value": "https://x/mcp/LEAKED"}]},
            ],
            "series": [],
        }],
    },
}


class FakeTransport:
    def __init__(self, observability: object = OBSERVABILITY, fail_sql: bool = False, fail_logs: bool = False):
        self.calls: list[tuple[str, dict, bytes]] = []
        self.observability = observability
        self.fail_sql = fail_sql
        self.fail_logs = fail_logs

    def __call__(self, url: str, headers: dict, body: bytes) -> bytes:
        self.calls.append((url, headers, body))
        if url.endswith("/analytics_engine/sql"):
            if self.fail_sql:
                raise et.ExportError("HTTP 403 from https://api.cloudflare.com/client/v4/accounts/…")
            sql = body.decode()
            if "GROUP BY hour, action, domain" in sql:
                return json.dumps(HOURLY).encode()
            if "GROUP BY action, outcome, error_code, size_bucket" in sql:
                return json.dumps(BREAKDOWN).encode()
            return json.dumps(DAILY).encode()
        if url.endswith("/workers/observability/telemetry/query"):
            if self.fail_logs:
                raise et.ExportError("HTTP 403 from https://api.cloudflare.com/client/v4/accounts/…")
            return json.dumps(self.observability).encode()
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
        self.assertEqual(urls, [sql_url, sql_url, sql_url, logs_url])
        for _, headers, _ in t.calls:
            self.assertEqual(headers["Authorization"], f"Bearer {TOKEN}")

    def test_sql_window_dataset_and_sample_weighting(self):
        _, _, t = _export()
        for _, _, body in t.calls[:3]:
            sql = body.decode()
            self.assertIn("FROM yfmcp_telemetry", sql)
            self.assertIn(f"timestamp >= toDateTime({START_S}) AND timestamp < toDateTime({END_S})", sql)
            self.assertIn("SUM(_sample_interval)", sql)
            self.assertIn("FORMAT JSON", sql)
            self.assertNotIn("count()", sql)  # counts are sample-weighted sums
            self.assertNotRegex(sql, r"\bblob(1[2-9]|20)\b")
        hourly = t.calls[0][2].decode()
        self.assertIn("toStartOfInterval(timestamp, INTERVAL '1' HOUR) AS hour", hourly)
        self.assertIn("quantileExactWeighted(0.95)(double1, _sample_interval)", hourly)
        self.assertIn("sumIf(_sample_interval, blob4 = 'tool_error')", hourly)
        for column in ("double4", "double5", "double6", "double7", "double10"):
            self.assertIn(f"SUM(_sample_interval * {column})", hourly)

    def test_observability_query_shape(self):
        _, _, t = _export()
        body = json.loads(t.calls[3][2])
        self.assertEqual(body["view"], "calculations")
        self.assertEqual(body["timeframe"], {"from": START_S * 1000, "to": END_S * 1000})
        self.assertTrue(body["dry"])
        params = body["parameters"]
        self.assertIn({"key": "t", "operation": "eq", "type": "string", "value": "yfmcp"}, params["filters"])
        self.assertIn({"key": "$workers.scriptName", "operation": "eq", "type": "string", "value": "yahoo-finance-mcp"},
                      params["filters"])
        self.assertEqual([g["value"] for g in params["groupBys"]], ["action", "domain", "build", "$workers.outcome"])
        self.assertEqual(params["calculations"], [{"operator": "count", "alias": "invocations"}])


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

    def test_invocations_joined_to_action(self):
        doc, _, _ = _export()
        logs = doc["workersLogs"]
        self.assertTrue(logs["available"])
        self.assertEqual(logs["invocations"], 42)
        self.assertEqual(logs["outcomes"], {"exceededCpu": 1, "ok": 41})
        self.assertEqual(logs["builds"], {"0123456": 42})
        self.assertEqual(logs["actions"][PRICE], {"domain": "stock_pricing", "invocations": 40,
                                                  "outcomes": {"ok": 39, "exceededCpu": 1}})
        self.assertNotIn("LEAKED", json.dumps(doc))  # only action, domain, build and outcome are read


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
            self.assertEqual(sorted(doc), ["analyticsEngine", "dataset", "date", "schemaVersion", "scriptName",
                                           "window", "workersLogs"])

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
            return json.dumps({"success": True, "result": {"calculations": []}}).encode()

        doc, problems = et.build_export(transport, ACCOUNT, TOKEN, END)
        self.assertEqual(problems, [])
        self.assertEqual(doc["analyticsEngine"]["actions"], {})
        self.assertEqual(doc["analyticsEngine"]["totals"]["requests"], 0)
        self.assertEqual(doc["workersLogs"]["invocations"], 0)


if __name__ == "__main__":
    unittest.main()
