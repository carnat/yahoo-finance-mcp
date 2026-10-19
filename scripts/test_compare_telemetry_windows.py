#!/usr/bin/env python3
"""scripts/compare_telemetry_windows.py over canned Analytics Engine responses (Phase 4 prep, F6, D10).

No network: a fake transport records each request and answers each window's
SQL from a canned per-group payload.

- Requests: the SQL API endpoint, bearer auth, one query per window, the
  window bounds, `blob7 != 'header'` (synthetic CI traffic excluded), grouping
  by blob1 (domain), `_sample_interval` weighting, weighted p50/p95.
- D10: groups with >= 30 requests in both windows are judged; p95 and p50 within
  +20 % and the error-plus-exception rate (tool_error + rpc_error + exception)
  within +1 percentage point pass; boundaries; low-volume skips; the exit codes.
- Output: JSON to stdout or --out; the token never appears in output or errors.
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

import compare_telemetry_windows as ctw  # noqa: E402
import export_telemetry as et  # noqa: E402

ACCOUNT = "0123456789abcdef0123456789abcdef"
TOKEN = "cf-telemetry-token-SECRET-9z8y7x"
ENV = {"CLOUDFLARE_ACCOUNT_ID": ACCOUNT, "CLOUDFLARE_TELEMETRY_TOKEN": TOKEN}

BASE_FROM = int(datetime(2026, 10, 17, tzinfo=timezone.utc).timestamp())
BASE_TO = int(datetime(2026, 10, 24, tzinfo=timezone.utc).timestamp())
COMP_FROM = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp())
COMP_TO = int(datetime(2026, 11, 8, tzinfo=timezone.utc).timestamp())
COMPARE_ARGS = ["--compare-start", "2026-11-01T00:00Z", "--compare-end", "2026-11-08T00:00Z"]


def row(group: str, requests: int, p50: float, p95: float, *, tool_error: int = 0, rpc_error: int = 0,
        exception: int = 0, rejected: int = 0) -> dict:
    ok = requests - tool_error - rpc_error - exception - rejected
    # The SQL API returns 64-bit integers as JSON strings.
    return {"action_group": group, "requests": str(requests), "outcome_ok": str(ok),
            "outcome_tool_error": str(tool_error), "outcome_rpc_error": str(rpc_error),
            "outcome_rejected": str(rejected), "outcome_exception": str(exception),
            "latency_p50": p50, "latency_p95": p95}


BASELINE_ROWS = [
    row("stock_pricing", 200, 100, 500, tool_error=2),           # 1 %
    row("sec_filings", 50, 300, 2000, exception=1),              # 2 %
    row("-", 100, 5, 20, rejected=40),                           # rejected is not an error
    row("thai_funds", 10, 50, 100),                              # low volume in the baseline
]
COMPARE_PASS = [
    row("stock_pricing", 180, 120, 600, tool_error=3, rpc_error=0),  # exactly +20 %, 1.667 % (<= 2 %)
    row("sec_filings", 40, 200, 1800, exception=1, tool_error=0),    # 2.5 % (<= 3 %)
    row("-", 90, 6, 24, rejected=60),
    row("thai_funds", 300, 999, 9999, tool_error=100),               # skipped: baseline < 30
    row("options_analysis", 29, 1, 1),                               # only in the comparison window
]


class FakeTransport:
    def __init__(self, baseline: list[dict] = BASELINE_ROWS, compare: list[dict] = COMPARE_PASS,
                 fail: bool = False):
        self.calls: list[tuple[str, dict, bytes]] = []
        self.baseline, self.compare, self.fail = baseline, compare, fail

    def __call__(self, url: str, headers: dict, body: bytes) -> bytes:
        self.calls.append((url, headers, body))
        if not url.endswith("/analytics_engine/sql"):
            raise AssertionError(f"unexpected URL {url}")
        if self.fail:
            raise et.ExportError("HTTP 403 from https://api.cloudflare.com/client/v4/accounts/…")
        sql = body.decode()
        rows = self.baseline if f"toDateTime({BASE_FROM})" in sql else self.compare
        return json.dumps({"meta": [], "data": rows, "rows": len(rows)}).encode()


def run(argv: list[str], transport: FakeTransport, env: dict = ENV) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.dict(os.environ, env), redirect_stdout(out), redirect_stderr(err):
        code = ctw.main(argv, transport=transport)
    return code, out.getvalue(), err.getvalue()


def report_for(compare: list[dict], baseline: list[dict] = BASELINE_ROWS) -> tuple[int, dict]:
    code, out, err = run(COMPARE_ARGS, FakeTransport(baseline, compare))
    assert TOKEN not in out + err
    return code, json.loads(out)


class TestRequests(unittest.TestCase):
    def test_endpoint_auth_and_sql(self):
        t = FakeTransport()
        code, _, _ = run(COMPARE_ARGS, t)
        self.assertEqual(code, 0)
        self.assertEqual(len(t.calls), 2)
        windows = [(BASE_FROM, BASE_TO), (COMP_FROM, COMP_TO)]
        for (url, headers, body), (start, end) in zip(t.calls, windows):
            self.assertEqual(url, f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/analytics_engine/sql")
            self.assertEqual(headers["Authorization"], f"Bearer {TOKEN}")
            sql = body.decode()
            self.assertIn("FROM yfmcp_telemetry", sql)
            self.assertIn(f"WHERE timestamp >= toDateTime({start}) AND timestamp < toDateTime({end}) "
                          "AND blob7 != 'header'\n", sql)
            self.assertIn("blob1 AS action_group", sql)
            self.assertIn("GROUP BY action_group", sql)
            self.assertIn("SUM(_sample_interval) AS requests", sql)
            self.assertIn("quantileExactWeighted(0.5)(double1, _sample_interval) AS latency_p50", sql)
            self.assertIn("quantileExactWeighted(0.95)(double1, _sample_interval) AS latency_p95", sql)
            for outcome in et.OUTCOMES:
                self.assertIn(f"sumIf(_sample_interval, blob4 = '{outcome}') AS outcome_{outcome}", sql)
            self.assertIn("FORMAT JSON", sql)
            self.assertNotIn("count()", sql)

    def test_default_baseline_window(self):
        t = FakeTransport()
        run(COMPARE_ARGS, t)
        self.assertIn(f"toDateTime({BASE_FROM}) AND timestamp < toDateTime({BASE_TO})", t.calls[0][2].decode())

    def test_bad_account_id_is_refused_before_any_request(self):
        t = FakeTransport()
        code, _, err = run(COMPARE_ARGS, t, {"CLOUDFLARE_ACCOUNT_ID": "../evil", "CLOUDFLARE_TELEMETRY_TOKEN": TOKEN})
        self.assertEqual(code, ctw.EXIT_QUERY_FAILED)
        self.assertEqual(t.calls, [])
        self.assertNotIn(TOKEN, err)


class TestD10(unittest.TestCase):
    def test_pass_at_the_bounds(self):
        code, report = report_for(COMPARE_PASS)
        self.assertEqual(code, 0)
        self.assertEqual(report["overall"], "pass")
        self.assertEqual(report["groupsJudged"], ["-", "sec_filings", "stock_pricing"])
        self.assertEqual(report["groupsSkippedLowVolume"], ["options_analysis", "thai_funds"])
        self.assertEqual(report["groupsFailed"], [])
        price = report["groups"]["stock_pricing"]
        self.assertTrue(price["judged"])
        self.assertTrue(price["pass"])
        self.assertEqual(price["checks"]["latencyP95"], {"baseline": 500, "compare": 600, "limit": 600, "pass": True})
        self.assertEqual(price["checks"]["latencyP50"], {"baseline": 100, "compare": 120, "limit": 120, "pass": True})
        self.assertEqual(price["checks"]["errorRatePct"], {"baseline": 1.0, "compare": 1.6667, "limit": 2.0, "pass": True})
        self.assertEqual(price["baseline"]["requests"], 200)
        self.assertEqual(price["compare"]["outcomes"]["tool_error"], 3)
        # rejected is reported but is not an error.
        self.assertEqual(report["groups"]["-"]["checks"]["errorRatePct"]["compare"], 0.0)
        self.assertEqual(report["groups"]["-"]["compare"]["outcomes"]["rejected"], 60)

    def test_low_volume_reasons(self):
        _, report = report_for(COMPARE_PASS)
        thai = report["groups"]["thai_funds"]
        self.assertEqual((thai["judged"], thai["pass"]), (False, None))
        self.assertEqual(thai["skipReason"], "fewer than 30 requests in the baseline window")
        self.assertNotIn("checks", thai)
        options = report["groups"]["options_analysis"]
        self.assertIsNone(options["baseline"])
        self.assertEqual(options["skipReason"], "fewer than 30 requests in both windows")

    def test_exactly_30_requests_is_judged(self):
        code, report = report_for([row("stock_pricing", 30, 100, 500)],
                                  baseline=[row("stock_pricing", 30, 100, 500), row("sec_filings", 30, 1, 1)])
        self.assertEqual(code, 0)
        self.assertEqual(report["groupsJudged"], ["stock_pricing"])
        self.assertEqual(report["groups"]["sec_filings"]["skipReason"], "fewer than 30 requests in the comparison window")

    def test_p95_regression_fails(self):
        compare = [row("stock_pricing", 200, 100, 600.5, tool_error=2)] + COMPARE_PASS[1:]
        code, report = report_for(compare)
        self.assertEqual(code, ctw.EXIT_FAIL)
        self.assertEqual(report["overall"], "fail")
        self.assertEqual(report["groupsFailed"], ["stock_pricing"])
        checks = report["groups"]["stock_pricing"]["checks"]
        self.assertFalse(checks["latencyP95"]["pass"])
        self.assertTrue(checks["latencyP50"]["pass"])
        self.assertTrue(checks["errorRatePct"]["pass"])

    def test_p50_regression_fails(self):
        compare = [row("stock_pricing", 200, 121, 500, tool_error=2)] + COMPARE_PASS[1:]
        code, report = report_for(compare)
        self.assertEqual(code, ctw.EXIT_FAIL)
        self.assertFalse(report["groups"]["stock_pricing"]["checks"]["latencyP50"]["pass"])

    def test_error_rate_regression_fails(self):
        # sec_filings: baseline 2 %; 2 exceptions + 2 rpc errors of 100 = 4 % > 3 %.
        compare = COMPARE_PASS[:1] + [row("sec_filings", 100, 300, 2000, exception=2, rpc_error=2)] + COMPARE_PASS[2:]
        code, report = report_for(compare)
        self.assertEqual(code, ctw.EXIT_FAIL)
        self.assertEqual(report["groupsFailed"], ["sec_filings"])
        self.assertEqual(report["groups"]["sec_filings"]["checks"]["errorRatePct"],
                         {"baseline": 2.0, "compare": 4.0, "limit": 3.0, "pass": False})

    def test_error_rate_at_plus_one_point_passes(self):
        compare = COMPARE_PASS[:1] + [row("sec_filings", 100, 300, 2000, exception=3)] + COMPARE_PASS[2:]
        code, report = report_for(compare)
        self.assertEqual(code, 0)
        self.assertTrue(report["groups"]["sec_filings"]["checks"]["errorRatePct"]["pass"])

    def test_latency_improvement_passes(self):
        code, _ = report_for([row("stock_pricing", 200, 10, 50, tool_error=0)])
        self.assertEqual(code, 0)

    def test_nothing_judgeable(self):
        code, report = report_for([row("stock_pricing", 29, 1, 1)])
        self.assertEqual(code, ctw.EXIT_NOTHING_JUDGEABLE)
        self.assertEqual(code, 3)
        self.assertEqual(report["overall"], "no_judgeable_groups")
        self.assertEqual(report["groupsJudged"], [])

    def test_empty_windows(self):
        code, report = report_for([], baseline=[])
        self.assertEqual(code, 3)
        self.assertEqual(report["groups"], {})

    def test_report_header(self):
        _, report = report_for(COMPARE_PASS)
        self.assertEqual(report["windows"], {
            "baseline": {"from": "2026-10-17T00:00:00Z", "to": "2026-10-24T00:00:00Z"},
            "compare": {"from": "2026-11-01T00:00:00Z", "to": "2026-11-08T00:00:00Z"}})
        self.assertEqual(report["excluded"], {"credentialTransport": "header"})
        self.assertEqual(report["groupBy"], "domain")
        self.assertEqual(report["bounds"], {"minRequestsBothWindows": 30, "latencyP95MaxIncreasePct": 20.0,
                                            "latencyP50MaxIncreasePct": 20.0, "errorRateMaxIncreasePp": 1.0,
                                            "errorOutcomes": ["tool_error", "rpc_error", "exception"]})

    def test_unsafe_group_names_are_counted_not_copied(self):
        _, report = report_for([row("AAPL secret'; DROP", 40, 1, 1)] + COMPARE_PASS)
        self.assertIn("invalid", report["groups"])
        self.assertNotIn("AAPL", json.dumps(report))


class TestOutput(unittest.TestCase):
    def test_out_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.json"
            code, out, err = run(COMPARE_ARGS + ["--out", str(path)], FakeTransport())
            self.assertEqual(code, 0, err)
            text = path.read_text(encoding="utf-8")
            self.assertEqual(json.loads(text)["overall"], "pass")
            self.assertIn("overall pass", out)
            self.assertNotIn(TOKEN, text + out + err)
            self.assertNotIn(ACCOUNT, text)
            for raw_key in ('"data"', '"meta"', "outcome_ok", "action_group"):
                self.assertNotIn(raw_key, text)

    def test_missing_secrets(self):
        t = FakeTransport()
        code, _, err = run(COMPARE_ARGS, t, {"CLOUDFLARE_ACCOUNT_ID": "", "CLOUDFLARE_TELEMETRY_TOKEN": ""})
        self.assertEqual(code, ctw.EXIT_USAGE)
        self.assertIn("must be set", err)
        self.assertEqual(t.calls, [])

    def test_query_failure_never_prints_the_token(self):
        code, out, err = run(COMPARE_ARGS, FakeTransport(fail=True))
        self.assertEqual(code, ctw.EXIT_QUERY_FAILED)
        self.assertEqual(out, "")
        self.assertIn("telemetry comparison failed", err)
        self.assertNotIn(TOKEN, out + err)

    def test_non_json_response_never_prints_the_token(self):
        def transport(url: str, headers: dict, body: bytes) -> bytes:
            return b"<html>" + headers["Authorization"].encode() + b"</html>"

        code, out, err = run(COMPARE_ARGS, transport)
        self.assertEqual(code, ctw.EXIT_QUERY_FAILED)
        self.assertNotIn(TOKEN, out + err)

    def test_bad_windows(self):
        code, _, err = run(["--compare-start", "2026-11-08T00:00Z", "--compare-end", "2026-11-01T00:00Z"],
                           FakeTransport())
        self.assertEqual(code, ctw.EXIT_USAGE)
        self.assertIn("compare window must start before it ends", err)
        code, _, err = run(["--baseline-start", "not-a-date"] + COMPARE_ARGS, FakeTransport())
        self.assertEqual(code, ctw.EXIT_USAGE)

    def test_default_compare_window_is_the_seven_days_before_the_current_hour(self):
        def hour() -> int:
            return int(datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).timestamp())

        t = FakeTransport()
        before = hour()
        run([], t)
        sql = t.calls[1][2].decode()
        self.assertTrue(any(f"timestamp >= toDateTime({end - 7 * 86400}) AND timestamp < toDateTime({end})" in sql
                            for end in {before, hour()}))


if __name__ == "__main__":
    unittest.main()
