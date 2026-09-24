#!/usr/bin/env python3
"""Completed-session freshness contract, in both runtimes.

expectedCompletedDate is the most recent session that should already be
complete. During an open regular session it is the previous session, named
only when proven (Yahoo previousClose equals the last completed close, or the
last completed bar is from the weekday just before today's session). The
downstream check latestAvailableBarDate == expectedCompletedDate then holds
exactly when the completed-session series is current.

Each scenario runs through the Worker's get_price_slope (bundled, with a
mocked Yahoo chart and a fixed clock) and the local server's
_prepare_completed_daily_history; both must give the expected result.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import zoneinfo
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

import server  # noqa: E402,F401  (registers tools; mirrors the other suites)
from yfmcp.tools.pricing import _prepare_completed_daily_history  # noqa: E402

NY = "America/New_York"
TOKYO = "Asia/Tokyo"


def _epoch(day: str, clock: str, tz: str) -> int:
    local = dt.datetime.fromisoformat(f"{day}T{clock}").replace(tzinfo=zoneinfo.ZoneInfo(tz))
    return int(local.timestamp())


def _scenario(*, tz: str, days: list[str], now: tuple[str, str], session: tuple[str, str, str] | None,
              market_time: tuple[str, str], previous_close_of: str | None = None,
              previous_close: float | None = None, expected: str | None, freshness: str,
              latest: str, excluded: bool) -> dict:
    open_clock, close_clock = ("09:00", "15:30") if tz == TOKYO else ("09:30", "16:00")
    closes = {day: 100.0 + index for index, day in enumerate(days)}
    meta: dict = {
        "exchangeTimezoneName": tz,
        "regularMarketTime": _epoch(market_time[0], market_time[1], tz),
    }
    if session:
        day, start_clock, end_clock = session
        meta["currentTradingPeriod"] = {"regular": {"start": _epoch(day, start_clock, tz), "end": _epoch(day, end_clock, tz)}}
    if previous_close_of is not None:
        meta["previousClose"] = closes[previous_close_of]
    if previous_close is not None:
        meta["previousClose"] = previous_close
    return {
        "tz": tz,
        "meta": meta,
        "now": _epoch(now[0], now[1], tz),
        "bars": [{"day": day, "open": _epoch(day, open_clock, tz), "close": closes[day]} for day in days],
        "expected": {"expectedCompletedDate": expected, "freshnessStatus": freshness,
                     "latestAvailableBarDate": latest, "excludedIncompleteBar": excluded},
    }


US_SESSION_SEP24 = ("2026-09-24", "09:30", "16:00")
WEEK = ["2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"]

SCENARIOS = {
    # Handoff case: AAPL during the Sep-24 regular session.
    "us_open_previous_close_matches": _scenario(
        tz=NY, days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SESSION_SEP24,
        market_time=("2026-09-24", "10:59"), previous_close_of="2026-09-23",
        expected="2026-09-23", freshness="CURRENT", latest="2026-09-23", excluded=True),
    "us_open_without_previous_close": _scenario(
        tz=NY, days=WEEK + ["2026-09-24"], now=("2026-09-24", "11:00"), session=US_SESSION_SEP24,
        market_time=("2026-09-24", "10:59"),
        expected="2026-09-23", freshness="CURRENT", latest="2026-09-23", excluded=True),
    "us_open_incomplete_bar_absent": _scenario(
        tz=NY, days=WEEK, now=("2026-09-24", "11:00"), session=US_SESSION_SEP24,
        market_time=("2026-09-24", "10:59"), previous_close_of="2026-09-23",
        expected="2026-09-23", freshness="CURRENT", latest="2026-09-23", excluded=False),
    "us_open_monday": _scenario(
        tz=NY, days=["2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21"],
        now=("2026-09-21", "11:00"), session=("2026-09-21", "09:30", "16:00"), market_time=("2026-09-21", "10:59"),
        expected="2026-09-18", freshness="CURRENT", latest="2026-09-18", excluded=True),
    # Tuesday after Labor Day (Mon 2026-09-07): only previousClose proves Friday is the previous session.
    "us_open_after_holiday_with_previous_close": _scenario(
        tz=NY, days=["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08"],
        now=("2026-09-08", "11:00"), session=("2026-09-08", "09:30", "16:00"), market_time=("2026-09-08", "10:59"),
        previous_close_of="2026-09-04",
        expected="2026-09-04", freshness="CURRENT", latest="2026-09-04", excluded=True),
    "us_open_after_holiday_unproven": _scenario(
        tz=NY, days=["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08"],
        now=("2026-09-08", "11:00"), session=("2026-09-08", "09:30", "16:00"), market_time=("2026-09-08", "10:59"),
        expected=None, freshness="UNKNOWN", latest="2026-09-04", excluded=True),
    # Yesterday's bar is missing; previousClose (yesterday's close) does not match Tuesday's.
    "us_open_previous_session_missing": _scenario(
        tz=NY, days=["2026-09-16", "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22", "2026-09-24"],
        now=("2026-09-24", "11:00"), session=US_SESSION_SEP24, market_time=("2026-09-24", "10:59"),
        previous_close=999.0,
        expected=None, freshness="STALE", latest="2026-09-22", excluded=True),
    "us_pre_market": _scenario(
        tz=NY, days=WEEK, now=("2026-09-24", "08:00"), session=US_SESSION_SEP24,
        market_time=("2026-09-23", "16:00"),
        expected="2026-09-23", freshness="CURRENT", latest="2026-09-23", excluded=False),
    "us_after_hours": _scenario(
        tz=NY, days=WEEK + ["2026-09-24"], now=("2026-09-24", "18:00"), session=US_SESSION_SEP24,
        market_time=("2026-09-24", "16:00"),
        expected="2026-09-24", freshness="CURRENT", latest="2026-09-24", excluded=False),
    "us_weekend": _scenario(
        tz=NY, days=WEEK + ["2026-09-24", "2026-09-25"], now=("2026-09-26", "11:00"),
        session=("2026-09-25", "09:30", "16:00"), market_time=("2026-09-25", "16:00"),
        expected="2026-09-25", freshness="CURRENT", latest="2026-09-25", excluded=False),
    # Handoff contrast: Tokyo's Sep-24 session is complete.
    "tokyo_completed": _scenario(
        tz=TOKYO, days=["2026-09-18", "2026-09-21", "2026-09-22", "2026-09-24"], now=("2026-09-24", "17:00"),
        session=("2026-09-24", "09:00", "15:30"), market_time=("2026-09-24", "15:30"),
        expected="2026-09-24", freshness="CURRENT", latest="2026-09-24", excluded=False),
    # US daylight time ends Sun 2026-11-01: Monday's open moves from 13:30Z to 14:30Z.
    "us_open_after_dst_change": _scenario(
        tz=NY, days=["2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30", "2026-11-02"],
        now=("2026-11-02", "11:00"), session=("2026-11-02", "09:30", "16:00"), market_time=("2026-11-02", "10:59"),
        expected="2026-10-30", freshness="CURRENT", latest="2026-10-30", excluded=True),
}

_HARNESS = r"""
const [scenariosPath, bundleUrl] = process.argv.slice(-2);
const { readFileSync } = await import("node:fs");
const scenarios = JSON.parse(readFileSync(scenariosPath, "utf8"));
const realNow = Date.now;
const out = {};
for (const [name, scenario] of Object.entries(scenarios)) {
  Date.now = () => scenario.now * 1000;
  globalThis.fetch = async (req) => {
    const u = new URL(typeof req === "string" ? req : req.url);
    if (!u.pathname.includes("/v8/finance/chart/")) return new Response("{}", { status: 404 });
    const closes = scenario.bars.map((bar) => bar.close);
    return Response.json({ chart: { result: [{
      meta: { symbol: "TEST", currency: "USD", ...scenario.meta },
      timestamp: scenario.bars.map((bar) => bar.open),
      indicators: { quote: [{ open: closes, high: closes, low: closes, close: closes, volume: closes.map(() => 1000) }], adjclose: [{ adjclose: closes }] },
    }] } });
  };
  const worker = (await import(`${bundleUrl}?scenario=${name}`)).default;
  const res = await worker.fetch(new Request("https://t/mcp", { method: "POST", body: JSON.stringify({
    jsonrpc: "2.0", id: 1, method: "tools/call",
    params: { name: "stock_pricing", arguments: { action: "get_price_slope", params: { ticker: "TEST", days: 2 } } },
  }) }), { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" });
  out[name] = (await res.json()).result.structuredContent;
}
Date.now = realNow;
console.log(JSON.stringify(out));
"""


def _run_worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".freshness-test-entry.ts"
        bundle = Path(tmp) / "worker.mjs"
        harness = Path(tmp) / "harness.mjs"
        scenarios = Path(tmp) / "scenarios.json"
        harness.write_text(_HARNESS, encoding="utf-8")
        scenarios.write_text(json.dumps(SCENARIOS), encoding="utf-8")
        entry.write_text('export { default } from "./src/index.ts";\n', encoding="utf-8")
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        result = subprocess.run([node, str(harness), str(scenarios), bundle.as_uri()],
                                check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout)


def _python_result(scenario: dict) -> dict:
    tz = scenario["tz"]
    index = pd.DatetimeIndex([pd.Timestamp(bar["day"], tz=tz) for bar in scenario["bars"]], name="Date")
    closes = [bar["close"] for bar in scenario["bars"]]
    hist = pd.DataFrame({"Close": closes, "Adj Close": closes, "Volume": [1000] * len(closes)}, index=index)
    prepared = _prepare_completed_daily_history(scenario["meta"], hist, scenario["now"])
    completed = prepared["completed"]
    return {
        "expectedCompletedDate": prepared["expectedCompletedDate"],
        "freshnessStatus": prepared["freshnessStatus"],
        "latestAvailableBarDate": completed.index[-1].date().isoformat() if not completed.empty else None,
        "excludedIncompleteBar": prepared["excludedIncompleteBar"],
    }


class TestCompletedSessionFreshness(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _run_worker()

    def test_worker_get_price_slope(self) -> None:
        for name, scenario in SCENARIOS.items():
            with self.subTest(scenario=name):
                payload = self.worker[name]
                data = payload.get("data") or {}
                actual = {key: data.get(key) for key in scenario["expected"]}
                self.assertEqual(actual, scenario["expected"], json.dumps(payload)[:1500])

    def test_python_prepare_completed_daily_history(self) -> None:
        for name, scenario in SCENARIOS.items():
            with self.subTest(scenario=name):
                self.assertEqual(_python_result(scenario), scenario["expected"])

    def test_current_results_satisfy_the_downstream_equality(self) -> None:
        # The CQ-98 acceptance check: CURRENT exactly when latest == expected.
        for name, scenario in SCENARIOS.items():
            with self.subTest(scenario=name):
                data = self.worker[name].get("data") or {}
                proven = data.get("expectedCompletedDate") is not None and \
                    data.get("latestAvailableBarDate") == data.get("expectedCompletedDate")
                self.assertEqual(proven, data.get("freshnessStatus") == "CURRENT")


if __name__ == "__main__":
    unittest.main()
