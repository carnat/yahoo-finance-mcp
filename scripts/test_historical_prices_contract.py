#!/usr/bin/env python3
"""get_historical_prices input and row contract, in both runtimes.

- Unknown periods and intervals are rejected: Yahoo answers them with a
  fallback single live bar instead of an error.
- Each row carries tradingDate, the bar's calendar date in the exchange's
  timezone; `date` (UTC) falls on the previous day for sessions east of UTC.
- OHLC rows are raw exchange prices and get no fact/evidence tagging.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

import server  # noqa: E402
from yfmcp.envelope import _envelope_tool_result  # noqa: E402

# Five ASX sessions dated at the 10:00 Sydney open. Sydney moves to daylight
# time on 2026-10-04, so from 2026-10-05 the open is 23:00 UTC the day before.
_SYDNEY_OPENS = [
    ("2026-09-30", 1790726400),  # 2026-09-30T00:00Z (AEST)
    ("2026-10-01", 1790812800),
    ("2026-10-02", 1790899200),
    ("2026-10-05", 1791154800),  # 2026-10-04T23:00Z (AEDT)
    ("2026-10-06", 1791241200),
]

_WORKER_HARNESS = r"""
const [bundleUrl] = process.argv.slice(-1);
const opens = %OPENS%;
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  if (!u.pathname.includes("/v8/finance/chart/")) return new Response("{}", { status: 404 });
  const n = opens.length;
  const close = opens.map((_, i) => 60 + i);
  return Response.json({ chart: { result: [{
    meta: { symbol: "BHP.AX", currency: "AUD", exchangeTimezoneName: "Australia/Sydney", regularMarketTime: opens[n - 1] + 21600 },
    timestamp: opens,
    indicators: { quote: [{ open: close, high: close, low: close, close, volume: close.map(() => 1000) }], adjclose: [{ adjclose: close }] },
  }] } });
};
const worker = (await import(bundleUrl)).default;
const env = { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" };
let id = 0;
async function call(params) {
  const res = await worker.fetch(new Request("https://t/mcp", { method: "POST", body: JSON.stringify({
    jsonrpc: "2.0", id: ++id, method: "tools/call",
    params: { name: "stock_pricing", arguments: { action: "get_historical_prices", params } },
  }) }), env);
  return (await res.json()).result.structuredContent;
}
const out = {
  ok: await call({ ticker: "BHP.AX", period: "5d", interval: "1d" }),
  badPeriod: await call({ ticker: "BHP.AX", period: "5days", interval: "1d" }),
  badInterval: await call({ ticker: "BHP.AX", period: "5d", interval: "1d&range=1y" }),
};
console.log(JSON.stringify(out));
"""

_FACT_KEYS = ("confidence", "evidence", "sourceType", "evidenceRequired", "decisionGrade")


def _run_worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry = WORKER / ".historical-test-entry.ts"
        bundle = Path(tmp) / "worker.mjs"
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(
            _WORKER_HARNESS.replace("%OPENS%", json.dumps([ts for _, ts in _SYDNEY_OPENS])),
            encoding="utf-8",
        )
        entry.write_text('export { default } from "./src/index.ts";\n', encoding="utf-8")
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        result = subprocess.run(
            [node, str(harness), bundle.as_uri()],
            check=True, capture_output=True, text=True, timeout=60,
        )
    return json.loads(result.stdout)


class TestWorkerHistoricalPrices(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _run_worker()

    def test_invalid_period_and_interval_are_rejected(self) -> None:
        for key, field in (("badPeriod", "period"), ("badInterval", "interval")):
            with self.subTest(case=key):
                payload = self.out[key]
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"]["code"], "INPUT_VALIDATION_ERROR")
                self.assertIn(f"{field} must be one of", payload["error"]["message"])

    def test_rows_carry_exchange_local_trading_date(self) -> None:
        rows = self.out["ok"]["data"]
        self.assertEqual([row["tradingDate"] for row in rows], [day for day, _ in _SYDNEY_OPENS])
        # The UTC instant of the first daylight-time session is the previous day.
        self.assertTrue(rows[3]["date"].startswith("2026-10-04T23:00"))

    def test_price_rows_are_not_fact_tagged(self) -> None:
        for row in self.out["ok"]["data"]:
            for key in _FACT_KEYS:
                self.assertNotIn(key, row)


def _history_frame(tz: str) -> pd.DataFrame:
    index = pd.DatetimeIndex([pd.Timestamp(day, tz=tz) for day in ("2026-09-22", "2026-09-23", "2026-09-24")], name="Date")
    closes = [60.0, 61.0, 62.0]
    return pd.DataFrame(
        {"Open": closes, "High": closes, "Low": closes, "Close": closes, "Volume": [1000, 1000, 1000]},
        index=index,
    )


class TestPythonHistoricalPrices(unittest.TestCase):
    def _call(self, **kwargs) -> dict:
        with patch.dict(os.environ, {"MCP_ENVELOPE_V2": "true"}):
            raw = asyncio.run(server.get_historical_prices("BHP.AX", **kwargs))
            return json.loads(_envelope_tool_result("get_historical_prices", raw))

    def test_invalid_period_and_interval_are_rejected(self) -> None:
        for kwargs, field in (({"period": "5days"}, "period"), ({"interval": "1d&range=1y"}, "interval")):
            with self.subTest(**kwargs):
                payload = self._call(**kwargs)
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error"]["code"], "INPUT_VALIDATION_ERROR")
                self.assertIn(f"{field} must be one of", payload["error"]["message"])

    def test_rows_carry_exchange_local_trading_date(self) -> None:
        for tz in ("Australia/Sydney", "Asia/Bangkok", "America/New_York"):
            with self.subTest(tz=tz):
                company = MagicMock()
                company.fast_info.currency = "AUD"
                company.history.return_value = _history_frame(tz)
                company.get_history_metadata.return_value = {"exchangeTimezoneName": tz}
                server._tool_cache._store.clear()
                with patch("yfmcp.tools.pricing.yf.Ticker", return_value=company):
                    payload = self._call(period="5d", interval="1d")
                rows = payload["data"]
                self.assertEqual([row["tradingDate"] for row in rows], ["2026-09-22", "2026-09-23", "2026-09-24"])
                for row in rows:
                    for key in _FACT_KEYS:
                        self.assertNotIn(key, row)

    def test_schema_lists_valid_values(self) -> None:
        tools = {tool.name: tool for tool in asyncio.run(server.yfinance_server.list_tools())}
        tool = tools["get_historical_prices"]
        schema = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", None)
        self.assertIn("5d", schema["properties"]["period"]["enum"])
        self.assertNotIn("5days", schema["properties"]["period"]["enum"])
        self.assertIn("1wk", schema["properties"]["interval"]["enum"])


if __name__ == "__main__":
    unittest.main()
