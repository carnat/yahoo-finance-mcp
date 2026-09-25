#!/usr/bin/env python3
"""Quote and calculation tools, the same in both runtimes (2.2.8).

The 2.2.7 audit recomputed every price-based figure from raw bars:

- RSI-14 and MACD ran on 64 bars (range=3mo), too few for either recursive
  average to settle: one more bar moved RSI from 62.2 to 61.5. The local
  server also seeded RSI differently (61.74 on the same bars).
- 30-day volatility used log returns with a population deviation (Worker,
  19.93) or simple returns with a sample deviation (local, 20.37).
- The liquidity gate for USD listings passed any stock trading at least half
  its own 20-day average volume, however thin.
- A Yahoo 429 returned as {error, message} became PROVIDER_ERROR.
- The options summary and flow scan read the chain expiring that day.

Recorded AAPL daily bars (scripts/aapl_daily_bars_fixture.json) pin the
settled values: RSI 61.36, MACD 6.0381 / 5.2354 / 0.8027, volatility 20.2718.
"""

from __future__ import annotations

import asyncio
import datetime
import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

FIXTURE = json.loads((ROOT / "scripts" / "aapl_daily_bars_fixture.json").read_text(encoding="utf-8"))
CLOSES = [bar[1] for bar in FIXTURE["bars"]]


def _weekdays_until_today(count: int) -> list[datetime.date]:
    day = datetime.datetime.now(datetime.timezone.utc).date()
    out: list[datetime.date] = []
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day)
        day -= datetime.timedelta(days=1)
    return list(reversed(out))


# Liquidity gate listings: (currency, close, daily volume). A pence listing
# trading £50M a day passes; a $5 stock trading 100k shares ($0.5M) a day
# fails although its latest session matches its own average.
GATE_DAYS = [d.isoformat() for d in _weekdays_until_today(30)]
GATE_LISTINGS = {
    "LSEX.L": ("GBp", 250.0, 20_000_000),
    "THIN": ("USD", 5.0, 100_000),
}
GBP_PER_USD = 0.8

_WORKER_HARNESS = r"""
import fs from "node:fs";
const [bundleUrl, dataPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const data = JSON.parse(fs.readFileSync(dataPath, "utf8"));
const closes = data.closes;
const out = {};
out.rsi = m.wilderRsi(closes);
out.rsiLast200 = m.wilderRsi(closes.slice(-200));
out.macd = m.macd(closes);
out.volatility = m.annualizedVolatility(closes, 30);
out.lookback = ["3mo", "6mo", "ytd", "1y", "2y", "max", null].map((p) => m.indicatorLookback(p));
out.tradedValue = [
  m.averageTradedValue([{ volume: 10, rawClose: 2 }, { volume: 20, rawClose: 3 }], 2),
  m.averageTradedValue([{ volume: 10, rawClose: 2 }, { volume: null, rawClose: 3 }], 2),
  m.averageTradedValue([{ volume: 10, rawClose: 2 }], 2),
];
out.currencyUnits = ["GBp", "ZAc", "ILA", "EUR", "USD"].map((c) => m.listingCurrencyUnit(c));
out.nextExpiry = [
  m.nextOptionExpiry(["2026-09-25", "2026-10-02"], "2026-09-25"),
  m.nextOptionExpiry(["2026-09-25", "2026-10-02"], "2026-09-24"),
  m.nextOptionExpiry(["2026-09-18", "2026-09-25"], "2026-09-26"),
];
out.marketDate = [m.usMarketDate(new Date("2026-09-25T02:00:00Z")), m.usMarketDate(new Date("2026-09-25T14:00:00Z"))];
out.classified = [
  m.classifyErrorMessage("Yahoo Finance API error 429 for: https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL"),
  m.classifyErrorMessage("request timed out"),
  m.classifyErrorMessage("Yahoo Finance API error 500 for: https://x/v8/finance/chart/AAPL?_=1742900429"),
];
m.setWorkerEnv({ MCP_ENVELOPE_V2: "true" });
out.envelope = JSON.parse(m.mcpSuccess("get_short_momentum", JSON.stringify({ error: true, message: "Yahoo Finance API error 429 for: https://x/v10/finance/quoteSummary/AAPL" })));

// Liquidity gate and short interest through the tool functions, on mocked Yahoo.
const epoch = (day) => Math.floor(Date.parse(`${day}T20:00:00Z`) / 1000);
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  const symbol = decodeURIComponent(u.pathname.split("/").pop());
  if (u.pathname.includes("/quoteSummary/")) {
    if (symbol === "GBP=X") return Response.json({ quoteSummary: { result: [{ price: { currency: "GBP", quoteType: "CURRENCY", regularMarketPrice: { raw: data.gbpPerUsd } } }] } });
    if (symbol === "SHRT") return Response.json({ quoteSummary: { result: [{ defaultKeyStatistics: {
      sharesShort: { raw: 128753092 }, shortPercentOfFloat: { raw: 0.0088 }, shortRatio: { raw: 3.03 },
      dateShortInterest: { raw: 1789430400 }, sharesShortPreviousMonthDate: { raw: 1786665600 } }, price: {} }] } });
    const listing = data.listings[symbol];
    return Response.json({ quoteSummary: { result: [{ price: { currency: listing[0], quoteType: "EQUITY", regularMarketPrice: { raw: listing[1] } }, summaryDetail: {}, defaultKeyStatistics: {} }] } });
  }
  if (u.pathname.includes("/chart/")) {
    const [, close, volume] = data.listings[symbol];
    return Response.json({ chart: { result: [{
      meta: { exchangeTimezoneName: "UTC" },
      timestamp: data.days.map(epoch),
      indicators: { quote: [{ close: data.days.map(() => close), volume: data.days.map(() => volume) }], adjclose: [{ adjclose: data.days.map(() => close) }] },
    }] } });
  }
  return new Response("not found", { status: 404 });
};
out.gates = {};
for (const symbol of Object.keys(data.listings)) out.gates[symbol] = JSON.parse(await m.getVolumeGate(symbol, false));
out.shortInterest = JSON.parse(await m.getShortInterest("SHRT"));
console.log(JSON.stringify(out));
"""


@functools.cache
def _worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        entry = WORKER / ".quote-calculations-test-entry.ts"
        bundle = tmp_path / "bundle.mjs"
        entry.write_text(
            "export {\n"
            "  wilderRsi, macd, annualizedVolatility, indicatorLookback, averageTradedValue, listingCurrencyUnit,\n"
            "  nextOptionExpiry, usMarketDate, getVolumeGate, getShortInterest,\n"
            '} from "./src/yahoo-finance.ts";\n'
            'export { classifyErrorMessage, mcpSuccess, setWorkerEnv } from "./src/response.ts";\n',
            encoding="utf-8",
        )
        try:
            subprocess.run(
                [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral",
                 "--main-fields=module,main", "--external:node:async_hooks", f"--outfile={bundle}", "--log-level=error"],
                cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
            )
        finally:
            entry.unlink(missing_ok=True)
        (tmp_path / "data.json").write_text(json.dumps({
            "closes": CLOSES, "days": GATE_DAYS, "listings": GATE_LISTINGS, "gbpPerUsd": GBP_PER_USD,
        }), encoding="utf-8")
        (tmp_path / "harness.mjs").write_text(_WORKER_HARNESS, encoding="utf-8")
        result = subprocess.run(
            [node, str(tmp_path / "harness.mjs"), bundle.as_uri(), str(tmp_path / "data.json")],
            check=True, capture_output=True, text=True, timeout=120,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


class _FakeTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        if symbol == "GBP=X":
            self.fast_info = type("FI", (), {"last_price": GBP_PER_USD})()
            self.info = {}
        elif symbol == "SHRT":
            self.info = {
                "sharesShort": 128753092, "shortPercentOfFloat": 0.0088, "shortRatio": 3.03,
                "dateShortInterest": 1789430400, "sharesShortPreviousMonthDate": 1786665600,
            }
        else:
            self.fast_info = {"currency": GATE_LISTINGS[symbol][0]}


def _prepared(symbol: str) -> dict:
    _, close, volume = GATE_LISTINGS[symbol]
    index = pd.DatetimeIndex([pd.Timestamp(d) for d in GATE_DAYS])
    completed = pd.DataFrame({"Close": [close] * len(index), "Volume": [volume] * len(index)}, index=index)
    return {
        "completed": completed, "timezoneName": "UTC", "freshnessStatus": "CURRENT",
        "expectedCompletedDate": GATE_DAYS[-1], "excludedIncompleteBar": False, "retryAttempted": False,
    }


@functools.cache
def _local() -> dict:
    import server  # noqa: F401  (registers the tools)
    from yfmcp import envelope
    from yfmcp.tools import pricing

    out: dict = {
        "rsi": pricing._wilder_rsi(CLOSES),
        "rsiLast200": pricing._wilder_rsi(CLOSES[-200:]),
        "macd": dict(zip(("macd", "signal", "histogram"), pricing._macd(CLOSES))),
        "volatility": pricing._annualized_volatility(CLOSES, 30),
        "lookback": [pricing._indicator_lookback(p) for p in ["3mo", "6mo", "ytd", "1y", "2y", "max", None]],
        "tradedValue": [
            pricing._average_traded_value([(10, 2), (20, 3)], 2),
            pricing._average_traded_value([(10, 2), (None, 3)], 2),
            pricing._average_traded_value([(10, 2)], 2),
        ],
        "currencyUnits": [dict(zip(("iso", "perUnit"), pricing._listing_currency_unit(c))) for c in ["GBp", "ZAc", "ILA", "EUR", "USD"]],
        "nextExpiry": [
            server._next_option_expiry(["2026-09-25", "2026-10-02"], "2026-09-25"),
            server._next_option_expiry(["2026-09-25", "2026-10-02"], "2026-09-24"),
            server._next_option_expiry(["2026-09-18", "2026-09-25"], "2026-09-26"),
        ],
        "marketDate": [
            server._us_market_date(datetime.datetime(2026, 9, 25, 2, tzinfo=datetime.timezone.utc)),
            server._us_market_date(datetime.datetime(2026, 9, 25, 14, tzinfo=datetime.timezone.utc)),
        ],
        "classified": [
            envelope._classify_error_message("Yahoo Finance API error 429 for: https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL"),
            envelope._classify_error_message("request timed out"),
            envelope._classify_error_message("Yahoo Finance API error 500 for: https://x/v8/finance/chart/AAPL?_=1742900429"),
        ],
        "envelope": json.loads(envelope._envelope_tool_result(
            "get_short_momentum",
            json.dumps({"error": True, "message": "Yahoo Finance API error 429 for: https://x/v10/finance/quoteSummary/AAPL"}),
        )),
    }
    out["classified"] = [None if c is None else {"code": str(c[0]), "retryable": c[1]} for c in out["classified"]]
    gates = {}
    with patch.object(pricing.yf, "Ticker", _FakeTicker):
        for symbol in GATE_LISTINGS:
            with patch.object(pricing, "_load_completed_daily_history", AsyncMock(return_value=_prepared(symbol))):
                gates[symbol] = json.loads(asyncio.run(pricing.get_volume_gate(symbol, False)))
        pricing._tool_cache.clear() if hasattr(pricing._tool_cache, "clear") else None
        raw = asyncio.run(pricing.get_short_interest("SHRT"))
    out["gates"] = gates
    parsed = json.loads(raw)
    out["shortInterest"] = parsed["data"] if isinstance(parsed, dict) and "ok" in parsed else parsed
    return out


GATE_FIELDS = ("status", "gatePass", "adv20dTradedValueUsd", "notionalUsd", "ratio20d", "fxRate", "fxCurrency",
               "gateBasis", "gateThresholdUsd", "adv20d", "recommendedNextAction")


class TestQuoteCalculations(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.worker = _worker()
        cls.local = _local()

    def both(self):
        return (("worker", self.worker), ("local", self.local))

    def test_indicators_are_settled_and_identical(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                self.assertEqual(round(out["rsi"], 2), 61.36)
                # A year of bars leaves no trace of the seed: 200 bars agree too.
                self.assertAlmostEqual(out["rsi"], out["rsiLast200"], places=3)
                self.assertEqual(
                    (round(out["macd"]["macd"], 4), round(out["macd"]["signal"], 4), round(out["macd"]["histogram"], 4)),
                    (6.0381, 5.2354, 0.8027),
                )
                self.assertEqual(round(out["volatility"], 4), 20.2718)
        self.assertAlmostEqual(self.worker["rsi"], self.local["rsi"], places=10)
        self.assertAlmostEqual(self.worker["macd"]["histogram"], self.local["macd"]["histogram"], places=10)
        self.assertAlmostEqual(self.worker["volatility"], self.local["volatility"], places=10)

    def test_short_periods_are_widened(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                self.assertEqual(out["lookback"], ["1y", "1y", "1y", "1y", "2y", "max", "1y"])

    def test_traded_value_and_currency_units(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                self.assertEqual(out["tradedValue"], [40, None, None])
                self.assertEqual(out["currencyUnits"], [
                    {"iso": "GBP", "perUnit": 100}, {"iso": "ZAR", "perUnit": 100}, {"iso": "ILS", "perUnit": 100},
                    {"iso": "EUR", "perUnit": 1}, {"iso": "USD", "perUnit": 1},
                ])

    def test_liquidity_gate_uses_average_traded_value_in_usd(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                pence, thin = out["gates"]["LSEX.L"], out["gates"]["THIN"]
                # 20M shares x 250p = £50M a day = $62.5M at 0.8 GBP per USD.
                self.assertEqual((pence["gatePass"], pence["adv20dTradedValueUsd"], pence["fxCurrency"], pence["fxRate"]),
                                 (True, 62_500_000, "GBP", 0.8))
                # $0.5M a day fails, although the latest session is 1.0x its own average.
                self.assertEqual((thin["gatePass"], thin["adv20dTradedValueUsd"], thin["ratio20d"]), (False, 500_000, 1.0))
                self.assertEqual((thin["gateBasis"], thin["gateThresholdUsd"]), ("ADV20_TRADED_VALUE_USD", 10_000_000))
        for symbol in GATE_LISTINGS:
            with self.subTest(parity=symbol):
                worker = {k: self.worker["gates"][symbol].get(k) for k in GATE_FIELDS}
                local = {k: self.local["gates"][symbol].get(k) for k in GATE_FIELDS}
                self.assertEqual(worker, local)

    def test_same_day_option_expiry_is_skipped(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                self.assertEqual(out["nextExpiry"], ["2026-10-02", "2026-09-25", "2026-09-25"])
                self.assertEqual(out["marketDate"], ["2026-09-24", "2026-09-25"])

    def test_rate_limit_payloads_are_retryable(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                self.assertEqual(out["classified"], [
                    {"code": "RATE_LIMIT", "retryable": True},
                    {"code": "PROVIDER_TIMEOUT", "retryable": True},
                    None,
                ])
                self.assertEqual(out["envelope"]["error"]["code"], "RATE_LIMIT")
                self.assertIs(out["envelope"]["meta"]["retryable"], True)

    def test_short_interest_dates_are_iso(self) -> None:
        for name, out in self.both():
            with self.subTest(runtime=name):
                data = out["shortInterest"]
                self.assertEqual((data["dateShortInterest"], data["sharesShortPreviousMonthDate"]), ("2026-09-15", "2026-08-14"))
                self.assertEqual((data["dataDate"], data["dataDateBasis"]), ("2026-09-15", "SHORT_INTEREST_OBSERVATION"))
                self.assertIn("shortPercentOfFloat", data["unitSemantics"]["decimalRatios"])


if __name__ == "__main__":
    unittest.main()
