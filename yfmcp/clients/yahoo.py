"""yfinance client helpers.

Extracted from server.py in Phase 1 of the refactoring plan. Phase 4 unit 2 moved the valuation market inputs
(_valuation_market_inputs, the Python twin of the Worker's valuationMarketInputs in worker/src/clients/yahoo.ts) here;
since Q3g (3.0.2) they are read from Yahoo quoteSummary, as the Worker reads them.
"""

import asyncio
import json
from urllib.parse import quote as _quote

import yfinance as yf

from yfmcp import valuation as _vl
from yfmcp.clients.endpoints import YAHOO_QUOTE_SUMMARY_PREFIX


def _safe_parse(result: object, ticker: str) -> object:
    """Parse a JSON result string, returning a structured error dict on failure.

    Handles both Exception objects returned by asyncio.gather(return_exceptions=True)
    and plain error strings returned by single-ticker handlers.
    """
    if isinstance(result, Exception):
        return {"error": True, "message": str(result), "ticker": ticker}
    try:
        return json.loads(result)  # type: ignore[arg-type]
    except Exception:
        return {"error": True, "message": str(result), "ticker": ticker}


# The Worker's valuationMarketInputs URL (worker/src/clients/yahoo.ts): one quoteSummary read with these modules.
_QUOTE_SUMMARY_URL = YAHOO_QUOTE_SUMMARY_PREFIX
_MARKET_INPUT_MODULES = "price,financialData,defaultKeyStatistics,earningsTrend"


class YahooHttpError(Exception):
    """The Worker's YahooHttpError: a non-OK Yahoo answer, with the URL the Worker names (never the crumb)."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"Yahoo Finance API error {status} for: {url}")
        self.status = status


def _quote_summary_result(ticker: str, modules: str) -> dict | None:
    """Blocking: the first quoteSummary result for ``modules`` through yfinance's crumb-aware session (Q3g, V6), or
    None when Yahoo answers without one. A non-OK answer raises YahooHttpError with the Worker's text."""
    url = _QUOTE_SUMMARY_URL + _quote(ticker, safe="-_.!~*'()")
    response = yf.Ticker(ticker)._data.get(url, params={"modules": modules})
    if not 200 <= int(response.status_code) < 300:
        raise YahooHttpError(int(response.status_code), f"{url}?modules={modules}")
    data = response.json()
    results = ((data.get("quoteSummary") or {}).get("result")) if isinstance(data, dict) else None
    return results[0] if isinstance(results, list) and results and isinstance(results[0], dict) else None


async def _valuation_market_inputs(ticker: str) -> dict:
    """The Worker's valuationMarketInputs: Yahoo quoteSummary mapped to MarketInputs (Q3g, 3.0.2)."""
    def fetch() -> dict:
        result = _quote_summary_result(ticker, _MARKET_INPUT_MODULES)
        if not result:
            raise ValueError(f"No Yahoo quote summary for {ticker.upper()}")
        return _vl.market_inputs_from_quote_summary(ticker, result)

    return await asyncio.to_thread(fetch)
