"""yfinance client helpers.

Extracted from server.py in Phase 1 of the refactoring plan. Phase 4 unit 2 moved the valuation market inputs
(_valuation_market_inputs, the Python twin of the Worker's valuationMarketInputs in worker/src/clients/yahoo.ts) here.
"""

import asyncio
import json
from typing import Any

import yfinance as yf

from yfmcp import valuation as _vl


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


def _estimate_rows(company: Any, attr: str) -> list[dict]:
    try:
        df = getattr(company, attr)
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    df = df.reset_index()
    df.columns = ["period", *[str(c) for c in df.columns[1:]]]
    return df.to_dict(orient="records")


async def _valuation_market_inputs(ticker: str) -> dict:
    def fetch() -> dict:
        company = yf.Ticker(ticker)
        info = company.info or {}
        # yfinance answers an unknown symbol with a near-empty info dict; with no quote type and no name there is no
        # quote summary, as the Worker's empty result says (2.26.5, F57).
        if not info or not any(info.get(k) for k in ("quoteType", "shortName", "longName")):
            raise ValueError(f"No Yahoo quote summary for {ticker.upper()}")
        return _vl.market_inputs_from_yfinance(ticker, info, _estimate_rows(company, "revenue_estimate"), _estimate_rows(company, "earnings_estimate"))

    return await asyncio.to_thread(fetch)
