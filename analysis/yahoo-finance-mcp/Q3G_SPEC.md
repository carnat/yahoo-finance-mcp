# Q3g: one `MarketInputs` contract from the Yahoo client (release 3.0.2)

Brief §3 Phase 4: "the Q3g `MarketInputs` release ships between units 2 and 3"; §7 Q3g (owner-approved CHANGE): "replace `.info` vs raw quoteSummary divergence with one normalized shared-client `MarketInputs` contract consumed by both runtimes"; V6: "Q3g needs Python to read Yahoo quoteSummary directly".

## Today (after unit 2, 3.0.1)

| | Worker | Python |
|---|---|---|
| Fetch | `valuationMarketInputs` (`clients/yahoo.ts`): `yGet` quoteSummary `price,financialData,defaultKeyStatistics,earningsTrend` on `query1` | `_valuation_market_inputs` (`yfmcp/clients/yahoo.py`): yfinance `.info` plus `revenue_estimate` / `earnings_estimate` DataFrames |
| Mapping | `marketInputsFromQuoteSummary` (`valuation.ts`) | `market_inputs_from_yfinance` (`yfmcp/valuation.py`). The Worker twin `market_inputs_from_quote_summary` exists but only tests call it (the "unused helper") |
| Price | `price.regularMarketPrice` | `info.currentPrice`, else `info.regularMarketPrice` |
| Estimates | every `earningsTrend.trend[]` row with a string `period` | yfinance's estimate rows (derived from `earningsTrend`, joined by period) |
| Unknown symbol | Yahoo 404 -> `Yahoo Finance API error 404 for: <query1 URL>` | a near-empty `.info` -> `No Yahoo quote summary for X` (2.26.5, F57) |

## Change (Python only; the Worker already consumes the client)

1. `yfmcp/clients/yahoo.py` gains `_quote_summary_result(ticker, modules)`: the Worker's quoteSummary URL (`query1`, `modules=`), read through yfinance's crumb-aware session. A non-2xx answer raises `YahooHttpError` with the Worker's text, `Yahoo Finance API error <status> for: <URL without crumb>` (the crumb never appears in a message). A body without `quoteSummary.result[0]` returns `None`.
2. `_valuation_market_inputs` reads that result and maps it with `market_inputs_from_quote_summary`. No result: `ValueError("No Yahoo quote summary for X")`, the Worker's text.
3. Removed after fixtures prove the replacement: `market_inputs_from_yfinance`, `empty_market_inputs` (only it called it) and `_estimate_rows`. The test that pinned the two mappings equal is replaced by a test that drives `_valuation_market_inputs` from a recorded quoteSummary fixture and compares it with the Worker's mapping of the same fixture.

No new field, status, warning code or enum. Output changes are limited to Python values now read from the Worker's source: the price (`regularMarketPrice`), the estimate rows, and the unknown-symbol message (now the Worker's 404 text). Consumers: `get_valuation_snapshot`, `compare_peer_valuations`, `get_historical_valuation_context` (ADR ratio).

## Verification

- `scripts/test_valuation.py`: the end-to-end runtime comparison (snapshot and peers) already feeds both runtimes the same quoteSummary fixtures; a new test drives the Python client through a stubbed yfinance session (known ticker equals the Worker's mapping, 404 gives the Worker's text, an empty result gives `No Yahoo quote summary`).
- Live check before release: Python and the deployed Worker give the same AAPL snapshot multiples and the same ZZZZQQ message.
- Full CI list; live: AAPL and TSM snapshot, AAPL/MSFT peers, ZZZZQQ snapshot.
