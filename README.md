# Yahoo Finance MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server that gives any MCP-compatible AI client (Claude, Cursor, VS Code Copilot, etc.) direct access to live financial data from Yahoo Finance.

36 tools cover the full research workflow — from a quick price check to earnings forecasts, SEC filings, technical indicators, options flow, geographic revenue intelligence, and market screening — without leaving your chat window.

## Demo

![MCP Demo](assets/demo.gif)

## MCP Tools

> **Token-efficiency tip:** `get_stock_info` now returns ~30 key fields by default — no need to enumerate fields for typical queries. Prefer `get_fast_info` for pure price/volume lookups (~20 fields, 85–90% fewer tokens). Use `get_financial_ratios` instead of fetching full financial statements when you only need computed ratios. Use `get_analyst_consensus` instead of `get_recommendations` for a ready-made summary. Use the pre-computed signal tools (`get_price_slope`, `get_volume_ratio`, `get_ma_position`, `get_earnings_momentum`, `get_short_momentum`, `get_credit_health`, `get_options_flow_summary`, `get_put_hedge_candidates`, `get_analyst_upgrade_radar`) instead of fetching raw data and computing signals yourself. Use `get_position_score_inputs` to aggregate all T1/T2/T4/T5 scoring inputs in one call instead of chaining five separate tools.

The server exposes the following tools through the Model Context Protocol:

### Price & Market Data

| Tool | Description |
|------|-------------|
| `get_fast_info` | **Lightweight.** Get current price, market cap, 52-week range, moving averages, and volume (~20 fields). Also includes pre-market/after-hours prices when available. Prefer this over `get_stock_info` for price lookups. |
| `get_historical_stock_prices` | Get historical OHLCV data with customizable period, interval, and optional `columns` filter (e.g. `["Close"]` to return only closing prices). |
| `get_stock_info` | **Fundamentals.** Returns ~30 key fields by default: identity (shortName, sector, industry, country), price (currentPrice, previousClose, marketCap, enterpriseValue), valuation (trailingPE, forwardPE, priceToBook, EV/EBITDA), earnings (EPS, growth rates), margins (gross/operating/profit, ROE, ROA), dividends, analyst ratings, and `longBusinessSummary`. Pass `include_all=true` for the full 120+ field payload. The `fields` parameter accepts exact field names **or** group aliases (`"identity"`, `"pricing"`, `"valuation"`, `"earnings"`, `"margins"`, `"dividends"`, `"analyst"`, `"description"`). For ETFs/funds, use `get_etf_info` instead. |
| `get_etf_info` | Get ETF or mutual fund data: identity (shortName, category, fundFamily), pricing (navPrice, previousClose, dayHigh/Low, volume), AUM/costs (totalAssets, yield, expenseRatio, ytdReturn, beta3Year), 52-week stats, moving averages, top-10 holdings, and sector weights. Use for fund tickers: SPY, QQQ, VTI, ARKK, VFIAX, etc. |
| `get_price_stats` | Get pre-computed price statistics: % change today, % distance from 52-week high/low and moving averages, 30-day annualized volatility, and CAGR over 1y/3y/5y. Works with index tickers (e.g. `^VIX`, `^GSPC`). |
| `get_stock_actions` | Get stock dividends and splits history. |
| `get_yahoo_finance_news` | Get latest news articles for a stock. |
| `get_short_interest` | Get short interest metrics: short % of float, shares short, days-to-cover ratio, prior-month comparison, and float shares. Data is reported bi-monthly by exchanges. |
| `get_short_momentum` | Get short interest with pre-computed momentum: MoM delta, direction (RISING/FALLING/FLAT), squeeze risk (HIGH/MODERATE/LOW), and a critical-short flag. Single ticker only. |
| `get_overnight_quote` | Get overnight trading data (20:00–04:00 ET). Returns overnightPrice, overnightHigh/Low/Open/Volume, previousClose, gapPct, gapDirection, dataSource (`EXCHANGE` for crypto/futures, `OTC_INDICATIVE` for equities), isBlueOceanWindow, isStale, dataAgeHours, and fallback flag. |

### Financials & Ratios

| Tool | Description |
|------|-------------|
| `get_financial_statement` | Get income statement, balance sheet, or cash flow (annual/quarterly). Also supports `ttm_income_stmt` and `ttm_cashflow` for trailing-twelve-months data (1 column vs 4 — ~75% fewer tokens). Accepts an optional `line_items` filter to return only specific rows. |
| `get_financial_ratios` | **Pre-computed.** Get key valuation, profitability, and leverage ratios: P/E (trailing/forward), PEG, P/S, P/B, EV/EBITDA, EV/Revenue, gross/operating/net margins, ROE, ROA, debt/equity, current ratio, quick ratio, FCF yield, and dividend yield. |
| `get_credit_health` | **Pre-computed.** Get credit/leverage metrics: Net Debt/EBITDA, interest coverage ratio, debt tier classification, and a credit stress flag. Single ticker only. |
| `get_holder_info` | Get major holders, institutional holders, mutual funds, or insider transactions. |

### Analyst & Forecasts

| Tool | Description |
|------|-------------|
| `get_analyst_consensus` | Get a compact analyst consensus: price targets (current/low/high/mean/median + % upside) and recommendation breakdown (strong buy/buy/hold/sell/strong sell counts + dominant rating). |
| `get_earnings_analysis` | Get all analyst forward-looking data in one call: EPS estimates, revenue estimates, EPS trend, earnings history (beat/miss), and growth estimates. Replaces 5 separate calls. |
| `get_recommendations` | Get raw analyst recommendations or upgrades/downgrades history. |
| `get_calendar` | Get the next earnings date with EPS/revenue guidance and upcoming ex-dividend/pay dates. |
| `get_earnings_momentum` | **Pre-computed.** Get earnings revision momentum: EPS estimate revisions over 7/30/90 days, revision direction (UPGRADING/STABLE/DOWNGRADING), momentum flag (STRONG/POSITIVE/NEGATIVE/COLLAPSING), beat rate, average surprise %, and current beat streak. Single ticker only. |
| `get_analyst_upgrade_radar` | Get recent analyst rating changes with pre-computed signal classification (UPGRADE/DOWNGRADE/MAINTAIN), price target direction, net sentiment score, and a mixed-signal flag. Supports batch. Args: `days_back` (default: 30). |

### Options Data

| Tool | Description |
|------|-------------|
| `get_option_expiration_dates` | Get available options expiration dates. |
| `get_option_chain` | Get options chain for a specific expiration date and type (calls/puts). Supports `min_strike`, `max_strike`, and `in_the_money_only` filters to reduce a 200-row chain to ~20 near-the-money rows. |
| `get_options_flow_summary` | **Pre-computed.** Get options flow summary for the nearest liquid expiry: put/call ratio, P/C sentiment (PUT_HEAVY/NEUTRAL/CALL_HEAVY), ATM IV, IV percentile, IV flag, max pain strike, and highest OI call/put strikes. Optional `expiry_hint` (YYYY-MM-DD). Single ticker only. |
| `get_put_hedge_candidates` | Get pre-filtered OTM put options within a configurable strike range and budget. Args: `otm_pct_min` (default: 8), `otm_pct_max` (default: 12), `budget_usd` (default: 500), `expiry_after` (YYYY-MM-DD). Single ticker only. |
| `get_options_flow_scan` | **Structured event-window scan.** Options flow snapshot for a binary event window. Returns pcRatio, ivPctile, putVolVs10dAvg, putVolTrend (INCREASING/STABLE/DECREASING), maxPainStrike, bracket (UPPER/MID/LOWER), and a formatted block for session output. Prior window-label readings are cached 72h server-side to enable trend computation (e.g. T-14 → T-7 → T-2). Args: `ticker`, `window_label` (free-form, e.g. `"T-14"`, `"pre-earnings"`). |

### Filings

| Tool | Description |
|------|-------------|
| `get_filing_data` | Retrieve structured XBRL-tagged EDGAR facts for known GAAP line items and geographic/segment revenue. Use this first for filing-derived metrics. Args: `ticker`, `fact_type`, optional `region`, `filing_type` (`10-K`/`10-Q`), `period` (`latest`/`all`). |
| `search_filing_text` | Search narrative filing HTML text or retrieve section-context snippets when a fact is not XBRL-tagged. Args: `ticker`, optional `search_terms`, `section_hint`, `filing_type`, `accession_number`, `context_chars`, `return_tables`. |
| `get_geographic_revenue` | **Deprecated.** Use `get_filing_data` with `fact_type="geographic_revenue"`. |
| `get_sec_filings` | **Deprecated.** Internal-only legacy helper; use `search_filing_text` (or `get_filing_data` first). |
| `get_filing_text_search` | **Deprecated.** Use `search_filing_text`. |
| `get_filing_document` | **Deprecated.** Use `search_filing_text` with `section_hint`. |

### Discovery

| Tool | Description |
|------|-------------|
| `search_ticker` | Search by company name, partial name, or ISIN to resolve matching ticker symbols. Solves the "I know the company but not its ticker" problem. |
| `screen_stocks` | Screen the market using predefined criteria. Available screeners: `aggressive_small_caps`, `day_gainers`, `day_losers`, `growth_technology_stocks`, `most_actives`, `most_shorted_stocks`, `small_cap_gainers`, `undervalued_growth_stocks`, `undervalued_large_caps`, `conservative_foreign_funds`, `high_yield_bond`, `portfolio_anchors`, `solid_large_growth_funds`, `solid_midcap_growth_funds`, `top_mutual_funds`. |

### Position Management

| Tool | Description |
|------|-------------|
| `get_price_target_bracket` | Compute EQF bracket relative to an IO price target. EQF = currentPrice / io_pt × 100. Brackets: ≤75% STRONG_BUY \| 75–90% ACCEPTABLE \| 90–100% CAUTION \| >100% AVOID. Tags: <40% SPECULATIVE \| 40–79% LONG \| 80–99% NEAR \| ≥100% INVERTED. Returns currentPrice, ioPt, eqfPct, bracket, tag, invertedFlag, dataDate. Args: `ticker`, `io_pt`. |
| `get_position_score_inputs` | Aggregate all T1, T2, T4, and T5 position-scoring inputs in a single call (6 parallel data fetches). Returns t1_inputs (analyst sentiment), t2_inputs (price vs 52-week range), t4_inputs (earnings momentum), t5_inputs (technical indicators), and dataDate. |
| `get_volume_gate` | DC Section 6.2 Volume Gate: checks whether regularMarketVolume ≥ 0.5 × 20-day ADV. Returns currency, fxRate, lastVolume, adv10d/20d/90d, ratio20d, gatePass, dataDate, and note. Set `foreign_exchange=true` for DC-80 FX gate (daily notional converted to USD vs $10M threshold). |

### Technical Analysis

| Tool | Description |
|------|-------------|
| `get_technical_indicators` | Get pre-computed RSI-14 (Wilder smoothing) and MACD (12, 26, 9) from historical daily prices. Use for momentum and oversold/overbought screening without fetching raw OHLCV history. Supports a configurable `period` parameter for lookback depth. |
| `get_price_slope` | **Pre-computed.** Get N-day price slope (% change) and direction (UP/DOWN/FLAT) for one or more tickers. Args: `days` (default: 5). Supports batch. |
| `get_volume_ratio` | **Pre-computed.** Get last-session volume vs 10-day and 90-day average volume ratios, with a volumeFlag (HIGH/NORMAL/LOW). Args: `period` (default: 10). Supports batch. |
| `get_ma_position` | **Pre-computed.** Get price position vs 50DMA and 200DMA with % distance and trend classification (BULLISH/BEARISH/MIXED). Supports batch. |

## Data Availability Notes

| Data Point | Available? | Details |
|-----------|-----------|---------|
| Short interest % of float | ✅ Yes | Available via `get_short_interest`. Sourced from yfinance `.info`. |
| Short interest momentum | ✅ Yes | Available via `get_short_momentum`. Includes MoM delta, direction, squeeze risk, and flag. |
| Pre-market / after-hours prices | ✅ Yes (intermittent) | Included in `get_fast_info` when Yahoo Finance provides extended-hours data. Also available via `get_stock_info` with `fields=["preMarketPrice", "postMarketPrice"]`. |
| RSI / MACD indicators | ✅ Yes | Computed server-side via `get_technical_indicators`. |
| Price slope / trend direction | ✅ Yes | Available via `get_price_slope` (N-day % change + UP/DOWN/FLAT). |
| Volume spike detection | ✅ Yes | Available via `get_volume_ratio` (ratio vs 10d/90d avg + HIGH/NORMAL/LOW flag). |
| Moving average position | ✅ Yes | Available via `get_ma_position` (% vs 50DMA/200DMA + BULLISH/BEARISH/MIXED). |
| Credit/leverage health | ✅ Yes | Available via `get_credit_health` (Net Debt/EBITDA, interest coverage, debt tier, stress flag). |
| Earnings revision momentum | ✅ Yes | Available via `get_earnings_momentum` (7/30/90d EPS revisions, beat rate, beat streak). |
| Options flow summary | ✅ Yes | Available via `get_options_flow_summary` (P/C ratio, IV percentile, max pain, highest OI strikes). |
| OTM put hedge candidates | ✅ Yes | Available via `get_put_hedge_candidates` (filtered by OTM %, budget, expiry). |
| Analyst upgrade/downgrade signals | ✅ Yes | Available via `get_analyst_upgrade_radar` (rating changes with signal classification, net sentiment). |
| Index tickers (^VIX, ^GSPC) | ✅ Yes | Supported by `get_fast_info`, `get_price_stats`, and `get_technical_indicators`. Note: `get_price_stats` CAGR values for volatility indices like `^VIX` may not be meaningful as investment return metrics. |
| Geographic revenue breakdown | ✅ Yes (partial) | Use `get_filing_data` with `fact_type="geographic_revenue"` first (XBRL companyconcept). If `confidence=NOT_DISCLOSED`, use `search_filing_text` for narrative table/section retrieval. |

## Real-World Use Cases

With this MCP server, you can use Claude to:

### Stock Analysis

- **Price Analysis**: "Show me the historical closing prices for AAPL over the last 6 months." *(use `columns=["Close"]` to reduce tokens)*
- **Quick Price Lookup**: "What is Apple's current price, market cap, and 52-week range?" *(use `get_fast_info`)*
- **Financial Health**: "Get the quarterly balance sheet for Microsoft."
- **TTM Financials**: "Show me Apple's trailing-twelve-months income statement." *(use `ttm_income_stmt` for a single compact column)*
- **Key Ratios**: "What are Tesla's P/E ratio, profit margins, and debt-to-equity?" *(use `get_financial_ratios`)*
- **Price Statistics**: "How far is NVIDIA from its 52-week high, and what is its 30-day volatility?" *(use `get_price_stats`)*
- **Trend Analysis**: "Compare the quarterly income statements of Amazon and Google."
- **Cash Flow Analysis**: "Show me the annual cash flow statement for NVIDIA."
- **Price Slope**: "Which of these tickers have been trending up over the last 5 days?" *(use `get_price_slope` with a batch list)*
- **Volume Spike**: "Is there unusual trading volume in TSLA today?" *(use `get_volume_ratio`)*
- **MA Position**: "Is AAPL trading above its 50DMA and 200DMA?" *(use `get_ma_position`)*
- **Overnight Gap**: "What was the overnight gap for BTC-USD and is the session data stale?" *(use `get_overnight_quote`)*
- **Credit Health**: "How leveraged is Boeing, and is there a credit stress flag?" *(use `get_credit_health`)*

### Market Research

- **News Analysis**: "Get the latest news articles about Meta Platforms."
- **Institutional Activity**: "Show me the institutional holders of Apple stock."
- **Insider Trading**: "What are the recent insider transactions for Tesla?"
- **Options Analysis**: "Get the in-the-money calls for SPY expiring 2024-06-21." *(use `in_the_money_only=True`)*
- **Options Flow**: "What is the put/call ratio and max pain strike for SPY?" *(use `get_options_flow_summary`)*
- **Options Event Scan**: "Run a T-7 options flow scan for ASTS ahead of the binary event." *(use `get_options_flow_scan`)*
- **Put Hedge**: "Find 8–12% OTM puts for AAPL expiring after June with a budget under $400 per contract." *(use `get_put_hedge_candidates`)*
- **Analyst Coverage**: "What is the analyst consensus price target for Amazon?" *(use `get_analyst_consensus`)*
- **Analyst Upgrades**: "Have any analysts upgraded or downgraded NVDA in the last 30 days?" *(use `get_analyst_upgrade_radar`)*
- **Earnings Outlook**: "What are the EPS and revenue estimates for Apple for the next two quarters?" *(use `get_earnings_analysis`)*
- **Earnings Momentum**: "Has analyst EPS consensus for MSFT been revised up or down over the last 30 days?" *(use `get_earnings_momentum`)*
- **Short Squeeze**: "What is the short squeeze risk for GameStop?" *(use `get_short_momentum`)*
- **Calendar**: "When is Microsoft's next earnings date and ex-dividend date?" *(use `get_calendar`)*
- **SEC Filing GAAP Fact**: "Get Corning's latest capex from SEC filings." *(use `get_filing_data` with `fact_type="capex"`)*
- **Geographic Revenue**: "What percentage of GLW revenue comes from China?" *(try `get_filing_data` first, then `search_filing_text` only if not XBRL-tagged)*

### Position Management

- **EQF Bracket**: "My IO price target for ASTS is $28. Is the current price in a buy zone?" *(use `get_price_target_bracket` with `io_pt=28`)*
- **Position Score**: "Pull all scoring inputs for NVDA to evaluate my T1/T2/T4/T5 scores." *(use `get_position_score_inputs`)*
- **Volume Gate**: "Does ASTS pass the DC volume gate today?" *(use `get_volume_gate`)*

### Discovery & Screening

- **Ticker Search**: "What is the ticker symbol for LVMH?" *(use `search_ticker`)*
- **Market Screening**: "Show me today's top gainers." *(use `screen_stocks` with `day_gainers`)*
- **Sector Screening**: "Find undervalued large-cap stocks." *(use `screen_stocks` with `undervalued_large_caps`)*
- **Most Active**: "Which stocks have the highest trading volume today?" *(use `screen_stocks` with `most_actives`)*

### Investment Research

- "Create a comprehensive analysis of Microsoft's financial health using their latest quarterly financial statements."
- "Compare the dividend history and stock splits of Coca-Cola and PepsiCo."
- "Analyze the institutional ownership changes in Tesla over the past year."
- "Generate a report on the options market activity for Apple stock with expiration in 30 days."
- "Summarize the latest analyst upgrades and downgrades in the tech sector over the last 6 months."
- "Find growth technology stocks and show their key financial ratios."

## Requirements

- Python 3.11 or higher
- Dependencies as listed in `pyproject.toml`, including:
  - mcp
  - yfinance
  - pandas
  - pydantic
  - and other packages for data processing

## Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/carnat/yahoo-finance-mcp.git
   cd yahoo-finance-mcp
   ```

2. Create and activate a virtual environment and install dependencies:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   uv pip install -e .
   ```

## Usage

### Development Mode

You can test the server with MCP Inspector by running:

```bash
uv run server.py
```

This will start the server and allow you to test the available tools.

### Integration with Claude for Desktop

To integrate this server with Claude for Desktop:

1. Install Claude for Desktop to your local machine.
2. Install VS Code to your local machine. Then run the following command to open the `claude_desktop_config.json` file:
   - MacOS: `code ~/Library/Application\ Support/Claude/claude_desktop_config.json`
   - Windows: `code $env:AppData\Claude\claude_desktop_config.json`

3. Edit the Claude for Desktop config file, located at:
   - macOS: 
     ```json
     {
       "mcpServers": {
         "yfinance": {
           "command": "uv",
           "args": [
             "--directory",
             "/ABSOLUTE/PATH/TO/PARENT/FOLDER/yahoo-finance-mcp",
             "run",
             "server.py"
           ]
         }
       }
     }
     ```
   - Windows:
     ```json
     {
       "mcpServers": {
         "yfinance": {
           "command": "uv",
           "args": [
             "--directory",
             "C:\\ABSOLUTE\\PATH\\TO\\PARENT\\FOLDER\\yahoo-finance-mcp",
             "run",
             "server.py"
           ]
         }
       }
     }
     ```

   - **Note**: You may need to put the full path to the uv executable in the command field. You can get this by running `which uv` on MacOS/Linux or `where uv` on Windows.

4. Restart Claude for Desktop

### Remote MCP (Replit / Claude.ai)

If you deploy `main.py` to Replit (or any public host), the server exposes a Streamable HTTP endpoint at `/mcp`.

**Claude.ai** (Settings → Integrations → Add integration):
```
https://<your-replit>.repl.co/mcp
```

**Claude Desktop** (`claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "yahoo-finance": {
      "url": "https://<your-replit>.repl.co/mcp"
    }
  }
}
```

## License

MIT

