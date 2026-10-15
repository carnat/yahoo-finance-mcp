# V3 — Cloudflare plan and subrequests per ticker

Measured 2026-10-15 against release 2.5.33. This answers brief §7 V3: is it safe to raise the batch cap from 5 to 10 (Q3c)?

## 1. Plan tier: Workers Free (confirmed by the owner, 2026-10-15)

The owner confirmed the account is on the **Free plan**. An earlier draft of this note inferred Paid from the 10 ms Free CPU limit; **that inference was wrong**. The CPU budget for the heavy SEC tools is a separate open risk (§6).

## 2. Cloudflare limits that apply

Source: developers.cloudflare.com/workers/platform/limits, read 2026-10-15.

| Limit | Free | Paid |
|---|---|---|
| Subrequests per invocation | 50 | **10,000 (default), configurable up to 10M** via the Wrangler `limits` configuration |
| What counts | every `fetch()`, **each redirect hop**, calls to R2/KV/D1, and **Cache API `match()` / `put()` / `delete()` (same quota)** | same |
| Simultaneous connections waiting for headers | 6 | 6 (excess requests queue; they don't fail) |
| CPU per HTTP request | 10 ms | 30 s default, 5 min max |
| Wall-clock duration for HTTP requests | no limit | no limit |

**The code comment at `worker/src/yahoo-finance.ts:58–64` is out of date.** It says "1000 on paid" and "1–3 subrequests per ticker". Paid now defaults to 10,000. As measured below, per-ticker cost runs from 1 to about 39, because Cache API calls and redirect hops also count.

## 3. Method

`run.sh` bundles `worker/src/yahoo-finance.ts` for Node with esbuild and runs each of the 16 batch tools that go through `MAX_TICKERS` at 1 and 5 tickers. Each run is a fresh process, so in-memory caches start empty. The tickers are AAPL, MSFT, NVDA, AMZN and GOOGL, and the calls hit the live Yahoo and SEC endpoints. `measure.mjs` counts:
- every `fetch()`;
- responses that were redirected, counted as at least one extra hop (a chain may have more);
- stubbed `caches.default.match()` and `put()` calls. Every lookup misses, which models a cold edge cache.

subrequests = fetch + redirect hops (lower bound) + cache match + cache put. Raw rows are in `results_batch_tools.jsonl`.

**What the method misses:**
- **No paid-provider keys were set.** Finnhub, Marketaux and Alpha Vantage calls were skipped, so news and ownership tools undercount by up to about 2 per ticker when the keys are configured.
- **Warm caches lower the count.** A warm edge or memory cache turns the fetch-plus-put pair into a single `match()`.
- **Redirect chains are a lower bound.** A response that followed several hops counted as one.

## 4. Results

The marginal cost per ticker is (n=5 − n=1) / 4. The first ticker also pays the shared Yahoo crumb and cookie setup, about 2 subrequests. "Est. n=10" extrapolates linearly: n=5 + 5 × marginal.

| Tool (Worker function) | n=1 | n=5 | Per extra ticker | Est. n=10 | Free (50) at cap 5 | Free (50) at cap 10 | Paid (10,000) at cap 10 |
|---|---:|---:|---:|---:|---|---|---|
| getTechnicalIndicators, getPriceSlope, getVolumeRatio | 1 | 5 | 1 | 10 | ok | ok | ok |
| getEtfInfo, getStockInfo, getFastInfo, getAnalystConsensus, getMaPosition, getShortMomentum | 3 | 7 | 1 | 12 | ok | ok | ok |
| getPriceStats | 4 | 12 | 2 | 22 | ok | ok | ok |
| getEarningsMomentum, getAnalystUpgradeRadar | 5 | 17 | 3 | 32 | ok | ok | ok |
| getCompanyNews (default sources) | 9 | 32 | ~6 | ~61 | ok | **over** | ok |
| getFinancialRatios | 6 | 22 | 4 | 42 | ok | ok (tight) | ok |
| getCreditHealth | 8 | 32 | 6 | 62 | ok | **over** | ok |
| getPositionScoreInputs | 13 | 57 | 11 | 112 | **over** | **over** | ok |
| getCompanyNews, all 8 sources incl. company IR, SEC, newswire | 39 | 218 | ~45 | ~440 | **over** | **over** | ok |

Every tool returned success for all 5 tickers.

**Concurrency.** `getPositionScoreInputs` had up to 15 requests in flight in Node. On Workers, anything past 6 queues, so it gets slower but doesn't fail. The all-sources news call took about 54–66 s for 5 tickers. There is no wall-clock limit, but the MCP client's timeout may be the real ceiling.

## 5. Which batch paths MCP callers can actually reach

The public input schemas (`worker/src/tools.ts`) accept a ticker array, `maxItems: 5`, on only **12 tools**: `get_market_quote`, `analyze_price_performance`, `analyze_moving_average_position`, `analyze_volume_ratio`, `get_company_profile`, `get_fund_profile`, `analyze_financial_ratios`, `analyze_credit_health`, `get_analyst_rating_changes`, `analyze_earnings_momentum`, `get_company_news` and `get_market_snapshot`.

Five of the measured functions are **not reachable as batches** from MCP, because their public schema takes a single string: `getPositionScoreInputs` (57 at n=5), `getTechnicalIndicators`, `getPriceSlope`, `getShortMomentum` and `getAnalystConsensus`. `get_market_snapshot` was not measured.

## 6. Live evidence from production (2026-10-15)

One read-only call to `https://yahoo-finance-mcp.artinatw.workers.dev/mcp`:
- **Call:** `news_events` / `get_company_news`, tickers `["AAPL","MSFT"]`, sources `yahoo_finance_news, yahoo_finance_press_releases, sec, newswire, company_ir, company_ir_page`. Finnhub and Marketaux were left out to spend no paid quota.
- **Errors:** the response contains **11 × "Too many subrequests by single Worker invocation"**, all inside GlobeNewswire feed errors.
- **AAPL:** 5 items, coverage `PARTIAL`.
- **MSFT:** **0 items**. `yahoo_finance_news` and `yahoo_finance_press_releases` were reported as `PROVIDER_ERROR`, `company_ir` as `DISCOVERY_BUDGET_EXHAUSTED`, and newswire as `PROVIDER_ERROR`.
- **Envelope:** top-level `ok: true`.

So the Free plan's 50-subrequest limit is **already starving later tickers in production at today's cap of 5**, and the cause surfaces as provider errors, not as a budget state. Coverage does say `PARTIAL`, so this is not a clean false `FULL`, but the reason is wrong. This is a present-day data-integrity defect, independent of the modernization.

**Free-plan CPU (10 ms per request).** The live calls above succeeded, but it has not been verified whether the heavy SEC-parsing tools hit Cloudflare's CPU-exceeded error under load. The Q10 telemetry baseline should record the invocation outcome (`exceededCpu` / `exceededResources`) per action.

## 7. Conclusion for V3 (Free plan)

- **A cap of 10 is not safe on Free** for the reachable tools that matter:

  | Tool | n=5 | Est. n=10 |
  |---|---:|---:|
  | `analyze_credit_health` | 32 | ~62 |
  | `get_company_news` (default sources) | 32 | ~61, more with paid keys |
  | `get_company_news` (company-IR / newswire sources) | already over 50 at **n=2** in production | — |
  | `analyze_financial_ratios` | 22 | ~42, little headroom |

  Quote, profile, fund, moving-average, volume, price-performance, rating-changes and earnings-momentum tools stay at or under about 32 at n=10.
- **The budget is per invocation, not per tool.** A JSON-RPC batch (Phase 1, S-03) shares the same 50. On Free, JSON-RPC batching of tool calls is effectively unusable beyond one or two cheap calls.
- **Options (owner decision, brief §7 Q12):**
  - **(A) Upgrade to Workers Paid.** This gives 10,000 subrequests (configurable) and 30 s CPU, keeps Q3c's single cap of 10, and removes the CPU risk.
  - **(B) Stay on Free.** Replace Q3c's "one cap of 10" with **cost-aware per-tool caps**, all at or under 40 to leave headroom for paid providers and redirects. For example:
    - quote, profile, fund, MA, volume, price performance, rating changes, earnings momentum: 10;
    - financial ratios: 5;
    - credit health: 5;
    - news with default sources: 5;
    - news with any of `company_ir`, `company_ir_page` or `newswire`: 1;
    - JSON-RPC batch: 1 tool call.
  - **Either way: add a per-invocation subrequest budget guard.** Count `fetch` and Cache API calls in the request scope, stop starting new upstream work before the limit, and mark the affected sources `PROVIDER_BUDGET_EXHAUSTED` (the Q11 name) instead of `PROVIDER_ERROR`. The R3 news repair (BR-NWS-007 honest coverage) should include this, because the production evidence above is a news case.
- **Fix the stale comment** in `yahoo-finance.ts:58–64` when Q3c lands.
