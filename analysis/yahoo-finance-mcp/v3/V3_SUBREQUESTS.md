# V3 — Cloudflare plan and subrequests per ticker

Measured 2026-10-15 against release 2.5.33. This answers brief §7 V3: is it safe to raise the batch cap from 5 to 10 (Q3c)?

## 1. Plan tier: not verifiable from here

The repository and this environment hold no Cloudflare account access (no API token; the deploy secrets live in GitHub Actions). The plan has to be read from the Cloudflare dashboard: **Workers & Pages → Plans**, for the account that owns `yahoo-finance-mcp.artinatw.workers.dev`.

**Strong inference: Workers Paid.**
- The Free plan allows **10 ms of CPU per HTTP request**. Paid defaults to **30 s** and allows up to 5 min.
- The deployed Worker parses multi-megabyte SEC filings (`search_filing_text`, `extract_*`, capital structure, transcript parsing). That work cannot fit in 10 ms of CPU.
- The production smoke suite in `deploy-worker.yml` runs these tools against production and gates promotion on them.

Treat this as an inference until the dashboard is checked.

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

## 5. Conclusion for V3

- **If production is on Paid (likely):** a cap of 10 is safe for every batch tool. The worst single call is all-sources news at about 440 subrequests, 4.4% of the 10,000 default.
- **The Phase 1 JSON-RPC batch cap multiplies that.** 10 entries per POST, each a 10-ticker all-sources news call, is about 4,400 subrequests in one invocation. That's still under 10,000 but leaves little margin once paid providers and cold redirect chains are added. **Recommendation:** keep the Q3c ticker cap at 10, and either set the JSON-RPC batch cap to 5 or add a per-invocation subrequest budget guard that returns a typed failure (`PROVIDER_BUDGET_EXHAUSTED`) before Cloudflare's hard error. Set `limits` explicitly in `wrangler.toml` so the budget is visible in config.
- **If production were on Free:** three tools already exceed 50 subrequests at today's cap of 5 (`getPositionScoreInputs`, all-sources news, and default news close behind at 32). A cap of 10 would break two more. The same Free plan's 10 ms CPU limit makes the SEC tools impossible regardless, which is why Free is implausible.
- **Fix the stale comment** in `yahoo-finance.ts:58–64` when Q3c lands in R4.
