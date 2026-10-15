// Usage: node measure.mjs <tool> <n>
const [tool, nStr] = process.argv.slice(2);
const n = Number(nStr);
const TICKERS = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "JPM", "XOM", "KO", "PEP"].slice(0, n);
const counts = { fetch: 0, redirected: 0, cacheMatch: 0, cachePut: 0, byHost: {} };
let inflight = 0, maxInflight = 0;
const realFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => {
  const url = typeof input === "string" ? input : input.url;
  const host = new URL(url).host;
  counts.fetch++; counts.byHost[host] = (counts.byHost[host] || 0) + 1;
  inflight++; maxInflight = Math.max(maxInflight, inflight);
  try { const r = await realFetch(input, init); if (r.redirected) counts.redirected++; return r; }
  finally { inflight--; }
};
globalThis.caches = { default: {
  match: async () => { counts.cacheMatch++; return undefined; },
  put: async () => { counts.cachePut++; },
  delete: async () => false,
} };
const yf = await import("./yf.mjs");
const ARGS = {
  getEtfInfo: [null], getStockInfo: [false], getFastInfo: [], getPriceStats: [], getAnalystConsensus: [],
  getFinancialRatios: [4, "quarterly"], getTechnicalIndicators: ["1y"], getPriceSlope: [5], getVolumeRatio: [10],
  getMaPosition: [], getCreditHealth: [], getShortMomentum: [], getEarningsMomentum: [], getAnalystUpgradeRadar: [30],
  getPositionScoreInputs: [], getCompanyNews: [10, 7], getCompanyNewsAll: [10, 7, ["yahoo_finance_news","yahoo_finance_press_releases","finnhub","marketaux","sec","newswire","company_ir","company_ir_page"]],
};
const t0 = Date.now();
let ok = null;
try {
  const { result } = await yf.withCacheScope(() => yf[tool === "getCompanyNewsAll" ? "getCompanyNews" : tool](TICKERS, ...ARGS[tool]));
  const parsed = JSON.parse(result);
  ok = Object.fromEntries(TICKERS.map((t) => [t, !(parsed[t]?.error || parsed[t]?.ok === false || parsed.error)]));
} catch (e) { ok = String(e); }
const total = counts.fetch + counts.redirected + counts.cacheMatch + counts.cachePut;
console.log(JSON.stringify({ tool, n, ms: Date.now() - t0, fetch: counts.fetch, redirectHopsAtLeast: counts.redirected,
  cacheMatch: counts.cacheMatch, cachePut: counts.cachePut, subrequests: total, maxInflight, byHost: counts.byHost, ok }));
