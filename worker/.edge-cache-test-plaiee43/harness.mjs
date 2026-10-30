
const [miniflareUrl, scriptPath] = process.argv.slice(-2);
const { Miniflare, convertV4MiniflareOptions } = await import(miniflareUrl);
const now = 1790000000;
const counts = {};
const count = (key) => { counts[key] = (counts[key] ?? 0) + 1; };

async function outbound(req) {
  const u = new URL(req.url);
  const sym = decodeURIComponent(u.pathname.split("/").pop());
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.includes("/fundamentals-timeseries/")) {
    count(`timeseries:${sym}`);
    if (sym === "EMPTY") {
      return Response.json({ timeseries: { result: [{ meta: { symbol: [sym], type: ["annualTotalRevenue"] }, timestamp: [] }], error: null } });
    }
    const types = (u.searchParams.get("type") ?? "").split(",").slice(0, 3);
    return Response.json({ timeseries: { error: null, result: types.map((t) => ({
      meta: { symbol: [sym], type: [t] },
      timestamp: [now],
      [t]: [{ asOfDate: "2025-09-27", periodType: "12M", currencyCode: "USD", reportedValue: { raw: 1000, fmt: "1k" } }],
    })) } });
  }
  if (u.pathname.includes("/quoteSummary/")) {
    const modules = u.searchParams.get("modules") ?? "";
    count(`quoteSummary:${modules}:${sym}`);
    const result = {};
    for (const m of modules.split(",")) {
      result[m] = m === "price" ? { regularMarketPrice: { raw: 101 }, currency: "USD", regularMarketTime: now }
        : m === "institutionOwnership" ? { ownershipList: [{ organization: "Vanguard", pctHeld: { raw: 0.08 }, position: { raw: 100 }, value: { raw: 1000 }, reportDate: { raw: now, fmt: "2026-06-30" } }] }
        : m === "recommendationTrend" ? { trend: [{ period: "0m", strongBuy: 5, buy: 10, hold: 3, sell: 0, strongSell: 0 }] }
        : {};
    }
    return Response.json({ quoteSummary: { result: [result], error: null } });
  }
  return new Response("{}", { status: 404 });
}

const worker = (name) => ({
  name, modules: true, scriptPath, compatibilityDate: "2026-09-21", compatibilityFlags: ["nodejs_als"],
  bindings: { TOOL_MODE: "grouped", MCP_ENVELOPE_V2: "true" }, outboundService: outbound,
});
const mf = new Miniflare(convertV4MiniflareOptions({ workers: [worker("first"), worker("second")] }));
let id = 0;
const headers = { first: {}, second: {} };
let current = "first";
async function call(isolate, group, action, params, label) {
  const res = await isolate.fetch("https://x/mcp", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: ++id, method: "tools/call", params: { name: group, arguments: { action, params } } }),
  });
  if (label) headers[current][label] = res.headers.get("x-yahoo-cache");
  return (await res.json()).result.structuredContent;
}
async function run(isolate) {
  const out = {};
  out.statement = await call(isolate, "stock_fundamentals", "get_financial_statement", { ticker: "AAPL", financial_type: "income_stmt" }, "statement");
  await new Promise((r) => setTimeout(r, 1100));  // the timeseries window end moves to the next second
  out.statementAgain = await call(isolate, "stock_fundamentals", "get_financial_statement", { ticker: "AAPL", financial_type: "income_stmt" }, "statementAgain");
  out.empty = await call(isolate, "stock_fundamentals", "get_financial_statement", { ticker: "EMPTY", financial_type: "income_stmt" }, "empty");
  // Two concurrent requests in one isolate: each header counts only its own reads.
  [out.holders, out.recommendations] = await Promise.all([
    call(isolate, "stock_fundamentals", "get_ownership_holders", { ticker: "AAPL", holder_type: "institutional_holders" }, "holders"),
    call(isolate, "analyst_data", "get_analyst_recommendations", { ticker: "AAPL", recommendation_type: "recommendations" }, "recommendations"),
  ]);
  out.quote = await call(isolate, "stock_pricing", "get_market_quote", { ticker: "AAPL" }, "quote");
  out.health = await (await isolate.fetch("https://x/health")).json();
  return out;
}
const first = await run(await mf.getWorker("first"));
const afterFirst = { ...counts };
current = "second";
const second = await run(await mf.getWorker("second"));
await mf.dispose();
console.log(JSON.stringify({ first, second, afterFirst, afterSecond: counts, headers }));
