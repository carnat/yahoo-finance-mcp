// Parity harness: runs the Worker side of every case in one Node process.
//
// Usage: node runner.mjs <bundleUrl> <casesJsonPath>
// casesJson: { cases: [{ name, module, fn, args, js, routes, isolate }] }
//   module/fn/args  call bundle[module][fn](...args)
//   js              body of an async function (m, args, fx) for setups a plain call can't express
//   routes          { "<url or url prefix ending in *>": { status, body, json, headers } } served to fetch();
//                   any fetch not matched throws UNSTUBBED_FETCH, so a case can never reach the network
//   isolate         load a fresh copy of the bundle for this case (module-level caches start empty)
// Prints one JSON line: { "<name>": { ok, value } | { ok: false, error } }.
import fs from "node:fs";

const [bundleUrl, casesPath] = process.argv.slice(-2);
const { cases } = JSON.parse(fs.readFileSync(casesPath, "utf8"));
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

let activeRoutes = {};
globalThis.fetch = async (input) => {
  const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
  let route = activeRoutes[url];
  if (!route) {
    const prefix = Object.keys(activeRoutes)
      .filter((k) => k.endsWith("*") && url.startsWith(k.slice(0, -1)))
      .sort((a, b) => b.length - a.length)[0];
    route = prefix ? activeRoutes[prefix] : undefined;
  }
  if (!route) throw new Error(`UNSTUBBED_FETCH ${url}`);
  const body = route.json !== undefined ? JSON.stringify(route.json) : route.body ?? "";
  return new Response(body, { status: route.status ?? 200, headers: route.headers ?? {} });
};
// No edge cache: the Worker code treats a missing caches.default as "no cache".
delete globalThis.caches;

function plain(value) {
  // Strings that hold JSON (most tools return JSON text) are compared as parsed JSON.
  if (typeof value === "string") {
    try { return JSON.parse(value); } catch { return value; }
  }
  return value === undefined ? null : value;
}

const shared = await import(bundleUrl);
const results = {};
let n = 0;
for (const c of cases) {
  activeRoutes = c.routes ?? {};
  try {
    const m = c.isolate ? await import(`${bundleUrl}?isolate=${++n}`) : shared;
    let value;
    if (c.js) value = await new AsyncFunction("m", "args", "fx", c.js)(m, c.args ?? [], c.fx ?? {});
    else value = await m[c.module][c.fn](...(c.args ?? []));
    results[c.name] = { ok: true, value: plain(value) };
  } catch (e) {
    results[c.name] = { ok: false, error: String(e && e.message ? e.message : e) };
  }
}
process.stdout.write(JSON.stringify(results) + "\n");
