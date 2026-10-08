#!/usr/bin/env python3
"""Yahoo press releases after Yahoo retired its press-releases tab endpoint (2.5.33), in both runtimes.

/xhr/ncp?queryRef=pressRelease answers HTTP 404 for every ticker, and yfinance's get_news(tab=...) returns an empty
list from the same endpoint. Both runtimes now read Yahoo's news search feed and keep, as press releases, the
company's own wire releases: a press-release wire publisher and a headline that leads with the company's name or
ticker. AEHR's ACCESS Newswire Sonoma order is kept; QuickLogic's PR Newswire release naming Aehr and an MT Newswires
story are not. Offline: the feed is mocked.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

NOW = int(time.time()) - 3600
NEWS = [
    {"title": "Aehr Receives Follow-On Sonoma(TM) Production Orders for Lead Hyperscale Customer's Next-Generation AI Processor",
     "publisher": "ACCESS Newswire", "link": "https://finance.yahoo.com/a.html", "providerPublishTime": NOW, "type": "STORY"},
    {"title": "QuickLogic Announces Participation in 18th Annual CEO Investor Summit with Aehr Test Systems",
     "publisher": "PR Newswire", "link": "https://finance.yahoo.com/b.html", "providerPublishTime": NOW - 60, "type": "STORY"},
    {"title": "Aehr Test Systems Receives $6 Million Order for AI Chip Testing Equipment",
     "publisher": "MT Newswires", "link": "https://finance.yahoo.com/c.html", "providerPublishTime": NOW - 120, "type": "STORY"},
    {"title": "AEHR vs. CAMT: Which Semiconductor Equipment Stock Is the Better Bet?",
     "publisher": "Zacks", "link": "https://finance.yahoo.com/d.html", "providerPublishTime": NOW - 180, "type": "STORY"},
]
INFO = {"shortName": "Aehr Test Systems, Inc.", "longName": "Aehr Test Systems, Inc.", "exchange": "NCM"}
FIELDS = ("title", "source", "sourceType", "originalSource", "issuer", "publishedAt")


def _retrieved() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _python(feed: str) -> dict:
    import server as srv

    class _Ticker:
        info = INFO

    with patch("server.yf") as mock_yf, patch("server._yahoo_search_news", return_value=NEWS):
        mock_yf.Ticker.return_value = _Ticker()
        items, warnings, used, diagnostics = asyncio.run(srv._collect_yahoo_events(
            "AEHR", retrieved_at=_retrieved(), max_results=10, feed=feed, include_diagnostics=True))
    return {"items": [{k: i.get(k) for k in FIELDS} for i in items], "used": used, "rejectionCounts": diagnostics["rejectionCounts"],
            "method": diagnostics.get("method")}


_ENTRY = 'export { collectYahooEvents } from "{SRC}/yahoo-finance.ts";\nexport { setWorkerEnv } from "{SRC}/response.ts";\n'
_HARNESS = r"""
const [bundleUrl, fixturesPath] = process.argv.slice(-2);
const m = await import(bundleUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
m.setWorkerEnv({});
const requested = [];
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  requested.push(u.toString());
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.endsWith("/v1/finance/search")) return Response.json({ news: f.news });
  return new Response("not found", { status: 404 });
};
// The identity the Worker builds from the same profile (yahooNewsIdentityFor) as Python's from INFO.
const identity = { status: "RESOLVED", companyName: "Aehr Test Systems, Inc.", aliases: ["aehr test systems inc", "aehr test systems"], acronyms: [], exchange: "NCM" };
const out = {};
for (const feed of ["press_releases", "news"]) {
  const r = await m.collectYahooEvents("AEHR", 10, new Date().toISOString(), identity, "", "", 30, feed);
  out[feed] = { items: r.items.map((i) => Object.fromEntries(f.fields.map((k) => [k, i[k] ?? null]))), used: r.used,
    rejectionCounts: r.diagnostics.rejectionCounts, method: r.diagnostics.method ?? null };
}
out.askedRetiredEndpoint = requested.some((r) => r.includes("/xhr/ncp"));
console.log(JSON.stringify(out));
"""


def _worker() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        entry, bundle, harness, fx = (Path(tmp) / n for n in ("entry.ts", "bundle.mjs", "harness.mjs", "fixtures.json"))
        entry.write_text(_ENTRY.replace("{SRC}", (WORKER / "src").as_posix()), encoding="utf-8")
        harness.write_text(_HARNESS, encoding="utf-8")
        fx.write_text(json.dumps({"news": NEWS, "fields": FIELDS}), encoding="utf-8")
        subprocess.run([str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node", f"--outfile={bundle}", "--log-level=error"],
                       cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
        result = subprocess.run([node, str(harness), bundle.as_uri(), str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


class TestYahooPressReleases(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ts = _worker()
        cls.py = {feed: _python(feed) for feed in ("press_releases", "news")}

    def test_only_the_issuers_own_wire_release(self) -> None:
        got = self.ts["press_releases"]
        self.assertEqual([(i["originalSource"], i["source"], i["issuer"]) for i in got["items"]],
                         [("ACCESS Newswire", "yahoo_finance_press_releases", "Aehr Test Systems, Inc.")])
        self.assertTrue(got["items"][0]["title"].startswith("Aehr Receives Follow-On Sonoma"))
        self.assertEqual(got["method"], "ISSUER_WIRE_RELEASES_IN_NEWS_FEED")
        self.assertEqual(got["rejectionCounts"].get("NOT_ISSUER_WIRE_RELEASE"), 3)

    def test_the_retired_endpoint_is_not_asked(self) -> None:
        self.assertFalse(self.ts["askedRetiredEndpoint"])

    def test_news_feed_is_unchanged(self) -> None:
        self.assertEqual(len(self.ts["news"]["items"]), 4)
        self.assertIsNone(self.ts["news"]["method"])

    def test_runtimes_agree(self) -> None:
        # The runtimes already format Yahoo timestamps differently (Python "...:48Z", the Worker "...:48.000Z"),
        # before and apart from this change; the items are compared to the second.
        def seconds(out: dict) -> dict:
            return {**out, "items": [{**i, "publishedAt": (i["publishedAt"] or "").replace(".000Z", "Z")} for i in out["items"]]}
        for feed in ("press_releases", "news"):
            self.assertEqual(seconds(self.py[feed]), seconds(self.ts[feed]), feed)


if __name__ == "__main__":
    unittest.main()
