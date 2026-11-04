"""Parity cases: parsed tool output keeps the failure (3.8.3, swallowed-error audit row 3.8.3).

- extract_sec_filing_fact: a fact reader result that cannot be parsed is PROVIDER_ERROR (retryable) "SEC fact result could
  not be read; retry.", no longer status NOT_DISCLOSED. A valid result that discloses nothing is unchanged.
- company IR page, adapter "structured": a body that is not JSON adds COMPANY_IR_PAGE_VALIDATION_FAILED ("unexpected non-JSON
  body for structured adapter", status SOURCE_VALIDATION_FAILED), no longer a silent EMPTY_RESULT. A valid empty JSON body
  is unchanged (EMPTY_RESULT, no warning).

The Worker side of the fact case runs a private bundle whose getFilingData result is replaced (esbuild onLoad on sec.ts),
the Python side patches get_filing_data: neither runtime can be made to return non-JSON through HTTP routes.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import patch

from scripts.parity.cases_news_b import _IR_STRUCTURED_ENTRY, _ir_page_case, _ir_view
from scripts.parity.harness import Case

import server  # noqa: E402
from yfmcp.tools import sec as sec_tools  # noqa: E402

CASES: list[Case] = []
IDS = ["PARSE-READS-383"]
WORKER = Path(__file__).resolve().parents[2] / "worker"

# ── extract_sec_filing_fact ────────────────────────────────────────────────────────────────────────
_FACT_JS = """
const esbuild = await import(fx.esbuild);
const build = esbuild.build ?? esbuild.default.build;
const entry = `export * as tools from ${JSON.stringify(fx.src + "/tools.ts")};\\n`
  + `export * as response from ${JSON.stringify(fx.src + "/response.ts")};\\n`;
const out = await build({
  stdin: { contents: entry, resolveDir: fx.src, loader: "ts" }, bundle: true, format: "esm", platform: "node",
  write: false, logLevel: "silent",
  plugins: [{ name: "fact-reader", setup(b) {
    b.onLoad({ filter: /[\\\\/]worker[\\\\/]src[\\\\/]sec\\.ts$/ }, async (a) => {
      const fs = await import("node:fs");
      const text = fs.readFileSync(a.path, "utf8");
      const needle = "const raw = await getFilingDataUnchecked(ticker, factType, region, filingType, period, periodMode, pinAccession);";
      if (!text.includes(needle)) throw new Error("getFilingData marker not found");
      return { contents: text.replace(needle, "const raw = " + JSON.stringify(args[0]) + ";"), loader: "ts", resolveDir: fx.src };
    });
  } }],
});
const fs = await import("node:fs"); const os = await import("node:os"); const path = await import("node:path");
const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "fact-reader-")), "bundle.mjs");
fs.writeFileSync(file, out.outputFiles[0].text);
const mod = await import("file://" + file);
mod.response.setWorkerEnv({});
return JSON.parse(await mod.tools.callTool("extract_sec_filing_fact", { ticker: "TST", fact: "total_revenue" }));
"""


def _fact_case(name: str, reader_result: str, expect: dict) -> Case:
    def py():
        async def fake(**_kw):
            return reader_result
        with patch.object(sec_tools, "get_filing_data", fake):
            return json.loads(asyncio.run(server.extract_sec_filing_fact(ticker="TST", fact="total_revenue")))

    def select(v):
        data = v.get("data") if isinstance(v.get("data"), dict) else v
        err = v.get("error") if isinstance(v.get("error"), dict) else v
        return {"ok": not v.get("error") and v.get("ok") is not False, "code": err.get("code") if v.get("error") else None,
                "message": err.get("message") if v.get("error") else None,
                "retryable": (err.get("retryable") if v.get("error") else None),
                "status": None if v.get("error") else data.get("status")}

    return Case(name=f"parse_reads_383.fact.{name}", ids=IDS, py=py, select=select, expect=expect,
                ts={"js": _FACT_JS, "args": [reader_result], "isolate": True,
                    "fx": {"src": str(WORKER / "src"), "esbuild": (WORKER / "node_modules" / "esbuild" / "lib" / "main.js").as_uri()}})


CASES += [
    _fact_case("error_text_reader_result", "Error: upstream read failed",
               {"ok": False, "code": "PROVIDER_ERROR", "message": "SEC fact result could not be read; retry.", "retryable": True, "status": None}),
    _fact_case("valid_reader_result_discloses_nothing", json.dumps({"status": "NOT_DISCLOSED", "value": None}),
               {"ok": True, "code": None, "message": None, "retryable": None, "status": "NOT_DISCLOSED"}),
]

# ── company IR page, structured adapter ───────────────────────────────────────────────────────────
CASES += [
    _ir_page_case("parse_reads_383.ir_structured.non_json_body", IDS, "<html><body>Page moved</body></html>",
                  entry=_IR_STRUCTURED_ENTRY, content_type="text/plain", select=_ir_view,
                  expect={"count": 0, "status": "SOURCE_VALIDATION_FAILED", "collection": "SOURCE_LIMITED_NOT_FOUND", "sourcesUsed": [], "warnings": [[
                      "COMPANY_IR_PAGE_VALIDATION_FAILED",
                      "Approved company IR page validation failed: unexpected non-JSON body for structured adapter"]]}),
    _ir_page_case("parse_reads_383.ir_structured.valid_empty_json_body", IDS, json.dumps({"items": []}),
                  entry=_IR_STRUCTURED_ENTRY, content_type="application/json", select=_ir_view,
                  expect={"count": 0, "status": "EMPTY_RESULT", "collection": "NOT_FOUND", "sourcesUsed": ["company_ir_page"],
                          "warnings": []}),
]
