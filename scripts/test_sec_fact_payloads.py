#!/usr/bin/env python3
"""SEC fact-read payloads, the same in both runtimes (2.5.16).

Worker getFilingData/extractTotalRevenue (worker/src/yahoo-finance.ts) and Python
get_filing_data/extract_total_revenue (server.py) read the same mocked SEC
responses (one route table per case, served by URL to both) and are compared
field by field. Cases, for ticker XYZ (CIK 0000000001):

- a. found: one 10-K FY fact is a decision-grade XBRL fact with its concept;
- b. no facts for the requested form (10-Q): SEC_FACT_NOT_AVAILABLE with code
  NO_COMPANYCONCEPT_FACT_FOR_FORM, no unit, and the latest 10-Q as evidence;
- c. IFRS-only filer (20-F): SEC_FACTS_IFRS_ONLY, no unit;
- d. companyconcept returns an empty USD object: the value is read from
  companyfacts and SEC_COMPANYCONCEPT_MALFORMED is warned.

The Worker runs in Node (esbuild bundle, stubbed fetch), one process per case
because it caches CIKs and submissions per isolate.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))


def _ensure_mcp_available() -> None:
    try:
        import mcp.server  # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    class _FastMCPStub:
        def __init__(self, *a: object, **kw: object) -> None:
            pass

        def tool(self, *a: object, **kw: object):  # type: ignore[return]
            if a and callable(a[0]):
                return a[0]
            return lambda fn: fn

    mcp_mod = types.ModuleType("mcp")
    server_mod = types.ModuleType("mcp.server")
    fastmcp_mod = types.ModuleType("mcp.server.fastmcp")
    fastmcp_mod.FastMCP = _FastMCPStub  # type: ignore[attr-defined]
    mcp_mod.server = server_mod  # type: ignore[attr-defined]
    server_mod.fastmcp = fastmcp_mod  # type: ignore[attr-defined]
    sys.modules.setdefault("mcp", mcp_mod)
    sys.modules.setdefault("mcp.server", server_mod)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_mod)


_ensure_mcp_available()
import server as srv  # noqa: E402
from yfmcp.clients.edgar import EdgarError  # noqa: E402

TICKER = "XYZ"
CIK = "0000000001"
ACCN_10K = "0000000001-26-000001"
ACCN_10Q = "0000000001-26-000002"
ACCN_20F = "0000000001-26-000003"
CONCEPT_BASE = f"https://data.sec.gov/api/xbrl/companyconcept/CIK{CIK}/us-gaap/"
COMPANYFACTS = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"
SUBMISSIONS = f"https://data.sec.gov/submissions/CIK{CIK}.json"
REVENUE = "RevenueFromContractWithCustomerExcludingAssessedTax"
REVENUE_CONCEPTS = [REVENUE, "RevenueFromContractWithCustomerIncludingAssessedTax", "Revenues", "SalesRevenueNet"]

FACT = {"start": "2025-01-01", "end": "2025-12-31", "val": 1000000, "accn": ACCN_10K, "fy": 2025, "fp": "FY", "form": "10-K", "filed": "2026-02-01"}


def _submissions(*rows: tuple[str, str, str, str]) -> dict:
    """rows: (form, accession, primaryDocument, filingDate)."""
    return {"cik": "1", "filings": {"recent": {
        "form": [r[0] for r in rows], "accessionNumber": [r[1] for r in rows], "primaryDocument": [r[2] for r in rows],
        "filingDate": [r[3] for r in rows], "reportDate": [r[3] for r in rows], "acceptanceDateTime": ["" for _ in rows],
    }}}


_10K_ROW = ("10-K", ACCN_10K, "xyz-10k.htm", "2026-02-01")
_10Q_ROW = ("10-Q", ACCN_10Q, "xyz-10q.htm", "2026-05-05")
_20F_ROW = ("20-F", ACCN_20F, "xyz-20f.htm", "2026-03-15")
_US_GAAP_FACTS = {"facts": {"us-gaap": {REVENUE: {"units": {"USD": [FACT]}}}}}

# Each case: the SEC responses (URL -> JSON body; unlisted URLs are 404) and the form asked for.
CASES: dict[str, dict] = {
    "found": {
        "filingType": "10-K",
        "routes": {CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": [FACT]}}, COMPANYFACTS: _US_GAAP_FACTS,
                   SUBMISSIONS: _submissions(_10K_ROW, _10Q_ROW)},
    },
    "no_facts_for_form": {
        "filingType": "10-Q",
        "routes": {CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": [FACT]}}, COMPANYFACTS: _US_GAAP_FACTS,
                   SUBMISSIONS: _submissions(_10Q_ROW, _10K_ROW)},
    },
    "ifrs_only": {
        "filingType": "20-F",
        "routes": {COMPANYFACTS: {"facts": {"ifrs-full": {"Revenue": {"units": {"USD": [{**FACT, "form": "20-F", "accn": ACCN_20F}]}}}}},
                   SUBMISSIONS: _submissions(_20F_ROW)},
    },
    "malformed_concept": {
        "filingType": "10-K",
        "routes": {CONCEPT_BASE + "Revenues.json": {"units": {"USD": {}}},
                   COMPANYFACTS: {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [FACT]}}}}},
                   SUBMISSIONS: _submissions(_10K_ROW)},
    },
}

_ENTRY = """
export { extractTotalRevenue } from "./src/yahoo-finance.ts";
export { setWorkerEnv } from "./src/response.ts";
"""

_HARNESS = r"""
import fs from "node:fs";
const [bundleUrl, casePath] = process.argv.slice(-2);
const m = await import(bundleUrl);
m.setWorkerEnv({});
const c = JSON.parse(fs.readFileSync(casePath, "utf8"));
const json = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
globalThis.fetch = async (req) => {
  const u = new URL(typeof req === "string" ? req : req.url);
  if (u.hostname === "fc.yahoo.com") return new Response("", { headers: { "set-cookie": "A3=abc; Path=/" } });
  if (u.pathname.includes("getcrumb")) return new Response("crumb123");
  if (u.pathname.includes("/quoteSummary/")) return json({ quoteSummary: { result: [{}], error: null } });
  if (u.pathname.endsWith("company_tickers.json")) return json({ "0": { cik_str: 1, ticker: "XYZ", title: "XYZ Corp" } });
  const body = c.routes[u.toString()];
  return body === undefined ? new Response("not found", { status: 404 }) : json(body);
};
console.log(JSON.stringify(JSON.parse(await m.extractTotalRevenue("XYZ", c.filingType))));
"""


def _node() -> str:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    return node


@functools.cache
def _bundle_dir() -> Path:
    """Bundle the Worker once; the directory lives for the process."""
    _node()
    tmp = Path(tempfile.mkdtemp(prefix="sec-fact-payloads-"))
    entry = WORKER / ".sec-fact-payloads-entry.ts"
    entry.write_text(_ENTRY, encoding="utf-8")
    try:
        subprocess.run(
            [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=neutral", "--main-fields=module,main",
             "--external:node:async_hooks", f"--outfile={tmp / 'bundle.mjs'}", "--log-level=error"],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=120,
        )
    finally:
        entry.unlink(missing_ok=True)
    (tmp / "harness.mjs").write_text(_HARNESS, encoding="utf-8")
    return tmp


def _worker(name: str) -> dict:
    tmp = _bundle_dir()
    case_path = tmp / f"{name}.json"
    case_path.write_text(json.dumps(CASES[name]), encoding="utf-8")
    result = subprocess.run([_node(), str(tmp / "harness.mjs"), (tmp / "bundle.mjs").as_uri(), str(case_path)],
                            check=True, capture_output=True, text=True, timeout=180)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _python(name: str) -> dict:
    case = CASES[name]

    async def edgar_get(url: str) -> dict:
        if url not in case["routes"]:
            raise EdgarError("SEC API returned HTTP 404: Not Found", status_code=404)
        return json.loads(json.dumps(case["routes"][url]))

    async def submissions(_ticker: str) -> tuple[str | None, dict | None]:
        return CIK, case["routes"].get(SUBMISSIONS)

    with patch("server._resolve_cik_for_ticker", new=AsyncMock(return_value=CIK)), \
            patch("server._edgar_get", new=edgar_get), \
            patch("server._get_submissions_for_ticker", new=submissions):
        return json.loads(asyncio.run(srv.extract_total_revenue(TICKER, case["filingType"])))


def _fields(payload: dict) -> dict:
    source = payload.get("sourceEvidence") or {}
    evidence = payload.get("evidence") or {}
    return {
        "status": payload.get("status"),
        "value": payload.get("value"),
        "unit": payload.get("unit"),
        "code": payload.get("code"),
        "decisionGrade": payload.get("decisionGrade"),
        "confidence": payload.get("confidence"),
        "sourceEvidence.concept": source.get("concept"),
        "evidence.accessionNumber": evidence.get("accessionNumber"),
        "evidence.filingType": evidence.get("filingType"),
        "warningCodes": [w.get("code") for w in payload.get("warnings") or []],
    }


class TestSecFactPayloadsAgree(unittest.TestCase):
    """Python and the Worker return the same fields for the same SEC responses."""

    def _same(self, name: str) -> dict:
        worker, local = _fields(_worker(name)), _fields(_python(name))
        for key in worker:
            self.assertEqual(worker[key], local[key], f"{name}: {key}: worker={worker[key]!r} python={local[key]!r}")
        return local

    def test_found(self) -> None:
        got = self._same("found")
        self.assertEqual((got["status"], got["value"], got["decisionGrade"]), ("FOUND", 1000000, True))
        self.assertEqual(got["sourceEvidence.concept"], REVENUE)
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_10K, "10-K"))
        self.assertEqual((got["unit"], got["code"], got["warningCodes"]), ("USD", None, []))

    def test_no_facts_for_the_form(self) -> None:
        got = self._same("no_facts_for_form")
        self.assertEqual((got["status"], got["code"], got["unit"], got["value"]), ("NOT_FOUND", "NO_COMPANYCONCEPT_FACT_FOR_FORM", None, None))
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_10Q, "10-Q"))
        self.assertFalse(got["decisionGrade"])
        self.assertEqual(got["warningCodes"], ["NO_COMPANYCONCEPT_FACT_FOR_FORM"])

    def test_ifrs_only(self) -> None:
        got = self._same("ifrs_only")
        self.assertEqual((got["code"], got["unit"], got["value"]), ("SEC_FACTS_IFRS_ONLY", None, None))
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_20F, "20-F"))
        self.assertEqual(got["warningCodes"], ["SEC_FACTS_IFRS_ONLY"])

    def test_malformed_concept_is_read_from_companyfacts(self) -> None:
        got = self._same("malformed_concept")
        self.assertEqual((got["status"], got["value"]), ("FOUND", 1000000))
        self.assertEqual(got["warningCodes"], ["SEC_COMPANYCONCEPT_MALFORMED"])
        self.assertEqual(got["sourceEvidence.concept"], "Revenues")


if __name__ == "__main__":
    unittest.main()
