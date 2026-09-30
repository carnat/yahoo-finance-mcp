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
  companyfacts and SEC_COMPANYCONCEPT_MALFORMED is warned;
- e. named fiscal years (2.5.16), on BE-like annual facts where each year is
  reported again as a comparative carrying the later filing's fy: "FY2025",
  "2025" and "latest" give FY2025, a restated year takes the latest filed
  value, a missing year is PERIOD_NOT_FOUND, a quarterly filing type is
  INVALID_PERIOD, and "junk" is INVALID_PERIOD at get_filing_data and
  INPUT_VALIDATION_ERROR at the tool (the Worker's _dispatchTool).

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

ACCN_10K_2018 = "0000000001-19-000010"
ACCN_10K_2025 = "0000000001-26-000010"


def _annual(year: int, val: int, accn: str, filed: str, fy: int, *, end: str | None = None, start: str | None = None, fp: str = "FY") -> dict:
    return {"start": start or f"{year}-01-01", "end": end or f"{year}-12-31", "val": val, "accn": accn, "fy": fy, "fp": fp, "form": "10-K", "filed": filed}


# BE-like: every 10-K reports its year and the earlier ones again as comparatives, each carrying THAT filing's fy
# (the 2016 revenue arrives in the FY2018 10-K with fy 2018), and the FY2025 10-K restates 2024 and adds a
# fourth-quarter row. SEC lists them by period end, then filing.
BE_ROWS = [
    _annual(2016, 208540000, "0000000001-17-000010", "2017-03-01", 2016),
    _annual(2016, 208540000, ACCN_10K_2018, "2019-03-01", 2018),
    _annual(2017, 376000000, "0000000001-18-000010", "2018-03-01", 2017),
    _annual(2017, 376000000, ACCN_10K_2018, "2019-03-01", 2018),
    _annual(2018, 785000000, ACCN_10K_2018, "2019-03-01", 2018),
    _annual(2023, 1200000000, "0000000001-25-000010", "2025-02-27", 2024),
    _annual(2023, 1200000000, ACCN_10K_2025, "2026-02-26", 2025),
    _annual(2024, 1330000000, "0000000001-25-000010", "2025-02-27", 2024),
    _annual(2024, 1330500000, ACCN_10K_2025, "2026-02-26", 2025),
    _annual(2025, 600000000, ACCN_10K_2025, "2026-02-26", 2025, start="2025-10-01"),
    _annual(2025, 2001614000, ACCN_10K_2025, "2026-02-26", 2025),
]
# DG-like: a 52/53-week year ending 2026-01-30 is the issuer's fiscal 2025.
DG_ROWS = [
    _annual(2024, 40600000000, "0000000001-25-000020", "2025-03-20", 2024, start="2024-02-03", end="2025-01-31"),
    _annual(2025, 41000000000, ACCN_10K_2025, "2026-03-20", 2025, start="2025-02-01", end="2026-01-30"),
]
_QUARTER_ROWS = [
    {"start": "2026-04-01", "end": "2026-06-30", "val": 700000000, "accn": ACCN_10Q, "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-08-14"},
    {"start": "2025-04-01", "end": "2025-06-30", "val": 500000000, "accn": ACCN_10Q, "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-08-14"},
]


def _facts_case(rows: list[dict], filing_type: str, period: str, call: str = "total_revenue", *submissions_rows: tuple[str, str, str, str]) -> dict:
    return {
        "filingType": filing_type, "period": period, "call": call,
        "routes": {CONCEPT_BASE + f"{REVENUE}.json": {"units": {"USD": rows}}, COMPANYFACTS: {"facts": {"us-gaap": {REVENUE: {"units": {"USD": rows}}}}},
                   SUBMISSIONS: _submissions(*(submissions_rows or (_10K_ROW, _10Q_ROW)))},
    }


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
    "be_fy2025": _facts_case(BE_ROWS, "10-K", "FY2025"),
    "be_2025": _facts_case(BE_ROWS, "10-K", "2025"),
    "be_fy_lowercase_spaced": _facts_case(BE_ROWS, "10-K", "fy 2025"),
    "be_latest": _facts_case(BE_ROWS, "10-K", "latest"),
    "be_fy2016": _facts_case(BE_ROWS, "10-K", "FY2016"),
    "be_fy2024_restated": _facts_case(BE_ROWS, "10-K", "FY2024"),
    "be_fy2030": _facts_case(BE_ROWS, "10-K", "FY2030"),
    "be_fy2025_filing_data": _facts_case(BE_ROWS, "10-K", "FY2025", "get_filing_data"),
    "be_fy2030_filing_data": _facts_case(BE_ROWS, "10-K", "FY2030", "get_filing_data"),
    "be_junk_filing_data": _facts_case(BE_ROWS, "10-K", "junk", "get_filing_data"),
    "be_fy25_filing_data": _facts_case(BE_ROWS, "10-K", "FY25", "get_filing_data"),
    "dg_fy2025": _facts_case(DG_ROWS, "10-K", "FY2025"),
    "dg_fy2026": _facts_case(DG_ROWS, "10-K", "2026"),
    "q_fy2025": _facts_case(_QUARTER_ROWS, "10-Q", "FY2025", "total_revenue", _10Q_ROW, _10K_ROW),
    "q_latest": _facts_case(_QUARTER_ROWS, "10-Q", "latest", "total_revenue", _10Q_ROW, _10K_ROW),
    "q_fy2025_filing_data": _facts_case(_QUARTER_ROWS, "10-Q", "FY2025", "get_filing_data", _10Q_ROW, _10K_ROW),
    "malformed_concept": {
        "filingType": "10-K",
        "routes": {CONCEPT_BASE + "Revenues.json": {"units": {"USD": {}}},
                   COMPANYFACTS: {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [FACT]}}}}},
                   SUBMISSIONS: _submissions(_10K_ROW)},
    },
}

# The tool-level check, through the Worker's callTool (_dispatchTool) and the Python tool functions.
_PERIOD_TOOLS = {
    "extract_sec_filing_fact": {"ticker": TICKER, "fact_type": "total_revenue"},
    "extract_geographic_revenue": {"ticker": TICKER, "region": "China"},
    "extract_segment_revenue": {"ticker": TICKER},
    "extract_total_revenue": {"ticker": TICKER},
    "extract_revenue_exposure": {"ticker": TICKER, "exposure_query": "China"},
    "extract_china_exposure": {"ticker": TICKER},
    "extract_exposure": {"ticker": TICKER, "topic": "China"},
    "query_sec_filing_index": {"ticker": TICKER, "query_type": "total_revenue"},
}
for _tool, _args in _PERIOD_TOOLS.items():
    CASES[f"dispatch_junk_{_tool}"] = {"call": "dispatch", "tool": _tool, "args": {**_args, "period": "junk"}, "filingType": "10-K", "routes": {}}
CASES["dispatch_fy25_extract_total_revenue"] = {"call": "dispatch", "tool": "extract_total_revenue", "args": {"ticker": TICKER, "period": "FY25"}, "filingType": "10-K", "routes": {}}

_ENTRY = """
export { extractTotalRevenue, getFilingData } from "./src/yahoo-finance.ts";
export { callTool } from "./src/tools.ts";
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
const period = c.period ?? "latest";
let out;
if (c.call === "get_filing_data") out = await m.getFilingData("XYZ", "total_revenue", null, c.filingType, period);
else if (c.call === "dispatch") out = await m.callTool(c.tool, c.args);
else out = await m.extractTotalRevenue("XYZ", c.filingType, period);
console.log(JSON.stringify(JSON.parse(out)));
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
        call = case.get("call", "total_revenue")
        period = case.get("period", "latest")
        if call == "dispatch":
            return json.loads(asyncio.run(getattr(srv, case["tool"])(**case["args"])))
        if call == "get_filing_data":
            return json.loads(asyncio.run(srv.get_filing_data(TICKER, srv.FilingFactType.total_revenue, None, case["filingType"], period)))
        return json.loads(asyncio.run(srv.extract_total_revenue(TICKER, case["filingType"], period)))


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
        "period": payload.get("period"),
        "requestedPeriod": payload.get("requestedPeriod"),
        "message": payload.get("message"),
        "warningMessages": [w.get("message") for w in payload.get("warnings") or []],
        "warningSeverities": [w.get("severity") for w in payload.get("warnings") or []],
        "error": payload.get("error"),
        "ok": payload.get("ok"),
    }


class TestSecFactPayloadsAgree(unittest.TestCase):
    """Python and the Worker return the same fields for the same SEC responses."""

    def _same(self, name: str) -> dict:
        worker, local = _fields(_worker(name)), _fields(_python(name))
        for key in worker:
            self.assertEqual(worker[key], local[key], f"{name}: {key}: worker={worker[key]!r} python={local[key]!r}")
        return local

    def _same_payload(self, name: str) -> dict:
        """The whole payload, top-level key order included (1e9 and 1e9.0 are the same number)."""
        worker, local = _worker(name), _python(name)
        for payload in (worker, local):
            # Left out: an existing difference outside this change (the Worker's empty-string denominator cell is None in Python).
            (payload.get("evidence") or {}).pop("sourceRows", None)
        self.assertEqual(worker, local, name)
        self.assertEqual(list(worker), list(local), f"{name}: key order")
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


class TestNamedFiscalYearAgrees(unittest.TestCase):
    """2.5.16: period is "latest" or a fiscal year, and both runtimes answer the same for BE-like annual facts."""

    _same = TestSecFactPayloadsAgree._same
    _same_payload = TestSecFactPayloadsAgree._same_payload

    def test_a_named_year_selects_its_period_not_the_first_row(self) -> None:
        # The first companyconcept row is the 2016 revenue (208,540,000): "FY2025" once returned it labelled FY2018.
        for name in ("be_fy2025", "be_2025", "be_fy_lowercase_spaced", "be_latest"):
            got = self._same(name)
            self.assertEqual((got["status"], got["value"], got["period"], got["decisionGrade"]), ("FOUND", 2001614000, "FY2025", True), name)
            self.assertEqual((got["evidence.accessionNumber"], got["warningCodes"]), (ACCN_10K_2025, []), name)

    def test_an_earlier_year_keeps_its_own_label_when_a_later_filing_carries_it(self) -> None:
        got = self._same("be_fy2016")
        self.assertEqual((got["value"], got["period"]), (208540000, "FY2016"))
        self.assertEqual(got["evidence.accessionNumber"], ACCN_10K_2018)

    def test_a_restated_year_takes_the_latest_filed_value(self) -> None:
        got = self._same("be_fy2024_restated")
        self.assertEqual((got["value"], got["period"], got["evidence.accessionNumber"]), (1330500000, "FY2024", ACCN_10K_2025))

    def test_a_year_that_is_not_there_is_period_not_found(self) -> None:
        got = self._same("be_fy2030")
        self.assertEqual((got["status"], got["code"], got["value"], got["unit"], got["decisionGrade"]), ("NOT_FOUND", "PERIOD_NOT_FOUND", None, None, False))
        self.assertEqual(got["warningCodes"], ["PERIOD_NOT_FOUND"])
        self.assertIn("fiscal years found: 2016, 2017, 2018, 2023, 2024, 2025.", got["warningMessages"][0])
        self.assertIn(f"No annual {REVENUE} fact is fiscal year 2030", got["warningMessages"][0])
        self.assertEqual(got["evidence.filingType"], "10-K")

    def test_a_52_53_week_year_is_the_issuers_year(self) -> None:
        got = self._same("dg_fy2025")
        self.assertEqual((got["value"], got["period"]), (41000000000, "FY2025"))
        got = self._same("dg_fy2026")
        self.assertEqual((got["code"], got["value"]), ("PERIOD_NOT_FOUND", None))
        self.assertIn("fiscal years found: 2024, 2025.", got["warningMessages"][0])

    def test_a_quarterly_filing_type_cannot_name_a_fiscal_year(self) -> None:
        got = self._same("q_fy2025")
        self.assertEqual((got["status"], got["code"], got["value"], got["decisionGrade"]), ("NOT_FOUND", "INVALID_PERIOD", None, False))
        self.assertEqual(got["warningMessages"], ["period FY2025 names a fiscal year, an annual period; request filing_type 10-K or 20-F (or period_mode annual)."])
        self.assertEqual((got["evidence.accessionNumber"], got["evidence.filingType"]), (ACCN_10Q, "10-Q"))
        latest = self._same("q_latest")
        self.assertEqual((latest["value"], latest["code"]), (700000000, None))
        self._same_payload("q_fy2025_filing_data")

    def test_get_filing_data_returns_the_same_payload(self) -> None:
        got = self._same_payload("be_fy2025_filing_data")
        self.assertEqual((got["value"], got["period"]), (2001614000, "FY2025"))
        missing = self._same_payload("be_fy2030_filing_data")
        self.assertEqual(missing["code"], "PERIOD_NOT_FOUND")

    def test_an_unreadable_period_is_invalid_period(self) -> None:
        for name in ("be_junk_filing_data", "be_fy25_filing_data"):
            got = self._same_payload(name)
            self.assertEqual(list(got), ["ticker", "factType", "value", "unit", "period", "filingType", "extractionMethod", "source", "confidence",
                                         "status", "code", "decisionGrade", "requestedPeriod", "warnings"], name)
            self.assertEqual((got["status"], got["code"], got["value"], got["source"], got["extractionMethod"]), ("INVALID_PERIOD", "INVALID_PERIOD", None, "INPUT", "NONE"), name)
            self.assertEqual(got["warnings"], [{"code": "INVALID_PERIOD", "message": 'period must be "latest" or a fiscal year ("FY2025" or "2025").', "severity": "error"}], name)
        self.assertEqual(_python("be_junk_filing_data")["requestedPeriod"], "junk")

    def test_the_tools_refuse_an_unreadable_period(self) -> None:
        """Worker _dispatchTool and the Python tool functions return the same failure for each tool that reads a period."""
        for tool in _PERIOD_TOOLS:
            name = f"dispatch_junk_{tool}"
            worker, local = _worker(name), _python(name)
            for key in ("ok", "data", "error"):
                self.assertEqual(worker[key], local[key], f"{name}: {key}")
            self.assertEqual(worker["meta"]["tool"], local["meta"]["tool"], name)
            self.assertEqual((local["ok"], local["error"]["code"]), (False, "INPUT_VALIDATION_ERROR"), name)
            self.assertEqual(local["error"]["message"], """period must be "latest" or a fiscal year ("FY2025" or "2025"). Got 'junk'.""", name)
            self.assertEqual(local["meta"]["tool"], tool, name)
        worker, local = _worker("dispatch_fy25_extract_total_revenue"), _python("dispatch_fy25_extract_total_revenue")
        self.assertEqual(worker["error"], local["error"])
        self.assertEqual(local["error"]["message"], """period must be "latest" or a fiscal year ("FY2025" or "2025"). Got 'FY25'.""")


if __name__ == "__main__":
    unittest.main()
