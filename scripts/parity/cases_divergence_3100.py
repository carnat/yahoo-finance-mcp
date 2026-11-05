"""Parity cases for 3.10.0: the last two Python/Worker differences left from 3.0.0.

1. Timestamps. Python writes the earnings tools' timestamps as the Worker's toIsoUtc does (a date-only value stays
   ...T00:00:00Z, any other instant carries three millisecond digits), and every "now" timestamp (meta.generatedAt,
   retrievedAt, indexedAt, fetchedAt) as the Worker's new Date().toISOString() does: ...T17:57:21.123Z, never six
   digits or +00:00. The news tools keep their own formatter, which drops zero milliseconds in both runtimes (2.17.0).
2. extract_earnings_metrics' top-level `source` names the release's source (sec_8k) in both runtimes; its evidence
   names the document that was read (sec_8k_ex991 for the EX-99.1 exhibit).

The third 3.0.0 leftover (Python meta carried reliabilityClass only) was aligned in 3.7.0 (D7, cases_major_300.py).
"""

from __future__ import annotations

import re

from scripts.parity.cases_sec_earnings import _doc_url, _run, _sec_routes, _srv
from scripts.parity.harness import Case
from yfmcp.util import _to_iso_utc_ms

_MS = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

_EX99_INDEX = ("<html><body><table>"
               "<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>"
               '<tr><td>1</td><td>8-K</td><td><a href="/Archives/edgar/data/{cik}/{nodash}/r8k.htm">r8k.htm</a></td><td>8-K</td><td>10</td></tr>'
               '<tr><td>2</td><td>Press release</td><td><a href="/Archives/edgar/data/{cik}/{nodash}/ex991.htm">ex991.htm</a></td><td>EX-99.1</td><td>10</td></tr>'
               "</table></body></html>")
_EIGHT_K = "<html><body><p>Item 2.02 Results of Operations. The press release is attached as Exhibit 99.1.</p></body></html>"
_EX99 = "<html><body><p>Acme Reports Fourth Quarter Fiscal 2026 Results. Revenue was $1.0 billion for the quarter.</p></body></html>"


def _routes(tkr: str, cik: int, with_exhibit: bool) -> dict:
    accn = f"0000{cik:06d}-26-000001"
    docs = {_doc_url(cik, accn, "r8k.htm"): _EIGHT_K}
    if with_exhibit:
        docs[_doc_url(cik, accn, "ex991.htm")] = _EX99
    index = _EX99_INDEX.format(cik=cik, nodash=accn.replace("-", "")) if with_exhibit else None
    routes = _sec_routes(tkr, cik, [{"accession": accn, "date": "2026-07-14", "primary": "r8k.htm"}], docs, index)
    routes["https://data.sec.gov/api/xbrl/companyconcept/*"] = {"status": 404}
    return routes


def _metrics_view(v: dict) -> dict:
    """Top-level source, reportedAt as written, and each evidence row's sourceType and timestamp shapes."""
    d = v.get("data") if isinstance(v.get("data"), dict) else v
    return {
        "source": d.get("source"),
        "reportedAt": d.get("reportedAt"),
        "evidence": [{"sourceType": e.get("sourceType"), "publishedAt": e.get("publishedAt"),
                      "retrievedAtHasMilliseconds": bool(_MS.match(str(e.get("retrievedAt") or "")))}
                     for e in d.get("evidence") or []],
    }


def _metrics_case(name: str, tkr: str, cik: int, with_exhibit: bool, expect: dict) -> Case:
    return Case(name=name, ids=["D-3.0.0"], py=_run(lambda: _srv().extract_earnings_metrics(tkr)),
                ts={"js": "m.response.setWorkerEnv({}); return m.yahooFinance.extractEarningsMetrics(args[0]);", "args": [tkr], "isolate": True},
                routes=_routes(tkr, cik, with_exhibit), select=_metrics_view, expect=expect)


CASES = [
    _metrics_case("divergence3100.earnings_metrics.source_is_the_8k_and_evidence_names_the_exhibit_read", "ZDV1", 861001, True, {
        "source": "sec_8k", "reportedAt": "2026-07-14T21:05:00.000Z",
        "evidence": [{"sourceType": "sec_8k_ex991", "publishedAt": "2026-07-14T00:00:00Z", "retrievedAtHasMilliseconds": True}],
    }),
    _metrics_case("divergence3100.earnings_metrics.without_an_exhibit_source_and_evidence_are_the_8k", "ZDV2", 861002, False, {
        "source": "sec_8k", "reportedAt": "2026-07-14T21:05:00.000Z", "evidence": [],
    }),
]


# The earnings timestamp helper on its own (Python _to_iso_utc_ms against the Worker's toIsoUtc).
for _key, _raw, _expect in [
    ("date_only_has_no_milliseconds", "2026-07-14", "2026-07-14T00:00:00Z"),
    ("zero_milliseconds_are_kept", "2026-07-14T21:05:00Z", "2026-07-14T21:05:00.000Z"),
    ("offset_is_converted_to_utc", "2026-07-14T17:05:00-04:00", "2026-07-14T21:05:00.000Z"),
    ("microseconds_are_cut_to_milliseconds", "2026-07-14T21:05:00.123456Z", "2026-07-14T21:05:00.123Z"),
    ("rfc_2822_date", "Tue, 14 Jul 2026 21:05:00 GMT", "2026-07-14T21:05:00.000Z"),
    ("compact_digits_are_not_a_date", "20260714", None),
    ("blank_is_null", "  ", None),
    ("unparseable_is_null", "not a date", None),
]:
    CASES.append(Case(name=f"divergence3100.to_iso_utc.{_key}", ids=["D-3.0.0"],
                      py=(lambda r=_raw: _to_iso_utc_ms(r)),
                      ts={"js": "return m.util.toIsoUtc(args[0]);", "args": [_raw]}, expect=_expect))

