#!/usr/bin/env python3
"""Python-only news tooling with no Worker counterpart (BR-NWS-235, BR-NWS-250).

These were parity-harness cases with a placeholder Worker side. They moved here in 3.7.0 because there is nothing to compare.
Run: python scripts/test_python_only_news_tooling.py -v
"""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import server  # noqa: E402


def _load_audit():
    spec = importlib.util.spec_from_file_location("audit_arsenal_news", ROOT / "scripts" / "audit_arsenal_news.py")
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    return audit


class TestPythonOnlyNewsTooling(unittest.TestCase):
    def test_safe_sec_url_and_xbrl_document_detection(self):
        safe = [server._safe_sec_url(u) for u in ("http://www.sec.gov/Archives/x.htm", "https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1",
                                                  "  https://www.sec.gov/Archives/xbrl/a.htm  ", None, "")]
        xbrl = [server._is_likely_xbrl_document_url(u) for u in ("https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1",
                                                                 "https://www.sec.gov/Archives/xbrl/a.htm",
                                                                 "https://www.sec.gov/Archives/edgar/data/1/a.htm")]
        self.assertEqual(safe, [None, "https://www.sec.gov/Archives/edgar/data/1/a.XML?x=1", "https://www.sec.gov/Archives/xbrl/a.htm", None, None])
        self.assertEqual(xbrl, [True, True, False])

    def test_audit_title_overlap_ratio_and_call_states(self):
        audit = _load_audit()

        def response(titles, source, status):
            return {"ok": True, "data": {"status": None, "items": [{"title": t, "evidenceClass": "CONTEXTUAL_NEWS"} for t in titles],
                                         "sourceStatus": {source: {"status": status}}, "coverage": {"state": "FULL"}}}

        both = audit.build_ticker_result("AAA", {"yahoo_finance_news": response(["A", "B", "C"], "yahoo_finance_news", "OK"),
                                                 "finnhub": response(["b", "D"], "finnhub", "OK")})
        none = audit.build_ticker_result("BBB", {"yahoo_finance_news": {"ok": False}, "finnhub": response([], "finnhub", "EMPTY_RESULT")})
        self.assertEqual(both["comparison"], {"sharedTitleCount": 1, "combinedUniqueTitleCount": 4, "titleOverlapRatio": 0.25})
        self.assertEqual(none["comparison"], {"sharedTitleCount": 0, "combinedUniqueTitleCount": 0, "titleOverlapRatio": None})
        self.assertEqual([none["providers"]["yahoo_finance_news"]["callState"], none["providers"]["finnhub"]["callState"]],
                         ["CALL_FAILED", "COMPLETED"])


if __name__ == "__main__":
    unittest.main()
