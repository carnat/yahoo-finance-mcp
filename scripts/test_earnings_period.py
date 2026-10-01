#!/usr/bin/env python3
"""Earnings-release period resolution (2.5.19): the Worker and Python must read the same fiscal period.

worker/src/earnings-period.ts and yfmcp/earnings_period.py are run on the same release text; each case
also states the period it must resolve to.
"""

from __future__ import annotations

import functools
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

from yfmcp.earnings_period import extract_earnings_period_from_text  # noqa: E402

# MU FQ4 2026 EX-99.1 opening (2026-09-30): a forward-looking "fiscal 2027" in the subheadline sat within the
# old 180-character window of "fourth quarter", and the Worker read FY2027 Q4.
MU_RELEASE = (
    "MICRON TECHNOLOGY, INC. REPORTS RECORD FISCAL FOURTH-QUARTER AND FULL-YEAR 2026 RESULTS "
    "AI-driven demand and strong operational execution position Micron for a record fiscal 2027 "
    "BOISE, Idaho, September 30, 2026 – Micron Technology, Inc. (Nasdaq: MU) today announced results for its "
    "fourth quarter and full year of fiscal 2026, which ended September 3, 2026. Fiscal Q4 2026 Highlights "
    "• Revenue of $54.23 billion versus $41.46 billion for the prior quarter"
)
MU_BODY_ONLY = (
    "AI-driven demand position Micron for a record fiscal 2027 BOISE, Idaho, September 30, 2026 – Micron "
    "Technology, Inc. today announced results for its fourth quarter and full year of fiscal 2026, which ended September 3, 2026."
)

CASES = {
    "mu_release": (MU_RELEASE, "FY2026 Q4"),
    "mu_body_without_headline": (MU_BODY_ONLY, "FY2026 Q4"),
    "aehr_heading_before_comparison": (
        "Aehr Reports Fiscal 2026 Fourth Quarter Results. Net revenue was $18.8 million, "
        "compared to $14.1 million in the fourth quarter of fiscal 2025.",
        "FY2026 Q4",
    ),
    "results_before_guidance": (
        "Revenue for the fourth quarter of fiscal 2026 was $1.83 billion. Business Outlook – First Quarter Fiscal 2027 "
        "Revenue for the first quarter of fiscal 2027 is expected to be between $2.2 billion and $2.4 billion.",
        "FY2026 Q4",
    ),
    "quarter_of_fiscal_year": ("Marvell Reports Second Quarter of Fiscal Year 2027 Financial Results", "FY2027 Q2"),
    "calendar_year_quarter": ("Arista Networks Reports Fourth Quarter and Full Year 2025 Financial Results", "FY2025 Q4"),
    "hyphenated_headline": ("REPORTS THIRD-QUARTER 2026 RESULTS", "FY2026 Q3"),
    "numeric_fy": ("Q4 FY2026 revenue was a record.", "FY2026 Q4"),
    "numeric_of_fiscal": ("Results for Q2 of fiscal 2026 exceeded guidance.", "FY2026 Q2"),
    "fiscal_numeric": ("Fiscal Q3 2026 Highlights: revenue grew.", "FY2026 Q3"),
    "year_first_short_gap": ("Fiscal 2026 third quarter revenue rose 20%.", "FY2026 Q3"),
    "ordinal_digits": ("Results for the 4th quarter of fiscal 2026 are below.", "FY2026 Q4"),
    "forward_year_across_sentence": ("We expect an even stronger fiscal 2027. Fourth quarter revenue was $54 billion.", None),
    "forward_year_across_dash": ("Positioned for a record fiscal 2027 – the company announced fourth quarter results.", None),
    "period_end_date_only": ("Results for the fourth quarter ended January 30, 2026.", None),
    "no_period": ("The company announced results today.", None),
    "year_too_far_from_quarter": (
        "Fiscal 2026 was a year of strong growth across every product line and region and customer type, with fourth quarter revenue up.",
        None,
    ),
}


@functools.lru_cache(maxsize=1)
def _worker_outputs() -> dict:
    if not ESBUILD.exists() or not shutil.which("node"):
        raise unittest.SkipTest("Worker toolchain (esbuild/node) is not installed")
    with tempfile.TemporaryDirectory() as tmp:
        bundle = Path(tmp) / "earnings-period.mjs"
        subprocess.run(
            [str(ESBUILD), str(WORKER / "src" / "earnings-period.ts"), "--bundle", "--format=esm", "--platform=node", f"--outfile={bundle}"],
            check=True, capture_output=True,
        )
        cases = {name: text for name, (text, _) in CASES.items()}
        script = (
            f"import {{ extractEarningsPeriodFromText }} from {json.dumps(bundle.as_uri())};\n"
            f"const cases = {json.dumps(cases)};\n"
            "const out = {}; for (const [k, v] of Object.entries(cases)) out[k] = extractEarningsPeriodFromText(v);\n"
            "console.log(JSON.stringify(out));\n"
        )
        result = subprocess.run(["node", "--input-type=module", "-e", script], check=True, capture_output=True, text=True)
        return json.loads(result.stdout)


class TestEarningsPeriodAgrees(unittest.TestCase):
    def test_each_case_resolves_as_stated(self) -> None:
        for name, (text, expected) in CASES.items():
            with self.subTest(name=name):
                self.assertEqual(extract_earnings_period_from_text(text)["period"], expected)

    def test_worker_and_python_agree(self) -> None:
        worker = _worker_outputs()
        for name, (text, _) in CASES.items():
            with self.subTest(name=name):
                self.assertEqual(worker[name], extract_earnings_period_from_text(text))

    def test_unresolved_shape(self) -> None:
        self.assertEqual(
            extract_earnings_period_from_text(""),
            {"period": None, "periodStatus": "UNRESOLVED", "periodEvidence": None},
        )


if __name__ == "__main__":
    unittest.main(verbosity=1)
