#!/usr/bin/env python3
"""Run the Phase 2 behaviour-contract parity harness (scripts/parity/).

    python scripts/test_parity_harness.py -v             # run every case
    python scripts/test_parity_harness.py --report FILE  # also write coverage/results JSON for BASELINE.md

One unittest per case (`test_<case name>`), plus self-tests proving the harness
can fail: a Python/Worker difference, a drifted expected value and a drifted
pinned value must each be reported.
"""

from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.parity import harness  # noqa: E402


def _results() -> tuple[list[harness.Case], dict, dict]:
    cases = harness.load_cases()
    worker = harness.run_worker(cases)
    python = {c.name: harness.run_python(c) for c in cases}
    return cases, python, worker


_CACHE: dict = {}


def _get() -> tuple[list[harness.Case], dict, dict]:
    if not _CACHE:
        _CACHE["v"] = _results()
    return _CACHE["v"]


class TestParityHarnessSelf(unittest.TestCase):
    """The harness must be able to fail."""

    def _case(self, **kw):
        base = dict(name="self", ids=["SELF"], py=lambda: None, ts={"js": "return null"})
        base.update(kw)
        return harness.Case(**base)

    def test_runtime_difference_is_reported(self):
        problems = harness.evaluate(self._case(), {"ok": True, "value": {"a": 1}}, {"ok": True, "value": {"a": 2}})
        self.assertTrue(problems and "differ" in problems[0])

    def test_expected_value_drift_is_reported_even_when_runtimes_agree(self):
        problems = harness.evaluate(self._case(expect={"a": 1}), {"ok": True, "value": {"a": 2}}, {"ok": True, "value": {"a": 2}})
        self.assertEqual(len(problems), 2)

    def test_pinned_drift_is_reported(self):
        case = self._case(mode="pinned", decision="Q0", expect_py={"a": 1}, expect_ts={"a": 2})
        self.assertEqual(harness.evaluate(case, {"ok": True, "value": {"a": 1}}, {"ok": True, "value": {"a": 2}}), [])
        self.assertTrue(harness.evaluate(case, {"ok": True, "value": {"a": 1}}, {"ok": True, "value": {"a": 1}}))

    def test_canonical_number_and_key_order(self):
        self.assertEqual(harness.canonical({"b": 1.0, "a": [2.5, float("nan")]}), {"a": [2.5, "NaN"], "b": 1})

    def test_error_on_one_side_is_a_difference(self):
        problems = harness.evaluate(self._case(), {"ok": False, "error": "x"}, {"ok": True, "value": None})
        self.assertTrue(problems)

    def test_python_network_stub_blocks_unrouted_requests(self):
        import urllib.request
        with harness.stub_urlopen({"https://example.invalid/a": {"json": {"x": 1}}}):
            with urllib.request.urlopen("https://example.invalid/a") as r:
                self.assertEqual(json.loads(r.read()), {"x": 1})
            with self.assertRaises(RuntimeError):
                urllib.request.urlopen("https://example.invalid/other")


class TestParityCases(unittest.TestCase):
    """Populated with one test per parity case below."""


def _make_test(name: str):
    def test(self):
        cases, python, worker = _get()
        case = next(c for c in cases if c.name == name)
        problems = harness.evaluate(case, python[name], worker[name])
        if problems:
            self.fail(f"[{', '.join(case.ids)}] {name} ({case.mode}"
                      f"{', ' + case.decision if case.decision else ''}):\n  " + "\n  ".join(problems))
    return test


def _attach_tests() -> None:
    if harness.node_binary() is None:
        def skip(self):
            raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
        TestParityCases.test_requires_node = skip
        return
    for case in harness.load_cases():
        attr = "test_" + re.sub(r"[^0-9A-Za-z_]", "_", case.name)
        setattr(TestParityCases, attr, _make_test(case.name))


_attach_tests()


def write_report(path: str) -> None:
    cases, python, worker = _get()
    rows = []
    for c in cases:
        problems = harness.evaluate(c, python[c.name], worker[c.name])
        rows.append({"name": c.name, "ids": c.ids, "mode": c.mode, "decision": c.decision,
                     "file": c.note, "passed": not problems, "problems": problems})
    report = {"cases": rows, "coverage": harness.coverage(cases),
              "summary": {"cases": len(rows), "passed": sum(r["passed"] for r in rows),
                          "pinned": sum(r["mode"] == "pinned" for r in rows),
                          "ruleIds": len(harness.coverage(cases))}}
    Path(path).write_text(json.dumps(report, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    if "--report" in sys.argv:
        i = sys.argv.index("--report")
        out = sys.argv[i + 1]
        del sys.argv[i:i + 2]
        write_report(out)
    unittest.main()
