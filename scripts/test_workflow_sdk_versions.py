#!/usr/bin/env python3
"""Guardrail: keep GitHub workflow SDK/runtime versions non-deprecated."""

from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = ROOT / ".github" / "workflows"

REQUIRED_ACTION_VERSIONS = {
    "actions/checkout": "v5",
    "actions/setup-python": "v6",
    "actions/setup-node": "v5",
}

NODE_VERSION_PATTERN = re.compile(r'node-version:\s*"(\d+)"')


class TestWorkflowSdkVersions(unittest.TestCase):
    def test_required_actions_use_current_major(self) -> None:
        workflows = sorted(WORKFLOWS_DIR.glob("*.yml"))
        self.assertTrue(workflows, "No workflow files found")
        for wf in workflows:
            content = wf.read_text(encoding="utf-8")
            for action, version in REQUIRED_ACTION_VERSIONS.items():
                if action in content:
                    expected = f"{action}@{version}"
                    self.assertIn(expected, content, f"{wf.name}: expected {expected}")

    def test_node_version_is_24_when_declared(self) -> None:
        workflows = sorted(WORKFLOWS_DIR.glob("*.yml"))
        self.assertTrue(workflows, "No workflow files found")
        for wf in workflows:
            content = wf.read_text(encoding="utf-8")
            for match in NODE_VERSION_PATTERN.finditer(content):
                self.assertEqual(
                    match.group(1),
                    "24",
                    f'{wf.name}: expected node-version "24", got "{match.group(1)}"',
                )


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
