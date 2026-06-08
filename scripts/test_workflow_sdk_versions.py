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

ACTION_USE_PATTERN = re.compile(r"^\s*-\s*uses:\s*(actions/[a-zA-Z0-9_-]+)@([vV][0-9]+)\s*$", re.MULTILINE)
NODE_VERSION_PATTERN = re.compile(r'^\s*node-version:\s*"?(\d+)(?:\.\d+)?(?:\.x)?"?\s*$', re.MULTILINE)


class TestWorkflowSdkVersions(unittest.TestCase):
    def test_required_actions_use_current_major(self) -> None:
        workflows = sorted(WORKFLOWS_DIR.glob("*.yml"))
        self.assertTrue(workflows, "No workflow files found")
        for wf in workflows:
            content = wf.read_text(encoding="utf-8")
            uses = ACTION_USE_PATTERN.findall(content)
            for action, major in uses:
                required = REQUIRED_ACTION_VERSIONS.get(action)
                if required is None:
                    continue
                self.assertEqual(major.lower(), required, f"{wf.name}: {action} must use {required}, got {major}")

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
