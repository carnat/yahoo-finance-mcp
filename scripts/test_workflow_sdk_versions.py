#!/usr/bin/env python3
"""Guardrail: keep GitHub workflow SDK/runtime versions non-deprecated."""

from __future__ import annotations

import re
import sys
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
PYPROJECT = ROOT / "pyproject.toml"

REQUIRED_ACTION_VERSIONS = {
    "actions/checkout": "v5",
    "actions/setup-python": "v6",
    "actions/setup-node": "v5",
    "actions/upload-artifact": "v7",
}

# Actions are pinned to a full commit SHA, with the release tag in a trailing comment (S-09).
ACTION_USE_PATTERN = re.compile(
    r"^\s*-?\s*uses:\s*(actions/[a-zA-Z0-9_-]+)@[0-9a-f]{40}\s*#\s*([vV][0-9]+)(?:\.[0-9]+)*\s*$", re.MULTILINE
)
ANY_USE_PATTERN = re.compile(r"^\s*-?\s*uses:\s*(\S+)", re.MULTILINE)
PINNED_REF_PATTERN = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")
NODE_VERSION_PATTERN = re.compile(r'^\s*node-version:\s*"?(\d+)(?:\.\d+)?(?:\.x)?"?\s*$', re.MULTILINE)


class TestWorkflowSdkVersions(unittest.TestCase):
    def test_mcp_python_sdk_range_is_supported(self) -> None:
        project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
        mcp_dependencies = [
            dependency
            for dependency in project["dependencies"]
            if dependency.startswith("mcp[cli]")
        ]
        self.assertEqual(len(mcp_dependencies), 1, "expected exactly one mcp[cli] dependency")
        # yfmcp.app subclasses mcp.server.mcpserver.MCPServer (2.x public API only, Q8); 1.x and 3.x are unsupported.
        self.assertIn(">=2.2", mcp_dependencies[0], "yfmcp.app needs the MCP Python SDK 2.2 or newer")
        self.assertIn("<3", mcp_dependencies[0], "yfmcp.app supports MCP Python SDK 2.x only")

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

    def test_every_action_is_pinned_to_a_commit_sha(self) -> None:
        workflows = sorted(WORKFLOWS_DIR.glob("*.yml"))
        self.assertTrue(workflows, "No workflow files found")
        for wf in workflows:
            content = wf.read_text(encoding="utf-8")
            for ref in ANY_USE_PATTERN.findall(content):
                if ref.startswith("./"):
                    continue
                self.assertRegex(ref, PINNED_REF_PATTERN, f"{wf.name}: {ref} must be pinned to a full commit SHA")

    def test_npm_ci_skips_install_scripts(self) -> None:
        for wf in sorted(WORKFLOWS_DIR.glob("*.yml")):
            for line in wf.read_text(encoding="utf-8").splitlines():
                if "npm ci" in line:
                    self.assertIn("--ignore-scripts", line, f"{wf.name}: npm ci must use --ignore-scripts")

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
