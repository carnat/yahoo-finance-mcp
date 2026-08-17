#!/usr/bin/env python3
"""Regression checks for semantic release and exact-build identity."""

from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import stamp_worker_build as stamp  # noqa: E402
from yfmcp.version import RELEASE_VERSION  # noqa: E402


class TestVersionContract(unittest.TestCase):
    def test_python_mcp_initialize_is_pinned_to_exact_build_version(self) -> None:
        app_source = (ROOT / "yfmcp" / "app.py").read_text(encoding="utf-8")
        self.assertIn("from yfmcp.build_info import BUILD_VERSION", app_source)
        self.assertIn("yfinance_server._mcp_server.version = BUILD_VERSION", app_source)

    def test_committed_version_sources_match_canonical_release(self) -> None:
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(pyproject["project"]["dynamic"], ["version"])
        self.assertEqual(
            pyproject["tool"]["setuptools"]["dynamic"]["version"]["attr"],
            "yfmcp.version.RELEASE_VERSION",
        )

        package = json.loads((ROOT / "worker" / "package.json").read_text(encoding="utf-8"))
        package_lock = json.loads((ROOT / "worker" / "package-lock.json").read_text(encoding="utf-8"))
        wrangler = tomllib.loads((ROOT / "worker" / "wrangler.toml").read_text(encoding="utf-8"))
        uv_lock = (ROOT / "uv.lock").read_text(encoding="utf-8")
        self.assertEqual(package["version"], RELEASE_VERSION)
        self.assertEqual(package_lock["version"], RELEASE_VERSION)
        self.assertEqual(package_lock["packages"][""]["version"], RELEASE_VERSION)
        self.assertEqual(wrangler["vars"]["SERVER_VERSION"], RELEASE_VERSION)
        self.assertIn(
            f'name = "yahoo-finance-mcp"\nversion = "{RELEASE_VERSION}"',
            uv_lock,
        )

    def test_build_version_uses_semver_build_metadata(self) -> None:
        self.assertEqual(stamp._build_version("1.5.0", "541DFA26A14C"), "1.5.0+git.541dfa2")
        self.assertEqual(stamp._build_version("1.5.0", "unknown"), "1.5.0")
        with self.assertRaisesRegex(ValueError, "BUILD_SHA"):
            stamp._build_version("1.5.0", "not-a-commit")

    def test_stamp_writes_release_and_exact_build_identity(self) -> None:
        initial = """[vars]\nSERVER_VERSION = \"old\"\nBUILD_VERSION = \"\"\nBUILD_SHA = \"\"\nBUILD_DATE = \"\"\nDEPLOYED_AT = \"\"\n"""
        with tempfile.TemporaryDirectory() as temp_dir:
            wrangler = pathlib.Path(temp_dir) / "wrangler.toml"
            wrangler.write_text(initial, encoding="utf-8")
            with (
                patch.object(stamp, "WRANGLER", wrangler),
                patch.dict(
                    os.environ,
                    {
                        "BUILD_SHA": "541dfa26a14ce7554773b9385d700a2808d68fb5",
                        "DEPLOYED_AT": "2026-08-17T04:05:06Z",
                    },
                    clear=False,
                ),
            ):
                self.assertEqual(stamp.main(), 0)

            values = tomllib.loads(wrangler.read_text(encoding="utf-8"))["vars"]
            self.assertEqual(values["SERVER_VERSION"], RELEASE_VERSION)
            self.assertEqual(values["BUILD_VERSION"], f"{RELEASE_VERSION}+git.541dfa2")
            self.assertEqual(values["BUILD_SHA"], "541dfa26a14ce7554773b9385d700a2808d68fb5")
            self.assertEqual(values["DEPLOYED_AT"], "2026-08-17T04:05:06Z")
            self.assertEqual(values["BUILD_DATE"], "2026-08-17")


if __name__ == "__main__":
    unittest.main()
