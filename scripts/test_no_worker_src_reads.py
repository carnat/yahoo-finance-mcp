#!/usr/bin/env python3
"""Guard (Phase 6, 3.6.12): the Python runtime never reads anything under worker/src.

The shared JSON registries live in yfmcp/data (yfmcp.data.DATA_DIR) and ship as package data, so an installed wheel
carries them. A path that joins "worker" and "src" in yfmcp/ or server.py would break that again. Comments and
docstrings are documentation ("mirrors worker/src/x.ts") and are allowed; only code lines are scanned.
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
import unittest
from pathlib import Path

from scripts.source_text import PYTHON, module_sources

ROOT = Path(__file__).resolve().parents[1]

# "worker/src", "worker" / "src", 'worker', 'src', "worker", "src" and spacing variants.
WORKER_SRC = re.compile(r"""worker["'\s/,)]*\s*/?\s*["']?src""")


def _blank(lines: list[str], start: int, end: int) -> None:
    for i in range(start - 1, end):
        lines[i] = ""


def code_only(text: str) -> str:
    """The source with comments and docstrings removed (line numbers kept)."""
    text = text.lstrip("\ufeff")
    lines = text.splitlines()
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
                _blank(lines, body[0].lineno, body[0].end_lineno)
    for tok in tokenize.generate_tokens(io.StringIO(text).readline):
        if tok.type == tokenize.COMMENT:
            row, col = tok.start
            if lines[row - 1]:
                lines[row - 1] = lines[row - 1][:col]
    return "\n".join(lines)


def offenders(sources: dict[Path, str] | None = None, root: Path = ROOT) -> list[str]:
    """Code lines that name worker/src, in every Python runtime module (server.py, tool_groups.py, yfmcp/)."""
    sources = module_sources(PYTHON) if sources is None else sources
    found: list[str] = []
    for path, text in sources.items():
        for number, line in enumerate(code_only(text).splitlines(), 1):
            if WORKER_SRC.search(line):
                found.append(f"{path.relative_to(root).as_posix()}:{number}: {line.strip()}")
    return found


class NoWorkerSrcReadsTests(unittest.TestCase):
    def test_python_runtime_does_not_read_worker_src(self):
        self.assertEqual(offenders(), [])

    def test_scanner_flags_path_joins_and_allows_documentation(self):
        flagged = [
            'P = Path(__file__).parent / "worker" / "src" / "x.json"',
            'P = "worker/src/x.json"',
            "P = os.path.join('worker', 'src', 'x.json')",
        ]
        for line in flagged:
            self.assertTrue(WORKER_SRC.search(code_only(line + "\n")), line)
        allowed = '"""Mirrors worker/src/x.ts."""\n\ndef f():\n    """See worker/src/y.ts."""\n    return 1  # worker/src/z.ts\n'
        self.assertFalse(WORKER_SRC.search(code_only(allowed)))

    def test_shared_data_ships_with_the_package(self):
        from yfmcp.data import DATA_DIR

        self.assertEqual(DATA_DIR, ROOT / "yfmcp" / "data")
        for name in (
            "company-ir-page-registry.json",
            "exchange-calendars.json",
            "exposure-presets.json",
            "news-source-capabilities.json",
            "news-source-registry.json",
        ):
            self.assertTrue((DATA_DIR / name).is_file(), name)


if __name__ == "__main__":
    unittest.main()
