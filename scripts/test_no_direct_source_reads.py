#!/usr/bin/env python3
"""Guard (Phase 4 entry criterion): no script reads a god file's source text directly.

Phases 4 and 5 move code out of ``server.py`` and ``worker/src/yahoo-finance.ts`` (and out of
``worker/src/tools.ts``, ``worker/src/news.ts`` and the ``yfmcp/tools/`` domain modules) into client
and domain modules. A test that opens one of those files by name breaks on every move even when
behaviour is unchanged. Tests that need runtime source text read it through ``scripts/source_text.py``
(every runtime module, or one definition wherever it lives), or check behaviour instead.

This guard parses every script under ``scripts/`` and fails when one builds a path to a guarded file:
``ROOT / "server.py"``, ``os.path.join(root, "worker", "src", "yahoo-finance.ts")``, a literal such as
``"worker/src/tools.ts"``, ``ROOT / "yfmcp" / "tools"`` (a partial set of domain modules), or a loop
over guarded file names that reads each one. Naming a file in a message, docstring or esbuild import
line is not a read and is not flagged.

Offline, standard library only. Run: python scripts/test_no_direct_source_reads.py
"""

from __future__ import annotations

import ast
import re
import sys
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
ROOT = SCRIPTS.parent
sys.path.append(str(SCRIPTS))
import source_text  # noqa: E402

# Files whose text moves in Phases 4-5. A path ending in one of these names is a guarded read.
GUARDED_BASENAMES = {
    "server.py": "server.py",
    "yahoo-finance.ts": "worker/src/yahoo-finance.ts",
    "news.ts": "worker/src/news.ts",
    "tools.ts": "worker/src/tools.ts",
}
GUARDED_LITERAL = re.compile(
    r"^(?:\.{0,2}/)?(?:[\w.-]+/)*(?:server\.py|worker/src/(?:yahoo-finance|news|tools)\.ts|src/(?:yahoo-finance|news|tools)\.ts|yfmcp/tools(?:/[\w-]+\.py)?/?)$"
)

# The helper is the one place allowed to open runtime modules; this guard holds the names it checks.
EXEMPT_SCRIPTS = {"source_text.py", "test_no_direct_source_reads.py"}

# Explicit allow-list for a whole-file guard that cannot read through scripts/source_text.py.
# Key: (script path relative to scripts/, guarded target); value: why the direct read is legitimate.
# Every entry must still match a real read (test_allow_list_has_no_stale_entries).
# It is empty: check_tool_sync, the MCP SDK public-API guard, the version contract and the privacy
# greps all read through scripts/source_text.py (check_tool_sync finds the Worker manifest module
# by the TOOLS definition it holds, not by file name).
ALLOWED: dict[tuple[str, str], str] = {}

_READ_CALLS = {"open", "join", "Path", "PurePath", "PosixPath", "read_text", "read_bytes", "glob", "rglob"}


def _const_str(node: ast.AST) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _div_chain(node: ast.BinOp) -> list[str | None]:
    """Components of ``a / "b" / "c"``, left to right; non-constant parts are None."""
    parts: list[str | None] = []
    while isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        parts.append(_const_str(node.right))
        node = node.left  # type: ignore[assignment]
    parts.append(_const_str(node))
    return list(reversed(parts))


def _call_name(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr
    if isinstance(func, ast.Name):
        return func.id
    return ""


def _guarded_target(parts: list[str | None]) -> str | None:
    """The guarded file a path built from ``parts`` points at, if any."""
    strings = [p for p in parts if p is not None]
    for i in range(len(strings) - 1):
        if strings[i] == "yfmcp" and strings[i + 1] == "tools":
            return "yfmcp/tools/"
    joined = "/".join(strings)
    if strings and GUARDED_LITERAL.match(joined):
        return "yfmcp/tools/" if "yfmcp/tools" in joined else GUARDED_BASENAMES.get(Path(joined).name, joined)
    last = parts[-1] if parts else None
    if last is not None and last in GUARDED_BASENAMES:
        return GUARDED_BASENAMES[last]
    return None


def _reads_name(body: list[ast.stmt], name: str) -> bool:
    """Whether ``body`` passes ``name`` to a path join, open/read call or a ``/`` path expression."""
    for stmt in body:
        for node in ast.walk(stmt):
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
                if any(isinstance(n, ast.Name) and n.id == name for n in ast.walk(node)):
                    return True
            if isinstance(node, ast.Call) and _call_name(node) in _READ_CALLS:
                if any(isinstance(n, ast.Name) and n.id == name for arg in node.args for n in ast.walk(arg)):
                    return True
    return False


def find_direct_reads(source: str, filename: str = "<script>") -> list[tuple[int, str]]:
    """``(line, guarded target)`` for each place ``source`` builds a path to a guarded file."""
    tree = ast.parse(source, filename=filename)
    hits: set[tuple[int, str]] = set()
    for node in ast.walk(tree):
        target = None
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            target = _guarded_target(_div_chain(node))
        elif isinstance(node, ast.Call) and _call_name(node) in _READ_CALLS:
            target = _guarded_target([_const_str(arg) for arg in node.args]) if node.args else None
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            value = node.value
            if value not in GUARDED_BASENAMES and not any(c.isspace() for c in value) and GUARDED_LITERAL.match(value):
                target = _guarded_target([value])
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
            names = [_const_str(elt) for elt in getattr(node.iter, "elts", [])]
            guarded = [n for n in names if n in GUARDED_BASENAMES]
            loop_var = node.target.id if isinstance(node.target, ast.Name) else None
            if guarded and loop_var:
                body = node.body if isinstance(node, (ast.For, ast.AsyncFor)) else []
                if isinstance(node, ast.comprehension):
                    parent = next(p for p in ast.walk(tree) if node in getattr(p, "generators", []))
                    body = [ast.Expr(value=getattr(parent, "elt", None) or getattr(parent, "key"))]
                if _reads_name(body, loop_var):
                    target = GUARDED_BASENAMES[guarded[0]]
        if target is not None:
            hits.add((getattr(node, "lineno", 0), target))
    return sorted(hits)


def _scripts() -> list[Path]:
    return [
        path for path in sorted(SCRIPTS.rglob("*.py"))
        if "__pycache__" not in path.parts and path.name not in EXEMPT_SCRIPTS
    ]


def _all_reads() -> dict[tuple[str, str], list[int]]:
    found: dict[tuple[str, str], list[int]] = {}
    for path in _scripts():
        rel = path.relative_to(SCRIPTS).as_posix()
        for line, target in find_direct_reads(path.read_text(encoding="utf-8"), str(path)):
            found.setdefault((rel, target), []).append(line)
    return found


class TestNoDirectSourceReads(unittest.TestCase):
    def test_no_script_opens_a_god_file_directly(self) -> None:
        offenders = [
            f"scripts/{rel}:{lines[0]}: reads {target} directly"
            for (rel, target), lines in sorted(_all_reads().items())
            if (rel, target) not in ALLOWED
        ]
        self.assertEqual(
            offenders, [],
            "read runtime source through scripts/source_text.py (python_runtime_source / worker_runtime_source / "
            "find_definition), check behaviour instead, or add a commented ALLOWED entry for a whole-file guard",
        )

    def test_allow_list_has_no_stale_entries(self) -> None:
        reads = _all_reads()
        self.assertEqual([key for key in ALLOWED if key not in reads], [])

    def test_scan_covers_nested_scripts(self) -> None:
        names = {p.relative_to(SCRIPTS).as_posix() for p in _scripts()}
        for expected in ("check_tool_sync.py", "test_mcp_sdk_public_api.py", "parity/harness.py"):
            self.assertIn(expected, names)


class TestDetector(unittest.TestCase):
    """The detector itself: every pre-cleanup read shape is caught; names in messages are not."""

    CAUGHT = {
        'SERVER = ROOT / "server.py"': "server.py",
        'x = (ROOT / "worker" / "src" / "yahoo-finance.ts").read_text()': "worker/src/yahoo-finance.ts",
        'open(os.path.join(root, "worker", "src", "tools.ts"))': "worker/src/tools.ts",
        'p = os.path.join(os.path.dirname(here), "server.py")': "server.py",
        'Path("worker/src/news.ts").read_text()': "worker/src/news.ts",
        'p = "worker/src/yahoo-finance.ts"': "worker/src/yahoo-finance.ts",
        'p = ROOT / "yfmcp" / "tools" / "news.py"': "yfmcp/tools/",
        'tools_dir = ROOT / "yfmcp" / "tools"': "yfmcp/tools/",
        'tools_dir = os.path.join(root, "yfmcp", "tools")': "yfmcp/tools/",
        'for name in ("yahoo-finance.ts", "news.ts"):\n    open(os.path.join(root, "worker", "src", name))': "worker/src/yahoo-finance.ts",
        'texts = [(SRC / n).read_text() for n in ("tools.ts",)]': "worker/src/tools.ts",
    }
    NOT_CAUGHT = (
        'self.assertIn("get_option_chain", x, "server.py: default not found")',
        'ENTRY = \'export { a } from "./src/yahoo-finance.ts";\\n\'',
        '"""Reads the earnings section of worker/src/yahoo-finance.ts."""',
        'for expected in ("server.py", "app.py"):\n    self.assertIn(expected, names)',
        'cfg = ROOT / "worker" / "src" / "index.ts"',
        'schemas = ROOT / "yfmcp" / "schemas.py"',
        'for ts in sorted((WORKER / "src").glob("*.ts")):\n    pass',
    )

    def test_catches_every_direct_read_shape(self) -> None:
        for snippet, target in self.CAUGHT.items():
            with self.subTest(snippet=snippet):
                self.assertIn(target, [t for _, t in find_direct_reads(snippet)])

    def test_ignores_names_that_are_not_reads(self) -> None:
        for snippet in self.NOT_CAUGHT:
            with self.subTest(snippet=snippet):
                self.assertEqual(find_direct_reads(snippet), [])


class TestSourceTextHelper(unittest.TestCase):
    """The helper reads every runtime module and finds a definition in whichever module holds it."""

    def test_python_runtime_files_cover_every_module(self) -> None:
        files = set(source_text.python_runtime_files())
        expected = {ROOT / "server.py", ROOT / "tool_groups.py"}
        expected |= {p for p in (ROOT / "yfmcp").rglob("*.py") if "__pycache__" not in p.parts}
        self.assertEqual(files, expected)

    def test_worker_runtime_files_cover_every_module(self) -> None:
        files = set(source_text.worker_runtime_files())
        expected = {p for p in (ROOT / "worker" / "src").rglob("*.ts") if not p.name.endswith(".d.ts")}
        self.assertEqual(files, expected)
        self.assertIn(ROOT / "worker" / "src" / "yahoo-finance.ts", files)

    def test_runtime_source_holds_every_module(self) -> None:
        for runtime in source_text.RUNTIMES:
            text = source_text.runtime_source(runtime)
            for path in source_text.runtime_files(runtime):
                self.assertIn(path.read_text(encoding="utf-8"), text, path.name)

    def test_find_definition_locates_definitions_across_modules(self) -> None:
        cases = {
            ("worker", "getCompanyNews"): "worker/src/news.ts",
            ("worker", "normalizeStatus"): None,  # unexported helper, wherever it lives
            ("worker", "TOOLS"): None,
            ("python", "get_option_chain"): None,
            ("python", "_contextual_output_schema"): None,
        }
        for (runtime, name), module in cases.items():
            with self.subTest(name=name):
                definition = source_text.find_definition(runtime, name)
                if module is not None:
                    self.assertEqual(definition.relpath, module)
                self.assertRegex(
                    definition.text, rf"(?m)^(?:export\s+)?(?:async\s+)?(?:def|function|const|let|class)\s+{re.escape(name)}\b"
                    rf"|^{re.escape(name)}\s*[:=]",
                )
                self.assertIn(definition.text.strip(), definition.path.read_text(encoding="utf-8"))

    def test_find_definition_bounds_a_ts_function_at_its_closing_brace(self) -> None:
        text = source_text.definition_source("worker", "normalizeStatus")
        self.assertTrue(text.rstrip().endswith("}"), text[-80:])
        self.assertEqual(text.count("function normalizeStatus("), 1)

    def test_find_definition_refuses_missing_and_ambiguous_names(self) -> None:
        with self.assertRaises(LookupError):
            source_text.find_definition("worker", "noSuchDefinitionAnywhere")
        with self.assertRaises(LookupError):
            source_text.find_definition("python", "no_such_definition_anywhere")
        with self.assertRaises(ValueError):
            source_text.find_definition("cobol", "x")

    def test_source_between_needs_a_unique_start(self) -> None:
        with self.assertRaises(LookupError):
            source_text.source_between("worker", "import ", "\n")


if __name__ == "__main__":
    result = unittest.main(verbosity=2, exit=False)
    sys.exit(0 if result.result.wasSuccessful() else 1)
