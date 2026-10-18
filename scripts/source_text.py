"""The one place tests read runtime source text.

Phases 4 and 5 move code out of ``server.py`` and ``worker/src/yahoo-finance.ts``
into client and domain modules. A test that reads one of those files directly
breaks on every move even when behaviour is unchanged. Tests that legitimately
need source text (structural guards, "this marker must never ship" greps, a
slice-and-run of an unexported helper) read it through this module instead:

* ``python_runtime_source()`` / ``worker_runtime_source()`` return the text of
  every runtime module of that runtime, so a marker is found wherever it lives
  and a "must not be present" check covers every module.
* ``find_definition(runtime, name)`` returns the text of one top-level
  function / const / class definition from whichever module holds it.

Moving code between runtime modules must not break a caller of this module.
``scripts/test_no_direct_source_reads.py`` keeps other scripts from bypassing it.
Standard library only: the tool-sync CI job runs without project dependencies.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = "python"
WORKER = "worker"
RUNTIMES = (PYTHON, WORKER)

_WORKER_SRC = ROOT / "worker" / "src"
_PY_PACKAGE = ROOT / "yfmcp"
_PY_ENTRY_MODULES = ("server.py", "tool_groups.py")
# Test and declaration files under worker/src are not runtime modules.
_WORKER_EXCLUDED_SUFFIXES = (".test.ts", ".spec.ts", ".d.ts")


def python_runtime_files() -> list[Path]:
    """server.py, tool_groups.py and every module under yfmcp/ (sorted, stable order)."""
    files = [ROOT / name for name in _PY_ENTRY_MODULES if (ROOT / name).is_file()]
    files.extend(
        path for path in sorted(_PY_PACKAGE.rglob("*.py"))
        if "__pycache__" not in path.parts
    )
    return files


def worker_runtime_files() -> list[Path]:
    """Every TypeScript module under worker/src/, excluding tests and declaration files."""
    return [
        path for path in sorted(_WORKER_SRC.rglob("*.ts"))
        if not path.name.endswith(_WORKER_EXCLUDED_SUFFIXES)
    ]


def runtime_files(runtime: str) -> list[Path]:
    if runtime == PYTHON:
        return python_runtime_files()
    if runtime == WORKER:
        return worker_runtime_files()
    raise ValueError(f"unknown runtime {runtime!r}; expected one of {RUNTIMES}")


@lru_cache(maxsize=None)
def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def runtime_source(runtime: str) -> str:
    """Concatenated text of every runtime module of ``runtime``, newline-separated."""
    return "\n".join(_read(path) for path in runtime_files(runtime))


def python_runtime_source() -> str:
    return runtime_source(PYTHON)


def worker_runtime_source() -> str:
    return runtime_source(WORKER)


def module_sources(runtime: str) -> dict[Path, str]:
    """``{path: text}`` for every runtime module, for guards that report file and line."""
    return {path: _read(path) for path in runtime_files(runtime)}


@dataclass(frozen=True)
class Definition:
    runtime: str
    name: str
    path: Path
    start_line: int  # 1-based, includes decorators / leading ``export``
    end_line: int  # 1-based, inclusive
    text: str
    kind: str  # "function", "class" or "binding"

    @property
    def relpath(self) -> str:
        return self.path.relative_to(ROOT).as_posix()


# ── Worker (TypeScript) ──────────────────────────────────────────────────────
# A top-level declaration starts at column 0. Its text runs to the line before
# the next column-0 declaration, import or comment. The repo's TS is formatted,
# so column-0 lines inside a body are only closing brackets.
_TS_FUNCTION = r"(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s*\*?\s*{name}\b"
_TS_CLASS = r"(?:export\s+)?(?:default\s+)?(?:abstract\s+)?class\s+{name}\b"
_TS_BINDING = r"(?:export\s+)?(?:declare\s+)?(?:const|let|var|type|interface|enum)\s+{name}\b"
_TS_TOP_LEVEL = re.compile(
    r"^(?:export\b|function\b|async\b|const\b|let\b|var\b|class\b|abstract\b|type\b|"
    r"interface\b|enum\b|declare\b|import\b|//|/\*)"
)


def _ts_definitions(name: str) -> list[Definition]:
    patterns = [
        ("function", re.compile("^" + _TS_FUNCTION.format(name=re.escape(name)))),
        ("class", re.compile("^" + _TS_CLASS.format(name=re.escape(name)))),
        ("binding", re.compile("^" + _TS_BINDING.format(name=re.escape(name)))),
    ]
    found: list[Definition] = []
    for path in worker_runtime_files():
        lines = _read(path).splitlines()
        for index, line in enumerate(lines):
            kind = next((kind for kind, pattern in patterns if pattern.match(line)), None)
            if kind is None:
                continue
            end = index + 1
            while end < len(lines) and not _TS_TOP_LEVEL.match(lines[end]):
                end += 1
            while end > index + 1 and not lines[end - 1].strip():
                end -= 1
            found.append(Definition(WORKER, name, path, index + 1, end, "\n".join(lines[index:end]) + "\n", kind))
    return found


# ── Python ───────────────────────────────────────────────────────────────────
@lru_cache(maxsize=None)
def _py_tree(path: Path) -> ast.Module:
    # tool_groups.py starts with a UTF-8 BOM, which ast.parse rejects in a str.
    return ast.parse(_read(path).removeprefix("\ufeff"), filename=str(path))


def _py_bound_names(node: ast.stmt) -> list[str]:
    targets: list[ast.expr] = []
    if isinstance(node, ast.Assign):
        targets = list(node.targets)
    elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
        targets = [node.target]
    names: list[str] = []
    for target in targets:
        for sub in ast.walk(target):
            if isinstance(sub, ast.Name):
                names.append(sub.id)
    return names


def _py_definitions(name: str) -> list[Definition]:
    found: list[Definition] = []
    for path in python_runtime_files():
        lines = _read(path).splitlines()
        for node in _py_tree(path).body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                kind = "function"
            elif isinstance(node, ast.ClassDef) and node.name == name:
                kind = "class"
            elif name in _py_bound_names(node):
                kind = "binding"
            else:
                continue
            decorators = getattr(node, "decorator_list", [])
            start = min([node.lineno, *(d.lineno for d in decorators)])
            end = node.end_lineno or node.lineno
            found.append(Definition(PYTHON, name, path, start, end, "\n".join(lines[start - 1:end]) + "\n", kind))
    return found


def find_definitions(runtime: str, name: str) -> list[Definition]:
    """Every top-level definition of ``name`` across the runtime's modules."""
    if runtime == PYTHON:
        return _py_definitions(name)
    if runtime == WORKER:
        return _ts_definitions(name)
    raise ValueError(f"unknown runtime {runtime!r}; expected one of {RUNTIMES}")


def find_definition(runtime: str, name: str) -> Definition:
    """The single top-level definition of ``name`` in whichever runtime module holds it.

    A function or class definition wins over a plain binding of the same name
    (a re-export such as ``name = module.name`` in a compatibility shim). Raises
    ``LookupError`` when there is no definition, or more than one candidate.
    """
    found = find_definitions(runtime, name)
    if not found:
        raise LookupError(f"no top-level definition of {name!r} in any {runtime} runtime module")
    real = [d for d in found if d.kind != "binding"]
    candidates = real or found
    if len(candidates) > 1:
        where = ", ".join(f"{d.relpath}:{d.start_line}" for d in candidates)
        raise LookupError(f"{name!r} is defined more than once in the {runtime} runtime: {where}")
    return candidates[0]


def definition_source(runtime: str, name: str) -> str:
    """Shorthand for ``find_definition(runtime, name).text``."""
    return find_definition(runtime, name).text


def module_defining(runtime: str, name: str) -> tuple[Path, str]:
    """``(path, full text)`` of the module that defines ``name``, e.g. the Worker tool manifest ``TOOLS``.

    For a guard whose regex must stay scoped to one module (a manifest), located by
    what it defines rather than by a hard-coded file name.
    """
    path = find_definition(runtime, name).path
    return path, _read(path)


def source_between(runtime: str, start_marker: str, end_marker: str) -> str:
    """Text from ``start_marker`` up to (not including) ``end_marker`` within one module.

    For a test that slices a region bounded by two markers. Both markers must be
    in the same module; the start marker must occur in exactly one module.
    """
    hits = [(path, text) for path, text in module_sources(runtime).items() if start_marker in text]
    if not hits:
        raise LookupError(f"{start_marker!r} not found in any {runtime} runtime module")
    if len(hits) > 1:
        where = ", ".join(path.relative_to(ROOT).as_posix() for path, _ in hits)
        raise LookupError(f"{start_marker!r} occurs in more than one {runtime} module: {where}")
    path, text = hits[0]
    start = text.index(start_marker)
    end = text.find(end_marker, start + len(start_marker))
    if end < 0:
        raise LookupError(f"{end_marker!r} not found after {start_marker!r} in {path.relative_to(ROOT).as_posix()}")
    return text[start:end]
