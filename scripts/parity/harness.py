"""Behaviour-contract parity harness (modernization Phase 2).

Each `Case` runs the same behaviour in both runtimes and compares the results:

    Case(
        name="ext.release_scale.millions",
        ids=["BR-EXT-015"],
        py=lambda: er.release_text_metric(TEXT, "revenue"),
        ts={"module": "extractionRules", "fn": "releaseTextMetric", "args": [TEXT, "revenue"]},
        expect={...},              # optional: the value both runtimes must produce
    )

Modes:
- ``equal`` (default): Python and Worker results must be identical after `canonical()`.
  If ``expect`` is given, both must also equal it, so a change made to both
  runtimes at once still fails.
- ``pinned``: today's behaviour is recorded as-is because an owner decision
  schedules it for repair (``decision`` names it, e.g. "Q3f" or "BR-VAL-025").
  ``expect`` pins a value both runtimes share; ``expect_py`` / ``expect_ts``
  pin a known Python/Worker divergence. The repair release flips the case to
  ``equal`` with the new expected value.

The Worker side is one esbuild bundle of every `worker/src` module, keyed by
camelCase file name (``capital-structure.ts`` -> ``capitalStructure``), run by
`runner.mjs` in a single Node process. Instead of ``module``/``fn``, ``ts`` may give
``js`` (an async function body receiving ``m``, ``args``, ``fx``) for setups a plain call
can't express, and ``"isolate": True`` loads a fresh bundle copy so module-level caches
start empty. ``routes`` serve canned HTTP responses to
both runtimes (Worker `fetch`, Python `urllib.request.urlopen`); any other
request fails with UNSTUBBED_FETCH, so the harness never touches the network.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import importlib
import io
import json
import math
import os
import shutil
import socket
import subprocess
import tempfile
import urllib.error
import urllib.request
from email.message import Message
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
RUNNER = Path(__file__).with_name("runner.mjs")
FIXTURES = ROOT / "fixtures" / "parity"


@dataclasses.dataclass
class Case:
    name: str
    ids: list[str]
    py: Callable[[], Any]
    ts: dict
    mode: str = "equal"
    decision: str | None = None
    expect: Any = dataclasses.field(default_factory=lambda: _UNSET)
    expect_py: Any = dataclasses.field(default_factory=lambda: _UNSET)
    expect_ts: Any = dataclasses.field(default_factory=lambda: _UNSET)
    select: Callable[[Any], Any] | None = None
    routes: dict | None = None
    note: str = ""
    source: str = ""  # the cases_*.py module, set by load_cases()

    def __post_init__(self) -> None:
        if self.mode not in ("equal", "pinned"):
            raise ValueError(f"{self.name}: mode must be 'equal' or 'pinned'")
        if self.mode == "pinned":
            if not self.decision:
                raise ValueError(f"{self.name}: a pinned case names the decision that schedules its repair")
            if self.expect is _UNSET and (self.expect_py is _UNSET or self.expect_ts is _UNSET):
                raise ValueError(f"{self.name}: a pinned case pins `expect`, or both `expect_py` and `expect_ts`")
        if not self.ids:
            raise ValueError(f"{self.name}: a case names at least one rule ID")


class _Unset:
    def __repr__(self) -> str:
        return "<unset>"


_UNSET = _Unset()


# ----------------------------------------------------------------- canonical form

def canonical(value: Any) -> Any:
    """JSON-comparable form shared by both runtimes.

    Dicts compare without key order; tuples become lists; dataclasses become
    dicts; integral floats become ints (Python 1.0 vs JS 1); NaN/inf become
    strings. Strings holding JSON are compared parsed, as the runner does.
    """
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = dataclasses.asdict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (ValueError, TypeError):
            return value
        return canonical(parsed) if isinstance(parsed, (dict, list)) else value
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return int(value) if value.is_integer() and abs(value) < 2**53 else value
    if isinstance(value, dict):
        return {str(k): canonical(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [canonical(v) for v in value]
    return value


def first_difference(a: Any, b: Any, path: str = "$") -> str | None:
    """Path and values of the first difference between two canonical values."""
    if type(a) is not type(b) and not (isinstance(a, (int, float)) and isinstance(b, (int, float))):
        return f"{path}: {a!r} != {b!r}"
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                return f"{path}.{k}: missing on the left, right={b[k]!r}"
            if k not in b:
                return f"{path}.{k}: left={a[k]!r}, missing on the right"
            d = first_difference(a[k], b[k], f"{path}.{k}")
            if d:
                return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f"{path}: length {len(a)} != {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            d = first_difference(x, y, f"{path}[{i}]")
            if d:
                return d
        return None
    return None if a == b else f"{path}: {a!r} != {b!r}"


# ----------------------------------------------------------------- Python network stub

class _FakeResponse(io.BytesIO):
    def __init__(self, url: str, status: int, body: bytes, headers: dict) -> None:
        super().__init__(body)
        self.url, self.status, self.code = url, status, status
        msg = Message()
        for k, v in (headers or {}).items():
            msg[k] = v
        self.headers = msg

    def getcode(self) -> int:
        return self.status

    def geturl(self) -> str:
        return self.url

    def info(self) -> Message:
        return self.headers


def _route_for(routes: dict, url: str) -> dict | None:
    if url in routes:
        return routes[url]
    prefixes = [k for k in routes if k.endswith("*") and url.startswith(k[:-1])]
    return routes[max(prefixes, key=len)] if prefixes else None


def _route_body(route: dict) -> bytes:
    if "json" in route:
        return json.dumps(route["json"]).encode("utf-8")
    body = route.get("body", "")
    return body.encode("utf-8") if isinstance(body, str) else body


@contextlib.contextmanager
def stub_urlopen(routes: dict | None):
    """Serve `routes` to every urllib request; anything else raises UNSTUBBED_FETCH.

    Covers `urllib.request.urlopen` and openers from `build_opener` (`OpenerDirector.open`),
    and refuses any other outbound socket connection, so a Python case never reaches the network.
    """
    routes = routes or {}
    original_urlopen = urllib.request.urlopen
    original_open = urllib.request.OpenerDirector.open
    original_connect = socket.socket.connect
    original_create = socket.create_connection

    def fake(req, *args, **kwargs):
        url = req if isinstance(req, str) else req.full_url
        route = _route_for(routes, url)
        if route is None:
            raise RuntimeError(f"UNSTUBBED_FETCH {url}")
        status = int(route.get("status", 200))
        body = _route_body(route)
        if status >= 400:
            raise urllib.error.HTTPError(url, status, "stubbed", Message(), io.BytesIO(body))
        return _FakeResponse(url, status, body, route.get("headers") or {})

    def fake_open(self, req, *args, **kwargs):
        return fake(req)

    def refuse(*args, **kwargs):
        raise RuntimeError(f"UNSTUBBED_FETCH socket connection {args[1:] or args}")

    urllib.request.urlopen = fake
    urllib.request.OpenerDirector.open = fake_open
    socket.socket.connect = refuse
    socket.create_connection = refuse
    try:
        yield
    finally:
        urllib.request.urlopen = original_urlopen
        urllib.request.OpenerDirector.open = original_open
        socket.socket.connect = original_connect
        socket.create_connection = original_create


# ----------------------------------------------------------------- Worker side

def _module_key(path: Path) -> str:
    """camelCase key: `yahoo-finance.ts` -> yahooFinance; a subdirectory module keeps its
    path, `clients/edgar.ts` -> clientsEdgar, `news/providers/sec.ts` -> newsProvidersSec."""
    rel = path.relative_to(WORKER / "src") if path.is_absolute() else path
    head, *rest = "-".join([*rel.parent.parts, rel.stem]).split("-")
    return head + "".join(p[:1].upper() + p[1:] for p in rest)


def node_binary() -> str | None:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    return node if node and ESBUILD.exists() else None


@functools.cache
def _bundle() -> Path:
    """One ESM bundle exposing every worker/src module under its camelCase name."""
    out_dir = Path(tempfile.mkdtemp(prefix="parity-bundle-"))
    entry = out_dir / "entry.ts"
    lines = []
    for ts in sorted((WORKER / "src").rglob("*.ts")):
        if ts.name.endswith(".d.ts"):
            continue
        lines.append(f'export * as {_module_key(ts)} from "{ts.as_posix()}";')
    entry.write_text("\n".join(lines) + "\n", encoding="utf-8")
    bundle = out_dir / "bundle.mjs"
    subprocess.run(
        [str(ESBUILD), str(entry), "--bundle", "--format=esm", "--platform=node",
         f"--outfile={bundle}", "--log-level=error"],
        cwd=WORKER, check=True, capture_output=True, text=True, timeout=180,
    )
    return bundle


def run_worker(cases: list[Case]) -> dict[str, dict]:
    node = node_binary()
    if node is None:
        raise RuntimeError("node and worker/node_modules (npm ci) are required for the parity harness")
    payload = {"cases": [{"name": c.name, "routes": c.routes or {}, **c.ts} for c in cases]}
    with tempfile.TemporaryDirectory(prefix="parity-cases-") as tmp:
        path = Path(tmp) / "cases.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        result = subprocess.run(
            [node, str(RUNNER), _bundle().as_uri(), str(path)],
            cwd=WORKER, check=True, capture_output=True, text=True, timeout=600,
        )
    return json.loads(result.stdout.strip().splitlines()[-1])


# ----------------------------------------------------------------- Python side

def run_python(case: Case) -> dict:
    try:
        with stub_urlopen(case.routes):
            value = case.py()
            if asyncio.iscoroutine(value):
                value = asyncio.run(value)
        return {"ok": True, "value": value}
    except Exception as exc:  # noqa: BLE001 - the error text is part of the compared behaviour
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ----------------------------------------------------------------- comparison

def _outcome(result: dict, select) -> Any:
    if not result["ok"]:
        return {"__error__": result["error"]}
    value = canonical(result["value"])
    return canonical(select(value)) if select else value


def evaluate(case: Case, py_result: dict, ts_result: dict) -> list[str]:
    """Problems with one case; an empty list means it passed."""
    problems = []
    py, ts = _outcome(py_result, case.select), _outcome(ts_result, case.select)
    if case.mode == "equal":
        d = first_difference(py, ts)
        if d:
            problems.append(f"Python and Worker differ at {d}")
        if case.expect is not _UNSET:
            exp = canonical(case.expect)
            for side, val in (("Python", py), ("Worker", ts)):
                d = first_difference(val, exp)
                if d:
                    problems.append(f"{side} differs from the expected value at {d}")
    else:
        exp_py = canonical(case.expect if case.expect is not _UNSET else case.expect_py)
        exp_ts = canonical(case.expect if case.expect is not _UNSET else case.expect_ts)
        for side, val, exp in (("Python", py, exp_py), ("Worker", ts, exp_ts)):
            d = first_difference(val, exp)
            if d:
                problems.append(f"{side} no longer matches the behaviour pinned for {case.decision} at {d}")
    return problems


# Pinned non-INTENTIONAL cases allowed during a release in progress; empty since 3.7.0.
_PINNED_FINDING_ALLOW_LIST: frozenset[str] = frozenset()


def pinned_decision_problems(cases: list[Case], allow: frozenset[str] | None = None) -> list[str]:
    """Names of pinned cases with separate expect_py/expect_ts whose decision does not start with "INTENTIONAL"."""
    allowed = _PINNED_FINDING_ALLOW_LIST if allow is None else allow
    return [
        c.name for c in cases
        if c.mode == "pinned" and c.expect is _UNSET and c.name not in allowed
        and not (c.decision or "").startswith("INTENTIONAL")
    ]


def load_cases() -> list[Case]:
    """Every CASES list in scripts/parity/cases_*.py, with names checked unique."""
    cases: list[Case] = []
    for path in sorted(Path(__file__).parent.glob("cases_*.py")):
        module = importlib.import_module(f"scripts.parity.{path.stem}")
        for case in getattr(module, "CASES", []):
            case.source = path.stem
            cases.append(case)
    names = [c.name for c in cases]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise ValueError(f"duplicate parity case names: {sorted(dupes)}")
    unlabelled = pinned_decision_problems(cases)
    if unlabelled:
        raise ValueError("pinned cases with separate expect_py/expect_ts must have a decision starting with "
                         f"'INTENTIONAL': {sorted(unlabelled)}")
    return cases


def coverage(cases: list[Case]) -> dict[str, list[str]]:
    """Rule ID -> case names."""
    out: dict[str, list[str]] = {}
    for c in cases:
        for rid in c.ids:
            out.setdefault(rid, []).append(c.name)
    return dict(sorted(out.items()))


def fixture(name: str) -> Any:
    """Load fixtures/parity/<name> (JSON parsed when the name ends in .json)."""
    path = FIXTURES / name
    text = path.read_text(encoding="utf-8")
    return json.loads(text) if path.suffix == ".json" else text
