#!/usr/bin/env python3
"""Verify that the Cloudflare Worker tool list matches the Python MCP server.

Both server.py and worker/src/tools.ts define their own tool manifests.
This script extracts tool names from each and fails if they diverge,
preventing accidental desync when new tools are added.
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER_PY = ROOT / "server.py"
TOOLS_TS = ROOT / "worker" / "src" / "tools.ts"


def get_python_tools() -> set:
    """Extract tool names from @yfinance_server.tool(name=...) decorators."""
    source = SERVER_PY.read_text()
    names = set()
    for match in re.finditer(r'@yfinance_server\.tool\(\s*name\s*=\s*"([^"]+)"', source):
        names.add(match.group(1))
    return names


def get_worker_tools() -> set:
    """Extract tool names from the TOOLS array in tools.ts."""
    source = TOOLS_TS.read_text()
    names = set()
    for match in re.finditer(r'name:\s*"([^"]+)"', source):
        names.add(match.group(1))
    return names


def main():
    py_tools = get_python_tools()
    ts_tools = get_worker_tools()

    if not py_tools:
        print("ERROR: found 0 tools in server.py — regex may need updating", file=sys.stderr)
        return 1
    if not ts_tools:
        print("ERROR: found 0 tools in worker/src/tools.ts — regex may need updating", file=sys.stderr)
        return 1

    only_py = py_tools - ts_tools
    only_ts = ts_tools - py_tools

    if only_py or only_ts:
        print("ERROR: Tool list mismatch between server.py and worker/src/tools.ts!\n", file=sys.stderr)
        if only_py:
            print("  In server.py but MISSING from worker/src/tools.ts:", file=sys.stderr)
            for name in sorted(only_py):
                print(f"    - {name}", file=sys.stderr)
        if only_ts:
            print("\n  In worker/src/tools.ts but MISSING from server.py:", file=sys.stderr)
            for name in sorted(only_ts):
                print(f"    - {name}", file=sys.stderr)
        print(
            "\n  When adding a new tool, update BOTH server.py AND worker/src/tools.ts.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: {len(py_tools)} tools in sync between server.py and worker/src/tools.ts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
