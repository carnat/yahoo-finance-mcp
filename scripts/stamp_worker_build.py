#!/usr/bin/env python3
"""Stamp Cloudflare Worker build metadata into wrangler.toml before deploy."""

from __future__ import annotations

import ast
import os
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WRANGLER = ROOT / "worker" / "wrangler.toml"
VERSION_FILE = ROOT / "yfmcp" / "version.py"


def _release_version(path: Path = VERSION_FILE) -> str:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "RELEASE_VERSION" for target in statement.targets):
            continue
        value = ast.literal_eval(statement.value)
        if isinstance(value, str) and re.fullmatch(r"\d+\.\d+\.\d+", value):
            return value
    raise ValueError("yfmcp/version.py must define a numeric RELEASE_VERSION")


def _build_version(release_version: str, build_sha: str) -> str:
    if build_sha == "unknown":
        return release_version
    if not re.fullmatch(r"[0-9a-fA-F]{7,64}", build_sha):
        raise ValueError("BUILD_SHA must be a 7-64 character hexadecimal Git commit")
    return f"{release_version}+git.{build_sha[:7].lower()}"


def _upsert_var(text: str, key: str, value: str) -> str:
    line = f'{key} = "{value}"'
    pattern = rf"^{re.escape(key)} = \".*\"$"
    if re.search(pattern, text, flags=re.MULTILINE):
        return re.sub(pattern, line, text, flags=re.MULTILINE)
    return text.replace("[vars]\n", f"[vars]\n{line}\n", 1)


def main() -> int:
    release_version = _release_version()
    build_sha = os.environ.get("BUILD_SHA") or os.environ.get("GITHUB_SHA") or "unknown"
    build_version = _build_version(release_version, build_sha)
    deployed_at = os.environ.get("DEPLOYED_AT") or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    build_date = deployed_at[:10]
    text = WRANGLER.read_text(encoding="utf-8")
    text = _upsert_var(text, "SERVER_VERSION", release_version)
    text = _upsert_var(text, "BUILD_VERSION", build_version)
    text = _upsert_var(text, "BUILD_SHA", build_sha)
    text = _upsert_var(text, "DEPLOYED_AT", deployed_at)
    text = _upsert_var(text, "BUILD_DATE", build_date)
    WRANGLER.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
