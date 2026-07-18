#!/usr/bin/env python3
"""Stamp Cloudflare Worker build metadata into wrangler.toml before deploy."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WRANGLER = ROOT / "worker" / "wrangler.toml"


def _upsert_var(text: str, key: str, value: str) -> str:
    line = f'{key} = "{value}"'
    pattern = rf"^{re.escape(key)} = \".*\"$"
    if re.search(pattern, text, flags=re.MULTILINE):
        return re.sub(pattern, line, text, flags=re.MULTILINE)
    return text.replace("[vars]\n", f"[vars]\n{line}\n", 1)


def main() -> int:
    build_sha = os.environ.get("BUILD_SHA") or os.environ.get("GITHUB_SHA") or "unknown"
    deployed_at = os.environ.get("DEPLOYED_AT") or datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    build_date = deployed_at[:10]
    text = WRANGLER.read_text(encoding="utf-8")
    text = _upsert_var(text, "BUILD_SHA", build_sha)
    text = _upsert_var(text, "DEPLOYED_AT", deployed_at)
    text = _upsert_var(text, "BUILD_DATE", build_date)
    WRANGLER.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
