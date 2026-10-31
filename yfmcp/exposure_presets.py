"""Governed exposure presets (Q5, R5-extractors-3).

The term lists live in yfmcp/data/exposure-presets.json, which the Worker compiles in, so both runtimes search the same
terms in the same order.
"""

from __future__ import annotations

import json
from yfmcp.data import DATA_DIR

_PATH = DATA_DIR / "exposure-presets.json"
PRESETS: dict = json.loads(_PATH.read_text(encoding="utf-8"))["presets"]
CHINA: dict = PRESETS["china"]
