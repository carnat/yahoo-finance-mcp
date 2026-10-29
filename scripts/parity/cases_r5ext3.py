"""Parity cases for R5-extractors-3 (3.0.7): the China exposure preset moved to data (Q5).

The monetary loss-sign repair (BR-EXT-015) is covered in cases_extraction_rules.py.

extract_china_exposure and extract_exposure used to carry their China term lists inline, in both runtimes. They now read
worker/src/exposure-presets.json: the Worker compiles it in and Python reads the file. This case pins the lists each
runtime ends up with to the lists the code held before the move, in the same order (the first matching term is the
one reported), so an edit to the data file shows up here as a deliberate change.
"""

from __future__ import annotations

from scripts.parity.harness import Case
from yfmcp.exposure_presets import CHINA

CASES: list[Case] = []

_KEYS = ("region", "riskTerms", "entityTerms", "bankTerms", "manufacturingTerms", "namedEntities")

CASES.append(Case(
    name="r5ext3.china_preset.terms_as_before_the_move", ids=["Q5", "F39", "F62"],
    py=lambda: {k: CHINA[k] for k in _KEYS},
    ts={"js": f"const p = m.yahooFinance.CHINA_PRESET; return Object.fromEntries({list(_KEYS)!r}.map((k) => [k, p[k]]));"},
    expect={
        "region": "China",
        "riskTerms": ["China", "tariff", "export control", "Bank of China"],
        "entityTerms": ["Tongmei", "JinMei", "BoYu"],
        "bankTerms": ["Bank of China"],
        "manufacturingTerms": ["manufacturing", "production", "supply chain", "fab", "manufactured", "outsourcing partner", "contract manufacturer"],
        "namedEntities": ["foxconn", "tsmc", "luxshare", "catl", "byd", "tongmei", "catcher", "pegatron"],
    },
    note="Q5 (R5-extractors-3): the China term lists are data, unchanged in content and order",
))
