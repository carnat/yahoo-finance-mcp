#!/usr/bin/env python3
"""Record live public inputs for the parity harness into fixtures/parity/ (run by hand, never in CI).

    python scripts/parity/record_fixtures.py                 # default ticker set
    python scripts/parity/record_fixtures.py --tickers AAPL  # a subset

SEC companyfacts are trimmed to the concepts the code base names (any CamelCase
identifier appearing in yfmcp/ or worker/src/), so a fixture holds what the rules
read and nothing else. Yahoo chart data is recorded for split-adjustment cases.
Fixtures are the equivalence oracle: re-recording changes expected values, so do
it deliberately and review the diff.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "fixtures" / "parity"
UA = "yahoo-finance-mcp parity-fixture-recorder admin@example.com"

# Chosen to exercise the P0 rules:
#   AAPL  52/53-week fiscal year, plain capital structure
#   COHR  fiscal-year naming offset, warrants/convertible history
#   MSTR  convertible notes, capped calls, net-share settlement
#   BE    ProfitLoss chosen with noncontrolling interests, convertibles
#   AEHR  files no borrowing concepts (zero-debt route)
#   BABA  20-F US-GAAP ADR (ordinary-shares-per-ADS ratio)
#   TSM   20-F IFRS ADR
#   NVDA  2024 10-for-1 split (split-adjusted historical prices)
DEFAULT_TICKERS = ["AAPL", "COHR", "MSTR", "BE", "AEHR", "BABA", "TSM", "NVDA"]
CHART_TICKERS = ["NVDA", "AAPL"]


def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 - fixed public hosts
        return resp.read()


def _code_concepts() -> set[str]:
    names: set[str] = set()
    for base in (ROOT / "yfmcp", ROOT / "worker" / "src"):
        for path in list(base.rglob("*.py")) + list(base.rglob("*.ts")):
            names.update(re.findall(r"\b[A-Z][a-z]+(?:[A-Z][a-z0-9]*)+\b", path.read_text(encoding="utf-8", errors="ignore")))
    return names


def record_companyfacts(tickers: list[str]) -> None:
    index = json.loads(_get("https://www.sec.gov/files/company_tickers.json"))
    cik_of = {row["ticker"].upper(): int(row["cik_str"]) for row in index.values()}
    keep = _code_concepts()
    (OUT / "sec").mkdir(parents=True, exist_ok=True)
    for t in tickers:
        cik = cik_of.get(t)
        if cik is None:
            print(f"skip {t}: no CIK", file=sys.stderr)
            continue
        facts = json.loads(_get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"))
        trimmed = {
            "cik": facts.get("cik"), "entityName": facts.get("entityName"),
            "facts": {tax: {c: v for c, v in concepts.items() if c in keep}
                      for tax, concepts in facts.get("facts", {}).items()},
            "_recorded": {"at": dt.date.today().isoformat(), "source": "data.sec.gov companyfacts", "trimmedTo": "concepts named in code"},
        }
        path = OUT / "sec" / f"companyfacts_{t}.json"
        path.write_text(json.dumps(trimmed, separators=(",", ":")) + "\n", encoding="utf-8")
        kept = sum(len(v) for v in trimmed["facts"].values())
        print(f"{t}: CIK {cik}, {kept} concepts, {path.stat().st_size // 1024} KiB")


def record_charts(tickers: list[str]) -> None:
    (OUT / "yahoo").mkdir(parents=True, exist_ok=True)
    for t in tickers:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range=5y&interval=1d&events=split%2Cdiv"
        body = json.loads(_get(url))
        body["_recorded"] = {"at": dt.date.today().isoformat(), "source": url}
        path = OUT / "yahoo" / f"chart_{t}_5y.json"
        path.write_text(json.dumps(body, separators=(",", ":")) + "\n", encoding="utf-8")
        print(f"{t}: chart {path.stat().st_size // 1024} KiB")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="*", default=DEFAULT_TICKERS)
    ap.add_argument("--charts", nargs="*", default=CHART_TICKERS)
    args = ap.parse_args()
    record_companyfacts([t.upper() for t in args.tickers])
    record_charts([t.upper() for t in args.charts])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
