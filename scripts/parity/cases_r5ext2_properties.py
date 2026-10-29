"""Property cases for the R5-extractors-2 repairs (3.0.6).

The brief asks date and boundary repairs to ship with property tests, not only worked examples. Each case runs the
same generated inputs through both runtimes and checks every output against an independent oracle, so the case
fails in whichever runtime breaks a property, and the harness still compares the two runtimes value for value.

- Year arithmetic (BR-EXT-110): adding 1 to 5, 7, 10 or 30 years to any date keeps the month and day, except 29 February,
  which becomes 28 February when the target year is a common year. The oracle is Python's datetime.
- The in-the-money boundary (Q3f): an option, warrant or convertible is in the money at price >= its threshold;
  treasury stock adds no shares at or below the strike, never more than the count, and never fewer as price rises.
"""

from __future__ import annotations

import datetime as _dt
import random

from scripts.parity.harness import Case
from yfmcp import capital_structure as cs
from yfmcp import funding_schedule as fs
from yfmcp import share_scenarios as ss

CASES: list[Case] = []
add = CASES.append

# ── Year arithmetic ──────────────────────────────────────────────────────────

_YEARS = (1, 2, 3, 4, 5, 7, 10, 30)  # ladder buckets reach 5 years; debt term ends can run 30


def _dates() -> list[str]:
    """The days around every end of February, plus the 1st and 15th of each month, for 1896 to 2404.

    The span covers the century rules: 1900, 2100, 2200 and 2300 are common years, 2000 and 2400 are leap years.
    """
    out: list[str] = []
    for y in range(1896, 2405):
        for m in range(1, 13):
            out += [f"{y:04d}-{m:02d}-01", f"{y:04d}-{m:02d}-15"]
        out += [f"{y:04d}-02-27", f"{y:04d}-02-28", f"{y:04d}-03-01"]
        if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0):
            out.append(f"{y:04d}-02-29")
    return out


_DATES = _dates()


def _oracle(date: str, years: int) -> str:
    d = _dt.date.fromisoformat(date)
    try:
        return d.replace(year=d.year + years).isoformat()
    except ValueError:  # 29 February into a common year
        return d.replace(year=d.year + years, day=28).isoformat()


def _year_mismatches(rows: list[list[str]]) -> dict:
    bad = [[d, n, got] for d, got_row in zip(_DATES, rows) for n, got in zip(_YEARS, got_row) if got != _oracle(d, n)]
    return {"checked": sum(len(r) for r in rows), "mismatches": len(bad), "first": bad[:5]}


_EXPECT_YEARS = {"checked": len(_DATES) * len(_YEARS), "mismatches": 0, "first": []}
_YEARS_JS = "return args[0].map((d) => args[1].map((n) => m.{mod}.addYears(d, n)));"

add(Case(name="r5ext2.prop.funding_add_years_matches_calendar", ids=["BR-EXT-110", "BR-EXT-044"],
         py=lambda: [[fs._add_years(d, n) for n in _YEARS] for d in _DATES],
         ts={"js": _YEARS_JS.format(mod="fundingSchedule"), "args": [_DATES, list(_YEARS)]},
         select=_year_mismatches, expect=_EXPECT_YEARS,
         note="R5-extractors-2: every later anniversary is a real calendar date"))
add(Case(name="r5ext2.prop.capital_add_years_matches_calendar", ids=["BR-EXT-110", "BR-EXT-045"],
         py=lambda: [[cs._add_years(d, n) for n in _YEARS] for d in _DATES],
         ts={"js": _YEARS_JS.format(mod="capitalStructure"), "args": [_DATES, list(_YEARS)]},
         select=_year_mismatches, expect=_EXPECT_YEARS,
         note="R5-extractors-2: every maturity-ladder bucket ends on a real calendar date"))

# ── In-the-money boundary ────────────────────────────────────────────────────

_COUNT = 1_000_000


def _boundary_runs() -> dict:
    """One bridge per strike (options, warrants and notes share it), priced at, just around, and well away from it."""
    rng = random.Random(306)
    strikes = [0.01, 1.0, 9.99, 10.0, 12.5, 33.33, 91.84, 250.0, 1234.56] + [round(rng.uniform(0.5, 500), 2) for _ in range(11)]
    runs = {}
    for i, k in enumerate(strikes):
        prices = sorted({k, round(k + 0.01, 2), max(0.01, round(k - 0.01, 2)), round(k * 0.5, 4) or 0.01, round(k * 2, 4),
                         round(k * 1.0001, 6), round(k * 0.9999, 6)} | {round(rng.uniform(k * 0.25, k * 4), 4) for _ in range(5)})
        bridge = {"basicShares": {"shares": 100_000_000}, "atmProgram": None, "unquantifiedShareClaims": [],
                  "components": [{"component": "stock_options", "outstanding": _COUNT, "weightedAverageExercisePrice": k},
                                 {"component": "warrants", "classes": [{"class": "W", "outstanding": _COUNT, "exercisable": _COUNT, "exercisePrice": k}]},
                                 {"component": "convertible_debt", "instruments": [{"instrument": "Notes", "principal": k * _COUNT, "ifConvertedShares": _COUNT,
                                                                                    "conversionPrice": k, "ifConvertedBasis": "principal / conversion_price",
                                                                                    "principalSettlement": None}]}]}
        # A call takes at most SHARE_SCENARIO_LIMIT scenarios, so each strike's prices run in chunks.
        for c in range(0, len(prices), ss.SHARE_SCENARIO_LIMIT):
            chunk = prices[c:c + ss.SHARE_SCENARIO_LIMIT]
            runs[f"k{i}c{c}"] = {"bridge": bridge, "scenarios": [{"name": f"p{c + j}", "price": p} for j, p in enumerate(chunk)]}
    return runs


_RUNS = _boundary_runs()
_SCEN_JS = """
const out = {};
for (const [label, run] of Object.entries(args[0])) {
  out[label] = m.shareScenarios.shareCountScenarios("tst", run.bridge, m.shareScenarios.parseShareScenarios(run.scenarios).scenarios);
}
return out;
"""


def _boundary_violations(v: dict) -> dict:
    bad: list = []
    lines = 0
    by_strike: dict = {}  # every scenario of one strike, across its chunks, for the monotone check
    for label, run in _RUNS.items():
        prices = {s["name"]: s["price"] for s in run["scenarios"]}
        by_strike.setdefault(label.split("c")[0], []).extend((prices[s["name"]], s) for s in v[label]["scenarios"])
    for label, rows in by_strike.items():
        last: dict = {}
        for price, s in sorted(rows, key=lambda r: r[0]):
            for ln in s["instruments"]:
                comp, k, itm, inc = ln["component"], ln.get("thresholdPrice"), ln.get("inTheMoney"), ln.get("incrementalShares")
                if comp not in ("stock_options", "warrants", "convertible_debt"):
                    continue
                lines += 1
                ok = k is not None and itm == (price >= k) and inc is not None
                if ok and comp != "convertible_debt":
                    ok = 0 <= inc <= _COUNT and (inc == 0 if price <= k else True) and inc >= last.get(comp, 0)
                    last[comp] = inc
                if ok and comp == "convertible_debt":
                    ok = inc == (_COUNT if price >= k else 0)
                if not ok:
                    bad.append([label, price, comp, k, itm, inc])
    return {"lines": lines, "violations": len(bad), "first": bad[:5]}


def _expected_lines() -> int:
    return sum(3 * len(r["scenarios"]) for r in _RUNS.values())


add(Case(name="r5ext2.prop.in_the_money_at_or_above_threshold", ids=["BR-EXT-003", "BR-EXT-018", "BR-EXT-021", "BR-EXT-082"],
         py=lambda: {k: ss.share_count_scenarios("tst", r["bridge"], ss.parse_share_scenarios(r["scenarios"])["scenarios"]) for k, r in _RUNS.items()},
         ts={"js": _SCEN_JS, "args": [_RUNS]},
         select=_boundary_violations, expect={"lines": _expected_lines(), "violations": 0, "first": []},
         note="Q3f (R5-extractors-2): one inclusive boundary for every instrument; treasury stock is 0 at the strike and monotone in price"))
