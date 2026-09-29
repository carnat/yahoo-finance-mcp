#!/usr/bin/env python3
"""Share-count scenarios and the funding/capex schedule (2.5.2): Worker vs local parity and semantics.

- Scenarios apply the caller's price and treatments to the dilution bridge's
  inventory; every instrument is included, excluded or unresolved with its
  mechanics, and no denominator is selected.
- The schedule classifies tagged schedules and filing statements as
  CONTRACTUAL, COMPANY_DISCLOSED_COMMITTED, COMPANY_GUIDED,
  AWARDED_CONTINGENT or UNRESOLVED, with timing and source.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"
ESBUILD = WORKER / "node_modules" / ".bin" / "esbuild"
sys.path.insert(0, str(ROOT))

from yfmcp import funding_schedule as fs  # noqa: E402
from yfmcp import share_scenarios as ss  # noqa: E402

BRIDGE = {
    "basis": "MECHANICAL_COMPANY_DISCLOSED",
    "periodEnd": "2026-06-30",
    "basicShares": {"shares": 300_000_000, "asOf": "2026-08-01", "basis": "cover_page"},
    "sources": [{"role": "primary", "filingType": "10-Q"}],
    "components": [
        {"component": "stock_options", "tranches": [
            {"range": "$0.00 - $5.00", "outstanding": 2_000_000, "weightedAverageExercisePrice": 3.0},
            {"range": "$20.00 - $40.00", "outstanding": 1_000_000, "weightedAverageExercisePrice": 30.0},
        ]},
        {"component": "unvested_share_awards", "unvested": 5_000_000, "countBasis": "nonvested"},
        {"component": "warrants", "classes": [
            {"class": "Public Warrants", "outstanding": 10_000_000, "exercisable": 10_000_000, "exercisePrice": 11.5},
            {"class": "Customer Warrant", "outstanding": 4_000_000, "exercisable": 1_000_000, "unvested": 3_000_000, "exercisePrice": 25.0},
            {"class": "Unpriced Warrants", "outstanding": 500_000, "exercisable": 500_000, "exercisePrice": None},
        ]},
        {"component": "convertible_debt", "instruments": [
            {"instrument": "4.25% Notes due 2032", "ifConvertedShares": 20_000_000, "conversionPrice": 50.0, "ifConvertedBasis": "principal / conversion_price"},
            {"instrument": "Convertible notes (not itemized)", "ifConvertedShares": None, "conversionPrice": None, "ifConvertedBasis": None},
        ]},
    ],
    "atmProgram": {"remainingCapacityUsd": 400_000_000},
    "notDisclosed": [],
    "reportedEpsDilution": None,
    "warnings": [],
}

# 2.5.9 (COHR): tagged instruments all resolved is not a full claim inventory.
_COVERED = {"scope": "TAGGED_INSTRUMENTS", "completeClaimInventory": False, "textScan": "READ"}
_PP = {"kind": "PRICE_PROTECTION", "status": "UNQUANTIFIED", "sentences": ["The Purchase Agreement includes a price protection provision."]}
CLAIM_BRIDGES = [
    {"basicShares": {"shares": 195_832_246}, "components": [], "atmProgram": None, "claimCoverage": _COVERED, "unquantifiedShareClaims": []},
    {"basicShares": {"shares": 195_832_246}, "components": [], "atmProgram": None, "claimCoverage": _COVERED, "unquantifiedShareClaims": [_PP]},
    {"basicShares": {"shares": 195_832_246}, "components": [], "atmProgram": None},
    # MRVL-like (2.5.9): convertible preferred, and a warrant class vesting on untagged conditions.
    {"basicShares": {"shares": 876_900_000}, "atmProgram": None, "claimCoverage": _COVERED, "unquantifiedShareClaims": [],
     "components": [
         {"component": "warrants", "classes": [{"class": "Customer Warrant C", "outstanding": 500_000, "exercisable": None,
                                                "exercisableBasis": "vesting_terms_without_vested_count", "exercisePrice": 50.0}]},
         {"component": "convertible_preferred", "instruments": [{"instrument": "Convertible preferred stock", "ifConvertedShares": 21_800_000,
                                                                 "ifConvertedBasis": "shares_issuable_tagged", "conversionPrice": 91.84}]},
     ]},
]

# 2.5.11 (LITE): principal settled in cash as the filing states, one note stated and one not.
NSS_BRIDGE = {"basicShares": {"shares": 74_000_000}, "atmProgram": None, "claimCoverage": _COVERED, "unquantifiedShareClaims": [],
              "components": [{"component": "convertible_debt", "instruments": [
                  {"instrument": "Notes due 2032", "principal": 1_000_000_000, "ifConvertedShares": 20_000_000, "conversionPrice": 50.0,
                   "ifConvertedBasis": "principal / conversion_price", "principalSettlement": {"stated": "PRINCIPAL_IN_CASH", "scope": "ALL_NOTES"},
                   # 2.5.12: a capped call with stated terms.
                   "cappedCall": {"strikePrice": 50.0, "capPrice": 100.0, "coveredShares": 20_000_000, "coverageBasis": "STATED_COUNT"}},
                  {"instrument": "Notes due 2030", "principal": 100_000_000, "ifConvertedShares": 2_000_000, "conversionPrice": 50.0,
                   "ifConvertedBasis": "principal / conversion_price", "principalSettlement": None,
                   "cappedCall": {"strikePrice": None, "capPrice": 90.0, "coveredShares": 2_000_000, "coverageBasis": "SHARES_UNDERLYING_NOTES_AT_PERIOD_END",
                                  "unresolvedReason": "CAPPED_CALL_TERMS_INCOMPLETE", "missingTerms": ["STRIKE_PRICE"]}},
              ]}]}
NSS_SCENARIOS = [
    {"name": "net-share", "price": 80, "convertibles": "net_share_settlement_when_stated"},
    {"name": "net-share-below", "price": 40, "convertibles": "net_share_settlement_when_stated"},
    {"name": "if-converted", "price": 80},
    {"name": "capped", "price": 80, "capped_calls": "offset_when_stated"},
    {"name": "above-cap", "price": 120, "capped_calls": "offset_when_stated"},
]

SCENARIOS = [
    {"name": "base", "price": 40},
    {"name": "all-in", "price": 40, "options": "gross", "warrants": "gross", "warrant_vesting": "all", "convertibles": "if_converted_all",
     "atm": "full_remaining_capacity", "known_issuance": [{"label": "Aug offering", "shares": 12_000_000, "source": "8-K 2026-08-15"}]},
    {"name": "low", "price": 10, "unvested_awards": "exclude", "convertibles": "exclude"},
    {"name": "above-conversion", "price": 60},
]

INVALID = [
    [],
    [{"name": "a"}],
    [{"name": "a", "price": 10, "options": "black_scholes"}],
    [{"name": "a", "price": 10}, {"name": "a", "price": 12}],
    [{"name": f"s{i}", "price": 10} for i in range(9)],
    [{"name": "a", "price": 10, "known_issuance": [{"label": "x", "shares": -5}]}],
    [{"name": "a", "price": True}],
]

PE = "2026-06-30"


def _fact(local, value, dims=None, period_end=PE, unit="USD"):
    return {"local": local, "value": value, "periodEnd": period_end, "dims": dims or {}, "unit": unit}


FACTS = [
    _fact("LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths", 0),
    _fact("LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo", 0),
    _fact("LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive", 1_150_000_000),
    _fact("LesseeOperatingLeaseLiabilityPaymentsRemainderOfFiscalYear", 2_100_000),
    _fact("LesseeOperatingLeaseLiabilityPaymentsDueNextTwelveMonths", 4_300_000),
    _fact("LesseeOperatingLeaseLiabilityPaymentsDue", 21_000_000),
    _fact("PurchaseObligationDueInNextTwelveMonths", 250_000_000),
    _fact("UnrecordedUnconditionalPurchaseObligationBalanceOnFirstAnniversary", 99),
    _fact("PurchaseObligationDueInNextTwelveMonths", 111, period_end="2025-12-31"),
    _fact("LongTermPurchaseCommitmentAmount", 180_000_000, {"LongTermPurchaseCommitmentByCategoryOfItemPurchasedAxis": "asts:LaunchServicesMember"}),
    _fact("LineOfCreditFacilityRemainingBorrowingCapacity", 100_000_000),
    # ASTS tags its purchase-commitment range as two plain facts (low and high).
    _fact("LongTermPurchaseCommitmentAmount", 775_000_000, {"srt:RangeAxis": "srt:MinimumMember"}),
    _fact("LongTermPurchaseCommitmentAmount", 795_000_000, {"srt:RangeAxis": "srt:MaximumMember"}),
]

STATEMENTS = [
    {"contextText": "We expect capital expenditures of $300 million to $350 million in 2026, primarily for satellite manufacturing. "
                    "As of June 30, 2026, we had non-cancelable purchase commitments of approximately $1.2 billion for launch services through 2028.",
     "sectionHeading": "Liquidity and Capital Resources", "documentUrl": "https://www.sec.gov/x/10q.htm", "filingDate": "2026-08-10", "accessionNumber": "0001193125-26-342550"},
    {"contextText": "In July 2026 the Company was awarded a grant of up to $43.5 million under the CHIPS Act, subject to the achievement of construction milestones. "
                    "We will need to fund significant capital expenditures for the deployment of our constellation. "
                    "We recognized stock-based compensation expense related to equity awards granted in 2026 of $12.3 million. "
                    "We anticipate spending $50 million in Q4 2026 on the new facility. "
                    "Restricted stock awarded to directors will vest in full on the one-year anniversary of the grant date. "
                    "We have reached a steady production stage and secured supply for our satellites and facilities funding. "
                    "As of June 30, 2026, the Company has purchase commitments of approximately $775.0 - $795.0 million primarily related to research and development, expected to be paid through 2027.",
     "sectionHeading": "Liquidity and Capital Resources", "documentUrl": "https://www.sec.gov/x/10q.htm", "filingDate": "2026-08-10", "accessionNumber": "0001193125-26-342550"},
    {"contextText": "We expect to have capital expenditures (including capitalized software) of $550.0 to $570.0 for the full year 2026 in order to support capacity. "
                    "We committed to purchase equipment at $2.50 per unit under the supply agreement.",
     "sectionHeading": "Liquidity", "documentUrl": "https://www.sec.gov/x/vrt.htm", "filingDate": "2026-07-30", "accessionNumber": "0001628280-26-000001"},
    {"contextText": "Risks include our ability to continue to raise funds to finance our capital expenditures;",
     "sectionHeading": "Forward-Looking Statements", "documentUrl": "https://www.sec.gov/x/10q.htm", "filingDate": "2026-08-10", "accessionNumber": "0001193125-26-342550"},
]

TIMING_CASES = [
    "Payments are due in the third quarter of 2027 under the agreement.",
    "We expect to spend the funds over the next twelve months.",
    "The remainder of 2026 capital spending is expected to be funded from cash.",
    "Capital spending in fiscal 2027 and 2028 will increase.",
    "The facility is funded through 2029.",
    "No timing is given for this capital program.",
    "Capital expenditures of $550.0 to $570.0 for the full year 2026.",
]


def _python_outputs() -> dict:
    parsed = ss.parse_share_scenarios(SCENARIOS)
    schedule = fs.funding_capex_schedule(
        ticker="asts", period_end=PE, source={"filingType": "10-Q"}, facts=FACTS, statements=STATEMENTS,
        balances={"cashAndEquivalents": 2_288_253_000, "shortTermInvestments": None}, atm_remaining_usd=400_000_000,
        atm_evidence={"kind": "remaining_capacity", "amountUsd": 400_000_000},
    )
    return {
        "scenarios": ss.share_count_scenarios("asts", BRIDGE, parsed["scenarios"]),
        "noBasic": ss.share_count_scenarios("asts", {**BRIDGE, "basicShares": None}, parsed["scenarios"][:1]),
        "claims": [ss.share_count_scenarios("cohr", b, parsed["scenarios"][:1]) for b in CLAIM_BRIDGES],
        "netShare": ss.share_count_scenarios("lite", NSS_BRIDGE, ss.parse_share_scenarios(NSS_SCENARIOS)["scenarios"]),
        "invalid": [ss.parse_share_scenarios(case) for case in INVALID],
        "schedule": schedule,
        "emptySchedule": fs.funding_capex_schedule(ticker="none", period_end=None, source=None, facts=[], statements=[], balances=None,
                                                    atm_remaining_usd=None, atm_evidence=None),
        "timing": [fs.timing_of(t) for t in TIMING_CASES],
    }


_HARNESS = r"""
const [ssUrl, fsUrl, fixturesPath] = process.argv.slice(-3);
const ss = await import(ssUrl);
const fs = await import(fsUrl);
const { readFileSync } = await import("node:fs");
const f = JSON.parse(readFileSync(fixturesPath, "utf8"));
const parsed = ss.parseShareScenarios(f.scenarios);
const out = {
  scenarios: ss.shareCountScenarios("asts", f.bridge, parsed.scenarios),
  noBasic: ss.shareCountScenarios("asts", { ...f.bridge, basicShares: null }, parsed.scenarios.slice(0, 1)),
  claims: f.claimBridges.map((b) => ss.shareCountScenarios("cohr", b, parsed.scenarios.slice(0, 1))),
  netShare: ss.shareCountScenarios("lite", f.nssBridge, ss.parseShareScenarios(f.nssScenarios).scenarios),
  invalid: f.invalid.map((c) => ss.parseShareScenarios(c)),
  schedule: fs.fundingCapexSchedule({ ticker: "asts", periodEnd: f.pe, source: { filingType: "10-Q" }, facts: f.facts, statements: f.statements,
    balances: { cashAndEquivalents: 2288253000, shortTermInvestments: null }, atmRemainingUsd: 400000000, atmEvidence: { kind: "remaining_capacity", amountUsd: 400000000 } }),
  emptySchedule: fs.fundingCapexSchedule({ ticker: "none", periodEnd: null, source: null, facts: [], statements: [], balances: null, atmRemainingUsd: null, atmEvidence: null }),
  timing: f.timingCases.map((t) => fs.timingOf(t)),
};
console.log(JSON.stringify(out));
"""


def _worker_outputs() -> dict:
    node = os.environ.get("NODE_BINARY") or shutil.which("node")
    if node is None or not ESBUILD.exists():
        raise unittest.SkipTest("node and worker/node_modules (npm ci) are required")
    with tempfile.TemporaryDirectory() as tmp:
        bundles = []
        for name in ("share-scenarios", "funding-schedule"):
            out = Path(tmp) / f"{name}.mjs"
            subprocess.run([str(ESBUILD), str(WORKER / "src" / f"{name}.ts"), "--bundle", "--format=esm", "--platform=neutral", f"--outfile={out}", "--log-level=error"],
                           cwd=WORKER, check=True, capture_output=True, text=True, timeout=120)
            bundles.append(out.as_uri())
        fx = Path(tmp) / "fixtures.json"
        fx.write_text(json.dumps({"bridge": BRIDGE, "claimBridges": CLAIM_BRIDGES, "scenarios": SCENARIOS, "invalid": INVALID, "pe": PE, "facts": FACTS,
                                  "statements": STATEMENTS, "timingCases": TIMING_CASES,
                                  "nssBridge": NSS_BRIDGE, "nssScenarios": NSS_SCENARIOS}), encoding="utf-8")
        harness = Path(tmp) / "harness.mjs"
        harness.write_text(_HARNESS, encoding="utf-8")
        result = subprocess.run([node, str(harness), *bundles, str(fx)], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout.strip().splitlines()[-1])


def _scenario(out: dict, name: str) -> dict:
    return next(s for s in out["scenarios"]["scenarios"] if s["name"] == name)


def _line(scenario: dict, instrument: str) -> dict:
    return next(ln for ln in scenario["instruments"] if ln["instrument"] == instrument)


class TestParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.py = _python_outputs()
        cls.ts = _worker_outputs()

    def test_outputs_match(self) -> None:
        for key in self.py:
            self.assertEqual(json.loads(json.dumps(self.py[key])), self.ts[key], key)


class TestShareScenarios(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()

    def test_base_scenario_mechanics(self) -> None:
        base = _scenario(self.out, "base")
        self.assertEqual(base["parameters"]["defaulted"], ["options", "unvested_awards", "warrants", "warrant_vesting", "convertibles", "atm", "capped_calls"])
        self.assertEqual(_line(base, "Options $0.00 - $5.00")["incrementalShares"], 1_850_000)
        self.assertEqual(_line(base, "Options $20.00 - $40.00")["incrementalShares"], 250_000)
        self.assertEqual(_line(base, "Public Warrants")["incrementalShares"], 7_125_000)
        customer = _line(base, "Customer Warrant")
        self.assertEqual((customer["count"], customer["countBasis"], customer["incrementalShares"]), (1_000_000, "vested_exercisable", 375_000))
        unpriced = _line(base, "Unpriced Warrants")
        self.assertEqual((unpriced["included"], unpriced["unresolvedReason"]), (False, "exercise price not tagged"))
        notes = _line(base, "4.25% Notes due 2032")
        self.assertEqual((notes["inTheMoney"], notes["thresholdPrice"], notes["incrementalShares"], notes["included"]), (False, 50.0, 0, True))
        self.assertEqual(_line(base, "At-the-market program")["treatment"], "exclude")
        self.assertEqual(base["totals"]["resultingShares"], 314_600_000)
        self.assertEqual(base["completeness"], "EXCLUDES_UNRESOLVED_INSTRUMENTS")
        self.assertEqual({u["instrument"] for u in base["unresolvedInstruments"]}, {"Unpriced Warrants", "Convertible notes (not itemized)"})
        self.assertIn({"component": "convertible_debt", "instrument": "4.25% Notes due 2032", "thresholdPrice": 50.0, "inTheMoney": False},
                      base["priceSensitiveInstruments"])

    def test_all_in_scenario(self) -> None:
        s = _scenario(self.out, "all-in")
        self.assertEqual(s["totals"]["incrementalByComponent"], {
            "stock_options": 3_000_000, "unvested_share_awards": 5_000_000, "warrants": 14_500_000,
            "convertible_debt": 20_000_000, "atm_program": 10_000_000})
        self.assertEqual(s["totals"]["knownIssuanceShares"], 12_000_000)
        self.assertEqual(s["totals"]["resultingShares"], 364_500_000)
        self.assertEqual(s["priceSensitiveInstruments"], [])

    def test_exclusions_and_price_flips(self) -> None:
        low = _scenario(self.out, "low")
        self.assertEqual(_line(low, "Unvested RSUs/PSUs")["included"], False)
        self.assertEqual(_line(low, "Public Warrants")["incrementalShares"], 0)
        self.assertEqual(_line(low, "4.25% Notes due 2032")["method"], "excluded by scenario")
        # A convertible excluded by treatment is not reported as unresolved.
        self.assertEqual([u["instrument"] for u in low["unresolvedInstruments"]], ["Unpriced Warrants"])
        high = _scenario(self.out, "above-conversion")
        self.assertEqual(_line(high, "4.25% Notes due 2032")["incrementalShares"], 20_000_000)

    def test_authority_boundary_and_status(self) -> None:
        out = self.out["scenarios"]
        self.assertEqual((out["selectedScenario"], out["selectedDenominator"], out["decisionUse"], out["status"]), (None, None, "EVIDENCE_ONLY", "PARTIAL"))
        self.assertEqual(self.out["noBasic"]["status"], "NOT_FOUND")
        self.assertIsNone(self.out["noBasic"]["scenarios"][0]["totals"]["resultingShares"])

    def test_claim_coverage(self) -> None:
        clean, claimed, unread = self.out["claims"][:3]
        self.assertEqual((clean["status"], clean["scenarios"][0]["completeness"]), ("COMPUTED", "TAGGED_INSTRUMENTS_RESOLVED"))
        self.assertFalse(clean["claimCoverage"]["completeClaimInventory"])
        self.assertEqual((claimed["status"], claimed["scenarios"][0]["completeness"]), ("PARTIAL", "EXCLUDES_UNQUANTIFIED_CLAIMS"))
        self.assertEqual(claimed["scenarios"][0]["totals"]["resultingShares"], 195_832_246, "an unquantified claim adds no shares")
        self.assertEqual(claimed["unquantifiedShareClaims"][0]["kind"], "PRICE_PROTECTION")
        self.assertEqual((unread["status"], unread["scenarios"][0]["completeness"], unread["claimCoverage"]["textScan"]), ("PARTIAL", "CLAIM_TEXT_NOT_READ", "NOT_READ"))

    def test_convertible_preferred_and_unknown_vesting(self) -> None:
        mrvl = self.out["claims"][3]["scenarios"][0]
        pref = next(ln for ln in mrvl["instruments"] if ln["component"] == "convertible_preferred")
        self.assertEqual((pref["thresholdPrice"], pref["count"]), (91.84, 21_800_000))
        warrant = next(ln for ln in mrvl["instruments"] if ln["component"] == "warrants")
        self.assertEqual((warrant["count"], warrant["included"], warrant["unresolvedReason"]), (None, False, "count not tagged"),
                         "a class vesting on untagged conditions is never counted as all 500,000")
        self.assertEqual(mrvl["completeness"], "EXCLUDES_UNRESOLVED_INSTRUMENTS")

    def test_net_share_settlement_treatment(self) -> None:
        out = self.out["netShare"]["scenarios"]
        by = {s["name"]: {ln["instrument"]: ln for ln in s["instruments"]} for s in out}
        # 20,000,000 if-converted less $1B / $80 = 7,500,000; the unstated note stays if-converted.
        self.assertEqual(by["net-share"]["Notes due 2032"]["incrementalShares"], 7_500_000)
        self.assertIn("net share settlement", by["net-share"]["Notes due 2032"]["method"])
        self.assertEqual(by["net-share"]["Notes due 2030"]["incrementalShares"], 2_000_000)
        self.assertIn("cash settlement of principal not stated", by["net-share"]["Notes due 2030"]["method"])
        self.assertEqual(by["net-share-below"]["Notes due 2032"]["incrementalShares"], 0)
        self.assertEqual(by["if-converted"]["Notes due 2032"]["incrementalShares"], 20_000_000)

    def test_capped_call_treatment(self) -> None:
        out = {sc["name"]: sc for sc in self.out["netShare"]["scenarios"]}
        lines = {ln["instrument"]: ln for ln in out["capped"]["instruments"]}
        # 20,000,000 x (80 - 50) / 80 = 7,500,000 shares back; above the $100 cap the value stops at $50 a share.
        self.assertEqual(lines["Notes due 2032 capped call"]["incrementalShares"], -7_500_000)
        above = {ln["instrument"]: ln for ln in out["above-cap"]["instruments"]}
        self.assertEqual(above["Notes due 2032 capped call"]["incrementalShares"], -8_333_333)
        # A capped call without a stated strike is unresolved, never netted.
        self.assertEqual(lines["Notes due 2030 capped call"]["unresolvedReason"], "capped call terms not stated: STRIKE_PRICE")
        self.assertEqual(out["capped"]["completeness"], "EXCLUDES_UNRESOLVED_INSTRUMENTS")
        self.assertEqual(out["capped"]["totals"]["incrementalByComponent"]["capped_call"], -7_500_000)
        # Ignored by default: no capped call line.
        self.assertFalse(any(ln["component"] == "capped_call" for ln in out["if-converted"]["instruments"]))

    def test_validation(self) -> None:
        errors = [r.get("error") for r in self.out["invalid"]]
        self.assertTrue(all(errors), errors)
        self.assertIn("price must be a positive number", errors[1])
        self.assertIn("options must be one of", errors[2])
        self.assertIn("repeated", errors[3])
        self.assertIn("At most 8", errors[4])


class TestFundingSchedule(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.out = _python_outputs()["schedule"]

    def test_contractual_schedules(self) -> None:
        rows = {(r["family"], r["bucket"]): r for r in self.out["contractual"]}
        self.assertEqual(rows[("debt_principal", "after_year_5")]["amount"], 1_150_000_000)
        self.assertEqual(rows[("debt_principal", "year_2")]["periodThrough"], "2028-06-30")
        self.assertEqual(rows[("operating_lease_payments", "remainder_of_fiscal_year")]["periodThrough"], None)
        self.assertEqual(rows[("purchase_obligations", "next_12_months")]["amount"], 250_000_000)  # first concept for a bucket wins
        commitments = [r for r in self.out["contractual"] if r["family"] == "purchase_commitment"]
        self.assertEqual([(r["amount"], r["counterpartyOrCategory"]) for r in commitments], [(180_000_000, "Launch Services"), (None, None)])
        self.assertEqual((commitments[1]["amountLow"], commitments[1]["amountHigh"], commitments[1]["amountBasis"]), (775_000_000, 795_000_000, "tagged_range"))
        self.assertEqual(self.out["contractualTotals"], [{"family": "operating_lease_payments", "concept": "LesseeOperatingLeaseLiabilityPaymentsDue", "amount": 21_000_000, "unit": "USD", "periodEnd": PE}])

    def test_text_classification(self) -> None:
        items = {i["evidence"]["sentence"][:30]: i for i in self.out["textItems"]}
        guided = items["We expect capital expenditures"]
        self.assertEqual((guided["classification"], guided["amountLow"], guided["amountHigh"], guided["amountQualifier"], guided["timing"]["year"]),
                         ("COMPANY_GUIDED", 300_000_000, 350_000_000, "range", 2026))
        committed = items["As of June 30, 2026, we had no"]
        self.assertEqual((committed["classification"], committed["amountLow"], committed["amountQualifier"], committed["timing"]["horizon"], committed["timing"]["year"]),
                         ("COMPANY_DISCLOSED_COMMITTED", 1_200_000_000, "approximately", "through_year", 2028))
        award = items["In July 2026 the Company was a"]
        self.assertEqual((award["classification"], award["amountLow"], award["amountQualifier"], award["timing"]["basis"]), ("AWARDED_CONTINGENT", 43_500_000, "up_to", "UNSTATED"))
        unresolved = items["We will need to fund significa"]
        self.assertEqual((unresolved["classification"], unresolved["unresolvedReason"], unresolved["amountLow"]), ("UNRESOLVED", "no amount stated", None))
        quarter = items["We anticipate spending $50 mil"]
        self.assertEqual((quarter["classification"], quarter["timing"]["quarter"], quarter["timing"]["year"]), ("COMPANY_GUIDED", 4, 2026))
        self.assertFalse(any("equity awards" in k for k in (i["evidence"]["sentence"] for i in self.out["textItems"])))
        range_commitment = items["As of June 30, 2026, the Compa"]
        self.assertEqual((range_commitment["classification"], range_commitment["amountLow"], range_commitment["amountHigh"], range_commitment["timing"]["year"]),
                         ("COMPANY_DISCLOSED_COMMITTED", 775_000_000, 795_000_000, 2027))
        sentences = " ".join(i["evidence"]["sentence"] for i in self.out["textItems"])
        for dropped in ("Restricted stock awarded", "raise funds to finance", "steady production stage"):
            self.assertNotIn(dropped, sentences)
        unscaled = items["We expect to have capital expen"[:30]]
        self.assertEqual((unscaled["classification"], unscaled["amountStatus"], unscaled["amountLow"], unscaled["amountAsWritten"], unscaled["currency"], unscaled["timing"]["year"]),
                         ("COMPANY_GUIDED", "SCALE_NOT_STATED", None, "$550.0 to $570.0", "USD", 2026))
        per_unit = items["We committed to purchase equip"]
        self.assertEqual((per_unit["classification"], per_unit["amountStatus"]), ("UNRESOLVED", "NOT_STATED"))
        self.assertEqual(guided["amountStatus"], "STATED")
        self.assertEqual(self.out["byClassification"], {"CONTRACTUAL": 8, "COMPANY_DISCLOSED_COMMITTED": 2, "COMPANY_GUIDED": 3, "AWARDED_CONTINGENT": 1, "UNRESOLVED": 2})

    def test_liquidity_and_boundary(self) -> None:
        self.assertEqual([(l["source"], l["classification"]) for l in self.out["liquiditySources"]], [
            ("cash_and_equivalents", "COMPANY_DISCLOSED_BALANCE"), ("undrawn_credit_facility", "CONTRACTUAL_AVAILABILITY"),
            ("atm_remaining_capacity", "AVAILABLE_AT_COMPANY_DISCRETION")])
        self.assertEqual(self.out["decisionUse"], "EVIDENCE_ONLY")
        self.assertIsNone(self.out["priceTarget"])
        empty = _python_outputs()["emptySchedule"]
        self.assertEqual((empty["status"], empty["warnings"][0]["code"]), ("NOT_FOUND", "NO_INLINE_XBRL"))

    def test_timing_phrases(self) -> None:
        timing = _python_outputs()["timing"]
        self.assertEqual([t.get("horizon", t["basis"]) for t in timing], ["quarter", "next_12_months", "remainder_of_fiscal_year", "years", "through_year", "UNSTATED", "year"])
        self.assertEqual(timing[6]["year"], 2026)
        self.assertEqual((timing[0]["quarter"], timing[0]["year"]), (3, 2027))
        self.assertEqual((timing[3]["fromYear"], timing[3]["toYear"]), (2027, 2028))


if __name__ == "__main__":
    unittest.main()
