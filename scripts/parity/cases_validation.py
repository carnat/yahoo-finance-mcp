"""Parity cases: central ticker and accession validation at the tool-call gate (2.12.0, R4 Q3b, spec §2 and §5).

Worker side: `validateToolArgs(action, args)` in worker/src/tools.ts, the one gate both tool modes run before any
handler code or fetch (it returns the failure envelope text, or null when the call may proceed), and
`validateAccession` in worker/src/validate.ts.

Python side: `_validate_accession` (yfmcp/validation.py) exists today, so the function-level accession cases are mode
"equal". The Python gate does not exist yet: `_py_gate` looks for `yfmcp.validation.validate_tool_args(action, args)`
(same contract as the Worker's `validateToolArgs`: the failure envelope JSON text, or None) and reports
PYTHON_GATE_PENDING while it is missing.
TODO(python-mirror): add the Python gate, then flip every case pinned under GATE_DECISION to mode "equal" with its
`expect_ts` as `expect`.

No case here touches the network: the gate rejects before any fetch, and a passing call is only checked, not run.
"""

from __future__ import annotations

import copy
import json

from scripts.parity.harness import Case
from yfmcp.validation import _validate_accession

CASES: list[Case] = []

GATE_DECISION = "Q3b TODO(python-mirror)"
PY_PENDING = "PYTHON_GATE_PENDING"
PASS = "PASS"

TICKER_MSG = "Invalid ticker symbol: '{}'. Must be 1-20 characters: uppercase letters, digits, or . - ^ ="
ACCESSION_MSG = "Invalid accession number: '{}'. Expected format: XXXXXXXXXX-YY-ZZZZZZ."
VALID_ACC = "0000320193-24-000123"


def _py_gate(rows):
    try:
        from yfmcp.validation import validate_tool_args  # type: ignore[attr-defined]  # TODO(python-mirror)
    except ImportError:
        return PY_PENDING
    return {label: validate_tool_args(action, copy.deepcopy(args)) for label, action, args in rows}


def gate_view(v):
    """PASS for a call the gate lets through; else the failure's code, message, tool and retryable."""
    if v is None:
        return PASS
    if isinstance(v, str):
        v = json.loads(v)
    err, meta = v.get("error") or {}, v.get("meta") or {}
    return {"code": err.get("code"), "message": err.get("message"), "tool": meta.get("tool"),
            "retryable": meta.get("retryable")}


def gate_case(name: str, ids: list[str], rows: list[tuple[str, str, dict]], expect_ts: dict, note: str = "") -> Case:
    """One table of (label, public action, args) through the gate in both runtimes."""
    js = ("const out = {};\n"
          f"for (const [label, action, a] of {json.dumps([list(r) for r in rows])}) out[label] = m.tools.validateToolArgs(action, a);\n"
          "return out;")
    return Case(name=name, ids=ids, py=lambda: _py_gate(rows), ts={"js": js},
                select=lambda v: v if v == PY_PENDING else {k: gate_view(x) for k, x in v.items()},
                mode="pinned", decision=GATE_DECISION, expect_py=PY_PENDING, expect_ts=expect_ts, note=note)


def reject(action: str, message: str) -> dict:
    return {"code": "INPUT_VALIDATION_ERROR", "message": message, "tool": action, "retryable": None}


# ── Function level: validateAccession mirrors _validate_accession exactly (message included) ────────────────────
ACCESSION_INPUTS = [VALID_ACC, f"  {VALID_ACC}\n", "000032019324000123", "320193-24-123", "", "0000320193-24-00012a",
                    # Python's str-pattern \d matches any Unicode decimal digit, so both runtimes accept these.
                    "٠" * 10 + "-" + "٠" * 2 + "-" + "٠" * 6]
CASES += [
    Case(name="validation.accession.function_mirrors_python_exactly", ids=["BR-ENV-005"],
         py=lambda: [_validate_accession(a) for a in ACCESSION_INPUTS],
         ts={"js": f"return {json.dumps(ACCESSION_INPUTS)}.map((a) => m.validate.validateAccession(a));"},
         expect=[None, None, ACCESSION_MSG.format("000032019324000123"), ACCESSION_MSG.format("320193-24-123"),
                 ACCESSION_MSG.format(""), ACCESSION_MSG.format("0000320193-24-00012a"), None]),
]

# ── The gate: tickers (string or each array element), subject, peers, tickers ───────────────────────────────────
TICKER_ROWS = [
    ("valid", "get_market_quote", {"ticker": "AAPL"}),
    ("lowercase_whitespace_normalised", "get_market_quote", {"ticker": " brk-b "}),
    ("index_fx_future_forms", "get_market_quote", {"ticker": ["^GSPC", "EURUSD=X", "ES=F", "BTC-USD", "7203.T"]}),
    ("slash", "list_sec_company_filings", {"ticker": "BRK/B"}),
    ("comma_string_is_not_split", "get_company_profile", {"ticker": "A,B"}),
    ("empty_string", "get_historical_prices", {"ticker": ""}),
    ("whitespace_only", "get_earnings_analysis", {"ticker": "   "}),
    ("twenty_chars", "get_market_quote", {"ticker": "A" * 20}),
    ("twenty_one_chars", "get_market_quote", {"ticker": "A" * 21}),
    ("raw_value_echoed", "get_historical_prices", {"ticker": " aapl$ "}),
    ("bad_element_in_array", "get_market_quote", {"ticker": ["AAPL", "BRK/B", "MSFT"]}),
    ("empty_element_in_array", "get_company_news", {"ticker": ["AAPL", ""]}),
    ("subject", "compare_peer_valuations", {"tickers": ["AAPL", "MSFT"], "subject": "BAD$"}),
    ("tickers_element", "compare_peer_valuations", {"tickers": ["AAPL", "M SFT"]}),
    ("peers_element", "get_historical_valuation_context", {"ticker": "AAPL", "peers": ["MSFT", "X|Y"]}),
    ("valid_peer_tools", "compare_peer_valuations", {"tickers": ["aapl", "msft"], "subject": "nvda"}),
    ("absent_optional_ticker", "get_uk_company_filings", {"company_number": "00102498"}),
    ("null_optional_ticker", "get_uk_company_filings", {"ticker": None, "company_number": "00102498"}),
    # Non-string values are left to the handler, as before the gate.
    ("non_string_ticker_left_to_handler", "get_market_quote", {"ticker": 123}),
    ("non_string_element_left_to_handler", "get_company_news", {"ticker": ["AAPL", 7]}),
    # Other grammars are out of scope (their own validators): Thai fund codes, UK company numbers, document URLs.
    ("thai_fund_class_not_checked", "get_thai_fund_nav", {"fund_class_name": "SCBSEMI(E)"}),
    ("document_url_not_checked", "get_sec_filing_outline", {"ticker": "AAPL", "document_url": "not a url"}),
    # An unknown tool name is left to the dispatcher ("Unknown tool name.").
    ("unknown_tool_left_to_dispatcher", "no_such_tool", {"ticker": "BRK/B"}),
]
CASES += [
    gate_case("validation.gate.tickers_subject_peers", ["BR-ENV-007", "Q3b"], TICKER_ROWS, {
        "valid": PASS,
        "lowercase_whitespace_normalised": PASS,
        "index_fx_future_forms": PASS,
        "slash": reject("list_sec_company_filings", TICKER_MSG.format("BRK/B")),
        "comma_string_is_not_split": reject("get_company_profile", TICKER_MSG.format("A,B")),
        "empty_string": reject("get_historical_prices", TICKER_MSG.format("")),
        "whitespace_only": reject("get_earnings_analysis", TICKER_MSG.format("   ")),
        "twenty_chars": PASS,
        "twenty_one_chars": reject("get_market_quote", TICKER_MSG.format("A" * 21)),
        "raw_value_echoed": reject("get_historical_prices", TICKER_MSG.format(" aapl$ ")),
        "bad_element_in_array": reject("get_market_quote", TICKER_MSG.format("BRK/B")),
        "empty_element_in_array": reject("get_company_news", TICKER_MSG.format("")),
        "subject": reject("compare_peer_valuations", TICKER_MSG.format("BAD$")),
        "tickers_element": reject("compare_peer_valuations", TICKER_MSG.format("M SFT")),
        "peers_element": reject("get_historical_valuation_context", TICKER_MSG.format("X|Y")),
        "valid_peer_tools": PASS,
        "absent_optional_ticker": PASS,
        "null_optional_ticker": PASS,
        "non_string_ticker_left_to_handler": PASS,
        "non_string_element_left_to_handler": PASS,
        "thai_fund_class_not_checked": PASS,
        "document_url_not_checked": PASS,
        "unknown_tool_left_to_dispatcher": PASS,
    }),
]

# ── The gate: accession_number / accessionNumber ────────────────────────────────────────────────────────────────
ACCESSION_ROWS = [
    ("dashed", "get_sec_filing_index", {"ticker": "AAPL", "accession_number": VALID_ACC}),
    ("dashed_padded", "extract_sec_filing_fact", {"ticker": "AAPL", "fact_name": "cash", "accession_number": f" {VALID_ACC} "}),
    ("eighteen_bare_digits", "get_sec_filing_index", {"ticker": "AAPL", "accession_number": "000032019324000123"}),
    ("short_form", "get_sec_filing_outline", {"ticker": "AAPL", "accession_number": "320193-24-123"}),
    ("camel_case_valid", "list_sec_filing_exhibits", {"ticker": "AAPL", "accessionNumber": VALID_ACC}),
    ("camel_case_invalid", "get_sec_filing_exhibit_content", {"ticker": "AAPL", "accessionNumber": "0000320193-24"}),
    ("null_optional", "get_sec_filing_index", {"ticker": "AAPL", "accession_number": None}),
    ("absent_optional", "get_sec_filing_index", {"ticker": "AAPL"}),
    # An empty accession reads as absent, as Python's `if accession_number:` call sites treat it today.
    ("empty_reads_as_absent", "get_sec_filing_index", {"ticker": "AAPL", "accession_number": ""}),
    ("ticker_checked_before_accession", "get_sec_filing_index", {"ticker": "BRK/B", "accession_number": "x"}),
]
CASES += [
    gate_case("validation.gate.accession_numbers", ["BR-ENV-005", "Q3b"], ACCESSION_ROWS, {
        "dashed": PASS,
        "dashed_padded": PASS,
        "eighteen_bare_digits": reject("get_sec_filing_index", ACCESSION_MSG.format("000032019324000123")),
        "short_form": reject("get_sec_filing_outline", ACCESSION_MSG.format("320193-24-123")),
        "camel_case_valid": PASS,
        "camel_case_invalid": reject("get_sec_filing_exhibit_content", ACCESSION_MSG.format("0000320193-24")),
        "null_optional": PASS,
        "absent_optional": PASS,
        "empty_reads_as_absent": PASS,
        "ticker_checked_before_accession": reject("get_sec_filing_index", TICKER_MSG.format("BRK/B")),
    }),
]
