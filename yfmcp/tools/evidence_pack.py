"""Evidence-pack MCP tools.

Phase 5 unit 7: extracted from server.py as a pure move (no behaviour change).
Covers ``get_evidence_quality``, ``build_valuation_evidence_pack``, ``get_evidence_cut`` and
``list_evidence_cuts`` with their helpers (``_sec_filing_rows``, ``_run_component``, ``_not_applicable``).
Shared logic lives in ``yfmcp.evidence`` (parity with worker/src/evidence.ts); storage in
``yfmcp.evidence_store`` (YFMCP_EVIDENCE_DIR, else UNAVAILABLE).

Shared dependencies are imported from ``yfmcp.*``; this module never imports ``server`` (no import cycle).
The pack calls the SEC, earnings and fundamentals handlers it assembles and does not use the valuation
handlers, so it does not import ``yfmcp.tools.valuation``. ``server.py`` re-exports the names that
remaining server code, grouped routing, scripts and tests still look up as ``server.<name>``.
"""

import asyncio
import json
import re as _re
from typing import Any

from yfmcp import evidence as _ev
from yfmcp import evidence_store as _es
from yfmcp import valuation as _vl
from yfmcp.app import yfinance_server
from yfmcp.build_info import BUILD_SHA as _BUILD_SHA
from yfmcp.clients.edgar import _get_submissions_for_ticker, _warm_sec_submissions
from yfmcp.envelope import SERVER_VERSION, ErrorCode, _mcp_failure, normalize_status_confidence
from yfmcp.tools.earnings import extract_guidance
from yfmcp.tools.exposure import extract_capital_structure, extract_dilution_bridge
from yfmcp.tools.fundamentals import (
    _consensus_naming,
    _consensus_policy,
    _consensus_providers,
    _write_consensus_observation,
)
from yfmcp.tools.sec import list_sec_material_filings
from yfmcp.util import _now_iso
from yfmcp.validation import _validate_ticker


async def _sec_filing_rows(ticker: str) -> tuple[list[dict] | None, str]:
    try:
        cik_padded, subs = await _get_submissions_for_ticker(ticker)
    except Exception as exc:  # noqa: BLE001
        return None, f"PROVIDER_ERROR: {exc}"
    if not cik_padded or not subs:
        return None, "TICKER_NOT_FOUND"
    recent = (subs.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []

    def at(field: str, i: int) -> Any:
        values = recent.get(field) or []
        return values[i] if i < len(values) else None

    rows = []
    for i, form in enumerate(forms):
        filing_date = str(at("filingDate", i) or "")
        if not filing_date:
            continue
        inline = at("isInlineXBRL", i)
        rows.append({
            "form": str(form),
            "filingDate": filing_date,
            "reportDate": str(at("reportDate", i)) if at("reportDate", i) else None,
            "items": str(at("items", i)) if at("items", i) is not None else None,
            "isInlineXBRL": (int(inline) == 1) if inline is not None and str(inline).strip() != "" else None,
        })
    return rows, "OK"


@yfinance_server.tool(
    name="get_evidence_quality",
    description="Preflight before a valuation evidence pack: status, freshness and coverage per evidence family (quote, latest SEC periodic filing, capital structure and dilution readiness, earnings-release guidance, material 8-Ks, FY0/FY+1 consensus cells, durable storage) and the blockers, from light requests only. Evidence only.",
)
async def get_evidence_quality(ticker: str) -> str:
    as_of = _now_iso()
    (inputs, quote, splits), (rows, status), naming = await asyncio.gather(_consensus_providers(ticker), _sec_filing_rows(ticker), _consensus_naming(ticker))
    curve = _ev.build_consensus_curve(ticker, inputs, as_of, None, naming, splits)
    return json.dumps(_ev.evidence_quality(ticker=ticker, as_of=as_of, quote=quote, filings=rows, filings_status=status,
                                           consensus=curve, storage_available=_es.get_store() is not None))


async def _run_component(source_tool: str, coro: Any) -> dict:
    try:
        text = await coro
        return _ev.component_from_tool_text(source_tool, text, _now_iso())
    except Exception as exc:  # noqa: BLE001 - a failed component is evidence of that failure
        return _ev.component_from_tool_text(source_tool, None, _now_iso(), exc)


def _not_applicable(source_tool: str, code: str, message: str, retrieved_at: str) -> dict:
    return {"status": "NOT_APPLICABLE", "sourceTool": source_tool, "retrievedAt": retrieved_at, "data": None, "warnings": [], "error": {"code": code, "message": message}}


@yfinance_server.tool(
    name="build_valuation_evidence_pack",
    description="One evidence cut for valuation work: quote, evidence quality, consensus curve, EPS revisions, current capital structure, dilution bridge at the current price, latest guidance and material filings, each with its source tool, status, warnings and failure, plus a provenance receipt hashing every component. decisionUse is EVIDENCE_ONLY. Stored immutably under a content-addressed evidenceCutId when storage is available; the full payload is returned either way.",
)
async def build_valuation_evidence_pack(ticker: str, horizon_years: int = 5, persist: bool = True) -> str:
    policy = _consensus_policy(horizon_years, _ev.DEFAULT_CONSENSUS_POLICY["minAnalystCount"], _ev.DEFAULT_CONSENSUS_POLICY["conflictTolerancePct"])
    if isinstance(policy, str):
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": policy})
    symbol = ticker.upper()
    cutoff = _now_iso()
    (inputs, quote, splits), (rows, filings_status), naming = await asyncio.gather(_consensus_providers(symbol), _sec_filing_rows(symbol), _consensus_naming(symbol))
    curve = _ev.build_consensus_curve(symbol, inputs, cutoff, policy, naming, splits)
    major_price, major_currency = _vl._major_price(quote["price"], quote["currency"]) if quote["price"] is not None else (None, None)

    async def dilution() -> dict:
        if major_price is None:
            return _not_applicable("extract_dilution_bridge", "PRICE_UNAVAILABLE", "No current price to run the dilution bridge at.", cutoff)
        if major_currency != "USD":
            return _not_applicable("extract_dilution_bridge", "NON_USD_LISTING", f"The dilution bridge reads SEC filings for USD listings; {symbol} is quoted in {quote['currency']}.", cutoff)
        return await _run_component("extract_dilution_bridge", extract_dilution_bridge(ticker=symbol, price=major_price, currency="USD", filing_type="latest", include_atm=True))

    await _warm_sec_submissions(symbol)
    capital, diluted, guidance, events = await asyncio.gather(
        _run_component("extract_capital_structure", extract_capital_structure(ticker=symbol, filing_type="latest", include_funding_statements=True)),
        dilution(),
        _run_component("extract_guidance", extract_guidance(symbol, "latest")),
        _run_component("list_sec_material_filings", list_sec_material_filings(symbol, None, 10)),
    )
    # The release can fail to load while the other SEC reads run; one sequential retry, and the result says which.
    if (guidance.get("error") or {}).get("code") == "RELEASE_TEXT_NOT_AVAILABLE":
        guidance = await _run_component("extract_guidance", extract_guidance(symbol, "latest"))
    components = {
        "quote": {
            "status": "OK" if quote["price"] is not None else "FAILED",
            "sourceTool": "yahoo_quote_summary",
            "retrievedAt": inputs[0]["retrievedAt"],
            "data": {**quote, "majorUnitPrice": major_price, "majorUnitCurrency": major_currency},
            "warnings": [],
            "error": None if quote["price"] is not None else {"code": quote["status"], "message": quote.get("message") or "No price."},
        },
        "evidenceQuality": _ev.component_from_value("get_evidence_quality", _ev.evidence_quality(
            ticker=symbol, as_of=cutoff, quote=quote, filings=rows, filings_status=filings_status, consensus=curve, storage_available=_es.get_store() is not None,
        ), cutoff),
        "consensus": _ev.component_from_value("get_consensus_forecast_curve", curve, cutoff),
        "epsRevisions": _ev.component_from_value("get_eps_revisions", _ev.build_eps_revisions(symbol, inputs, cutoff, splits), cutoff),
        "currentCapitalStructure": capital,
        "currentDilution": diluted,
        "latestGuidance": guidance,
        "materialEvents": events,
    }
    # A pack whose providers all failed is still evidence of that; its components say so.
    if all(not i["periods"] for i in inputs):
        components["consensus"]["status"] = "LIMITED"
    # Components carry confidence levels only (F20), fixed before they are hashed.
    for component in components.values():
        normalize_status_confidence(component)
    hashes = {name: _es.sha256_hex(_ev.canonical_json(c)) for name, c in components.items()}
    receipt = _ev.build_receipt(ticker=symbol, evidence_cutoff=cutoff, server_version=SERVER_VERSION, build_sha=_BUILD_SHA, runtime="python_local",
                                components=components, component_hashes=hashes, components_sha256=_es.sha256_hex(_ev.canonical_json(components)))
    document = _ev.evidence_cut_document(ticker=symbol, evidence_cutoff=cutoff, components=components, receipt=receipt)
    body = _ev.canonical_json(document)
    sha = _es.sha256_hex(body)
    key = _ev.evidence_cut_key(symbol, _ev.compact_timestamp(cutoff), sha)
    stored = _es.put_once(key, body, {"ticker": symbol, "sha256": sha, "serverVersion": SERVER_VERSION}) if persist is not False else {"status": "SKIPPED"}
    observation = _write_consensus_observation(symbol, curve, cutoff) if persist is not False else {"status": "SKIPPED", "key": None}
    return json.dumps({
        **document,
        "evidenceCut": {
            "evidenceCutId": _ev.evidence_cut_id(symbol, cutoff, sha),
            "contentSha256": sha,
            "storageStatus": stored["status"],
            "storageKey": key if stored["status"] in ("STORED", "ALREADY_STORED") else None,
            **({"storageMessage": stored["message"]} if stored.get("message") else {}),
            "consensusObservation": observation,
            "verification": "contentSha256 is SHA-256 over canonical JSON (sorted keys, no whitespace, ECMAScript numbers) of this payload without the evidenceCut field.",
        },
    })


@yfinance_server.tool(
    name="get_evidence_cut",
    description="Retrieve a stored evidence cut by evidenceCutId and verify its integrity: the SHA-256 of the stored canonical JSON must equal the hash in the id (integrity VERIFIED or MISMATCH).",
)
async def get_evidence_cut(evidence_cut_id: str) -> str:
    parsed = _ev.parse_evidence_cut_id(evidence_cut_id or "")
    if parsed is None:
        return json.dumps({"error": True, "code": "INPUT_VALIDATION_ERROR", "message": "evidence_cut_id must look like ec1_<TICKER>_<YYYYMMDDTHHMMSSZ>_<sha256>."})
    store = _es.get_store()
    if store is None:
        return json.dumps({"error": True, "code": "STORAGE_UNAVAILABLE", "message": "Durable evidence storage is not configured; evidence cuts cannot be retrieved."})
    try:
        text = store.get(parsed["key"])
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"error": True, "code": "STORAGE_ERROR", "message": str(exc)})
    if text is None:
        return json.dumps({"error": True, "code": "NOT_FOUND", "message": f"No evidence cut is stored under {evidence_cut_id}."})
    actual = _es.sha256_hex(text)
    try:
        document = json.loads(text)
    except ValueError:
        document = None
    return json.dumps({
        "evidenceCutId": evidence_cut_id,
        "storageKey": parsed["key"],
        "integrity": "VERIFIED" if actual == parsed["sha256"] and document is not None else "MISMATCH",
        "expectedSha256": parsed["sha256"],
        "actualSha256": actual,
        "document": document,
    })


@yfinance_server.tool(
    name="list_evidence_cuts",
    description="Stored evidence cuts for a ticker, newest first, with their ids and cutoff times.",
)
async def list_evidence_cuts(ticker: str, limit: int = 20) -> str:
    ticker_err = _validate_ticker(ticker)
    if ticker_err:
        return _mcp_failure("list_evidence_cuts", ErrorCode.INPUT_VALIDATION_ERROR, ticker_err)
    symbol = ticker.upper()
    store = _es.get_store()
    if store is None:
        return json.dumps({"ticker": symbol, "storageStatus": "UNAVAILABLE", "cuts": []})
    cap = max(1, min(100, int(limit) if isinstance(limit, (int, float)) else 20))
    try:
        keys = store.list(f"evidence-cuts/{symbol}/", 1000)
    except Exception as exc:  # noqa: BLE001
        return json.dumps({"ticker": symbol, "storageStatus": "FAILED", "cuts": [], "message": str(exc)})
    cuts = []
    for key in keys:
        m = _re.match(r"^evidence-cuts/([^/]+)/(\d{8}T\d{6}Z)/([0-9a-f]{64})\.json$", key)
        if m:
            cuts.append({"evidenceCutId": f"ec1_{m.group(1)}_{m.group(2)}_{m.group(3)}", "cutoff": m.group(2), "storageKey": key})
    cuts.sort(key=lambda c: c["cutoff"], reverse=True)
    return json.dumps({"ticker": symbol, "storageStatus": "AVAILABLE", "cuts": cuts[:cap]})
