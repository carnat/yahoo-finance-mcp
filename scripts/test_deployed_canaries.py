#!/usr/bin/env python3
"""Registry-driven deployed MCP contract canaries.

This is the small blocking deploy gate. Broad parser/provider quality sweeps
belong in the larger deployed smoke scripts and may stay audit-only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
from pathlib import Path
from typing import Any, Callable

from live_smoke_utils import call_tool


ROOT = Path(__file__).resolve().parent
REGISTRY_PATH = ROOT / "deployed_canaries.json"
MCP_URL = os.environ.get("MCP_URL", "https://yahoo-finance-mcp.artinatw.workers.dev/mcp").strip()
UA = "Mozilla/5.0 (compatible; yahoo-finance-mcp-canary/1.0)"

EXPECTED_SCHEMA_VERSION = "2026-07-08"
EXPECTED_TOOL_MODE = os.environ.get("EXPECTED_TOOL_MODE", os.environ.get("TOOL_MODE", "expanded")).lower()
EXPECTED_BUILD_SHA = os.environ.get("EXPECTED_BUILD_SHA", "").strip()
ALLOW_NETWORK_SKIP = os.environ.get("ALLOW_NETWORK_SKIP", "1").lower() in {"1", "true", "yes"}


def extract_data(payload: Any) -> Any:
    if isinstance(payload, dict) and payload.get("ok") is True and "data" in payload:
        return payload.get("data")
    return payload


def assert_no_jsonrpc_error(payload: Any, tool: str) -> None:
    if isinstance(payload, dict) and payload.get("jsonrpc") == "2.0" and payload.get("error"):
        raise AssertionError(f"{tool} returned JSON-RPC error: {payload['error']}")


def assert_no_unknown_tool(payload: Any, tool: str) -> None:
    text = json.dumps(payload, sort_keys=True).lower()
    for needle in ("unknown tool", "method not found", "unregistered dispatch"):
        if needle in text:
            raise AssertionError(f"{tool} returned non-callable error: {payload}")


def assert_not_double_enveloped_failure(payload: Any, tool: str) -> None:
    if not isinstance(payload, dict) or payload.get("ok") is not True:
        return
    data = payload.get("data")
    if isinstance(data, dict) and data.get("ok") is False:
        raise AssertionError(f"{tool} wrapped inner ok:false as top-level ok:true: {payload}")
    if isinstance(data, dict) and isinstance(data.get("error"), (dict, str)):
        raise AssertionError(f"{tool} wrapped inner error as top-level ok:true: {payload}")


def _warning_codes(data: dict[str, Any]) -> set[str]:
    return {
        str(item.get("code"))
        for item in data.get("warnings", [])
        if isinstance(item, dict) and item.get("code")
    }


def _assert_contract(data: Any, label: str) -> None:
    if not isinstance(data, dict):
        raise AssertionError(f"{label} returned non-object data: {data!r}")
    required = (
        "envelopeSchemaVersion",
        "toolMode",
        "defaultToolMode",
        "groupedAvailable",
        "groupedEnabled",
        "responseFieldContract",
        "hiddenAliases",
        "doctrineToolStatus",
        "batchContracts",
        "buildSha",
    )
    for field in required:
        if field not in data:
            raise AssertionError(f"{label} missing {field}: {data}")
    if data.get("envelopeSchemaVersion") != EXPECTED_SCHEMA_VERSION:
        raise AssertionError(f"{label} envelopeSchemaVersion mismatch: {data}")
    if EXPECTED_TOOL_MODE and str(data.get("toolMode", "")).lower() != EXPECTED_TOOL_MODE:
        raise AssertionError(f"{label} toolMode mismatch: {data}")
    if data.get("defaultToolMode") != "expanded":
        raise AssertionError(f"{label} defaultToolMode mismatch: {data}")
    if data.get("groupedAvailable") is not True:
        raise AssertionError(f"{label} groupedAvailable must be true: {data}")
    if "get_holder_info" not in (data.get("hiddenAliases") or {}):
        raise AssertionError(f"{label} missing get_holder_info hidden alias: {data}")
    if EXPECTED_BUILD_SHA and data.get("buildSha") != EXPECTED_BUILD_SHA:
        raise AssertionError(
            f"{label} buildSha mismatch: {data.get('buildSha')!r} != {EXPECTED_BUILD_SHA!r}"
        )

    news_contract = ((data.get("batchContracts") or {}).get("get_company_news") or {})
    if news_contract.get("batchMode") != "independent_per_ticker":
        raise AssertionError(f"{label} missing independent get_company_news batch contract: {data}")
    ir_contract = data.get("companyIrRssDiscovery") or {}
    if ir_contract.get("provider") != "company_ir_rss" or ir_contract.get("source") != "company_ir":
        raise AssertionError(f"{label} missing company IR RSS discovery contract: {data}")
    if ir_contract.get("defaultInGetCompanyNews") is not False:
        raise AssertionError(f"{label} company IR RSS must not be in get_company_news defaults: {data}")

    status_map = data.get("doctrineToolStatus") or {}
    press = status_map.get("get_company_press_releases") or {}
    if press.get("capabilityStatus") != "ACTIVE" or press.get("doctrineUse") != "ALLOWED":
        raise AssertionError(f"{label} press-release tool status mismatch: {press}")
    overnight = status_map.get("get_overnight_quote") or {}
    if overnight.get("doctrineUse") != "DIAGNOSTICS_ONLY":
        raise AssertionError(f"{label} overnight tool status mismatch: {overnight}")


def health_contract(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    data = extract_data(payload)
    _assert_contract(data, "health_check")
    if isinstance(data, dict) and data.get("envelopeV2") is not True:
        raise AssertionError(f"health_check envelopeV2 expected true: {data}")


def manifest_contract(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    _assert_contract(extract_data(payload), "get_manifest_diagnostics")


def press_release_payload_gate(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    data = extract_data(payload)
    if not isinstance(data, dict):
        raise AssertionError(f"get_company_press_releases returned non-object: {data!r}")
    if not isinstance(data.get("items"), list):
        raise AssertionError(f"get_company_press_releases missing items[]: {data}")
    if not isinstance(data.get("warnings"), list):
        raise AssertionError(f"get_company_press_releases missing warnings[]: {data}")
    for field in ("coverageStatus", "decisionGrade", "decisionGradeBasis"):
        if field not in data:
            raise AssertionError(f"get_company_press_releases missing {field}: {data}")
    if data.get("decisionGrade") is True and data.get("coverageStatus") != "SEC_EX99_RESOLVED":
        raise AssertionError(f"decisionGrade true without SEC_EX99_RESOLVED: {data}")
    if data.get("coverageStatus") == "SEC_8K_FOUND_EX99_NOT_FOUND":
        if not isinstance(data.get("secEvidence"), dict):
            raise AssertionError(f"SEC_8K_FOUND_EX99_NOT_FOUND missing secEvidence: {data}")
        if "SEC_8K_FOUND_EX99_NOT_FOUND" not in _warning_codes(data):
            raise AssertionError(f"SEC_8K_FOUND_EX99_NOT_FOUND missing warning: {data}")


def news_batch_independent(payload: dict[str, Any], canary: dict[str, Any]) -> None:
    data = extract_data(payload)
    if not isinstance(data, dict):
        raise AssertionError(f"get_company_news batch returned non-object: {data!r}")
    expected = canary.get("args", {}).get("ticker") or []
    for ticker in expected:
        if ticker not in data:
            raise AssertionError(f"get_company_news batch missing {ticker}: {data}")
        if not isinstance(data.get(ticker), dict):
            raise AssertionError(f"get_company_news batch entry for {ticker} must be object: {data}")


def company_ir_source_status(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    data = extract_data(payload)
    if not isinstance(data, dict):
        raise AssertionError(f"company_ir news call returned non-object: {data!r}")
    source_status = data.get("sourceStatus") or {}
    if not isinstance(source_status, dict) or "company_ir" not in source_status:
        raise AssertionError(f"company_ir news call missing sourceStatus.company_ir: {data}")
    company_ir = source_status.get("company_ir") or {}
    if not isinstance(company_ir, dict):
        raise AssertionError(f"sourceStatus.company_ir must be an object: {data}")
    allowed = {
        "OK",
        "WEBSITE_NOT_AVAILABLE",
        "FEED_NOT_FOUND",
        "DISCOVERY_NOT_FOUND",
        "PROVIDER_ERROR",
        "PARSE_ERROR",
        "EMPTY_RESULT",
    }
    if company_ir.get("status") not in allowed:
        raise AssertionError(f"unexpected company_ir source status: {data}")
    if "companyName" not in company_ir or "identityConfidence" not in company_ir:
        raise AssertionError(f"company_ir source status missing identity diagnostics: {data}")
    if data.get("sourceCoverage") not in {"FULL", "PARTIAL"}:
        raise AssertionError(f"company_ir news call missing sourceCoverage: {data}")


def overnight_diagnostics_only(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    data = extract_data(payload)
    if not isinstance(data, dict):
        raise AssertionError(f"get_overnight_quote returned non-object data: {data!r}")
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    diagnostics = payload.get("diagnostics") if isinstance(payload, dict) else {}
    if not isinstance(diagnostics, dict):
        diagnostics = data.get("diagnostics") or {}
    provider = data.get("provider") or meta.get("provider") or diagnostics.get("provider")
    provider_status = data.get("providerStatus") or meta.get("providerStatus") or diagnostics.get("providerStatus")
    if provider != "yahoo":
        raise AssertionError(f"get_overnight_quote provider should be yahoo, got {provider!r}")
    if not provider_status:
        raise AssertionError(f"get_overnight_quote missing providerStatus: {data}")
    if data.get("decisionGrade") is not False:
        raise AssertionError(f"get_overnight_quote must be decisionGrade:false: {data}")
    if data.get("doctrineUse") != "DIAGNOSTICS_ONLY" or meta.get("doctrineUse") != "DIAGNOSTICS_ONLY":
        raise AssertionError(f"get_overnight_quote missing DIAGNOSTICS_ONLY metadata: payload={data} meta={meta}")
    if data.get("dataKind") != "yahoo_extended_hours_proxy":
        raise AssertionError(f"get_overnight_quote dataKind mismatch: {data}")
    if "TRUE_OVERNIGHT_PROVIDER_REMOVED" not in _warning_codes(data):
        raise AssertionError(f"get_overnight_quote missing TRUE_OVERNIGHT_PROVIDER_REMOVED warning: {data}")


def unsupported_query_error(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    if payload.get("ok") is not False:
        raise AssertionError(f"unsupported query_sec_filing_index must be top-level ok:false: {payload}")
    error = payload.get("error") or {}
    if error.get("code") != "UNSUPPORTED_QUERY_TYPE":
        raise AssertionError(f"unsupported query_sec_filing_index wrong code: {payload}")
    if not (payload.get("meta") or {}).get("supportedQueryTypes"):
        raise AssertionError(f"unsupported query_sec_filing_index missing supportedQueryTypes: {payload}")


def deprecated_alias_error(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    if payload.get("ok") is not False:
        raise AssertionError(f"deprecated alias validation failure must be top-level ok:false: {payload}")
    meta = payload.get("meta") or {}
    if meta.get("canonicalTool") != "get_historical_prices":
        raise AssertionError(f"deprecated alias missing canonicalTool: {payload}")
    if meta.get("deprecatedTool") is not True:
        raise AssertionError(f"deprecated alias missing deprecatedTool=true: {payload}")
    if meta.get("useInstead") != "get_historical_prices":
        raise AssertionError(f"deprecated alias missing useInstead: {payload}")
    warnings = meta.get("warnings") or []
    if not any(isinstance(item, dict) and item.get("code") == "DEPRECATED_ALIAS" for item in warnings):
        raise AssertionError(f"deprecated alias missing DEPRECATED_ALIAS warning: {payload}")


def aaoi_china_positive(payload: dict[str, Any], _canary: dict[str, Any]) -> None:
    data = extract_data(payload)
    if not isinstance(data, dict):
        raise AssertionError(f"AAOI China geographic revenue returned non-object: {data!r}")
    value = data.get("value")
    pct = data.get("valuePct")
    if not isinstance(value, (int, float)) or not (200_000_000 <= value <= 350_000_000):
        raise AssertionError(f"AAOI China value outside expected live range: {data}")
    if not isinstance(pct, (int, float)) or not (45 <= pct <= 70):
        raise AssertionError(f"AAOI China valuePct outside expected live range: {data}")
    evidence = data.get("evidence")
    if isinstance(evidence, list):
        evidence_ok = bool(evidence) and isinstance(evidence[0], dict)
        evidence_doc_url = evidence[0].get("documentUrl") or evidence[0].get("primaryDocumentUrl") or evidence[0].get("url") if evidence_ok else None
    else:
        evidence_ok = isinstance(evidence, dict) and bool(evidence)
        evidence_doc_url = evidence.get("documentUrl") or evidence.get("primaryDocumentUrl") or evidence.get("url") if evidence_ok else None
    if not evidence_ok:
        raise AssertionError(f"AAOI China positive extraction missing evidence: {data}")
    if not (data.get("documentUrl") or data.get("primaryDocumentUrl") or evidence_doc_url):
        raise AssertionError(f"AAOI China positive extraction missing document URL: {data}")


ASSERTIONS: dict[str, Callable[[dict[str, Any], dict[str, Any]], None]] = {
    "health_contract": health_contract,
    "manifest_contract": manifest_contract,
    "press_release_payload_gate": press_release_payload_gate,
    "news_batch_independent": news_batch_independent,
    "company_ir_source_status": company_ir_source_status,
    "overnight_diagnostics_only": overnight_diagnostics_only,
    "unsupported_query_error": unsupported_query_error,
    "deprecated_alias_error": deprecated_alias_error,
    "aaoi_china_positive": aaoi_china_positive,
}


def load_registry(path: Path = REGISTRY_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_registry(registry: dict[str, Any]) -> list[dict[str, Any]]:
    if registry.get("schemaVersion") != EXPECTED_SCHEMA_VERSION:
        raise AssertionError(f"canary registry schemaVersion mismatch: {registry.get('schemaVersion')!r}")
    canaries = registry.get("canaries")
    if not isinstance(canaries, list) or not canaries:
        raise AssertionError("canary registry must contain non-empty canaries[]")
    seen: set[str] = set()
    for canary in canaries:
        if not isinstance(canary, dict):
            raise AssertionError(f"canary must be object: {canary!r}")
        cid = canary.get("id")
        if not isinstance(cid, str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]*", cid):
            raise AssertionError(f"invalid canary id: {cid!r}")
        if cid in seen:
            raise AssertionError(f"duplicate canary id: {cid}")
        seen.add(cid)
        for field in ("description", "tool", "args", "assertion", "blocking", "volatility"):
            if field not in canary:
                raise AssertionError(f"{cid} missing {field}")
        if not isinstance(canary["args"], dict):
            raise AssertionError(f"{cid} args must be object")
        if canary["assertion"] not in ASSERTIONS:
            raise AssertionError(f"{cid} references unknown assertion {canary['assertion']!r}")
        if canary["blocking"] is not True:
            raise AssertionError(f"{cid} must be blocking in this registry")
    return canaries


def run_canaries(canaries: list[dict[str, Any]]) -> None:
    if not MCP_URL:
        raise AssertionError("MCP_URL is required")
    for idx, canary in enumerate(canaries, start=1):
        tool = str(canary["tool"])
        payload = call_tool(
            MCP_URL,
            tool,
            canary["args"],
            req_id=3000 + idx,
            user_agent=UA,
            timeout=120,
            retries=5,
        )
        assert_no_jsonrpc_error(payload, tool)
        assert_no_unknown_tool(payload, tool)
        assert_not_double_enveloped_failure(payload, tool)
        ASSERTIONS[str(canary["assertion"])](payload, canary)
        print(f"  PASS {canary['id']}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate-only", action="store_true", help="validate registry shape without live MCP calls")
    parser.add_argument("--require-network", action="store_true", help="fail instead of skipping if the Worker is unreachable")
    args = parser.parse_args(argv)

    registry = load_registry()
    canaries = validate_registry(registry)
    if args.validate_only:
        print(f"PASS deployed canary registry validation ({len(canaries)} canaries)")
        return 0

    try:
        run_canaries(canaries)
    except (TimeoutError, urllib.error.URLError) as exc:
        if ALLOW_NETWORK_SKIP and not args.require_network:
            print(f"SKIP deployed canaries: worker unreachable ({exc})")
            return 0
        raise
    print(f"PASS deployed contract canaries ({len(canaries)} canaries)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
