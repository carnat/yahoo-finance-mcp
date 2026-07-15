#!/usr/bin/env python3
"""Fixture-backed Thai SEC Fund v1 contract tests; no live SEC key required."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import unittest
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = json.loads((Path(__file__).with_name("thai_sec_fund_fixtures.json")).read_text(encoding="utf-8"))
os.environ["MCP_ENVELOPE_V2"] = "true"
os.environ.setdefault("SEC_OPEN_DATA_API_KEY", "fixture-key")

from yfmcp.tools import thai_funds as thai  # noqa: E402


def decode(raw: str) -> dict:
    return json.loads(raw)


class ThaiFundFixtureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_request = thai._request_json
        self.original_key = os.environ.get("SEC_OPEN_DATA_API_KEY")
        os.environ["SEC_OPEN_DATA_API_KEY"] = "fixture-key"

    def tearDown(self) -> None:
        thai._request_json = self.original_request
        if self.original_key is None:
            os.environ.pop("SEC_OPEN_DATA_API_KEY", None)
        else:
            os.environ["SEC_OPEN_DATA_API_KEY"] = self.original_key

    def set_responses(self, responses: dict[str, object]) -> None:
        async def fake_request(path: str, _params: dict[str, object]) -> dict:
            value = responses[path]
            if isinstance(value, Exception):
                raise value
            return copy.deepcopy(value)  # type: ignore[arg-type]
        thai._request_json = fake_request

    def test_exact_resolution_and_unordered_nav_use_maximum_date(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        async def fake_request(path: str, params: dict[str, object]) -> dict:
            calls.append((path, params))
            return copy.deepcopy(FIXTURES["profile"] if path == "/general-info/profiles" else FIXTURES["nav_unordered"])

        thai._request_json = fake_request
        payload = decode(asyncio.run(thai.get_thai_fund_nav("SCBSEMI(E)", as_of_date="2026-07-10", lookback_days=5)))
        self.assertTrue(payload["ok"])
        data = payload["data"]
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["identity"]["projId"], "M0232_2564")
        self.assertEqual(data["requestedWindow"]["startDate"], "2026-07-06")
        self.assertEqual(data["nav"]["navDate"], "2026-07-10")
        self.assertEqual(data["freshness"]["calendarDaysFromAsOf"], 0)
        self.assertEqual(calls[0][1]["fund_status"], "Registered")
        self.assertEqual(calls[0][1]["next_cursor"], "")
        self.assertEqual(calls[1][1]["page_size"], 100)

    def test_explicit_project_nav_bypasses_profile_and_accepts_main_alias(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        async def fake_request(path: str, params: dict[str, object]) -> dict:
            calls.append((path, params))
            self.assertEqual(path, "/daily-info/nav")
            return copy.deepcopy(FIXTURES["nav_main_project"])

        thai._request_json = fake_request
        payload = decode(asyncio.run(thai.get_thai_fund_nav(
            "KFINFRASSF", "M0370_2564", "2026-07-10", 5,
        )))
        self.assertTrue(payload["ok"])
        data = payload["data"]
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["identity"]["projId"], "M0370_2564")
        self.assertEqual(data["identity"]["fundClassName"], "main")
        self.assertEqual(data["identity"]["requestedFundClassName"], "KFINFRASSF")
        self.assertEqual(data["identityResolution"]["status"], "NAV_PROJECT_ID_CONFIRMED_CLASS_ALIAS")
        self.assertEqual(data["identityResolution"]["method"], "EXPLICIT_PROJ_ID_DIRECT_NAV")
        self.assertEqual(data["nav"]["navDate"], "2026-07-10")
        self.assertEqual([path for path, _params in calls], ["/daily-info/nav"])
        self.assertNotIn("fund_class_name", calls[0][1])

    def test_explicit_project_nav_does_not_infer_multiple_sec_classes(self) -> None:
        self.set_responses({"/daily-info/nav": FIXTURES["nav_multiple_classes"]})
        payload = decode(asyncio.run(thai.get_thai_fund_nav(
            "PUBLIC-CODE", "M0999_2564", "2026-07-10", 5,
        )))
        data = payload["data"]
        self.assertEqual(data["status"], "AMBIGUOUS_SHARE_CLASS")
        self.assertEqual([candidate["fundClassName"] for candidate in data["candidates"]], ["CLASS-A", "CLASS-B"])
        self.assertEqual(data["recovery"]["action"], "PROVIDE_SEC_FUND_CLASS_NAME")

    def test_profile_lookup_uses_project_info_and_bounded_ipo_fallback(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        async def fake_request(path: str, params: dict[str, object]) -> dict:
            calls.append((path, params))
            if path == "/general-info/profiles" and params["fund_status"] == "Registered":
                return {"message": "success", "page_size": 100, "next_cursor": "", "items": []}
            return copy.deepcopy(FIXTURES["profile"] if path == "/general-info/profiles" else FIXTURES["nav_unordered"])

        thai._request_json = fake_request
        payload = decode(asyncio.run(thai.get_thai_fund_nav(
            "SCBSEMI(E)", as_of_date="2026-07-10", lookback_days=5, project_info="SCB Semiconductor",
        )))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["data"]["status"], "OK")
        profile_calls = [params for path, params in calls if path == "/general-info/profiles"]
        self.assertEqual([params["fund_status"] for params in profile_calls], ["Registered", "IPO"])
        self.assertTrue(all(params["project_info"] == "SCB Semiconductor" for params in profile_calls))
        self.assertTrue(all(params["next_cursor"] == "" for params in profile_calls))

    def test_ambiguous_and_mismatched_share_classes_are_not_selected(self) -> None:
        ambiguous = copy.deepcopy(FIXTURES["profile"])
        ambiguous["items"].append({**ambiguous["items"][0], "proj_id": "M0999_2564"})
        self.set_responses({"/general-info/profiles": ambiguous})
        response = decode(asyncio.run(thai.get_thai_fund_nav("SCBSEMI(E)", as_of_date="2026-07-10")))
        self.assertTrue(response["ok"])
        self.assertEqual(response["data"]["status"], "AMBIGUOUS_SHARE_CLASS")
        self.assertEqual(len(response["data"]["candidates"]), 2)

        self.set_responses({"/general-info/profiles": copy.deepcopy(FIXTURES["profile"])})
        resolution = asyncio.run(thai._resolve_fund("SCBSEMI(E)", "M0000_0000", None))
        self.assertEqual(resolution.status, "FUND_IDENTITY_MISMATCH")
        self.assertIsNone(resolution.identity)

    def test_empty_nav_is_scoped_to_the_requested_window(self) -> None:
        empty_nav = {"message": "success", "page_size": 100, "next_cursor": "", "items": []}
        self.set_responses({"/general-info/profiles": FIXTURES["profile"], "/daily-info/nav": empty_nav})
        response = decode(asyncio.run(thai.get_thai_fund_nav("SCBSEMI(E)", "M0232_2564", "2026-07-10", 45)))
        data = response["data"]
        self.assertEqual(data["status"], "NAV_NOT_FOUND_IN_WINDOW")
        self.assertEqual(data["nav"], None)
        self.assertEqual(data["recovery"]["action"], "EXPAND_WINDOW_UP_TO_90_DAYS")
        self.assertEqual(data["identityResolution"]["status"], "NAV_PROJECT_ID_UNVERIFIED_NO_ROWS")

    def test_factsheet_preserves_success_when_one_section_fails(self) -> None:
        self.set_responses({
            "/general-info/profiles": FIXTURES["profile"],
            "/factsheet/statistics": FIXTURES["statistics"],
            "/factsheet/top5-holdings": thai.SecThailandProviderError("RATE_LIMIT", "limited", "RETRY_LATER"),
            "/factsheet/urls": FIXTURES["urls"],
        })
        response = decode(asyncio.run(thai.get_thai_fund_factsheet("SCBSEMI(E)", "M0232_2564")))
        data = response["data"]
        self.assertEqual(data["status"], "PARTIAL")
        self.assertEqual(data["sections"]["statistics"]["scope"], "SHARE_CLASS")
        self.assertEqual(data["sections"]["top_holdings"]["status"], "RATE_LIMIT")
        self.assertEqual(data["sections"]["urls"]["asOfDate"], "2026-05-31")

    def test_dividend_page_is_project_scoped_and_sorted(self) -> None:
        self.set_responses({
            "/general-info/profiles": FIXTURES["profile"],
            "/daily-info/dividend-history": FIXTURES["dividends_unordered"],
        })
        response = decode(asyncio.run(thai.get_thai_fund_dividend_history("SCBSEMI(E)", "M0232_2564", 100)))
        data = response["data"]
        self.assertEqual(data["scope"], "PROJECT")
        self.assertTrue(data["hasMore"])
        self.assertEqual(data["nextCursor"], "cursor-page-2")
        self.assertEqual([row["dividendDate"] for row in data["dividends"]], ["2026-01-12", "2025-12-12"])
        self.assertEqual(data["dividends"][0]["classAbbrName"], "SCBSEMI")

    def test_missing_secret_and_http_auth_are_safe(self) -> None:
        os.environ.pop("SEC_OPEN_DATA_API_KEY", None)
        response = decode(asyncio.run(thai.get_thai_fund_nav("SCBSEMI(E)", "M0232_2564")))
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "SOURCE_UNCONFIGURED")
        self.assertNotIn("fixture-key", json.dumps(response))

        os.environ["SEC_OPEN_DATA_API_KEY"] = "fixture-key"
        original = thai._urlrequest.urlopen
        try:
            def reject(*_args, **_kwargs):
                raise HTTPError("https://api.sec.or.th", 403, "Forbidden", {}, io.BytesIO())
            thai._urlrequest.urlopen = reject
            with self.assertRaises(thai.SecThailandProviderError) as captured:
                thai._request_json_sync("/general-info/profiles", {"fund_class_name": "SCBSEMI(E)", "page_size": 1})
            self.assertEqual(captured.exception.code, "AUTH_ERROR")
        finally:
            thai._urlrequest.urlopen = original

    def test_provider_uses_query_string_get_without_body(self) -> None:
        captured: dict[str, object] = {}
        original = thai._urlrequest.urlopen
        try:
            class FakeResponse:
                status = 200
                headers = {"Content-Type": "application/json"}

                def __enter__(self):
                    return self
                def __exit__(self, *_args):
                    return False
                def read(self):
                    return b'{"message":"success","page_size":1,"next_cursor":"","items":[]}'

            def fake_urlopen(request, timeout):
                captured["url"] = request.full_url
                captured["data"] = request.data
                captured["method"] = request.get_method()
                captured["timeout"] = timeout
                captured["headers"] = dict(request.header_items())
                return FakeResponse()

            thai._urlrequest.urlopen = fake_urlopen
            thai._request_json_sync("/daily-info/nav", {"proj_id": "M0232_2564", "start_nav_date": "2026-07-01", "latest": True})
        finally:
            thai._urlrequest.urlopen = original
        self.assertEqual(captured["method"], "GET")
        self.assertIsNone(captured["data"])
        self.assertIn("proj_id=M0232_2564", captured["url"])
        self.assertIn("start_nav_date=2026-07-01", captured["url"])
        self.assertIn("latest=true", captured["url"])
        query = parse_qs(urlparse(str(captured["url"])).query, keep_blank_values=True)
        self.assertEqual(query["next_cursor"], [""])
        request_headers = {key.lower(): value for key, value in captured["headers"].items()}
        self.assertEqual(request_headers["content-type"], "application/json")
        self.assertEqual(request_headers["cache-control"], "no-cache")
        self.assertEqual(request_headers["ocp-apim-subscription-key"], "fixture-key")

    def test_invalid_json_is_redacted_but_diagnosable(self) -> None:
        original = thai._urlrequest.urlopen
        raw = b"<html>upstream error</html>"
        try:
            class FakeResponse:
                status = 200
                headers = {"Content-Type": "text/html; charset=utf-8"}

                def __enter__(self):
                    return self

                def __exit__(self, *_args):
                    return False

                def read(self):
                    return raw

            thai._urlrequest.urlopen = lambda *_args, **_kwargs: FakeResponse()
            with self.assertRaises(thai.SecThailandProviderError) as captured:
                thai._request_json_sync("/general-info/profiles", {"fund_class_name": "SCBRMJP"})
        finally:
            thai._urlrequest.urlopen = original
        self.assertEqual(captured.exception.code, "PROVIDER_ERROR")
        self.assertEqual(captured.exception.diagnostics, {
            "httpStatus": 200,
            "contentType": "text/html; charset=utf-8",
            "bodyBytes": len(raw),
            "bodySha256": hashlib.sha256(raw).hexdigest(),
        })
        self.assertNotIn(raw.decode(), json.dumps(captured.exception.diagnostics))

    def test_provider_diagnostics_are_preserved_in_the_mcp_failure(self) -> None:
        diagnostics = {"httpStatus": 200, "contentType": "text/html", "bodyBytes": 11, "bodySha256": "abc"}
        self.set_responses({
            "/general-info/profiles": thai.SecThailandProviderError(
                "PROVIDER_ERROR", "SEC Open Data returned an invalid JSON response.", "RETRY_LATER", diagnostics,
            ),
        })
        response = decode(asyncio.run(thai.get_thai_fund_nav("SCBRMJP")))
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["code"], "PROVIDER_ERROR")
        self.assertEqual(response["diagnostics"], diagnostics)


class ThaiFundWorkerParityTest(unittest.TestCase):
    def test_worker_has_same_bounded_provider_contract(self) -> None:
        worker = (ROOT / "worker" / "src" / "sec-thailand.ts").read_text(encoding="utf-8")
        tools = (ROOT / "worker" / "src" / "tools.ts").read_text(encoding="utf-8")
        catalog = json.loads((ROOT / "tool_catalog.json").read_text(encoding="utf-8"))
        for endpoint in ("/general-info/profiles", "/daily-info/nav", "/factsheet/statistics", "/factsheet/top5-holdings", "/factsheet/urls", "/daily-info/dividend-history"):
            self.assertIn(endpoint, worker)
        for token in (
            "url.searchParams.set", 'method: "GET"', "Ocp-Apim-Subscription-Key", "Content-Type", "Cache-Control",
            "SOURCE_UNCONFIGURED", "AUTH_ERROR", "RATE_LIMIT", "PROVIDER_TIMEOUT", 'fund_status: "Registered"',
            'fund_status: "IPO"', "projectInfoRaw", "bodySha256",
        ):
            self.assertIn(token, worker)
        for action in ("get_thai_fund_nav", "get_thai_fund_factsheet", "get_thai_fund_dividend_history"):
            self.assertIn(action, tools)
            self.assertIn(action, catalog["groups"]["thai_funds"]["actions"])
        self.assertIn("project_info", tools)
        self.assertIn("project_info", catalog["groups"]["thai_funds"]["description"])
        self.assertIn("NAV_NOT_FOUND_IN_WINDOW", worker)
        self.assertIn("EXPLICIT_PROJ_ID_DIRECT_NAV", worker)
        self.assertIn("NAV_PROJECT_ID_CONFIRMED_CLASS_ALIAS", worker)
        self.assertIn("identityFromNav", worker)
        self.assertIn('scope: "PROJECT"', worker)
        self.assertIn("rows.reduce", worker)


if __name__ == "__main__":
    unittest.main(verbosity=2)
