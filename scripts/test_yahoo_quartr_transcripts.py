import asyncio
import copy
import json
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import server as srv
from yfmcp.tools import earnings as earnings_tools
from yfmcp.clients import yahoo_transcripts as yahoo_client
from yfmcp.clients.yahoo_transcripts import (
    YahooTranscriptResult,
    assess_yahoo_transcript_payload_completeness,
    discover_yahoo_transcript_event,
    parse_yahoo_transcript_source_url,
    validate_yahoo_transcript_payload_identity,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads((ROOT / "fixtures" / "yahoo_quartr_transcript.json").read_text(encoding="utf-8"))
PREVIEW_FIXTURE = json.loads(
    (ROOT / "fixtures" / "yahoo_quartr_transcript_preview.json").read_text(encoding="utf-8")
)


def _run(coro):
    return asyncio.run(coro)


def _data(raw: str) -> dict:
    payload = json.loads(raw)
    return payload.get("data", payload)


class YahooTranscriptIdentityTests(unittest.TestCase):
    def test_valid_source_url_extracts_event_and_fiscal_quarter(self):
        info, error = parse_yahoo_transcript_source_url(
            "LITE",
            "https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html?guccounter=1",
        )
        self.assertIsNone(error)
        self.assertEqual(info["eventId"], "660925")
        self.assertEqual(info["fiscalQuarter"], "2026Q4")
        self.assertNotIn("?", info["sourceUrl"])

    def test_source_url_rejects_host_and_ticker_mismatch(self):
        _, host_error = parse_yahoo_transcript_source_url(
            "LITE", "https://example.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html"
        )
        _, ticker_error = parse_yahoo_transcript_source_url(
            "LITE", "https://finance.yahoo.com/quote/COHR/earnings/COHR-Q4-2026-earnings_call-661756.html"
        )
        self.assertIn("finance.yahoo.com", host_error)
        self.assertIn("does not match", ticker_error)

    def test_discovery_selects_latest_or_requested_fiscal_quarter(self):
        html = """
        /quote/LITE/earnings/LITE-Q3-2026-earnings_call-600001.html
        /quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html
        /quote/COHR/earnings/COHR-Q4-2026-earnings_call-661756.html
        """
        self.assertEqual(discover_yahoo_transcript_event("LITE", html)["eventId"], "660925")
        self.assertEqual(discover_yahoo_transcript_event("LITE", html, "2026Q3")["eventId"], "600001")
        self.assertIsNone(discover_yahoo_transcript_event("LITE", html, "2025Q4"))

    def test_provider_payload_requires_matching_event_and_fiscal_quarter(self):
        self.assertIsNone(validate_yahoo_transcript_payload_identity(FIXTURE, "660925", "2026Q4"))

        wrong_event = copy.deepcopy(FIXTURE)
        wrong_event["transcriptMetadata"]["eventId"] = 600001
        self.assertIn(
            "expected 660925",
            validate_yahoo_transcript_payload_identity(wrong_event, "660925", "2026Q4"),
        )

        wrong_quarter = copy.deepcopy(FIXTURE)
        wrong_quarter["transcriptMetadata"]["fiscalPeriod"] = "Q3"
        self.assertIn(
            "expected 2026Q4",
            validate_yahoo_transcript_payload_identity(wrong_quarter, "660925", "2026Q4"),
        )

    def test_provider_payload_rejects_missing_or_conflicting_event_metadata(self):
        missing = copy.deepcopy(FIXTURE)
        missing["transcriptMetadata"].pop("eventId")
        missing["transcriptContent"].pop("event_id")
        self.assertIn("omitted", validate_yahoo_transcript_payload_identity(missing, "660925"))

        conflict = copy.deepcopy(FIXTURE)
        conflict["transcriptMetadata"]["eventId"] = 600001
        self.assertIn("600001, 660925", validate_yahoo_transcript_payload_identity(conflict, "660925"))

    def test_completeness_distinguishes_full_transcript_from_provider_preview(self):
        full = assess_yahoo_transcript_payload_completeness(FIXTURE)
        preview = assess_yahoo_transcript_payload_completeness(PREVIEW_FIXTURE)

        self.assertEqual(full["contentCompleteness"], "FULL")
        self.assertIsNone(full["reasonCode"])
        self.assertEqual(preview["contentCompleteness"], "PARTIAL")
        self.assertEqual(preview["reasonCode"], "YAHOO_PREVIEW_ONLY")
        self.assertEqual(preview["usableParagraphCount"], 1)
        self.assertEqual(preview["advertisedSpeakerCount"], 16)


class YahooTranscriptClientValidationTests(unittest.TestCase):
    def setUp(self):
        yahoo_client._discard_cached_payload("LITE", "660925")
        yahoo_client._discard_cached_payload("NVDA", "409967")

    def tearDown(self):
        yahoo_client._discard_cached_payload("LITE", "660925")
        yahoo_client._discard_cached_payload("NVDA", "409967")

    @staticmethod
    def _transport_responses(payload: dict) -> list[bytes]:
        return [
            b"",
            b"crumb",
            json.dumps({"quoteType": {"result": [{"quartrId": 8801}]}}).encode("utf-8"),
            json.dumps(payload).encode("utf-8"),
        ]

    def test_mismatching_live_payload_is_not_cached(self):
        wrong_quarter = copy.deepcopy(FIXTURE)
        wrong_quarter["transcriptMetadata"]["fiscalPeriod"] = "Q3"
        with patch.object(
            yahoo_client,
            "_response_bytes",
            side_effect=self._transport_responses(wrong_quarter),
        ):
            result = yahoo_client._blocking_fetch("LITE", "660925", None, "2026Q4")
        self.assertEqual(result.status, "YAHOO_METADATA_MISMATCH")
        self.assertIn("expected 2026Q4", result.message)
        self.assertIsNone(yahoo_client._cached_payload("LITE", "660925"))

    def test_invalid_cache_is_discarded_and_refetched_once(self):
        wrong_event = copy.deepcopy(FIXTURE)
        wrong_event["transcriptMetadata"]["eventId"] = 600001
        yahoo_client._store_payload("LITE", "660925", wrong_event)
        with patch.object(
            yahoo_client,
            "_response_bytes",
            side_effect=self._transport_responses(FIXTURE),
        ) as response_bytes:
            result = yahoo_client._blocking_fetch("LITE", "660925", None, "2026Q4")
        self.assertEqual(result.status, "OK")
        self.assertEqual(result.cache_status, "MISS")
        self.assertTrue(result.provider_attempted)
        self.assertEqual(response_bytes.call_count, 4)
        cached = yahoo_client._cached_payload("LITE", "660925")
        self.assertIsNotNone(cached)
        self.assertEqual(cached[0]["transcriptMetadata"]["eventId"], 660925)

    def test_preview_payload_is_recoverable_and_not_cached(self):
        with patch.object(
            yahoo_client,
            "_response_bytes",
            side_effect=self._transport_responses(PREVIEW_FIXTURE),
        ):
            result = yahoo_client._blocking_fetch("NVDA", "409967", None, "2026Q4")
        self.assertEqual(result.status, "INCOMPLETE_TRANSCRIPT")
        self.assertEqual(result.content_diagnostics["contentCompleteness"], "PARTIAL")
        self.assertEqual(result.content_diagnostics["reasonCode"], "YAHOO_PREVIEW_ONLY")
        self.assertIn("16 advertised speakers", result.message)
        self.assertIsNone(yahoo_client._cached_payload("NVDA", "409967"))

    def test_cached_preview_is_discarded_before_successful_refetch(self):
        full = copy.deepcopy(FIXTURE)
        full["transcriptContent"]["event_id"] = 409967
        full["transcriptMetadata"]["eventId"] = 409967
        yahoo_client._store_payload("NVDA", "409967", PREVIEW_FIXTURE)
        with patch.object(
            yahoo_client,
            "_response_bytes",
            side_effect=self._transport_responses(full),
        ) as response_bytes:
            result = yahoo_client._blocking_fetch("NVDA", "409967", None, "2026Q4")
        self.assertEqual(result.status, "OK")
        self.assertEqual(result.cache_status, "MISS")
        self.assertEqual(result.content_diagnostics["contentCompleteness"], "FULL")
        self.assertEqual(response_bytes.call_count, 4)


class YahooTranscriptNormalizationTests(unittest.TestCase):
    def _result(self) -> YahooTranscriptResult:
        return YahooTranscriptResult(
            payload=FIXTURE,
            status="OK",
            source_url="https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
            event_id="660925",
            provider_attempted=True,
            cache_status="MISS",
            fetched_at="2026-08-14T00:00:00+00:00",
        )

    def test_explicit_source_returns_paginated_structured_transcript(self):
        with patch("yfmcp.tools.earnings.fetch_yahoo_quartr_transcript", new_callable=AsyncMock, return_value=self._result()) as fetch:
            raw = _run(srv.get_earnings_call_transcript(
                ticker="LITE",
                source_url="https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
                paragraph_limit=2,
            ))
        data = _data(raw)
        fetch.assert_awaited_once()
        self.assertEqual(data["sourceType"], "yahoo_quartr")
        self.assertEqual(data["evidenceClass"], "CONTEXTUAL_TRANSCRIPT")
        self.assertFalse(data["decisionGrade"])
        self.assertEqual(data["sourceUrl"], "https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html")
        self.assertEqual(len(data["paragraphs"]), 2)
        self.assertEqual(data["paragraphs"][1]["speaker"], "Alex Morgan")
        self.assertEqual(data["pagination"]["nextCursor"], "2")
        self.assertFalse(data["pagination"]["pageExhausted"])
        self.assertEqual(data["contentCompleteness"], "FULL")
        self.assertEqual(len(data["contentSha256"]), 64)
        self.assertEqual(data["attemptedSources"], [{
            "sourceType": "yahoo_quartr",
            "status": "SUCCESS",
            "url": "https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
            "eventId": "660925",
            "fiscalQuarter": "2026Q4",
            "cacheStatus": "MISS",
            "attempted": True,
        }])

    def test_topic_filter_and_cursor_apply_to_matching_paragraphs(self):
        with patch("yfmcp.tools.earnings.fetch_yahoo_quartr_transcript", new_callable=AsyncMock, return_value=self._result()):
            raw = _run(srv.get_earnings_call_transcript(
                ticker="LITE",
                event_id="660925",
                fiscal_quarter="2026Q4",
                topics=["revenue"],
                paragraph_limit=1,
                paragraph_cursor="1",
            ))
        data = _data(raw)
        self.assertIsNone(data["paragraphs"])
        self.assertEqual(len(data["matchedParagraphs"]), 1)
        self.assertIn("additional revenue", data["matchedParagraphs"][0]["text"])
        self.assertEqual(data["pagination"]["matchingParagraphs"], 2)
        self.assertIsNone(data["pagination"]["nextCursor"])
        self.assertTrue(data["pagination"]["pageExhausted"])

    def test_default_path_uses_yahoo_before_alpha_when_sec_is_missing(self):
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=None), \
             patch("yfmcp.tools.earnings.fetch_yahoo_quartr_transcript", new_callable=AsyncMock, return_value=self._result()), \
             patch("yfmcp.tools.earnings._attempt_alpha_vantage_transcript", new_callable=AsyncMock) as alpha:
            raw = _run(srv.get_earnings_call_transcript(ticker="LITE", fiscal_quarter="2026Q4"))
        data = _data(raw)
        alpha.assert_not_awaited()
        self.assertEqual(data["sourceType"], "yahoo_quartr")
        self.assertEqual([row["sourceType"] for row in data["attemptedSources"]], [
            "sec_8k_exhibit", "company_ir", "yahoo_quartr",
        ])

    def test_preview_only_yahoo_continues_to_alpha_fallback(self):
        diagnostics = assess_yahoo_transcript_payload_completeness(PREVIEW_FIXTURE)
        preview_result = YahooTranscriptResult(
            payload=PREVIEW_FIXTURE,
            status="INCOMPLETE_TRANSCRIPT",
            source_url="https://finance.yahoo.com/quote/NVDA/earnings/NVDA-Q4-2026-earnings_call-409967.html",
            event_id="409967",
            provider_attempted=True,
            cache_status="MISS",
            message="Yahoo returned only a preview paragraph.",
            content_diagnostics=diagnostics,
        )
        alpha_payload = {
            "sourceType": "alpha_vantage",
            "status": "OK",
            "evidenceClass": "CONTEXTUAL_TRANSCRIPT",
            "decisionGrade": False,
            "content": "Operator: Full alternate-source transcript.",
            "warnings": [],
        }
        alpha_attempt = {"sourceType": "alpha_vantage", "status": "SUCCESS"}
        resolution = {"fiscalQuarter": "2026Q4", "fiscalQuarterStatus": "EXPLICIT", "periodEvidence": None}
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=None), \
             patch("yfmcp.tools.earnings.fetch_yahoo_quartr_transcript", new_callable=AsyncMock, return_value=preview_result), \
             patch(
                 "yfmcp.tools.earnings._attempt_alpha_vantage_transcript",
                 new_callable=AsyncMock,
                 return_value=(alpha_payload, alpha_attempt, resolution),
             ):
            raw = _run(srv.get_earnings_call_transcript(ticker="NVDA", fiscal_quarter="2026Q4"))
        data = _data(raw)
        self.assertEqual(data["sourceType"], "alpha_vantage")
        yahoo_attempt = next(row for row in data["attemptedSources"] if row["sourceType"] == "yahoo_quartr")
        self.assertEqual(yahoo_attempt["status"], "INCOMPLETE_TRANSCRIPT")
        self.assertEqual(yahoo_attempt["contentCompleteness"], "PARTIAL")
        self.assertEqual(yahoo_attempt["reasonCode"], "YAHOO_PREVIEW_ONLY")
        self.assertEqual(yahoo_attempt["recommendedNextAction"], "TRY_ALTERNATE_SOURCE")

    def test_attempt_defensively_rejects_preview_marked_ok(self):
        preview_result = YahooTranscriptResult(
            payload=PREVIEW_FIXTURE,
            status="OK",
            source_url="https://finance.yahoo.com/quote/NVDA/earnings/NVDA-Q4-2026-earnings_call-409967.html",
            event_id="409967",
            provider_attempted=True,
            cache_status="MISS",
        )
        with patch(
            "yfmcp.tools.earnings.fetch_yahoo_quartr_transcript",
            new_callable=AsyncMock,
            return_value=preview_result,
        ):
            payload, attempt = _run(earnings_tools._attempt_yahoo_quartr_transcript(
                "NVDA",
                fiscal_quarter="2026Q4",
                topics=None,
                event_id="409967",
                source_url=None,
                paragraph_limit=20,
                paragraph_cursor=0,
            ))
        self.assertIsNone(payload)
        self.assertEqual(attempt["status"], "INCOMPLETE_TRANSCRIPT")
        self.assertEqual(attempt["reasonCode"], "YAHOO_PREVIEW_ONLY")

    def test_preview_is_top_level_incomplete_when_alpha_is_unavailable(self):
        diagnostics = assess_yahoo_transcript_payload_completeness(PREVIEW_FIXTURE)
        preview_result = YahooTranscriptResult(
            payload=PREVIEW_FIXTURE,
            status="INCOMPLETE_TRANSCRIPT",
            source_url="https://finance.yahoo.com/quote/NVDA/earnings/NVDA-Q4-2026-earnings_call-409967.html",
            event_id="409967",
            provider_attempted=True,
            cache_status="MISS",
            message="Yahoo returned only a preview paragraph.",
            content_diagnostics=diagnostics,
        )
        alpha_attempt = {
            "sourceType": "alpha_vantage",
            "status": "SOURCE_UNCONFIGURED",
            "reasonCode": "ALPHA_VANTAGE_API_KEY_MISSING",
        }
        resolution = {"fiscalQuarter": "2026Q4", "fiscalQuarterStatus": "EXPLICIT", "periodEvidence": None}
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=None), \
             patch("yfmcp.tools.earnings.fetch_yahoo_quartr_transcript", new_callable=AsyncMock, return_value=preview_result), \
             patch(
                 "yfmcp.tools.earnings._attempt_alpha_vantage_transcript",
                 new_callable=AsyncMock,
                 return_value=(None, alpha_attempt, resolution),
             ):
            raw = _run(srv.get_earnings_call_transcript(ticker="NVDA", fiscal_quarter="2026Q4"))
        data = _data(raw)
        self.assertEqual(data["status"], "INCOMPLETE_TRANSCRIPT")
        self.assertEqual(data["sourceType"], "yahoo_quartr")
        self.assertEqual(data["contentCompleteness"], "PARTIAL")
        self.assertEqual(data["recommendedNextAction"], "TRY_ALTERNATE_SOURCE")
        self.assertEqual(data["eventId"], "409967")
        self.assertEqual(data["nextRecommendedFallback"]["sourceType"], "alternate_transcript_source")

    def test_attempt_rejects_event_id_mismatch_from_provider_result(self):
        wrong_event = copy.deepcopy(FIXTURE)
        wrong_event["transcriptMetadata"]["eventId"] = 600001
        result = YahooTranscriptResult(
            payload=wrong_event,
            status="OK",
            source_url="https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
            event_id="660925",
            provider_attempted=True,
            cache_status="MISS",
        )
        with patch(
            "yfmcp.tools.earnings.fetch_yahoo_quartr_transcript",
            new_callable=AsyncMock,
            return_value=result,
        ):
            payload, attempt = _run(earnings_tools._attempt_yahoo_quartr_transcript(
                "LITE",
                fiscal_quarter="2026Q4",
                topics=None,
                event_id="660925",
                source_url=None,
                paragraph_limit=20,
                paragraph_cursor=0,
            ))
        self.assertIsNone(payload)
        self.assertEqual(attempt["status"], "YAHOO_METADATA_MISMATCH")
        self.assertEqual(attempt["reasonCode"], "YAHOO_METADATA_MISMATCH")

    def test_attempt_rejects_quarter_mismatch_from_provider_result(self):
        wrong_quarter = copy.deepcopy(FIXTURE)
        wrong_quarter["transcriptMetadata"]["fiscalPeriod"] = "Q3"
        result = YahooTranscriptResult(
            payload=wrong_quarter,
            status="OK",
            source_url="https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
            event_id="660925",
            provider_attempted=True,
            cache_status="MISS",
        )
        with patch(
            "yfmcp.tools.earnings.fetch_yahoo_quartr_transcript",
            new_callable=AsyncMock,
            return_value=result,
        ):
            payload, attempt = _run(earnings_tools._attempt_yahoo_quartr_transcript(
                "LITE",
                fiscal_quarter="2026Q4",
                topics=None,
                event_id="660925",
                source_url=None,
                paragraph_limit=20,
                paragraph_cursor=0,
            ))
        self.assertIsNone(payload)
        self.assertEqual(attempt["status"], "YAHOO_METADATA_MISMATCH")
        self.assertIn("expected 2026Q4", attempt["reason"])

    def test_conflicting_source_identity_is_input_error(self):
        raw = _run(srv.get_earnings_call_transcript(
            ticker="LITE",
            event_id="1",
            source_url="https://finance.yahoo.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
        ))
        payload = json.loads(raw)
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "INPUT_VALIDATION_ERROR")

    def test_non_yahoo_source_url_is_rejected_before_provider_access(self):
        with patch("yfmcp.tools.earnings.fetch_yahoo_quartr_transcript", new_callable=AsyncMock) as fetch:
            raw = _run(srv.get_earnings_call_transcript(
                ticker="LITE",
                source_url="https://example.com/quote/LITE/earnings/LITE-Q4-2026-earnings_call-660925.html",
            ))
        payload = json.loads(raw)
        fetch.assert_not_awaited()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"]["code"], "INPUT_VALIDATION_ERROR")
        self.assertIn("finance.yahoo.com", payload["error"]["message"])


class SecTranscriptValidationTests(unittest.TestCase):
    def test_slide_deck_is_not_validated_as_transcript(self):
        text = ("Investor presentation Slide 1 Revenue growth and product roadmap. " * 20)
        valid, diagnostics = earnings_tools._validate_sec_transcript_text(text)
        self.assertFalse(valid)
        self.assertTrue(diagnostics["presentationMarkersFound"])

    def test_multiple_speaker_call_is_validated(self):
        text = (
            "Operator: Welcome to the quarterly earnings call. "
            "Alex Morgan: Thank you. We delivered strong revenue growth and expanded capacity across our network. "
            "Taylor Chen: Gross margin improved and operating expenses remained controlled throughout the quarter. "
            "Alex Morgan: We will now open the call for questions and answers from analysts. "
            "Operator: Our first question comes from the research team. "
        ) * 3
        valid, diagnostics = earnings_tools._validate_sec_transcript_text(text)
        self.assertTrue(valid)
        self.assertGreaterEqual(diagnostics["distinctSpeakerCount"], 2)

    def test_ex992_presentation_falls_through_to_valid_transcript_candidate(self):
        mock_sec = {"accessionNumber": "0000320193-26-000081", "filingDate": "2026-08-12"}
        presentation = "Investor presentation Slide 1 quarterly revenue chart. " * 20
        transcript = (
            "Operator: Welcome to the quarterly earnings call. "
            "Alex Morgan: We delivered strong revenue growth and improved customer demand during the quarter. "
            "Taylor Chen: Gross margin expanded while we maintained disciplined operating expense control. "
            "Alex Morgan: We will now open the call for questions and answers from analysts. "
        ) * 4
        with patch("yfmcp.tools.earnings._resolve_latest_earnings_sec_source", new_callable=AsyncMock, return_value=mock_sec), \
             patch("yfmcp.tools.earnings._edgar_cik_from_accession", return_value=320193), \
             patch("yfmcp.tools.earnings._edgar_list_exhibits_from_index", new_callable=AsyncMock, return_value=[
                 {"type": "EX-99.2", "description": "Investor presentation", "document": "slides.htm"},
                 {"type": "EX-99.3", "description": "Earnings call transcript", "document": "call.htm"},
             ]), \
             patch("yfmcp.tools.earnings._edgar_get_html", new_callable=AsyncMock, side_effect=[presentation, transcript]):
            raw = _run(srv.get_earnings_call_transcript(ticker="AAPL"))
        data = _data(raw)
        self.assertEqual(data["status"], "OK")
        self.assertEqual(data["sourceType"], "sec_8k_exhibit")
        self.assertEqual(data["exhibitType"], "EX-99.3")
        self.assertTrue(data["decisionGrade"])


class WorkerParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker_source = (ROOT / "worker" / "src" / "yahoo-finance.ts").read_text(encoding="utf-8")
        cls.worker_tools = (ROOT / "worker" / "src" / "tools.ts").read_text(encoding="utf-8")

    def test_worker_uses_structured_yahoo_endpoint_and_validates_sec_content(self):
        self.assertIn("https://finance.yahoo.com/xhr/transcript", self.worker_source)
        self.assertIn("parseYahooTranscriptSourceUrl", self.worker_source)
        self.assertIn("validateSecTranscriptText", self.worker_source)
        self.assertIn("candidateDiagnostics", self.worker_source)

    def test_worker_validates_yahoo_identity_before_cache_and_success(self):
        fetch_start = self.worker_source.index("async function fetchYahooQuartrTranscript(")
        normalize_start = self.worker_source.index("async function normalizeYahooQuartrPayload(", fetch_start)
        fetch_source = self.worker_source[fetch_start:normalize_start]
        self.assertIn("validateYahooTranscriptPayloadIdentity", fetch_source)
        self.assertIn("assessYahooTranscriptPayloadCompleteness", fetch_source)
        self.assertIn('status: completeness.contentCompleteness === "PARTIAL" ? "INCOMPLETE_TRANSCRIPT"', fetch_source)
        self.assertIn("YAHOO_TRANSCRIPT_CACHE_VERSION", fetch_source)
        self.assertIn('status: "YAHOO_METADATA_MISMATCH"', fetch_source)
        self.assertIn("await deleteProviderCache(cacheKey)", fetch_source)
        self.assertLess(
            fetch_source.rindex("validateYahooTranscriptPayloadIdentity"),
            fetch_source.index("await setProviderCache(cacheKey, payload"),
        )

        attempt_start = self.worker_source.index("async function attemptYahooQuartrTranscript(")
        attempt_end = self.worker_source.index("const SEC_TRANSCRIPT_MARKER_RE", attempt_start)
        attempt_source = self.worker_source[attempt_start:attempt_end]
        self.assertIn("validateYahooTranscriptPayloadIdentity", attempt_source)
        self.assertIn("assessYahooTranscriptPayloadCompleteness", attempt_source)
        self.assertIn('reasonCode: "YAHOO_METADATA_MISMATCH"', attempt_source)
        self.assertIn('recommendedNextAction: "TRY_ALTERNATE_SOURCE"', attempt_source)

    def test_worker_llm_contract_separates_content_from_page_completion(self):
        self.assertIn('contentCompleteness: "FULL"', self.worker_source)
        self.assertIn("pageExhausted: nextCursor == null", self.worker_source)
        transcript_tool = next(
            line for line in self.worker_tools.splitlines()
            if 'name: "get_earnings_call_transcript"' in line
        )
        self.assertIn("Inspect contentCompleteness", transcript_tool)

    def test_worker_schema_exposes_bounded_continuation_fields(self):
        transcript_tool = next(
            line for line in self.worker_tools.splitlines()
            if 'name: "get_earnings_call_transcript"' in line
        )
        for field in ("event_id", "source_url", "paragraph_limit", "paragraph_cursor"):
            self.assertIn(field, transcript_tool)
        self.assertIn("maximum: 50", transcript_tool)

    def test_worker_dispatch_passes_transcript_identity_and_pagination(self):
        dispatch_start = self.worker_tools.index('case "get_earnings_call_transcript":')
        dispatch_end = self.worker_tools.index('case "extract_geographic_revenue":', dispatch_start)
        dispatch = self.worker_tools[dispatch_start:dispatch_end]
        for field in ("args.event_id", "args.source_url", "args.paragraph_limit", "args.paragraph_cursor"):
            self.assertIn(field, dispatch)
        self.assertIn(
            "args.paragraph_cursor != null ? Number(str(args.paragraph_cursor).trim()) : 0",
            dispatch,
        )
        self.assertNotIn("num(args.paragraph_cursor, 0)", dispatch)


if __name__ == "__main__":
    unittest.main()
