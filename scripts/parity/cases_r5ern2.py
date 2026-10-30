"""Cases for R5-earnings part 2 (3.4.0): the transcript next-source suggestion (BR-ERN-009, owner 2026-10-30).

nextTranscriptFallback / _next_transcript_fallback on attempt lists: Yahoo that matched another event is suggested
first, then Alpha Vantage when it was not queried only because the fiscal quarter was unknown and a key is
configured; otherwise company IR, a public URL, Yahoo, Alpha Vantage, in that order. The end-to-end waterfall cases
are in cases_sec_earnings.py.
"""

from __future__ import annotations

import os
from unittest.mock import patch

from scripts.parity.harness import Case
from yfmcp.tools.earnings import _next_transcript_fallback

CASES: list[Case] = []

_SKIPPED = [{"sourceType": "company_ir", "status": "SKIPPED"}, {"sourceType": "public_transcript_url", "status": "SKIPPED"}]
_MISMATCH = {"sourceType": "yahoo_quartr", "status": "YAHOO_METADATA_MISMATCH", "reasonCode": "YAHOO_METADATA_MISMATCH"}
_AV_QUARTER = {"sourceType": "alpha_vantage", "status": "NOT_ELIGIBLE", "reasonCode": "FISCAL_QUARTER_UNRESOLVED"}
_AV_NO_KEY = {"sourceType": "alpha_vantage", "status": "SOURCE_UNCONFIGURED", "reasonCode": "ALPHA_VANTAGE_API_KEY_MISSING"}
_YAHOO_NONE = {"sourceType": "yahoo_quartr", "status": "NOT_FOUND"}

_YAHOO = {"sourceType": "yahoo_quartr", "action": "Retry with a validated Yahoo earnings-call source_url or event_id."}
_ALPHA = {"sourceType": "alpha_vantage", "action": "Retry with fiscal_quarter as YYYYQn so Alpha Vantage can be queried for that quarter."}
_IR = {"sourceType": "company_ir", "action": "Provide or discover a company IR earnings-call/transcript URL, then call parse_public_transcript."}
_PUBLIC = {"sourceType": "public_transcript_url", "action": "Call parse_public_transcript with a verified public transcript URL."}


def _case(name, attempts, env, expect):
    def py():
        with patch.dict(os.environ, env, clear=False):
            for key in ("ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY"):
                if key not in env:
                    os.environ.pop(key, None)
            return _next_transcript_fallback(attempts)
    CASES.append(Case(name=f"r5ern2.next_fallback.{name}", ids=["BR-ERN-009"], py=py, decision="owner 2026-10-30",
                      ts={"js": "m.response.setWorkerEnv(args[1]); return m.yahooFinance.nextTranscriptFallback(args[0]);",
                          "args": [attempts, env], "isolate": True},
                      expect=expect))


_KEY = {"ALPHA_VANTAGE_API_KEY": "parity-test-key"}
_case("yahoo_metadata_mismatch_comes_first", [_SKIPPED[0], _MISMATCH, _SKIPPED[1], _AV_QUARTER], _KEY, _YAHOO)
_case("alpha_vantage_when_only_the_quarter_blocked_it", [_SKIPPED[0], _YAHOO_NONE, _SKIPPED[1], _AV_QUARTER], _KEY, _ALPHA)
_case("alpha_vantage_without_a_key_is_not_suggested", [_SKIPPED[0], _YAHOO_NONE, _SKIPPED[1], _AV_QUARTER], {}, _IR)
_case("alpha_vantage_with_no_key_configured_falls_to_company_ir", [_SKIPPED[0], _YAHOO_NONE, _SKIPPED[1], _AV_NO_KEY], _KEY, _IR)
_case("public_url_after_company_ir_was_tried", [{"sourceType": "company_ir", "status": "NOT_FOUND"}, _YAHOO_NONE, _SKIPPED[1], _AV_NO_KEY], {}, _PUBLIC)
