"""Offline regression coverage for universal-alias live-payload handling."""

from __future__ import annotations

import unittest

from scripts.test_universal_aliases import returned_provider_error


class TestReturnedProviderError(unittest.TestCase):
    def test_recognizes_enveloped_tool_error_payload(self) -> None:
        payload = {
            "ok": True,
            "data": {"ticker": "AAPL", "error": "No options data available"},
        }

        self.assertEqual(returned_provider_error(payload), "No options data available")

    def test_ignores_success_payload(self) -> None:
        payload = {"ok": True, "data": {"ticker": "AAPL", "dataQuality": {}}}

        self.assertIsNone(returned_provider_error(payload))


if __name__ == "__main__":
    unittest.main()
