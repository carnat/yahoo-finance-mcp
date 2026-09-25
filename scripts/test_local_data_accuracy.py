#!/usr/bin/env python3
"""Local-server counterparts of the Worker data-accuracy fixes (2.2.5, 2.2.6).

Price-target direction, corporate-action limits, news ranking/search and
issuer labelling, SEC XBRL flags, meta.dataDate, Yahoo transcript URLs,
risk factor excerpts and section text entities behave as the Worker does.
"""

from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server as srv  # noqa: E402
from yfmcp import envelope  # noqa: E402
from yfmcp.tools import earnings  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _data(raw: str) -> dict:
    parsed = json.loads(raw)
    return parsed["data"] if isinstance(parsed, dict) and "ok" in parsed else parsed


class TestLocalDataAccuracy(unittest.TestCase):
    def test_price_target_direction_uses_yahoo_targets(self) -> None:
        self.assertEqual(srv._analyst_price_target_change({"currentPriceTarget": 380, "priorPriceTarget": 365}, "MAINTAIN"), (365.0, 380.0, "RAISED"))
        self.assertEqual(srv._analyst_price_target_change({"currentPriceTarget": 280, "priorPriceTarget": 0, "priceTargetAction": "Announces"}, "DOWNGRADE"), (None, 280.0, "INITIATED"))
        # A rating upgrade alone no longer implies a raised target.
        self.assertEqual(srv._analyst_price_target_change({"currentPriceTarget": 0, "priorPriceTarget": 0}, "UPGRADE"), (None, None, None))

    def test_corporate_actions_keep_every_split(self) -> None:
        rows = [
            {"Date": "1987-06-16T00:00:00.000", "Dividends": 0.0, "Stock Splits": 2.0},
            {"Date": "2024-02-09T00:00:00.000", "Dividends": 0.24, "Stock Splits": 0.0},
            {"Date": "2025-02-10T00:00:00.000", "Dividends": 0.25, "Stock Splits": 0.0},
            {"Date": "2026-02-09T00:00:00.000", "Dividends": 0.26, "Stock Splits": 0.0},
        ]
        self.assertEqual([r["Date"][:4] for r in srv._limit_corporate_actions(rows, "", 2)], ["1987", "2025", "2026"])
        self.assertEqual([r["Date"][:4] for r in srv._limit_corporate_actions(rows, "2026-01-01", 40)], ["1987", "2026"])

    def test_issuer_is_only_the_announcing_company(self) -> None:
        identity = {"aliases": ("apple", "apple inc"), "companyName": "Apple Inc."}
        release = {"source": "yahoo_finance_press_releases", "tickers": ["AAPL"]}
        self.assertIsNone(srv._yahoo_item_issuer({**release, "title": "Qualcomm Announces Renewal of Global Patent License Agreement with Apple"}, identity))
        self.assertEqual(srv._yahoo_item_issuer({**release, "title": "Apple Reports Fourth Quarter Results"}, identity), "Apple Inc.")
        self.assertIsNone(srv._yahoo_item_issuer({"source": "yahoo_finance_news", "title": "Apple Reports Results", "tickers": ["AAPL"]}, identity))

    def test_news_search_filters_the_full_pool(self) -> None:
        items = [{"title": f"Garmin story {i}", "tickerRelevance": "LOW"} for i in range(12)]
        items += [{"title": "New iPhone launch", "tickerRelevance": "LOW"}, {"title": "Apple iPhone sales", "tickerRelevance": "HIGH"}]
        collect = AsyncMock(return_value=(items, ["finnhub"], [], "2026-09-25T00:00:00Z", {}))
        with patch.object(srv, "_collect_company_events", collect):
            data = _data(_run(srv.search_company_news("AAPL", "iphone", max_results=10)))
        self.assertEqual(collect.await_args.kwargs["max_results"], 100)
        self.assertEqual(data["matchCount"], 2)
        self.assertEqual([item["title"] for item in data["items"]], ["Apple iPhone sales", "New iPhone launch"])

    def test_company_news_ranks_relevant_items_first(self) -> None:
        items = [{"title": "Garmin", "tickerRelevance": "LOW"}, {"title": "Apple", "tickerRelevance": "HIGH"}]
        collect = AsyncMock(return_value=(items, ["finnhub"], [], "2026-09-25T00:00:00Z", {}))
        with patch.object(srv, "_collect_company_events", collect):
            data = _data(_run(srv.get_company_news("AAPL", max_results=1)))
        self.assertEqual(collect.await_args.kwargs["max_results"], 3)
        self.assertEqual([item["title"] for item in data["items"]], ["Apple"])

    def test_material_filings_use_sec_xbrl_flags(self) -> None:
        submissions = {"filings": {"recent": {
            "form": ["10-Q", "8-K"], "filingDate": ["2026-07-31", "2026-07-30"],
            "accessionNumber": ["0000320193-26-000020", "0000320193-26-000018"],
            "primaryDocument": ["aapl-20260627.htm", "aapl-20260730.htm"],
            "acceptanceDateTime": ["", ""], "isXBRL": [1, 0], "isInlineXBRL": [1, 0],
        }}}
        with patch.object(srv, "_get_submissions_for_ticker", AsyncMock(return_value=("0000320193", submissions))):
            data = _data(_run(srv.list_sec_material_filings("AAPL", limit=5)))
        self.assertEqual([f["xbrl_available"] for f in data["filings"]], [True, False])

    def test_meta_data_date_comes_from_the_payload(self) -> None:
        wrapped = json.loads(envelope._envelope_tool_result("get_price_slope", json.dumps({"dataDate": "2026-09-24", "x": 1})))
        self.assertEqual(wrapped["meta"]["dataDate"], "2026-09-24")
        wrapped = json.loads(envelope._envelope_tool_result("analyze_share_count_trend", json.dumps({"dataDate": "2026-08-04T00:00:00.000Z"})))
        self.assertEqual(wrapped["meta"]["dataDate"], "2026-08-04")

    def test_yahoo_transcript_url_reads_the_structured_transcript(self) -> None:
        pages = [
            {"paragraphs": [{"speaker": "Tim Cook", "text": "Gross margin was strong this quarter."}], "pagination": {"hasMore": True, "nextCursor": "1"}},
            {"paragraphs": [{"speaker": "Kevan Parekh", "text": "Services revenue grew."}], "pagination": {"hasMore": False, "nextCursor": None}},
        ]
        fake = AsyncMock(side_effect=[json.dumps({"ok": True, "data": page}) for page in pages])
        url = "https://finance.yahoo.com/quote/AAPL/earnings/AAPL-Q3-2026-earnings_call-658553.html"
        with patch.object(earnings, "get_earnings_call_transcript", fake):
            data = _data(_run(earnings.parse_public_transcript(url, topics=["gross margin"])))
        self.assertEqual(data["source"], "public_url")
        self.assertEqual([p["paragraph"] for p in data["matchedParagraphs"]], ["Tim Cook: Gross margin was strong this quarter."])
        self.assertEqual(fake.await_count, 2)

    def test_risk_excerpt_is_centered_on_the_term(self) -> None:
        window = (
            "rk carriers and other channel partners. The Company has a large, global business with sales outside the U.S. "
            "representing a majority of the Company’s total net sales, and the Company believes that it generally benefits "
            "from growth in international trade. A significant majority of the Company’s manufacturing is performed in whole "
            "or in part by outsourcing partners located primarily in China mainland, India, Japan, South Korea, Taiwan and "
            "Vietnam. Restrictions on international trade, such as tariffs and other controls on imports or exports of goods, "
            "technology or data, can materially adversely affect the Company’s business and supply chain."
        )
        excerpt = srv._readable_exposure_excerpt(window, ["tariff"])
        self.assertTrue(excerpt.startswith("...Restrictions on international trade, such as tariffs"))
        self.assertLessEqual(len(excerpt), 246)
        self.assertEqual(srv._readable_exposure_excerpt(window, ["tari"]), "")

    def test_section_text_decodes_entities(self) -> None:
        html = (
            "<html><body><p><span style=\"font-weight:700\">Item 1A.&#160;&#160;Risk Factors</span></p>"
            "<p>The Company&#8217;s business is subject to risks.</p>"
            "<p><span style=\"font-weight:700\">Item 1B.&#160;&#160;Unresolved Staff Comments</span></p><p>None.</p></body></html>"
        )

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def read(self):
                return html.encode()

        with patch("urllib.request.urlopen", return_value=_Resp()):
            raw = _run(srv.get_filing_section("AAPL", "Item 1A", "https://www.sec.gov/Archives/edgar/data/320193/x/a.htm"))
        text = _data(raw)["text"]
        self.assertIn("The Company’s business", text)
        self.assertNotIn("&#", text)


if __name__ == "__main__":
    unittest.main()
