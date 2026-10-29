#!/usr/bin/env python3
"""Exchange holiday calendar data checks (BR-PRC-002, 3.1.0).

worker/src/exchange-calendars.json names the previous session for the open-session freshness fallback. These checks
keep the file well formed and fail 90 days before the last covered year ends, so the next year's closures are added
while there is time. Without them, a listing would read UNKNOWN freshness from 1 January of the first uncovered year.
The rule-by-rule match of the closures against the NYSE holiday rules is the parity case module
scripts/parity/cases_r5price2.py.
"""

from __future__ import annotations

import datetime
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CALENDAR_PATH = ROOT / "worker" / "src" / "exchange-calendars.json"
RENEWAL_DAYS = 90


class ExchangeCalendarDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.calendars = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))["calendars"]

    def test_closures_are_sorted_unique_weekdays_inside_the_covered_years(self) -> None:
        for name, calendar in self.calendars.items():
            closures = calendar["closures"]
            self.assertEqual(closures, sorted(set(closures)), f"{name}: closures must be sorted and unique")
            for iso in closures:
                day = datetime.date.fromisoformat(iso)
                self.assertLess(day.weekday(), 5, f"{name}: {iso} is a weekend day")
                self.assertTrue(calendar["firstYear"] <= day.year <= calendar["lastYear"], f"{name}: {iso} outside years")
            for year in range(calendar["firstYear"], calendar["lastYear"] + 1):
                self.assertTrue(any(c.startswith(f"{year}-") for c in closures), f"{name}: no closures for {year}")

    def test_each_exchange_code_has_one_calendar(self) -> None:
        codes = [code for calendar in self.calendars.values() for code in calendar["exchanges"]]
        self.assertEqual(len(codes), len(set(codes)))

    def test_calendar_is_renewed_before_it_runs_out(self) -> None:
        today = datetime.date.today()
        for name, calendar in self.calendars.items():
            end = datetime.date(calendar["lastYear"], 12, 31)
            self.assertGreater(
                (end - today).days, RENEWAL_DAYS,
                f"{name}: closures end {end}; add the next year's closures to {CALENDAR_PATH.name} and raise lastYear")

    def test_python_helper_reads_the_same_file(self) -> None:
        from yfmcp.exchange_calendars import CALENDARS, previous_exchange_session

        self.assertEqual(CALENDARS, self.calendars)
        self.assertEqual(previous_exchange_session("NMS", "2026-11-27"), "2026-11-25")  # Thanksgiving
        self.assertIsNone(previous_exchange_session("LSE", "2026-11-27"))


if __name__ == "__main__":
    unittest.main()
