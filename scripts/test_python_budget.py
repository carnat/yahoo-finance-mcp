#!/usr/bin/env python3
"""Python per-invocation subrequest budget guard (2.11.0, Phase 1 release 3c; spec section 4).

Drives the contextvars counter in yfmcp/url_policy.py through a fake HTTPS handler, so nothing reaches the
network:

- 45 fetches succeed in an invocation scope; the 46th raises BudgetExhaustedError without opening anything.
- Every redirect hop counts, and a refused hop is never requested.
- Outside a scope nothing is counted and nothing is refused.
- The counter resets for each call (the tool-call wrapper opens one scope per call).
- A refusal is charged to the watch that asked, and to every watch around it.
- The tool-call wrapper turns a failed call into PROVIDER_BUDGET_EXHAUSTED and adds one meta.warnings row.
"""

from __future__ import annotations

import asyncio
import email.message
import io
import json
import sys
import unittest
import urllib.error
import urllib.request
import urllib.response
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from yfmcp import url_policy as up  # noqa: E402
from yfmcp.envelope import _budget_guarded_result  # noqa: E402

URL = "https://www.sec.gov/a"
WARNING_TEXT = ("Stopped starting new upstream requests at the per-request subrequest budget (45 of 50); "
                "some sources were not queried.")


class _FakeHttps(urllib.request.HTTPSHandler):
    def __init__(self, routes: dict | None = None) -> None:
        super().__init__()
        self.routes = routes or {}
        self.seen: list[str] = []

    def https_open(self, req):  # noqa: D102
        self.seen.append(req.full_url)
        status, location, body = self.routes.get(req.full_url, (200, None, b"ok"))
        headers = email.message.Message()
        if location:
            headers["Location"] = location
        response = urllib.response.addinfourl(io.BytesIO(body), headers, req.full_url, status)
        response.msg = "stub"
        return response


def _fetch(handler: _FakeHttps, url: str = URL) -> bytes:
    opener = up.build_opener(handler)
    with up.open_checked(opener, urllib.request.Request(url), timeout=5) as resp:
        return resp.read()


class TestBudgetCounter(unittest.TestCase):
    def test_constants(self) -> None:
        self.assertEqual((up.SUBREQUEST_LIMIT, up.SUBREQUEST_RESERVE, up.SUBREQUEST_BUDGET), (50, 5, 45))

    def test_45_succeed_and_the_46th_is_refused_without_opening(self) -> None:
        handler = _FakeHttps()
        with up.invocation_scope():
            for _ in range(45):
                self.assertEqual(_fetch(handler), b"ok")
            self.assertEqual(len(handler.seen), 45)
            self.assertFalse(up.subrequest_budget_exhausted())
            with self.assertRaises(up.BudgetExhaustedError) as ctx:
                _fetch(handler)
            self.assertEqual(len(handler.seen), 45)
            self.assertTrue(up.subrequest_budget_exhausted())
            self.assertEqual(up.budget_refusal_count(), 1)
        self.assertIsInstance(ctx.exception, up.UrlPolicyError)
        self.assertIsInstance(ctx.exception, urllib.error.URLError)
        self.assertEqual(str(ctx.exception),
                         "PROVIDER_BUDGET_EXHAUSTED: the per-request subrequest budget (45 of 50) is spent; the request was not sent.")

    def test_urlopen_counts_too(self) -> None:
        handler = _FakeHttps()
        opener = up.build_opener(handler)
        real = up._OPENER
        up._OPENER = opener
        try:
            with up.invocation_scope(spent=44):
                with up.urlopen(URL) as resp:
                    self.assertEqual(resp.read(), b"ok")
                with self.assertRaises(up.BudgetExhaustedError):
                    up.urlopen(URL)
        finally:
            up._OPENER = real
        self.assertEqual(handler.seen, [URL])

    def test_redirect_hops_count_and_a_refused_hop_is_not_requested(self) -> None:
        routes = {
            "https://www.sec.gov/a": (302, "https://www.sec.gov/b", b""),
            "https://www.sec.gov/b": (200, None, b"done"),
        }
        handler = _FakeHttps(routes)
        with up.invocation_scope(spent=43):
            self.assertEqual(_fetch(handler, "https://www.sec.gov/a"), b"done")   # request + one hop = units 44 and 45
            self.assertEqual(len(handler.seen), 2)
            with self.assertRaises(up.BudgetExhaustedError):
                _fetch(handler)
        handler = _FakeHttps(routes)
        with up.invocation_scope(spent=44):
            with self.assertRaises(up.BudgetExhaustedError):
                _fetch(handler, "https://www.sec.gov/a")   # the request takes the last unit, the hop is refused
            self.assertEqual(handler.seen, ["https://www.sec.gov/a"])
            self.assertTrue(up.subrequest_budget_exhausted())

    def test_no_op_outside_a_scope(self) -> None:
        handler = _FakeHttps()
        for _ in range(60):
            _fetch(handler)
        self.assertEqual(len(handler.seen), 60)
        self.assertTrue(up.consume_subrequest())
        self.assertFalse(up.subrequest_budget_exhausted())
        self.assertEqual(up.budget_refusal_count(), 0)

    def test_each_scope_starts_a_fresh_budget(self) -> None:
        handler = _FakeHttps()
        for _ in range(2):
            with up.invocation_scope():
                for _ in range(45):
                    _fetch(handler)
                with self.assertRaises(up.BudgetExhaustedError):
                    _fetch(handler)
        self.assertEqual(len(handler.seen), 90)
        with up.invocation_scope():
            self.assertFalse(up.subrequest_budget_exhausted())
            self.assertEqual(up.budget_refusal_count(), 0)
        self.assertFalse(up.subrequest_budget_exhausted())

    def test_a_refusal_is_charged_to_the_asking_watch_and_those_around_it(self) -> None:
        handler = _FakeHttps()

        async def source(spent_first: bool) -> bool:
            with up.budget_watch() as watch:
                if spent_first:
                    with self.assertRaises(up.BudgetExhaustedError):
                        _fetch(handler)
                return watch.starved

        async def run() -> tuple:
            with up.invocation_scope(spent=45):
                with up.budget_watch() as outer:
                    quiet = await asyncio.gather(source(False))
                    starved, quiet_again = await asyncio.gather(source(True), source(False))
                    return quiet[0], starved, quiet_again, outer.starved, up.budget_refusal_count()

        self.assertEqual(asyncio.run(run()), (False, True, False, True, 1))

    def test_counter_is_shared_by_threads_and_tasks_of_one_call(self) -> None:
        handler = _FakeHttps()

        async def run() -> int:
            with up.invocation_scope(spent=40):
                def work() -> int:
                    refused = 0
                    for _ in range(4):
                        try:
                            _fetch(handler)
                        except up.BudgetExhaustedError:
                            refused += 1
                    return refused
                refused = await asyncio.gather(*(asyncio.to_thread(work) for _ in range(3)))
                return sum(refused)

        self.assertEqual(asyncio.run(run()), 7)    # 12 attempts, 5 units left
        self.assertEqual(len(handler.seen), 5)

    def test_executor_threads_see_the_invocation(self) -> None:
        handler = _FakeHttps()

        async def run() -> tuple:
            loop = asyncio.get_running_loop()
            with up.invocation_scope(spent=45):
                with up.budget_watch() as watch:
                    def work() -> str:
                        try:
                            _fetch(handler)
                        except up.BudgetExhaustedError:
                            return "refused"
                        return "fetched"
                    bare = await loop.run_in_executor(None, work)      # run_in_executor alone drops the context
                    bound = await loop.run_in_executor(None, up.in_context(work))
                return bare, bound, watch.starved

        self.assertEqual(asyncio.run(run()), ("fetched", "refused", True))

    def test_cloudflare_text_counts_as_a_refusal(self) -> None:
        class Boom(urllib.request.HTTPSHandler):
            def https_open(self, req):  # noqa: D102
                raise urllib.error.URLError("Too many subrequests by single Worker invocation")

        opener = up.build_opener(Boom())
        with up.invocation_scope():
            with self.assertRaises(urllib.error.URLError):
                up.open_checked(opener, urllib.request.Request(URL))
            self.assertEqual(up.budget_refusal_count(), 1)
            self.assertFalse(up.subrequest_budget_exhausted())   # as the Worker: counted for attribution, not flagged

    def test_batch_ticker_keeps_message_and_gets_the_budget_code(self) -> None:
        from yfmcp.tools.pricing import _run_ticker_batch

        async def call(ticker: str) -> str:
            if ticker == "BAD":
                _fetch(_FakeHttps())
            return json.dumps({"ok": ticker})

        async def run() -> list:
            with up.invocation_scope(spent=45):
                return await _run_ticker_batch(["GOOD", "BAD"], call)

        good, bad = asyncio.run(run())
        self.assertEqual(json.loads(good), {"ok": "GOOD"})
        self.assertEqual(json.loads(bad), {
            "error": True, "code": "PROVIDER_BUDGET_EXHAUSTED", "ticker": "BAD", "retryable": True, "recoveryAction": "RETRY_LATER",
            "message": "PROVIDER_BUDGET_EXHAUSTED: the per-request subrequest budget (45 of 50) is spent; the request was not sent.",
        })


class TestToolCallWrapper(unittest.TestCase):
    def _ok(self) -> str:
        return json.dumps({"ok": True, "data": {"x": 1}, "meta": {"tool": "t", "warnings": []}, "error": None})

    def _failed(self) -> str:
        return json.dumps({"ok": False, "data": None, "meta": {"tool": "t", "warnings": []},
                           "error": {"code": "PROVIDER_ERROR", "message": "boom"}})

    def test_untouched_when_the_guard_did_not_fire(self) -> None:
        self.assertEqual(_budget_guarded_result("t", self._failed(), refusals=0, exhausted=False), self._failed())

    def test_success_gets_one_warning_row(self) -> None:
        out = json.loads(_budget_guarded_result("t", self._ok(), refusals=0, exhausted=True))
        self.assertTrue(out["ok"])
        self.assertEqual(out["meta"]["warnings"], [{"code": "PROVIDER_BUDGET_EXHAUSTED", "message": WARNING_TEXT, "severity": "warning"}])
        again = _budget_guarded_result("t", json.dumps(out), refusals=0, exhausted=True)
        self.assertEqual(json.loads(again)["meta"]["warnings"], out["meta"]["warnings"])

    def test_failed_call_with_a_refusal_becomes_budget_exhausted(self) -> None:
        out = json.loads(_budget_guarded_result("t", self._failed(), refusals=1, exhausted=True))
        self.assertFalse(out["ok"])
        self.assertEqual(out["error"], {
            "code": "PROVIDER_BUDGET_EXHAUSTED",
            "message": "The per-request subrequest budget (45 of 50) was spent before this request could complete. Retry later.",
        })
        self.assertIs(out["meta"]["retryable"], True)
        self.assertEqual(out["meta"]["recoveryAction"], "RETRY_LATER")
        self.assertEqual([w["code"] for w in out["meta"]["warnings"]], ["PROVIDER_BUDGET_EXHAUSTED"])

    def test_successful_call_with_a_refusal_stays_a_success(self) -> None:
        out = json.loads(_budget_guarded_result("t", self._ok(), refusals=2, exhausted=True))
        self.assertTrue(out["ok"])
        self.assertEqual(out["data"], {"x": 1})

    def test_wrapper_opens_a_fresh_scope_per_call(self) -> None:
        from yfmcp import app

        seen: list[tuple] = []

        async def tool() -> str:
            seen.append((up.consume_subrequest(), up.budget_refusal_count(), up.subrequest_budget_exhausted()))
            return json.dumps({"ok": True, "data": None, "meta": {"warnings": []}, "error": None})

        wrapped = app._offload_to_thread(tool, "t")
        asyncio.run(wrapped())
        asyncio.run(wrapped())
        self.assertEqual(seen, [(True, 0, False), (True, 0, False)])
        with up.invocation_scope(spent=45):
            pass  # a scope opened by a caller does not leak into the wrapper's own call
        asyncio.run(wrapped())
        self.assertEqual(seen[-1], (True, 0, False))


if __name__ == "__main__":
    unittest.main(verbosity=2)
