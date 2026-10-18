#!/usr/bin/env python3
"""scripts/manage_access_keys.py over a fake wrangler runner (2.18.0, Phase 1b).

No network and no wrangler: a fake runner records each argv and answers from an
in-memory KV.

- Argument building: `kv key put|get|list` with `--binding ACCESS_KEYS --remote`,
  argument lists only, the record under `key:<sha256>`, `--text` on get,
  `--prefix key:` on list; the default runner uses no shell and runs in worker/.
- Key format: `yfk_` + 43 base64url characters; the id is the hash's first 8 hex.
- Issue prints the key exactly once, after the record is written; a failed write
  prints no key.
- list never shows a key or a hash; revoke and set-limits find a record by id.
- Error paths (wrangler failure, bad JSON, unknown or ambiguous id, a key passed
  as a label) never carry a key or a hash.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import manage_access_keys as mak  # noqa: E402

FAKE_KEY = "yfk_" + "A" * 43            # obviously fake
FAKE_HASH = hashlib.sha256(FAKE_KEY.encode()).hexdigest()
NOW = datetime(2026, 10, 18, 9, 30, tzinfo=timezone.utc)


class FakeWrangler:
    def __init__(self, store: dict[str, str] | None = None, fail: str | None = None, stderr: str = ""):
        self.store = dict(store or {})
        self.calls: list[list[str]] = []
        self.fail = fail
        self.stderr = stderr

    def __call__(self, argv: list[str]) -> mak.RunResult:
        self.calls.append(list(argv))
        assert all(isinstance(a, str) for a in argv)
        op = argv[2]
        if self.fail == op:
            return mak.RunResult(1, "", self.stderr)
        if op == "put":
            self.store[argv[3]] = argv[4]
            return mak.RunResult(0, "", "")
        if op == "get":
            return mak.RunResult(0, self.store[argv[3]] + "\n", "")
        if op == "list":
            prefix = argv[argv.index("--prefix") + 1]
            return mak.RunResult(0, json.dumps([{"name": n} for n in sorted(self.store) if n.startswith(prefix)]), "")
        raise AssertionError(argv)


def record(key: str, **overrides) -> dict:
    digest = hashlib.sha256(key.encode()).hexdigest()
    base = {"id": digest[:8], "label": "Alice", "createdAt": "2026-10-01T00:00:00Z", "status": "active",
            "dailyLimit": 2000, "perMinuteLimit": 30, "paidProviders": False}
    base.update(overrides)
    return base


def seeded(*keys: str) -> FakeWrangler:
    return FakeWrangler({f"key:{hashlib.sha256(k.encode()).hexdigest()}": json.dumps(record(k, label=f"L{i}"))
                         for i, k in enumerate(keys)})


def run(argv: list[str], runner: FakeWrangler, keygen=None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        if argv and argv[0] == "issue":
            try:
                args = mak.build_parser().parse_args(argv)
            except SystemExit as exit_:
                return int(exit_.code or 0), out.getvalue(), err.getvalue()
            try:
                code = mak.cmd_issue(runner, args, out=out, now=NOW, keygen=keygen or (lambda: FAKE_KEY))
            except mak.AdminError as error:
                print(f"manage_access_keys: {error}", file=err)
                code = 1
        else:
            try:
                code = mak.main(argv, runner=runner)
            except SystemExit as exit_:
                code = int(exit_.code or 0)
    return code, out.getvalue(), err.getvalue()


class TestKeyFormat(unittest.TestCase):
    def test_generated_keys(self):
        keys = {mak.generate_key() for _ in range(50)}
        self.assertEqual(len(keys), 50)
        for key in keys:
            self.assertRegex(key, r"^yfk_[A-Za-z0-9_-]{43}$")
            self.assertTrue(mak.KEY_SHAPE.match(key))

    def test_hash_and_id(self):
        self.assertEqual(mak.key_hash(FAKE_KEY), FAKE_HASH)


class TestIssue(unittest.TestCase):
    def test_arguments_and_record(self):
        w = FakeWrangler()
        code, out, err = run(["issue", "--label", "Alice", "--daily", "500", "--per-minute", "10", "--paid-providers"], w)
        self.assertEqual(code, 0, err)
        self.assertEqual(len(w.calls), 1)
        argv = w.calls[0]
        self.assertEqual(argv[:4], ["kv", "key", "put", f"key:{FAKE_HASH}"])
        self.assertEqual(argv[5:], ["--binding", "ACCESS_KEYS", "--remote"])
        self.assertEqual(json.loads(argv[4]), {
            "id": FAKE_HASH[:8], "label": "Alice", "createdAt": "2026-10-18T09:30:00Z", "status": "active",
            "dailyLimit": 500, "perMinuteLimit": 10, "paidProviders": True})
        self.assertNotIn(FAKE_KEY, json.dumps(w.calls))  # only the hash is stored

    def test_defaults(self):
        w = FakeWrangler()
        run(["issue", "--label", "Bob"], w)
        rec = json.loads(w.calls[0][4])
        self.assertEqual((rec["dailyLimit"], rec["perMinuteLimit"], rec["paidProviders"]), (2000, 30, False))

    def test_key_printed_exactly_once_with_reminder(self):
        code, out, _ = run(["issue", "--label", "Alice"], FakeWrangler())
        self.assertEqual(code, 0)
        self.assertEqual(out.count(FAKE_KEY), 1)
        self.assertIn("privately", out)
        self.assertNotIn(FAKE_HASH, out)

    def test_failed_write_prints_no_key_or_hash(self):
        w = FakeWrangler(fail="put", stderr=f"error writing key:{FAKE_HASH} value {FAKE_KEY}")
        code, out, err = run(["issue", "--label", "Alice"], w)
        self.assertEqual(code, 1)
        self.assertNotIn(FAKE_KEY, out + err)
        self.assertNotIn(FAKE_HASH, out + err)
        self.assertIn("writing the record failed", err)

    def test_label_that_looks_like_a_key_is_refused_without_echo(self):
        code, out, err = run(["issue", "--label", FAKE_KEY], FakeWrangler())
        self.assertEqual(code, 2)
        self.assertNotIn(FAKE_KEY, out + err)

    def test_bad_limits_refused(self):
        for bad in ("0", "-1", "abc"):
            code, _, _ = run(["issue", "--label", "Alice", "--daily", bad], FakeWrangler())
            self.assertEqual(code, 2, bad)


class TestListRevokeSetLimits(unittest.TestCase):
    OTHER = "yfk_" + "B" * 43

    def test_list_arguments_and_output(self):
        w = seeded(FAKE_KEY, self.OTHER)
        code, out, err = run(["list"], w)
        self.assertEqual(code, 0, err)
        self.assertEqual(w.calls[0], ["kv", "key", "list", "--prefix", "key:", "--binding", "ACCESS_KEYS", "--remote"])
        for argv in w.calls[1:]:
            self.assertEqual(argv[:3], ["kv", "key", "get"])
            self.assertEqual(argv[4:], ["--text", "--binding", "ACCESS_KEYS", "--remote"])
        self.assertIn(FAKE_HASH[:8], out)
        self.assertIn("L0", out)
        self.assertIn("active", out)
        for secret in (FAKE_KEY, self.OTHER, FAKE_HASH, hashlib.sha256(self.OTHER.encode()).hexdigest()):
            self.assertNotIn(secret, out + err)

    def test_revoke_by_id(self):
        w = seeded(FAKE_KEY, self.OTHER)
        code, out, err = run(["revoke", "--id", FAKE_HASH[:8]], w)
        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(w.store[f"key:{FAKE_HASH}"])["status"], "revoked")
        other = json.loads(w.store[f"key:{hashlib.sha256(self.OTHER.encode()).hexdigest()}"])
        self.assertEqual(other["status"], "active")
        put = [c for c in w.calls if c[2] == "put"]
        self.assertEqual(len(put), 1)
        self.assertEqual(put[0][5:], ["--binding", "ACCESS_KEYS", "--remote"])
        self.assertNotIn(FAKE_HASH, out + err)

    def test_set_limits(self):
        w = seeded(FAKE_KEY)
        code, _, err = run(["set-limits", "--id", FAKE_HASH[:8], "--daily", "100", "--paid-providers"], w)
        self.assertEqual(code, 0, err)
        rec = json.loads(w.store[f"key:{FAKE_HASH}"])
        self.assertEqual((rec["dailyLimit"], rec["perMinuteLimit"], rec["paidProviders"]), (100, 30, True))
        code, _, _ = run(["set-limits", "--id", FAKE_HASH[:8], "--no-paid-providers", "--per-minute", "5"], w)
        rec = json.loads(w.store[f"key:{FAKE_HASH}"])
        self.assertEqual((rec["dailyLimit"], rec["perMinuteLimit"], rec["paidProviders"]), (100, 5, False))

    def test_set_limits_needs_a_change(self):
        code, _, err = run(["set-limits", "--id", FAKE_HASH[:8]], seeded(FAKE_KEY))
        self.assertEqual(code, 1)
        self.assertIn("nothing to change", err)

    def test_unknown_bad_and_ambiguous_ids_do_not_leak(self):
        code, out, err = run(["revoke", "--id", "deadbeef"], seeded(FAKE_KEY))
        self.assertEqual(code, 1)
        self.assertIn("no key with id deadbeef", err)
        code, out, err = run(["revoke", "--id", FAKE_KEY], seeded(FAKE_KEY))
        self.assertEqual(code, 1)
        self.assertNotIn(FAKE_KEY, out + err)
        twin = FakeWrangler({f"key:{FAKE_HASH}": json.dumps(record(FAKE_KEY)),
                             f"key:{FAKE_HASH[:8]}{'0' * 56}": json.dumps(record(FAKE_KEY))})
        code, out, err = run(["revoke", "--id", FAKE_HASH[:8]], twin)
        self.assertEqual(code, 1)
        self.assertIn("more than one key", err)
        self.assertNotIn(FAKE_HASH, out + err)

    def test_wrangler_failure_and_bad_json_do_not_leak(self):
        w = seeded(FAKE_KEY)
        w.fail = "get"
        w.stderr = f"KV get failed for key:{FAKE_HASH}"
        code, out, err = run(["list"], w)
        self.assertEqual(code, 1)
        self.assertNotIn(FAKE_HASH, out + err)
        self.assertIn("<redacted>", err)
        broken = FakeWrangler({f"key:{FAKE_HASH}": "not json " + FAKE_KEY})
        code, out, err = run(["list"], broken)
        self.assertEqual(code, 1)
        self.assertNotIn(FAKE_KEY, out + err)
        self.assertNotIn(FAKE_HASH, out + err)


class TestDefaultRunner(unittest.TestCase):
    def test_argument_list_no_shell_in_worker_dir(self):
        completed = mock.Mock(returncode=0, stdout="[]", stderr="")
        with mock.patch.object(mak.subprocess, "run", return_value=completed) as run_:
            result = mak.npx_wrangler(["kv", "key", "list", "--prefix", "key:", *mak.KV_FLAGS])
        self.assertEqual(result.stdout, "[]")
        args, kwargs = run_.call_args
        self.assertEqual(args[0][:2], ["npx", "wrangler"])
        self.assertIsInstance(args[0], list)
        self.assertNotIn("shell", kwargs)
        self.assertEqual(kwargs["cwd"], mak.WORKER)

    def test_redaction(self):
        self.assertEqual(mak.redact(f"a {FAKE_KEY} b key:{FAKE_HASH}"), "a <redacted> b key:<redacted>")
        self.assertIsNone(re.search(r"[0-9a-f]{64}", mak.redact(FAKE_HASH.upper())))


if __name__ == "__main__":
    unittest.main()
