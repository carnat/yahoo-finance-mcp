#!/usr/bin/env python3
"""Owner-only admin for per-person access keys (2.18.0, Phase 1b).

Keys live hashed in the KV namespace `yfmcp_access_keys` (binding
`ACCESS_KEYS`): record `key:<sha256 hex of the key>` with the JSON value
{id, label, createdAt, status, dailyLimit, perMinuteLimit, paidProviders}.
The full key is printed once, by `issue`, and is never stored.

Commands (run from anywhere; wrangler runs in worker/ against --remote):
  issue --label NAME [--daily N] [--per-minute N] [--paid-providers]
  list
  revoke --id ID
  set-limits --id ID [--daily N] [--per-minute N] [--paid-providers | --no-paid-providers]

Calls `npx wrangler kv key put|get|list --binding ACCESS_KEYS --remote` through
subprocess with argument lists (no shell). No key and no hash is printed by
`list`, or appears in any error message: wrangler's own output is reported only
with key-shaped and hash-shaped strings redacted.

Requires the ACCESS_KEYS binding in worker/wrangler.toml (the activation
release adds it) and a wrangler login or CLOUDFLARE_API_TOKEN.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import secrets
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker"

KEY_PREFIX = "yfk_"
KEY_SHAPE = re.compile(r"^yfk_[A-Za-z0-9_-]{43}$")
ID_SHAPE = re.compile(r"^[0-9a-f]{8}$")
RECORD_PREFIX = "key:"
KV_FLAGS = ["--binding", "ACCESS_KEYS", "--remote"]
DEFAULT_DAILY = 2000
DEFAULT_PER_MINUTE = 30
LABEL_MAX = 64

_REDACT = (re.compile(r"yfk_[A-Za-z0-9_-]{8,}"), re.compile(r"[0-9a-fA-F]{64}"))


@dataclass
class RunResult:
    returncode: int
    stdout: str
    stderr: str


# argv (after `npx wrangler`) -> result; replaced by a fake in tests.
Runner = Callable[[list[str]], RunResult]


class AdminError(RuntimeError):
    """An error whose message never carries a key or a hash."""


def redact(text: str) -> str:
    for pattern in _REDACT:
        text = pattern.sub("<redacted>", text)
    return text


def npx_wrangler(argv: list[str]) -> RunResult:
    proc = subprocess.run(["npx", "wrangler", *argv], cwd=WORKER, capture_output=True, text=True, check=False)
    return RunResult(proc.returncode, proc.stdout, proc.stderr)


def generate_key() -> str:
    """`yfk_` + 43 base64url characters (32 random bytes, unpadded)."""
    return KEY_PREFIX + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")


def key_hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _run(runner: Runner, argv: list[str], what: str) -> str:
    try:
        result = runner(argv)
    except OSError as error:
        raise AdminError(f"{what} failed: could not run wrangler ({error.__class__.__name__})") from None
    if result.returncode != 0:
        detail = redact((result.stderr or result.stdout or "").strip())[-500:]
        raise AdminError(f"{what} failed (wrangler exit {result.returncode}){': ' + detail if detail else ''}")
    return result.stdout


def kv_put(runner: Runner, name: str, record: dict) -> None:
    _run(runner, ["kv", "key", "put", name, json.dumps(record, separators=(",", ":")), *KV_FLAGS], "writing the record")


def kv_get(runner: Runner, name: str) -> dict:
    text = _run(runner, ["kv", "key", "get", name, "--text", *KV_FLAGS], "reading a record").strip()
    try:
        value = json.loads(text)
    except ValueError:
        raise AdminError("a stored record is not valid JSON") from None
    if not isinstance(value, dict):
        raise AdminError("a stored record is not a JSON object")
    return value


def kv_list(runner: Runner) -> list[str]:
    text = _run(runner, ["kv", "key", "list", "--prefix", RECORD_PREFIX, *KV_FLAGS], "listing records").strip()
    try:
        rows = json.loads(text)
    except ValueError:
        raise AdminError("wrangler kv key list did not return JSON") from None
    if not isinstance(rows, list):
        raise AdminError("wrangler kv key list did not return a list")
    return [row["name"] for row in rows
            if isinstance(row, dict) and isinstance(row.get("name"), str) and row["name"].startswith(RECORD_PREFIX)]


def all_records(runner: Runner) -> list[tuple[str, dict]]:
    return [(name, kv_get(runner, name)) for name in kv_list(runner)]


def find_by_id(runner: Runner, key_id: str) -> tuple[str, dict]:
    if not ID_SHAPE.match(key_id):
        raise AdminError("--id must be the 8-character hex id shown by `list`")
    matches = [(name, record) for name, record in all_records(runner) if record.get("id") == key_id]
    if not matches:
        raise AdminError(f"no key with id {key_id}")
    if len(matches) > 1:
        raise AdminError(f"more than one key has id {key_id}; revoke and reissue them")
    return matches[0]


def _positive(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a whole number") from None
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _label(value: str) -> str:
    text = value.strip()
    if not text or len(text) > LABEL_MAX or any(ord(c) < 32 or ord(c) == 127 for c in text):
        raise argparse.ArgumentTypeError(f"must be 1-{LABEL_MAX} printable characters")
    if KEY_SHAPE.search(text) or text.startswith(KEY_PREFIX):
        raise argparse.ArgumentTypeError("must be a plain name, not a key")
    return text


# ── Commands ────────────────────────────────────────────────────────────────

def cmd_issue(runner: Runner, args: argparse.Namespace, out=None, now: datetime | None = None,
              keygen: Callable[[], str] = generate_key) -> int:
    out = out or sys.stdout
    key = keygen()
    if not KEY_SHAPE.match(key):
        raise AdminError("generated key has the wrong shape")
    digest = key_hash(key)
    record = {
        "id": digest[:8],
        "label": args.label,
        "createdAt": (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "active",
        "dailyLimit": args.daily,
        "perMinuteLimit": args.per_minute,
        "paidProviders": bool(args.paid_providers),
    }
    kv_put(runner, RECORD_PREFIX + digest, record)
    # Printed once, only after the record was written.
    print(f"Issued key id {record['id']} for {record['label']}:", file=out)
    print(key, file=out)
    print("Share it privately (not in a group chat or email thread). It is shown only this once and cannot be recovered;",
          file=out)
    print("if it is lost, issue a new key and revoke this id.", file=out)
    return 0


def cmd_list(runner: Runner, _args: argparse.Namespace, out=None) -> int:
    out = out or sys.stdout
    records = sorted((record for _, record in all_records(runner)), key=lambda r: str(r.get("createdAt", "")))
    print(f"{'id':<8}  {'status':<8}  {'daily':>6}  {'/min':>5}  {'paid':<4}  {'createdAt':<20}  label", file=out)
    for r in records:
        print(f"{str(r.get('id', '?')):<8}  {str(r.get('status', '?')):<8}  {str(r.get('dailyLimit', '?')):>6}  "
              f"{str(r.get('perMinuteLimit', '?')):>5}  {'yes' if r.get('paidProviders') is True else 'no':<4}  "
              f"{str(r.get('createdAt', '?')):<20}  {redact(str(r.get('label', '')))}", file=out)
    return 0


def cmd_revoke(runner: Runner, args: argparse.Namespace, out=None) -> int:
    out = out or sys.stdout
    name, record = find_by_id(runner, args.id)
    record["status"] = "revoked"
    kv_put(runner, name, record)
    print(f"Revoked key id {args.id}. It stops working within 60 seconds.", file=out)
    return 0


def cmd_set_limits(runner: Runner, args: argparse.Namespace, out=None) -> int:
    out = out or sys.stdout
    if args.daily is None and args.per_minute is None and args.paid_providers is None:
        raise AdminError("nothing to change: pass --daily, --per-minute or --paid-providers/--no-paid-providers")
    name, record = find_by_id(runner, args.id)
    if args.daily is not None:
        record["dailyLimit"] = args.daily
    if args.per_minute is not None:
        record["perMinuteLimit"] = args.per_minute
    if args.paid_providers is not None:
        record["paidProviders"] = bool(args.paid_providers)
    kv_put(runner, name, record)
    print(f"Updated key id {args.id}: daily {record.get('dailyLimit')}, per minute {record.get('perMinuteLimit')}, "
          f"paid providers {'yes' if record.get('paidProviders') is True else 'no'}. Takes effect within 60 seconds.",
          file=out)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    issue = sub.add_parser("issue", help="issue a new key (printed once)")
    issue.add_argument("--label", required=True, type=_label, help="non-secret name, e.g. a first name")
    issue.add_argument("--daily", type=_positive, default=DEFAULT_DAILY)
    issue.add_argument("--per-minute", type=_positive, default=DEFAULT_PER_MINUTE)
    issue.add_argument("--paid-providers", action="store_true", help="allow Finnhub, Marketaux and Alpha Vantage")
    sub.add_parser("list", help="show id, label, status, limits and createdAt")
    revoke = sub.add_parser("revoke", help="revoke a key by id")
    revoke.add_argument("--id", required=True)
    limits = sub.add_parser("set-limits", help="change a key's limits or provider access")
    limits.add_argument("--id", required=True)
    limits.add_argument("--daily", type=_positive)
    limits.add_argument("--per-minute", type=_positive)
    limits.add_argument("--paid-providers", action=argparse.BooleanOptionalAction, default=None)
    return parser


COMMANDS = {"issue": cmd_issue, "list": cmd_list, "revoke": cmd_revoke, "set-limits": cmd_set_limits}


def main(argv: list[str] | None = None, runner: Runner = npx_wrangler) -> int:
    args = build_parser().parse_args(argv)
    try:
        return COMMANDS[args.command](runner, args)
    except AdminError as error:
        print(f"manage_access_keys: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
