#!/usr/bin/env python3
"""Fail-closed helpers for previewing and promoting Cloudflare Worker versions."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any


OPTIONAL_SECRETS = ("FINNHUB_API_KEY", "SEC_OPEN_DATA_API_KEY")


def build_secrets(environ: Mapping[str, str]) -> dict[str, str]:
    tool_mode = environ.get("TOOL_MODE", "expanded").strip() or "expanded"
    secrets = {"TOOL_MODE": tool_mode}
    for name in OPTIONAL_SECRETS:
        value = environ.get(name, "").strip()
        if value:
            secrets[name] = value
    return secrets


def write_secrets(path: pathlib.Path, environ: Mapping[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(build_secrets(environ)), encoding="utf-8")
    path.chmod(0o600)


def load_wrangler_events(path: pathlib.Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid Wrangler output on line {line_number}") from exc
        if not isinstance(event, dict):
            raise ValueError(f"Wrangler output line {line_number} is not an object")
        events.append(event)
    return events


def _worker_preview_urls(value: Any) -> list[str]:
    urls: list[str] = []
    if isinstance(value, dict):
        for nested in value.values():
            urls.extend(_worker_preview_urls(nested))
    elif isinstance(value, list):
        for nested in value:
            urls.extend(_worker_preview_urls(nested))
    elif isinstance(value, str):
        parsed = urllib.parse.urlsplit(value)
        if parsed.scheme == "https" and parsed.hostname and parsed.hostname.endswith(".workers.dev"):
            urls.append(f"https://{parsed.hostname}")
    return urls


def parse_version_upload(
    events: list[dict[str, Any]], expected_preview_alias: str, worker_name: str
) -> tuple[str, str]:
    if not re.fullmatch(r"[a-z][a-z0-9-]*", expected_preview_alias):
        raise ValueError("preview alias must contain only lowercase letters, numbers, and dashes")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", worker_name):
        raise ValueError("Worker name contains unsupported characters")
    if len(f"{expected_preview_alias}-{worker_name}") > 63:
        raise ValueError("preview alias and Worker name exceed the DNS label limit")

    uploads = [event for event in events if event.get("type") == "version-upload"]
    if len(uploads) != 1:
        raise ValueError(f"expected exactly one version-upload event, found {len(uploads)}")

    upload = uploads[0]
    version_id = upload.get("version_id") or upload.get("versionId")
    if not isinstance(version_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_-]{7,127}", version_id.strip()
    ):
        raise ValueError("version-upload event is missing a valid version_id")

    alias_label = f"{expected_preview_alias}-{worker_name}"
    preview_urls = sorted(set(_worker_preview_urls(upload)))
    matching_urls = [
        url
        for url in preview_urls
        if (urllib.parse.urlsplit(url).hostname or "").split(".", 1)[0] == alias_label
    ]
    if len(matching_urls) == 1:
        return version_id.strip(), matching_urls[0]

    # Wrangler's structured event may contain only the generated version URL.
    # The aliased URL is deterministic from the documented hostname format.
    suffixes = {
        hostname.split(".", 1)[1]
        for url in preview_urls
        if (hostname := urllib.parse.urlsplit(url).hostname)
        and "." in hostname
        and hostname.split(".", 1)[0].endswith(f"-{worker_name}")
    }
    if len(suffixes) != 1:
        raise ValueError(
            f"could not resolve one preview hostname for Worker {worker_name!r}; "
            f"found {len(suffixes)}"
        )
    return version_id.strip(), f"https://{alias_label}.{suffixes.pop()}"


def append_github_outputs(path: pathlib.Path, version_id: str, preview_url: str) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as output:
        output.write(f"version_id={version_id}\n")
        output.write(f"preview_url={preview_url}\n")


def fetch_health(url: str, timeout: int) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "yahoo-finance-mcp-version-verifier/1.0"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    if not isinstance(payload, dict):
        raise ValueError("health endpoint returned a non-object response")
    return payload


def wait_for_worker_version(
    health_url: str,
    expected_version_id: str,
    *,
    attempts: int,
    delay_seconds: float,
    timeout: int,
    fetch: Callable[[str, int], dict[str, Any]] = fetch_health,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    last_error = "no response"
    for attempt in range(1, attempts + 1):
        try:
            payload = fetch(health_url, timeout)
            actual_version_id = payload.get("workerVersionId")
            if payload.get("status") != "ok":
                last_error = f"health status was {payload.get('status')!r}"
            elif actual_version_id != expected_version_id:
                last_error = (
                    f"workerVersionId was {actual_version_id!r}, expected {expected_version_id!r}"
                )
            else:
                return payload
        except (OSError, ValueError, urllib.error.URLError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        if attempt < attempts:
            sleep(delay_seconds)
    raise RuntimeError(f"Worker version verification failed after {attempts} attempts: {last_error}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    write = subparsers.add_parser("write-secrets")
    write.add_argument("path", type=pathlib.Path)

    parse = subparsers.add_parser("parse-upload")
    parse.add_argument("path", type=pathlib.Path)
    parse.add_argument("--expected-preview-alias", required=True)
    parse.add_argument("--worker-name", required=True)
    parse.add_argument("--github-output", required=True, type=pathlib.Path)

    verify = subparsers.add_parser("verify-health")
    verify.add_argument("--health-url", required=True)
    verify.add_argument("--expected-version-id", required=True)
    verify.add_argument("--attempts", type=int, default=18)
    verify.add_argument("--delay-seconds", type=float, default=5)
    verify.add_argument("--timeout", type=int, default=15)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "write-secrets":
            write_secrets(args.path, os.environ)
            print(f"Wrote {len(build_secrets(os.environ))} Worker secret binding(s)")
        elif args.command == "parse-upload":
            version_id, preview_url = parse_version_upload(
                load_wrangler_events(args.path), args.expected_preview_alias, args.worker_name
            )
            append_github_outputs(args.github_output, version_id, preview_url)
            print(f"Uploaded Worker version {version_id} at {preview_url}")
        else:
            wait_for_worker_version(
                args.health_url,
                args.expected_version_id,
                attempts=args.attempts,
                delay_seconds=args.delay_seconds,
                timeout=args.timeout,
            )
            print(f"Verified Worker version {args.expected_version_id} at {args.health_url}")
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
