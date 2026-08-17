"""Release and exact-build identity shared by the Python MCP runtime."""

from __future__ import annotations

import os
import re

from yfmcp.version import RELEASE_VERSION


def _value(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _short_git_sha(value: str | None) -> str | None:
    if value and re.fullmatch(r"[0-9a-fA-F]{7,64}", value):
        return value[:7].lower()
    return None


SERVER_VERSION = _value("SERVER_VERSION") or RELEASE_VERSION
BUILD_SHA = _value("BUILD_SHA")
DEPLOYED_AT = _value("DEPLOYED_AT")
BUILD_DATE = _value("BUILD_DATE") or (DEPLOYED_AT[:10] if DEPLOYED_AT else "unknown")


def get_build_version() -> str:
    explicit = _value("BUILD_VERSION")
    if explicit:
        return explicit
    short_sha = _short_git_sha(BUILD_SHA)
    return f"{SERVER_VERSION}+git.{short_sha}" if short_sha else SERVER_VERSION


BUILD_VERSION = get_build_version()
