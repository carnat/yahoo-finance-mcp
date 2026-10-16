"""Outbound fetch allow-list (2.10.0); mirrors worker/src/url-policy.ts.

Every urllib fetch in the server goes through ``urlopen`` / ``build_opener``
here. A URL is allowed only when it is https, carries no credentials, uses the
default port, does not name an IP literal or a local-only name, and its host is
exactly on the allow-list below. The first URL is checked before the request
and every redirect hop is re-checked by ``CheckedRedirectHandler`` (at most 5
hops). Reason strings are identical to the Worker's.

DNS is not checked; the exact-host list means a DNS-based bypass needs a host
we already trust.

Per-invocation subrequest budget (2.11.0): the Worker may make 50 subrequests
per invocation and stops starting new work at 45. This module mirrors that
guard for the tool-call boundary so both runtimes report the same typed state
(``PROVIDER_BUDGET_EXHAUSTED``). Python has no hard limit and no Cache API;
only urllib fetches through this seam are counted (yfinance is not).
"""

from __future__ import annotations

import contextlib
import contextvars
import functools
import ipaddress
import json
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

MAX_REDIRECT_HOPS = 5

SUBREQUEST_LIMIT = 50
SUBREQUEST_RESERVE = 5
SUBREQUEST_BUDGET = SUBREQUEST_LIMIT - SUBREQUEST_RESERVE


class UrlPolicyError(urllib.error.URLError):
    """A refused fetch: the first URL or a redirect hop failed the allow-list.

    A URLError, so every call site's existing fetch-failure path handles it.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason

    def __str__(self) -> str:
        return str(self.reason)


class BudgetExhaustedError(UrlPolicyError):
    """A fetch the per-invocation subrequest budget refused; raised before the request is opened."""

    def __init__(self) -> None:
        super().__init__(
            f"PROVIDER_BUDGET_EXHAUSTED: the per-request subrequest budget ({SUBREQUEST_BUDGET} of {SUBREQUEST_LIMIT}) "
            "is spent; the request was not sent."
        )


def is_budget_exhausted_error(error: object) -> bool:
    """A BudgetExhaustedError, or Cloudflare's own "Too many subrequests" error, which maps to the same state."""
    if isinstance(error, BudgetExhaustedError):
        return True
    return bool(re.search(r"too many subrequests", str(error), re.IGNORECASE))


# The meta.warnings row of a response in an invocation where the guard refused a fetch.
SUBREQUEST_BUDGET_WARNING: dict = {
    "code": "PROVIDER_BUDGET_EXHAUSTED",
    "message": (
        f"Stopped starting new upstream requests at the per-request subrequest budget ({SUBREQUEST_BUDGET} of {SUBREQUEST_LIMIT}); "
        "some sources were not queried."
    ),
    "severity": "warning",
}


# ── per-invocation budget ───────────────────────────────────────────────────────────────────────────────────────
# One mutable _Invocation is shared by every task and thread the tool call starts (contextvars copy the reference,
# not the object); _Watch objects form a chain so a refusal is charged to every enclosing watch, as the Worker's
# nested scopes do.

_budget_lock = threading.Lock()


class _Invocation:
    __slots__ = ("used", "exhausted")

    def __init__(self, used: int = 0) -> None:
        self.used = used
        self.exhausted = False


class BudgetWatch:
    """Fetches refused for budget while this watch, or a watch nested in it, was current."""

    __slots__ = ("refusals", "parent")

    def __init__(self, parent: "BudgetWatch | None") -> None:
        self.refusals = 0
        self.parent = parent

    @property
    def starved(self) -> bool:
        return self.refusals > 0


_invocation: contextvars.ContextVar[_Invocation | None] = contextvars.ContextVar("yfmcp_subrequest_invocation", default=None)
_watch: contextvars.ContextVar[BudgetWatch | None] = contextvars.ContextVar("yfmcp_subrequest_watch", default=None)


@contextlib.contextmanager
def invocation_scope(spent: int = 0):
    """One tool call: a fresh budget with ``spent`` units already used (tests and the parity harness pre-spend).

    Always starts a new budget, so each call resets it; outside any scope counting is a no-op.
    """
    invocation_token = _invocation.set(_Invocation(spent))
    watch_token = _watch.set(BudgetWatch(None))
    try:
        yield
    finally:
        _watch.reset(watch_token)
        _invocation.reset(invocation_token)


@contextlib.contextmanager
def budget_watch():
    """A nested watch: ``watch.starved`` says whether a fetch inside was refused for budget."""
    watch = BudgetWatch(_watch.get())
    token = _watch.set(watch)
    try:
        yield watch
    finally:
        _watch.reset(token)


def _count_refusal() -> None:
    with _budget_lock:
        watch = _watch.get()
        while watch is not None:
            watch.refusals += 1
            watch = watch.parent


def consume_subrequest() -> bool:
    """Count one fetch (each redirect hop included). False means the budget is spent: the caller must not fetch.

    A refusal marks the invocation as budget-exhausted and is charged to the current watch chain.
    Outside an invocation scope nothing is counted and the answer is always True.
    """
    invocation = _invocation.get()
    if invocation is None:
        return True
    with _budget_lock:
        if invocation.used >= SUBREQUEST_BUDGET:
            invocation.exhausted = True
            refused = True
        else:
            invocation.used += 1
            refused = False
    if refused:
        _count_refusal()
    return not refused


def in_context(fn, *args):
    """``fn(*args)`` bound to a copy of the caller's context, for ``loop.run_in_executor``.

    run_in_executor does not carry contextvars into the worker thread; wrapping the callable keeps the
    invocation's budget and watches visible to the fetch it makes. Call it where the executor call is made.
    """
    return functools.partial(contextvars.copy_context().run, fn, *args)


def subrequest_budget_exhausted() -> bool:
    """The guard refused at least one fetch in the current invocation (drives the meta.warnings row)."""
    invocation = _invocation.get()
    return invocation is not None and invocation.exhausted


def budget_refusal_count() -> int:
    """Fetches refused for budget while the current watch was current, nested watches included."""
    watch = _watch.get()
    return watch.refusals if watch is not None else 0


SEC_HOSTS: tuple[str, ...] = ("www.sec.gov", "data.sec.gov", "efts.sec.gov")
YAHOO_HOSTS: tuple[str, ...] = (
    "finance.yahoo.com",
    "query1.finance.yahoo.com",
    "query2.finance.yahoo.com",
    "fc.yahoo.com",
)
PROVIDER_HOSTS: tuple[str, ...] = (
    "www.globenewswire.com",
    "finnhub.io",
    "www.alphavantage.co",
    "api.marketaux.com",
    "api.sec.or.th",
    "api.company-information.service.gov.uk",
)
_BLOCKED_SUFFIXES = (".localhost", ".local", ".internal", ".invalid")

_REGISTRY_PATH = Path(__file__).resolve().parents[1] / "worker" / "src" / "company-ir-page-registry.json"


def _normalize_host(host: str) -> str:
    host = str(host).lower()
    return host[:-1] if host.endswith(".") else host


def _load_registry() -> dict:
    try:
        return json.loads(_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"sources": []}


_approved_entries: list[dict] = []
REGISTRY_HOSTS: tuple[str, ...] = ()
_allowed_hosts: frozenset[str] = frozenset()


def _install_registry(registry: dict) -> None:
    """Set the approved registry entries and the allow-list built from them."""
    global _approved_entries, REGISTRY_HOSTS, _allowed_hosts
    sources = registry.get("sources") if isinstance(registry, dict) else None
    _approved_entries = [e for e in (sources or []) if isinstance(e, dict) and e.get("status") == "approved"]
    seen: dict[str, None] = {}
    for entry in _approved_entries:
        for host in entry.get("allowedHosts") or []:
            seen[_normalize_host(host)] = None
    REGISTRY_HOSTS = tuple(seen)
    _allowed_hosts = frozenset((*SEC_HOSTS, *YAHOO_HOSTS, *PROVIDER_HOSTS, *REGISTRY_HOSTS))


# Hosts of approved company IR registry entries, read once at import.
_install_registry(_load_registry())


@contextlib.contextmanager
def registry_override(registry: dict):
    """Use ``registry`` for the approved-host allow-list inside the block (tests only)."""
    saved = (_approved_entries, REGISTRY_HOSTS, _allowed_hosts)
    _install_registry(registry)
    try:
        yield
    finally:
        _restore(saved)


def _restore(saved: tuple) -> None:
    global _approved_entries, REGISTRY_HOSTS, _allowed_hosts
    _approved_entries, REGISTRY_HOSTS, _allowed_hosts = saved


# ── URL parsing (the WHATWG behaviour the Worker's `new URL()` gives, for the cases that matter here) ────────────

_SCHEME_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*):")
_FORBIDDEN_HOST_CHARS = set(" #/:<>?@[\\]^|%") | {chr(c) for c in range(0x20)} | {chr(0x7F)}
_HEX_LABEL_RE = re.compile(r"^0[xX][0-9a-fA-F]*$")
_DEC_LABEL_RE = re.compile(r"^[0-9]+$")


def _ipv4_number(label: str) -> int | None:
    if label == "":
        return None
    try:
        if _HEX_LABEL_RE.match(label):
            return int(label[2:] or "0", 16)
        if len(label) > 1 and label.startswith("0") and _DEC_LABEL_RE.match(label):
            return int(label, 8)
        if _DEC_LABEL_RE.match(label):
            return int(label, 10)
    except ValueError:
        return None
    return None


def _ends_in_number(host: str) -> bool:
    parts = host.split(".")
    if parts and parts[-1] == "" and len(parts) > 1:
        parts.pop()
    last = parts[-1] if parts else ""
    return bool(last) and bool(_DEC_LABEL_RE.match(last) or _HEX_LABEL_RE.match(last))


def _is_valid_ipv4(host: str) -> bool:
    parts = host.split(".")
    if parts and parts[-1] == "" and len(parts) > 1:
        parts.pop()
    if len(parts) > 4:
        return False
    numbers = [_ipv4_number(p) for p in parts]
    if any(n is None for n in numbers):
        return False
    if any(n > 255 for n in numbers[:-1]):
        return False
    return numbers[-1] < 256 ** (5 - len(numbers))


class _Parsed:
    __slots__ = ("scheme", "username", "password", "port", "host", "path")

    def __init__(self, scheme: str, username: str, password: str, port: int | None, host: str, path: str) -> None:
        self.scheme, self.username, self.password, self.port, self.host, self.path = scheme, username, password, port, host, path


def _normalize_path(path: str) -> str:
    """Resolve "." and ".." segments the way a URL parser does."""
    out: list[str] = []
    segments = path.split("/")[1:]
    for index, segment in enumerate(segments):
        last = index == len(segments) - 1
        low = segment.lower()
        if low in (".", "%2e"):
            if last:
                out.append("")
        elif low in ("..", ".%2e", "%2e.", "%2e%2e"):
            if out:
                out.pop()
            if last:
                out.append("")
        else:
            out.append(segment)
    return "/" + "/".join(out)


def _parse(url: object) -> _Parsed | None:
    """Parse an absolute URL; None when it would not parse (``invalid URL``)."""
    if not isinstance(url, str):
        return None
    text = url.strip("".join(chr(c) for c in range(0x21))).translate({9: None, 10: None, 13: None})
    match = _SCHEME_RE.match(text)
    if not match:
        return None
    scheme = match.group(1).lower()
    rest = text[match.end():]
    if scheme != "https":
        # Rule 2 reports these; only the scheme matters.
        return _Parsed(scheme, "", "", None, "", "")
    rest = rest.lstrip("/\\")
    end = len(rest)
    for ch in "/\\?#":
        idx = rest.find(ch)
        if idx != -1:
            end = min(end, idx)
    authority, remainder = rest[:end], rest[end:]
    userinfo = ""
    if "@" in authority:
        userinfo, _, authority = authority.rpartition("@")
    username, _, password = userinfo.partition(":")
    port_text = ""
    if authority.startswith("["):
        close = authority.find("]")
        if close == -1:
            return None
        host, tail = authority[: close + 1], authority[close + 1:]
        if tail:
            if not tail.startswith(":"):
                return None
            port_text = tail[1:]
        inner = host[1:-1]
        if "%" in inner:
            return None
        try:
            ipaddress.IPv6Address(inner)
        except ValueError:
            return None
    else:
        host, sep, port_text = authority.partition(":")
        if ":" in port_text:
            return None
        if host == "":
            return None
        try:
            host = urllib.parse.unquote(host, errors="strict")
        except UnicodeDecodeError:
            return None
        try:
            host = host.encode("idna").decode("ascii") if not host.isascii() else host
        except UnicodeError:
            return None
        host = host.lower()
        if any(ch in _FORBIDDEN_HOST_CHARS for ch in host):
            return None
        if _ends_in_number(host) and not _is_valid_ipv4(host):
            return None
    port: int | None = None
    if port_text != "":
        if not port_text.isascii() or not port_text.isdigit():
            return None
        port = int(port_text)
        if port > 65535:
            return None
    path_end = len(remainder)
    for ch in "?#":
        idx = remainder.find(ch)
        if idx != -1:
            path_end = min(path_end, idx)
    path = _normalize_path(remainder[:path_end].replace("\\", "/") or "/")
    return _Parsed(scheme, username, password, port, host, path)


def _is_ip_literal(host: str) -> bool:
    """Bracketed IPv6, or a dotted/integer/hex/octal IPv4 form."""
    if host.startswith("[") or ":" in host:
        return True
    labels = host.split(".")
    return bool(labels) and all(re.match(r"^(0x[0-9a-f]*|\d+)$", label, re.IGNORECASE) for label in labels)


def check_fetch_url(url: str) -> tuple[bool, str]:
    """(allowed, reason). The reason is empty when allowed and names the first failing rule otherwise."""
    parsed = _parse(url)
    if parsed is None:
        return False, "invalid URL"
    if parsed.scheme != "https":
        return False, "URL must use https"
    if parsed.username or parsed.password:
        return False, "URL must not contain credentials"
    if parsed.port is not None and parsed.port != 443:
        return False, "URL port not allowed"
    host = _normalize_host(parsed.host)
    if host == "" or _is_ip_literal(host) or host == "localhost" or any(host.endswith(s) for s in _BLOCKED_SUFFIXES):
        return False, "URL host not allowed"
    if host not in _allowed_hosts:
        return False, "URL host not allowed"
    return True, ""


def allowed_host_kind(url: str) -> str:
    """Which part of the allow-list a host (already passed by check_fetch_url) belongs to."""
    parsed = _parse(url)
    host = _normalize_host(parsed.host) if parsed else ""
    if host in SEC_HOSTS:
        return "sec"
    if host in YAHOO_HOSTS:
        return "yahoo"
    if host in REGISTRY_HOSTS:
        return "registry"
    return "provider"


def _host_without_www(host: str) -> str:
    host = str(host).lower()
    return host[4:] if host.startswith("www.") else host


def company_ir_page_url_allowed(entry: dict, raw_url: str) -> bool:
    """A URL inside a registry entry's allowed hosts and path prefixes."""
    parsed = _parse(raw_url)
    if parsed is None or parsed.scheme != "https":
        return False
    host = _host_without_www(parsed.host)
    allowed_host = any(host == _host_without_www(candidate) for candidate in entry.get("allowedHosts") or [])
    allowed_path = any(parsed.path.startswith(prefix) for prefix in entry.get("allowedPathPrefixes") or [])
    return allowed_host and allowed_path


def sec_archives_url(url: str) -> bool:
    """The normalized URL is on www.sec.gov under /Archives/ (what ``new URL(url).toString().startsWith(...)`` tests in the Worker)."""
    parsed = _parse(url)
    return bool(parsed and parsed.scheme == "https" and _normalize_host(parsed.host) == "www.sec.gov" and parsed.path.startswith("/Archives/"))


def approved_registry_url_allowed(raw_url: str) -> bool:
    """A caller URL on a registry host must sit inside an approved entry's path prefixes."""
    return any(company_ir_page_url_allowed(entry, raw_url) for entry in _approved_entries)


# ── fetching ────────────────────────────────────────────────────────────────────────────────────────────────

def _origin(url: str) -> tuple[str, str, int | None]:
    parsed = urllib.parse.urlsplit(url)
    return parsed.scheme.lower(), (parsed.hostname or "").lower(), parsed.port or (443 if parsed.scheme.lower() == "https" else None)


def require_allowed(url: str) -> None:
    """Raise UrlPolicyError unless ``url`` passes check_fetch_url."""
    ok, reason = check_fetch_url(url)
    if not ok:
        raise UrlPolicyError(reason)


class CheckedRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects, re-checking every hop against the allow-list (at most 5 hops)."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        followed = sum(getattr(req, "redirect_dict", {}).values())
        if followed >= MAX_REDIRECT_HOPS:
            raise UrlPolicyError("too many redirects")
        require_allowed(newurl)
        if not consume_subrequest():
            raise BudgetExhaustedError()
        method = req.get_method()
        if code == 303 and method not in ("GET", "HEAD"):
            # 303 continues as GET, without the body or its content headers.
            skip = {"content-length", "content-type", "transfer-encoding"}
            new = urllib.request.Request(
                newurl,
                headers={k: v for k, v in req.headers.items() if k.lower() not in skip},
                origin_req_host=req.origin_req_host,
                unverifiable=True,
                method="GET",
            )
        else:
            new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None and _origin(req.full_url) != _origin(newurl):
            new.remove_header("Authorization")   # a credential header stays with its origin
        return new


def build_opener(*handlers) -> urllib.request.OpenerDirector:
    """An opener that re-checks every redirect hop; extra handlers (cookies) are added as given."""
    return urllib.request.build_opener(CheckedRedirectHandler(), *handlers)


_OPENER = build_opener()


def open_checked(opener: urllib.request.OpenerDirector, req, timeout=None):
    """opener.open after checking the first URL and counting it against the invocation's subrequest budget."""
    require_allowed(req if isinstance(req, str) else req.full_url)
    if not consume_subrequest():
        raise BudgetExhaustedError()
    try:
        return opener.open(req, timeout=timeout) if timeout is not None else opener.open(req)
    except BudgetExhaustedError:
        raise
    except Exception as error:
        # Cloudflare's own "Too many subrequests" counts as a refusal too, and is raised as is.
        if is_budget_exhausted_error(error) and _invocation.get() is not None:
            _count_refusal()
        raise


def urlopen(req, timeout=None):
    """Drop-in for urllib.request.urlopen: checks the first URL, then re-checks every redirect hop."""
    return open_checked(_OPENER, req, timeout)
