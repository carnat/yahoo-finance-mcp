"""Durable evidence storage for the local server, mirroring worker/src/evidence-store.ts.

Set YFMCP_EVIDENCE_DIR to keep evidence cuts and consensus observations on
disk under the same object keys the Worker writes to R2. Without it, research
still returns its full payload and receipt; only durable retrieval and
history are unavailable (storageStatus UNAVAILABLE). Objects are written once.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

_KEY_RE = re.compile(r"^(?:evidence-cuts|consensus-history)/[A-Za-z0-9._^=/-]+\.json$")


class LocalDirStore:
    kind = "local_dir"

    def __init__(self, root: str) -> None:
        self.root = Path(root).resolve()

    def _path(self, key: str) -> Path:
        if not _KEY_RE.match(key) or ".." in key:
            raise ValueError(f"invalid evidence key: {key}")
        path = (self.root / key).resolve()
        if self.root not in path.parents:
            raise ValueError(f"invalid evidence key: {key}")
        return path

    def get(self, key: str) -> str | None:
        path = self._path(key)
        return path.read_text(encoding="utf-8") if path.exists() else None

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def put(self, key: str, body: str, metadata: dict[str, str]) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(body)
        os.replace(tmp, path)

    def list(self, prefix: str, limit: int) -> list[str]:
        base = self.root / prefix
        if not base.exists():
            return []
        keys = sorted(str(p.relative_to(self.root)).replace(os.sep, "/") for p in base.rglob("*.json"))
        return keys[:limit]


class MemoryStore:
    kind = "memory"

    def __init__(self) -> None:
        self.objects: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        return self.objects.get(key)

    def exists(self, key: str) -> bool:
        return key in self.objects

    def put(self, key: str, body: str, metadata: dict[str, str]) -> None:
        self.objects[key] = body

    def list(self, prefix: str, limit: int) -> list[str]:
        return sorted(k for k in self.objects if k.startswith(prefix))[:limit]


_UNSET = object()
_injected: object = _UNSET


def set_store_for_tests(store: object) -> None:
    """Tests only: replace the store (None forces UNAVAILABLE); pass the module's _UNSET to reset."""
    global _injected
    _injected = store


def get_store():
    if _injected is not _UNSET:
        return _injected
    root = os.environ.get("YFMCP_EVIDENCE_DIR")
    return LocalDirStore(root) if root else None


def put_once(key: str, body: str, metadata: dict[str, str]) -> dict:
    """Write once: an existing key is left as it is. Never raises."""
    store = get_store()
    if store is None:
        return {"status": "UNAVAILABLE"}
    try:
        if store.exists(key):
            return {"status": "ALREADY_STORED"}
        store.put(key, body, metadata)
        return {"status": "STORED"}
    except Exception as e:  # noqa: BLE001 - persistence never fails research
        return {"status": "FAILED", "message": str(e)}


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
