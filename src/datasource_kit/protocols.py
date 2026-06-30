"""Structural protocols and stdlib-only fakes for datasource seams.

``DataSource`` describes the *batch reference-data* model (download an official
dataset, reload a local store, answer lookups). ``IngestActor`` describes the
*scraper/crawler* model (handle a job emitted by a queue, yield domain objects).
``ArtifactStore`` describes the tiny blob seam scraper runtimes can share.

The lower-level ``Enumerator``, ``Fetcher``, ``StoragePort``, and
``ArtifactStore`` protocols name the seams a consuming runtime can inject for an
``enumerate -> fetch -> persist`` pipeline. They mirror the existing sibling
protocols by staying structural and ``runtime_checkable``, so concrete classes do
not need to inherit from this package.

``StoragePort`` intentionally covers only ``upsert`` and ``replace_all``.
By-id diff paths that need current keys should probe for ``SupportsExistingIds``;
full-replace stores may omit ``existing_ids`` and still satisfy the base storage
port at runtime.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "ArtifactStore",
    "DataSource",
    "Enumerator",
    "Fetcher",
    "InMemoryArtifactStore",
    "InMemoryStore",
    "IngestActor",
    "MockEnumerator",
    "MockFetcher",
    "StoragePort",
    "SupportsExistingIds",
]


@runtime_checkable
class DataSource(Protocol):
    """Batch reference-data source: look records up, refresh the local store."""

    def lookup(self, identifier: str) -> list[Any]:
        """Return records matching ``identifier`` (e.g. CAS/EC number)."""
        ...

    def refresh(self) -> dict:
        """Rebuild the local store from upstream; return a summary dict."""
        ...


@runtime_checkable
class IngestActor(Protocol):
    """Scraper-style source: turn a queued job into domain objects."""

    name: str

    def handle_job(self, job: dict) -> Iterable[Any]:
        """Process one job and yield (or return) the resulting objects."""
        ...


@runtime_checkable
class Enumerator(Protocol):
    """Enumerate opaque source references for a caller-defined window."""

    def enumerate(self, window: object) -> Iterable[object]:
        """Return refs for ``window`` without prescribing their shape."""
        ...


@runtime_checkable
class Fetcher(Protocol):
    """Fetch an opaque source reference into an opaque payload."""

    def fetch(self, ref: object) -> object:
        """Return deterministic payload data for ``ref``."""
        ...


@runtime_checkable
class StoragePort(Protocol):
    """Persist records either incrementally or by replacing the whole set."""

    def upsert(self, records: Iterable[object]) -> dict[str, int]:
        """Merge records into storage and return a count summary."""
        ...

    def replace_all(self, records: Iterable[object]) -> dict[str, int]:
        """Replace all records in storage and return a count summary."""
        ...


@runtime_checkable
class SupportsExistingIds(Protocol):
    """Optional by-id diff seam for storage ports that expose current keys."""

    def existing_ids(self) -> set[str]:
        """Return the currently persisted record identifiers."""
        ...


@runtime_checkable
class ArtifactStore(Protocol):
    """Store and resolve binary payloads by string reference."""

    def store(self, payload: bytes) -> str:
        """Persist ``payload`` and return its reference."""
        ...

    def resolve(self, ref: str) -> bytes:
        """Return the payload for ``ref`` or raise ``KeyError``."""
        ...


class InMemoryStore:
    """Dictionary-backed ``StoragePort`` fake keyed by a record id field."""

    def __init__(self, *, id_key: str = "id") -> None:
        self.id_key = id_key
        self._records: dict[str, object] = {}

    def upsert(self, records: Iterable[object]) -> dict[str, int]:
        incoming = list(records)
        for record in incoming:
            self._records[self._record_id(record)] = record
        return {"upserted": len(incoming)}

    def replace_all(self, records: Iterable[object]) -> dict[str, int]:
        incoming = list(records)
        self._records = {self._record_id(record): record for record in incoming}
        return {"replaced": len(incoming)}

    def existing_ids(self) -> set[str]:
        return set(self._records.keys())

    def all(self) -> list[object]:
        return list(self._records.values())

    def _record_id(self, record: object) -> str:
        if isinstance(record, Mapping):
            if self.id_key not in record:
                raise KeyError(f"record is missing id key {self.id_key!r}")
            return str(record[self.id_key])
        if not hasattr(record, self.id_key):
            raise KeyError(f"record is missing id key {self.id_key!r}")
        return str(getattr(record, self.id_key))


class InMemoryArtifactStore:
    """Content-addressed in-memory ``ArtifactStore`` fake."""

    def __init__(self) -> None:
        self._payloads: dict[str, bytes] = {}

    def store(self, payload: bytes) -> str:
        ref = hashlib.sha256(payload).hexdigest()
        self._payloads[ref] = payload
        return ref

    def resolve(self, ref: str) -> bytes:
        return self._payloads[ref]


@dataclass(frozen=True, slots=True)
class MockEnumerator:
    """Deterministic ``Enumerator`` fake for zero-network tests."""

    count: int = 3

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("count must be >= 0")

    def enumerate(self, window: object) -> Iterable[object]:
        return (f"{window}:{index}" for index in range(self.count))


class MockFetcher:
    """Deterministic ``Fetcher`` fake that derives bytes from the ref."""

    def fetch(self, ref: object) -> bytes:
        return f"mock:{ref}".encode()
