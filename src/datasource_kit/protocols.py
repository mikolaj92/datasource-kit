"""Structural protocols for the two datasource models the kit supports.

``DataSource`` describes the *batch reference-data* model (download an official
dataset, reload a local store, answer lookups). ``IngestActor`` describes the
*scraper/crawler* model (handle a job emitted by a queue, yield domain objects).

Both are ``runtime_checkable`` Protocols, so existing classes satisfy them
structurally without importing anything from this package.
"""

from __future__ import annotations

from typing import Any, Iterable, Protocol, runtime_checkable

__all__ = ["DataSource", "IngestActor"]


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
