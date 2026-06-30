"""datasource-kit: domain-blind ingest FRAMEWORK + primitives library.

Two honest faces behind one dependency-free surface:

* **Primitives library** — :class:`~datasource_kit.protocols.DataSource` /
  :class:`~datasource_kit.protocols.IngestActor` protocols, pure-data shapes,
  :class:`~datasource_kit.registry.Registry`, :class:`~datasource_kit.manifest.Manifest`,
  journal helpers, :class:`~datasource_kit.rate_limit.TokenBucket`, and
  :func:`~datasource_kit.retry.retry`.  The batch MSDS consumer uses these directly.

* **Opt-in archetype runtime** — :func:`~datasource_kit.ingest.run_ingest` drives
  ``enumerate -> fetch -> persist -> diff -> assess -> report`` over a window/checkpoint
  loop.  diff and assess are profile-named providers so the batch full-replace model
  is expressible without window/id ceremony.  Never the only entry point.

:func:`~datasource_kit.providers.builtin_registry` ships a complete chain:
``diff.by_id``, ``diff.full_replace``, ``assess.passthrough``.

Run the standalone demo (no network, no extras, no consumer code)::

    datasource-kit examples run demo-scraper

An optional :class:`~datasource_kit.scheduler.WorkerScheduler` (behind the
``scheduler`` extra) drives a poll loop; it is imported lazily so plain
``import datasource_kit`` never pulls in APScheduler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .ingest import IngestReport, run_ingest
from .journal import ensure_update_log, now_utc, record_update
from .ledger import DiscoveryLedger, Evidence
from .manifest import Manifest, SourceContract
from .profile import SourceProfile, load_profile, validate_source
from .protocols import DataSource, IngestActor
from .providers import ProviderRegistry, builtin_registry
from .rate_limit import TokenBucket
from .registry import Registry
from .retry import retry
from .storage import ArtifactStore, InMemoryArtifactStore

if TYPE_CHECKING:
    from .scheduler import WorkerScheduler

__all__ = [
    "ArtifactStore",
    "DataSource",
    "DiscoveryLedger",
    "Evidence",
    "InMemoryArtifactStore",
    "IngestActor",
    "IngestReport",
    "Manifest",
    "ProviderRegistry",
    "Registry",
    "SourceContract",
    "SourceProfile",
    "TokenBucket",
    "WorkerScheduler",
    "builtin_registry",
    "ensure_update_log",
    "load_profile",
    "now_utc",
    "record_update",
    "retry",
    "run_ingest",
    "validate_source",
]


def __getattr__(name: str) -> object:
    # Lazy access so importing datasource_kit never requires apscheduler.
    if name == "WorkerScheduler":
        from .scheduler import WorkerScheduler

        return WorkerScheduler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
