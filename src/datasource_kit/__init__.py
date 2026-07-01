"""datasource-kit: a generic, dependency-free toolkit for managing many datasources.

It supports two models behind one small surface:

* batch reference-data updaters -- :class:`~datasource_kit.protocols.DataSource`
  plus the :mod:`~datasource_kit.journal` update-log primitives and
  :func:`~datasource_kit.retry.retry`;
* long-running scraper workers -- :class:`~datasource_kit.protocols.IngestActor`
  plus :class:`~datasource_kit.rate_limit.TokenBucket`.
* artifact backends -- :class:`~datasource_kit.protocols.ArtifactStore` for
  bytes-in/ref-out payload persistence.

Both kinds register into one :class:`~datasource_kit.registry.Registry` and can
be described declaratively with :class:`~datasource_kit.manifest.Manifest`.

An optional :class:`~datasource_kit.scheduler.WorkerScheduler` (behind the
``scheduler`` extra) can drive a poll loop; it is imported lazily so the core
package stays dependency-free.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .completeness import CompletenessReport, LayerCoverage, layers_from_names
from .errors import (
    DatasourceKitError,
    ProfileError,
    ProviderError,
    RegistryError,
    RuntimeStepError,
    SourceError,
    TransportError,
    ValidationError,
)
from .journal import ensure_update_log, now_utc, record_update
from .ledger import DiscoveredItem, DiscoveryLedgerStore, Evidence, LedgerSummary
from .manifest import Manifest, SourceContract
from .profile import SourceProfile, load_profile, validate_source
from .protocols import (
    ArtifactStore,
    DataSource,
    Enumerator,
    Fetcher,
    InMemoryArtifactStore,
    InMemoryStore,
    IngestActor,
    MockEnumerator,
    MockFetcher,
    StoragePort,
    SupportsExistingIds,
)
from .providers import ProviderRegistry, builtin_registry
from .rate_limit import TokenBucket
from .registry import Registry
from .report import IngestReport
from .results import (
    Cursor,
    WorkerResult,
    blocked_result,
    completed_result,
    working_result,
)
from .retry import retry, retry_decorator
from .runtime import run_ingest
from .window import DayWindow, WindowIterator, split_range_into_days

if TYPE_CHECKING:
    from .scheduler import WorkerScheduler

__all__ = [
    "ArtifactStore",
    "CompletenessReport",
    "Cursor",
    "DataSource",
    "DatasourceKitError",
    "DayWindow",
    "DiscoveredItem",
    "DiscoveryLedgerStore",
    "Enumerator",
    "Evidence",
    "Fetcher",
    "IngestReport",
    "InMemoryArtifactStore",
    "InMemoryStore",
    "IngestActor",
    "LayerCoverage",
    "LedgerSummary",
    "Manifest",
    "MockEnumerator",
    "MockFetcher",
    "ProfileError",
    "ProviderError",
    "ProviderRegistry",
    "Registry",
    "RegistryError",
    "RuntimeStepError",
    "SourceContract",
    "SourceError",
    "SourceProfile",
    "StoragePort",
    "SupportsExistingIds",
    "TokenBucket",
    "TransportError",
    "ValidationError",
    "WorkerScheduler",
    "WindowIterator",
    "WorkerResult",
    "blocked_result",
    "builtin_registry",
    "completed_result",
    "ensure_update_log",
    "layers_from_names",
    "load_profile",
    "now_utc",
    "record_update",
    "retry",
    "retry_decorator",
    "run_ingest",
    "split_range_into_days",
    "validate_source",
    "working_result",
]


def __getattr__(name: str) -> object:
    # Lazy access so importing datasource_kit never requires apscheduler.
    if name == "WorkerScheduler":
        from .scheduler import WorkerScheduler

        return WorkerScheduler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
