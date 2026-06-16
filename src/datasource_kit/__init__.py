"""datasource-kit: a generic, dependency-free toolkit for managing many datasources.

It supports two models behind one small surface:

* batch reference-data updaters -- :class:`~datasource_kit.protocols.DataSource`
  plus the :mod:`~datasource_kit.journal` update-log primitives and
  :func:`~datasource_kit.retry.retry`;
* long-running scraper workers -- :class:`~datasource_kit.protocols.IngestActor`
  plus :class:`~datasource_kit.rate_limit.TokenBucket`.

Both kinds register into one :class:`~datasource_kit.registry.Registry` and can
be described declaratively with :class:`~datasource_kit.manifest.Manifest`.

An optional :class:`~datasource_kit.scheduler.WorkerScheduler` (behind the
``scheduler`` extra) can drive a poll loop; it is imported lazily so the core
package stays dependency-free.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .journal import ensure_update_log, now_utc, record_update
from .manifest import Manifest, SourceContract
from .protocols import DataSource, IngestActor
from .rate_limit import TokenBucket
from .registry import Registry
from .retry import retry

if TYPE_CHECKING:
    from .scheduler import WorkerScheduler

__all__ = [
    "DataSource",
    "IngestActor",
    "Manifest",
    "Registry",
    "SourceContract",
    "TokenBucket",
    "WorkerScheduler",
    "ensure_update_log",
    "now_utc",
    "record_update",
    "retry",
]


def __getattr__(name: str) -> object:
    # Lazy access so importing datasource_kit never requires apscheduler.
    if name == "WorkerScheduler":
        from .scheduler import WorkerScheduler

        return WorkerScheduler
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
