"""datasource-kit: a generic, dependency-free toolkit for managing many datasources.

It supports two models behind one small surface:

* batch reference-data updaters -- :class:`~datasource_kit.protocols.DataSource`
  plus the :mod:`~datasource_kit.journal` update-log primitives and
  :func:`~datasource_kit.retry.retry`;
* long-running scraper workers -- :class:`~datasource_kit.protocols.IngestActor`
  plus :class:`~datasource_kit.rate_limit.TokenBucket`.

Both kinds register into one :class:`~datasource_kit.registry.Registry` and can
be described declaratively with :class:`~datasource_kit.manifest.Manifest`.
"""

from __future__ import annotations

from .journal import ensure_update_log, now_utc, record_update
from .manifest import Manifest, SourceContract
from .protocols import DataSource, IngestActor
from .rate_limit import TokenBucket
from .registry import Registry
from .retry import retry

__all__ = [
    "DataSource",
    "IngestActor",
    "Manifest",
    "Registry",
    "SourceContract",
    "TokenBucket",
    "ensure_update_log",
    "now_utc",
    "record_update",
    "retry",
]
