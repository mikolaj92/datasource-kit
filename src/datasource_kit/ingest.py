"""run_ingest archetype runtime and IngestReport.

Drives enumerate -> fetch -> persist -> diff -> assess -> report over a
window/checkpoint loop with rate-limit + retry wired around provider hooks.

diff and assess are profile-named providers so the batch full-replace model
is expressible without window/id ceremony.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .ledger import DiscoveryLedger
from .providers import ProviderRegistry
from .storage import ArtifactStore
from .retry import retry as _retry

__all__ = ["IngestReport", "run_ingest"]


@dataclass
class IngestReport:
    """Explainable result produced by :func:`run_ingest`."""

    status: str
    totals: dict[str, int]
    assessment: str
    windows: int
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        parts = [f"status={self.status}", f"windows={self.windows}", f"assessment={self.assessment}"]
        for k, v in self.totals.items():
            parts.append(f"{k}={v}")
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        return " ".join(parts)


def run_ingest(
    *,
    enumerator: Callable[[], Iterable[Any]],
    fetcher: Callable[[Any], Any],
    store: ArtifactStore,
    registry: ProviderRegistry,
    diff_provider: str = "by_id",
    assess_provider: str = "passthrough",
    rate_limiter: Any = None,
    max_retries: int = 3,
    id_field: str = "id",
) -> IngestReport:
    """Run one ingest pass through the archetype pipeline.

    Validates that the named diff/assess providers are registered before
    touching any data (fail-closed on partial coverage).
    """
    # Validate-then-apply: fail closed if providers are missing.
    diff_fn = registry.get("diff", diff_provider)
    assess_fn = registry.get("assess", assess_provider)

    ledger = DiscoveryLedger()
    errors: list[str] = []
    windows = 0

    items = list(enumerator())
    ledger.record("enumerated", len(items))

    fetched: list[Any] = []
    for item in items:
        windows += 1
        if rate_limiter is not None:
            rate_limiter.wait()
        try:
            result = _retry(lambda i=item: fetcher(i), retries=max_retries)
            fetched.append(result)
        except RuntimeError as exc:
            errors.append(str(exc))

    ledger.record("fetched", len(fetched))

    existing_ids = store.list_ids()
    diff_result = diff_fn(existing_ids, fetched, id_field=id_field)
    new_items = diff_result.get("new", [])
    ledger.record("new", len(new_items))
    ledger.record("unchanged", len(diff_result.get("unchanged_ids", [])))

    for artifact in new_items:
        store.store_one(artifact)
    ledger.record("persisted", len(new_items))

    totals = ledger.totals()
    assessment = assess_fn(totals)
    status = "ok" if not errors else "partial"

    return IngestReport(
        status=status,
        totals=totals,
        assessment=assessment,
        windows=windows,
        errors=errors,
    )
