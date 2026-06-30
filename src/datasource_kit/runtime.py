"""Named Core Archetype runtime: enumerate -> fetch -> persist -> diff -> assess -> report.

``run_ingest`` is the opt-in composition over registered providers.  It is the
structural twin of ``splot.run_round`` and ``reviewkit.review_document``: a
single named entry point that chains the primitives, never the only way to use
them.  The ``DataSource``/``IngestActor`` protocols and every other primitive in
the kit remain fully usable without this module.

Provider registry
-----------------
A ``ProviderRegistry`` maps dot-namespaced string names to callables.  Built-in
providers shipped with the kit:

``enumerate.passthrough``
    Wraps the ``windows`` iterable so each element is its own ref list.

``diff.by_id``
    Set-difference: compares the enumerated record ids against the store's
    ``existing_ids()`` and classifies each record as added/updated/unchanged.

``diff.full_replace``
    Whole-window replace: calls ``store.replace_all(records)``; never reads
    ``existing_ids()``.

``assess.passthrough``
    Returns the string ``"ok"`` regardless of counts.  Consumers replace this
    with their own provider that maps counts/evidence to a domain status string.

``records.passthrough``
    Identity mapper: returns the payload unchanged as a single-element list.

Storage ports
-------------
``StoragePort`` and ``ArtifactStore`` are structural Protocols; any object with
the right methods satisfies them.  ``InMemoryStore`` and ``InMemoryArtifactStore``
are the zero-setup defaults used in tests and the standalone demo.

Standalone demo
---------------
>>> report = run_ingest("demo")
>>> report.status
'ok'

This works with zero consumer code, zero network, and zero third-party deps.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Protocol, runtime_checkable

from .rate_limit import TokenBucket
from .report import CompletenessReport, IngestReport
from .retry import retry as _retry

__all__ = [
    "ProviderRegistry",
    "StoragePort",
    "ArtifactStore",
    "InMemoryStore",
    "InMemoryArtifactStore",
    "SourceProfile",
    "builtin_registry",
    "run_ingest",
]


# ---------------------------------------------------------------------------
# Storage protocols (structural — no import required from consumer)
# ---------------------------------------------------------------------------


@runtime_checkable
class StoragePort(Protocol):
    """Minimal persistence interface required by ``run_ingest``."""

    def upsert(self, records: list[dict]) -> dict[str, int]:
        """Persist *records*, returning ``{"added": N, "updated": M}``."""
        ...

    def replace_all(self, records: list[dict]) -> dict[str, int]:
        """Replace the entire store with *records*; return diff counts."""
        ...

    def existing_ids(self) -> set[Any]:
        """Return the set of ids currently in the store."""
        ...


@runtime_checkable
class ArtifactStore(Protocol):
    """Content-addressed payload cache."""

    def store(self, payload: Any) -> str:
        """Persist *payload* and return its content address (hex digest)."""
        ...

    def resolve(self, address: str) -> Any:
        """Retrieve the payload for *address*; raise ``KeyError`` if absent."""
        ...


# ---------------------------------------------------------------------------
# In-memory defaults (zero-setup, no I/O, suitable for tests and the demo)
# ---------------------------------------------------------------------------


class InMemoryStore:
    """Thread-unsafe in-memory ``StoragePort`` for tests and the demo."""

    def __init__(self) -> None:
        self._records: dict[Any, dict] = {}

    def upsert(self, records: list[dict]) -> dict[str, int]:
        added = updated = 0
        for r in records:
            rid = r.get("id")
            if rid in self._records:
                updated += 1
            else:
                added += 1
            self._records[rid] = r
        return {"added": added, "updated": updated}

    def replace_all(self, records: list[dict]) -> dict[str, int]:
        removed = len(self._records)
        self._records = {r.get("id"): r for r in records}
        return {"removed": removed, "added": len(records), "updated": 0}

    def existing_ids(self) -> set[Any]:
        return set(self._records.keys())

    def all(self) -> list[dict]:
        return list(self._records.values())


class InMemoryArtifactStore:
    """Content-addressed in-memory ``ArtifactStore``."""

    def __init__(self) -> None:
        self._blobs: dict[str, Any] = {}

    def store(self, payload: Any) -> str:
        digest = hashlib.sha256(repr(payload).encode()).hexdigest()
        self._blobs[digest] = payload
        return digest

    def resolve(self, address: str) -> Any:
        if address not in self._blobs:
            raise KeyError(address)
        return self._blobs[address]


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------


class ProviderRegistry:
    """Maps dot-namespaced names to provider callables."""

    def __init__(self) -> None:
        self._providers: dict[str, Callable] = {}

    def register(self, name: str, fn: Callable) -> None:
        self._providers[name] = fn

    def get(self, name: str) -> Callable:
        if name not in self._providers:
            raise KeyError(f"Provider '{name}' is not registered")
        return self._providers[name]

    def __contains__(self, name: object) -> bool:
        return name in self._providers


def builtin_registry() -> ProviderRegistry:
    """Return a new ``ProviderRegistry`` pre-loaded with all builtin providers."""
    reg = ProviderRegistry()

    def enumerate_passthrough(window: object) -> list:
        # window is already a list of refs, or wrap it as a single ref
        return list(window) if isinstance(window, (list, tuple)) else [window]

    def records_passthrough(payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return [payload]
        return [{"value": payload}]

    def diff_by_id(
        records: list[dict], store: StoragePort
    ) -> tuple[list[dict], dict[str, int]]:
        existing = store.existing_ids()
        new, updated, unchanged = [], [], []
        for r in records:
            rid = r.get("id")
            if rid in existing:
                updated.append(r)
            else:
                new.append(r)
        valid = new + updated
        return valid, {
            "added": len(new),
            "updated": len(updated),
            "removed": 0,
            "unchanged": len(unchanged),
        }

    def diff_full_replace(
        records: list[dict], store: StoragePort
    ) -> tuple[list[dict], dict[str, int]]:
        counts = store.replace_all(records)
        return records, {
            "added": counts.get("added", len(records)),
            "updated": counts.get("updated", 0),
            "removed": counts.get("removed", 0),
            "unchanged": 0,
        }

    def assess_passthrough(counts: dict, evidence: list) -> str:
        return "ok"

    reg.register("enumerate.passthrough", enumerate_passthrough)
    reg.register("records.passthrough", records_passthrough)
    reg.register("diff.by_id", diff_by_id)
    reg.register("diff.full_replace", diff_full_replace)
    reg.register("assess.passthrough", assess_passthrough)
    return reg


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------


@dataclass
class SourceProfile:
    """Resolved configuration for one ingest source.

    ``providers`` maps logical step names to registered provider names::

        providers = {
            "enumerate": "enumerate.passthrough",
            "records":   "records.passthrough",
            "diff":      "diff.by_id",
            "assess":    "assess.passthrough",
        }

    ``policies`` carries throttle and retry parameters::

        policies = {"rate_per_sec": 10.0, "burst": 5.0, "retries": 3, "backoff": 1.0}
    """

    name: str
    providers: dict[str, str] = field(default_factory=dict)
    policies: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def _default(cls, name: str) -> "SourceProfile":
        return cls(
            name=name,
            providers={
                "enumerate": "enumerate.passthrough",
                "records": "records.passthrough",
                "diff": "diff.by_id",
                "assess": "assess.passthrough",
            },
            policies={
                "rate_per_sec": 100.0,
                "burst": 100.0,
                "retries": 3,
                "backoff": 0.0,
            },
        )

    def digest(self) -> str:
        """Stable SHA-256 hex of the profile's provider + policy data."""
        payload = repr(
            (self.name, sorted(self.providers.items()), sorted(self.policies.items()))
        )
        return hashlib.sha256(payload.encode()).hexdigest()


def _load_profile(profile: str | os.PathLike | SourceProfile) -> SourceProfile:
    if isinstance(profile, SourceProfile):
        return profile
    name = str(profile)
    # Future: load from a JSON file at the given path.  For now, return the
    # default profile so the standalone demo works without any file on disk.
    return SourceProfile._default(name)


def _kit_version() -> str:
    try:
        return importlib.metadata.version("datasource-kit")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def _validate_record(record: Any) -> tuple[bool, str]:
    """Return (valid, reason).  A valid record must be a dict."""
    if not isinstance(record, dict):
        return False, f"record is not a dict: {type(record).__name__}"
    return True, ""


# ---------------------------------------------------------------------------
# run_ingest
# ---------------------------------------------------------------------------


def run_ingest(
    profile: str | os.PathLike | SourceProfile,
    *,
    registry: ProviderRegistry | None = None,
    store: StoragePort | None = None,
    artifact_store: ArtifactStore | None = None,
    windows: Iterable[object] | None = None,
) -> IngestReport:
    """Run the enumerate→fetch→persist→diff→assess→report archetype.

    Parameters
    ----------
    profile:
        A ``SourceProfile`` instance, or a path/name string that resolves to
        one.  The default profile uses only builtin providers and no network.
    registry:
        Provider registry.  Defaults to ``builtin_registry()``.
    store:
        Persistence layer.  Defaults to ``InMemoryStore()``.
    artifact_store:
        Payload cache.  Defaults to ``InMemoryArtifactStore()``.
    windows:
        Iterable of window objects passed to the enumerate provider.  Defaults
        to ``[None]`` (a single anonymous window) so the demo executes without
        any consumer setup.
    """
    resolved_profile = _load_profile(profile)
    if registry is None:
        registry = builtin_registry()
    if store is None:
        store = InMemoryStore()
    if artifact_store is None:
        artifact_store = InMemoryArtifactStore()
    if windows is None:
        windows = [None]

    # Resolve provider callables
    enumerate_fn = registry.get(
        resolved_profile.providers.get("enumerate", "enumerate.passthrough")
    )
    records_fn = registry.get(
        resolved_profile.providers.get("records", "records.passthrough")
    )
    diff_name = resolved_profile.providers.get("diff", "diff.by_id")
    diff_fn = registry.get(diff_name)
    assess_fn = registry.get(
        resolved_profile.providers.get("assess", "assess.passthrough")
    )

    # Throttle configuration
    rate_per_sec: float = float(resolved_profile.policies.get("rate_per_sec", 100.0))
    burst: float = float(resolved_profile.policies.get("burst", 100.0))
    retries: int = int(resolved_profile.policies.get("retries", 3))
    backoff: float = float(resolved_profile.policies.get("backoff", 0.0))

    bucket = TokenBucket(rate_per_sec=rate_per_sec, burst=burst)

    # Accumulators
    window_results: list[dict] = []
    agg_diff: dict[str, int] = {"added": 0, "updated": 0, "removed": 0, "unchanged": 0}
    total_retries: int = 0
    total_wait: float = 0.0
    all_warnings: list[str] = []
    all_evidence: list[dict] = []
    last_status: str = "ok"

    for window_idx, window in enumerate(windows):
        refs = enumerate_fn(window)
        window_records: list[dict] = []
        window_retries = 0
        window_wait = 0.0
        window_warnings: list[str] = []
        window_evidence: list[dict] = []
        fetched_count = 0
        failed_count = 0

        for ref in refs:
            # Throttle
            t0 = time.monotonic()
            bucket.wait(1.0)
            window_wait += time.monotonic() - t0

            # Fetch with retry (the "fetch" step uses the ref directly;
            # consumers replace records_fn to deserialize the payload)
            attempt_count = 0

            def _fetch(ref=ref):
                nonlocal attempt_count
                attempt_count += 1
                # Default fetcher: treat the ref as the payload
                return ref

            try:
                payload = _retry(_fetch, retries=retries, backoff_seconds=backoff)
            except RuntimeError as exc:
                failed_count += 1
                window_warnings.append(
                    f"window[{window_idx}] ref={ref!r}: fetch failed after {retries} retries: {exc}"
                )
                continue

            window_retries += max(0, attempt_count - 1)

            # Artifact store (content-addressed dedup)
            address = artifact_store.store(payload)
            cached_payload = artifact_store.resolve(address)

            # Map payload -> records
            raw_records = records_fn(cached_payload)

            # Validate then collect only safe records
            valid_records = []
            for rec in raw_records:
                ok, reason = _validate_record(rec)
                if ok:
                    valid_records.append(rec)
                else:
                    window_warnings.append(
                        f"window[{window_idx}] ref={ref!r}: invalid record skipped: {reason}"
                    )

            window_records.extend(valid_records)
            fetched_count += 1
            window_evidence.append({"ref": ref, "address": address})

        # Diff step
        if diff_name == "diff.full_replace":
            persisted, diff_counts = diff_fn(window_records, store)
        else:
            # diff.by_id (and any custom provider): pass records + store
            persisted, diff_counts = diff_fn(window_records, store)

        # Persist only for diff.by_id (full_replace already called store)
        if diff_name != "diff.full_replace":
            if persisted:
                upsert_counts = store.upsert(persisted)
                diff_counts["added"] = upsert_counts.get("added", diff_counts["added"])
                diff_counts["updated"] = upsert_counts.get("updated", diff_counts["updated"])

        # Duplicate-identity check
        seen_ids: set = set()
        for rec in persisted:
            rid = rec.get("id")
            if rid in seen_ids:
                window_warnings.append(
                    f"window[{window_idx}]: duplicate identity id={rid!r} in persisted records"
                )
            seen_ids.add(rid)

        # Fail-closed: partial coverage
        if failed_count > 0:
            window_warnings.append(
                f"window[{window_idx}]: {failed_count}/{failed_count + fetched_count} refs failed; "
                "partial coverage recorded"
            )

        # Assess step: consumer maps counts + evidence -> status string
        window_counts = {**diff_counts, "fetched": fetched_count, "failed": failed_count}
        last_status = assess_fn(window_counts, window_evidence)

        # Accumulate
        for k in agg_diff:
            agg_diff[k] += diff_counts.get(k, 0)
        total_retries += window_retries
        total_wait += window_wait
        all_warnings.extend(window_warnings)
        all_evidence.extend(window_evidence)

        window_results.append(
            {
                "window_index": window_idx,
                "window": repr(window),
                "refs_enumerated": len(refs),
                "fetched": fetched_count,
                "failed": failed_count,
                "diff": diff_counts,
                "retries": window_retries,
                "rate_limit_wait": window_wait,
                "status": last_status,
                "warnings": list(window_warnings),
            }
        )

    return IngestReport(
        source=str(profile) if not isinstance(profile, SourceProfile) else profile.name,
        status=last_status,
        windows=window_results,
        completeness=None,
        diff=agg_diff,
        retries_used=total_retries,
        rate_limit_waits=total_wait,
        source_digest=resolved_profile.digest(),
        kit_version=_kit_version(),
        warnings=all_warnings,
    )
