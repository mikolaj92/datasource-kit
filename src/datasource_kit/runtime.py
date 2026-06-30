"""run_ingest: the enumerate -> fetch -> map -> diff -> assess -> persist archetype.

Drives the full pipeline for one source profile using the supplied registry.
Returns an :class:`~datasource_kit.report.IngestReport` with counts and a
diff summary; no third-party deps required.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .completeness import CompletenessReport
from .profile import load_profile, validate_source
from .providers import builtin_registry
from .report import IngestReport

__all__ = ["run_ingest"]


def run_ingest(
    source: str | Path,
    *,
    registry: dict[str, Any] | None = None,
) -> IngestReport:
    """Run the full ingest archetype for the given source folder.

    Parameters
    ----------
    source:
        Path to a source folder (or a ``source.json`` file directly).
    registry:
        Provider registry to resolve provider names against.  Defaults to
        :func:`~datasource_kit.providers.builtin_registry`.
    """
    if registry is None:
        registry = builtin_registry()

    profile = load_profile(source)
    validate_source(profile, registry)

    providers = profile.get("providers", {})
    policies = profile.get("policies", {})
    completeness_layers: list[str] = profile.get("completeness_layers", [])
    source_name: str = profile.get("name", str(source))

    # Compute a digest of the profile for traceability.
    source_digest = hashlib.sha256(
        json.dumps(profile, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()[:16]

    enumerator = registry[providers["enumerator"]]
    fetcher = registry[providers["fetcher"]]
    mapper = registry[providers["mapper"]]
    differ = registry[providers["diff"]]
    assessor = registry[providers["assess"]]
    store_factory = registry[providers["store"]]

    store = store_factory()
    stored_records = store.load()

    # Enumerate windows using the enumerator config embedded in policies.
    windows = enumerator(policies.get("enumerator", {}))

    all_fetched: list[dict] = []
    for window in windows:
        raw = fetcher(window, policies.get("fetcher", {}))
        mapped = mapper(raw, policies.get("mapper", {}))
        all_fetched.extend(mapped)

    # Diff and persist.
    diff_result = differ(all_fetched, stored_records, policies.get("diff", {}))

    # Prefer full_replace when diff says so.
    if "replaced" in diff_result:
        store.replace_all(all_fetched)
    else:
        store.save(all_fetched)

    # Assess each window independently (use last window for summary).
    last_assessment: dict = {}
    for window in windows:
        last_assessment = assessor(all_fetched, window, policies.get("assess", {}))

    # Build completeness counts from named layers.
    layer_counts: dict[str, int] = {}
    for layer in completeness_layers:
        layer_counts[layer] = last_assessment.get("count", 0)

    present = last_assessment.get("count", 0)
    completeness = CompletenessReport(
        layers=layer_counts,
        present=present,
        truth=present,
    )

    return IngestReport(
        source_name=source_name,
        windows_processed=len(windows),
        records_fetched=len(all_fetched),
        diff=diff_result,
        completeness=completeness,
        source_digest=source_digest,
    )
