"""run_ingest: the enumerate -> fetch -> persist -> diff -> assess -> report archetype."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .completeness import CompletenessReport, LayerCoverage
from .errors import RuntimeStepError
from .profile import SourceProfile, load_profile, validate_source
from .providers import ProviderRegistry, builtin_registry
from .ratelimit import TokenBucket, with_retry
from .report import IngestReport

__all__ = ["run_ingest"]


def run_ingest(
    source: str | Path | SourceProfile,
    *,
    registry: ProviderRegistry | None = None,
) -> IngestReport:
    """Run the opt-in ingest archetype for a source profile."""

    registry = registry or builtin_registry()
    profile = source if isinstance(source, SourceProfile) else load_profile(source)
    validate_source(profile, registry)

    providers = profile.providers
    policies = profile.policies

    enumerator = registry[providers["enumerator"]]
    fetcher = registry[providers["fetcher"]]
    mapper = registry[providers["mapper"]]
    differ = registry[providers["diff"]]
    assessor = registry[providers["assess"]]
    store_factory = registry[providers["store"]]

    store = store_factory()
    stored_records = store.load()
    windows = enumerator(policies.get("enumerator", {}))
    throttle = _bucket(policies.get("rate_limit", {}))
    retry_policy = _retry_policy(policies.get("retry", {}))

    warnings: list[str] = []
    window_reports: list[dict[str, Any]] = []
    all_fetched: list[dict[str, Any]] = []
    retries_used = 0
    rate_limit_waits = 0.0

    for window in windows:
        try:
            rate_limit_waits += throttle.acquire()
            attempts = {"count": 0}

            def fetch_once() -> list[dict[str, Any]]:
                attempts["count"] += 1
                return fetcher(window, policies.get("fetcher", {}))

            raw = with_retry(fetch_once, **retry_policy)
            retries_used += max(attempts["count"] - 1, 0)
            mapped = mapper(raw, policies.get("mapper", {}))
            valid, invalid = _valid_records(mapped)
            if invalid:
                warnings.append(f"{len(invalid)} invalid record(s) in {window}")
            all_fetched.extend(valid)
            window_reports.append(
                {
                    "window": window,
                    "fetched": len(raw),
                    "persistable": len(valid),
                    "invalid": len(invalid),
                }
            )
        except Exception as exc:  # noqa: BLE001 - report shortfall, then continue.
            warnings.append(f"window {window!r} failed: {exc}")

    diff_result = differ(all_fetched, stored_records, policies.get("diff", {}))
    try:
        if "replaced" in diff_result:
            store.replace_all(all_fetched)
        else:
            if hasattr(store, "upsert"):
                store.upsert(all_fetched)
            else:
                store.save(all_fetched)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeStepError(f"persist failed: {exc}") from exc

    assessment = assessor(all_fetched, windows[-1], policies.get("assess", {})) if windows else {}
    status = str(assessment.get("status", "partial" if warnings else "ok"))
    completeness = _completeness(profile.completeness_layers, int(assessment.get("count", 0)))

    return IngestReport(
        source_name=profile.name,
        status=status,
        windows=window_reports,
        windows_processed=len(windows),
        records_fetched=len(all_fetched),
        diff=diff_result,
        completeness=completeness,
        retries_used=retries_used,
        rate_limit_waits=rate_limit_waits,
        warnings=warnings,
        source_digest=_digest(profile),
    )


def _bucket(raw: Any) -> TokenBucket:
    if not isinstance(raw, dict):
        raw = {}
    rate = raw.get("rate", raw.get("rate_per_sec", 1_000_000.0))
    capacity = raw.get("capacity", raw.get("burst", 1_000_000.0))
    return TokenBucket(rate=float(rate), capacity=float(capacity))


def _retry_policy(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    return {
        "attempts": int(raw.get("attempts", 1)),
        "base_delay": float(raw.get("base_delay", 0.0)),
        "max_delay": float(raw.get("max_delay", 0.0)),
    }


def _valid_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[Any]]:
    valid: list[dict[str, Any]] = []
    invalid: list[Any] = []
    for record in records:
        if isinstance(record, dict) and record.get("id"):
            valid.append(record)
        else:
            invalid.append(record)
    return valid, invalid


def _completeness(layers: list[str], count: int) -> CompletenessReport | None:
    if not layers:
        return None
    return CompletenessReport(
        layers={
            layer: LayerCoverage(layer=layer, truth_count=count, present_count=count)
            for layer in layers
        },
        present=count,
        truth=count,
    )


def _digest(profile: SourceProfile) -> str:
    payload = json.dumps(
        profile.digest_payload(),
        sort_keys=True,
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(payload).hexdigest()[:16]
