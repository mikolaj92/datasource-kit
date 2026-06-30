"""Explainable output object returned by run_ingest.

``IngestReport`` records what happened during a full ingest run: diff counts,
retry telemetry, rate-limit wait time, optional per-layer completeness, and
enough identity information (source_digest, kit_version) for deterministic
replay and audit.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any

__all__ = ["CompletenessReport", "IngestReport"]


@dataclass(slots=True)
class CompletenessReport:
    """Per-layer coverage counts filled by the consumer's assess provider.

    ``layers`` maps a consumer-named layer to ``{"found": int, "expected": int}``.
    ``None`` is the right default when no per-layer measurement is meaningful
    (e.g. a batch full-replace that does not count by layer).
    """

    layers: dict[str, dict[str, int]] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"layers": dict(self.layers)}


@dataclass(slots=True)
class IngestReport:
    """Serializable summary of one ``run_ingest`` execution.

    Fields
    ------
    source:
        Profile identifier (path or name).
    status:
        Consumer-injected status string produced by the registered ``assess``
        provider.  The runtime does not interpret this value.
    windows:
        Per-window result dicts accumlated during the run.
    completeness:
        ``CompletenessReport`` when the assess provider fills layers; ``None``
        for a batch refresh that has nothing meaningful to measure per layer.
    diff:
        Aggregate counts: ``added``, ``updated``, ``removed``, ``unchanged``.
    retries_used:
        Total number of retry attempts across all fetches (0 when every fetch
        succeeded on the first try).
    rate_limit_waits:
        Total wall-clock seconds spent waiting on the ``TokenBucket``.
    source_digest:
        Stable SHA-256 hex digest of the resolved profile, enabling replay.
    kit_version:
        ``datasource-kit`` package version read from package metadata.
    warnings:
        Non-fatal issues: validation failures, duplicate identities, partial
        coverage shortfalls.
    """

    source: str
    status: str
    windows: list[dict[str, Any]] = field(default_factory=list)
    completeness: CompletenessReport | None = None
    diff: dict[str, int] = field(
        default_factory=lambda: {"added": 0, "updated": 0, "removed": 0, "unchanged": 0}
    )
    retries_used: int = 0
    rate_limit_waits: float = 0.0
    source_digest: str = ""
    kit_version: str = ""
    warnings: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "source": self.source,
            "status": self.status,
            "windows": list(self.windows),
            "completeness": self.completeness.as_dict() if self.completeness else None,
            "diff": dict(self.diff),
            "retries_used": self.retries_used,
            "rate_limit_waits": self.rate_limit_waits,
            "source_digest": self.source_digest,
            "kit_version": self.kit_version,
            "warnings": list(self.warnings),
        }
        return d

    def save_json(self, path: str | os.PathLike) -> None:
        """Write ``as_dict()`` to ``path`` using stdlib ``json`` only."""
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self.as_dict(), fh, ensure_ascii=False, indent=2)
