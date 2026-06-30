"""Ingest report: explainable trace produced by one run_ingest call."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .completeness import CompletenessReport

__all__ = ["IngestReport"]

# Keep in sync with datasource_kit version string if one is added.
_KIT_VERSION = "0.1.0"


@dataclass
class IngestReport:
    source_name: str
    status: str = "ok"
    windows: list[dict[str, Any]] = field(default_factory=list)
    windows_processed: int = 0
    records_fetched: int = 0
    diff: dict[str, Any] = field(default_factory=dict)
    completeness: CompletenessReport | None = field(default_factory=CompletenessReport)
    retries_used: int = 0
    rate_limit_waits: float = 0.0
    warnings: list[str] = field(default_factory=list)
    source_digest: str = ""
    kit_version: str = _KIT_VERSION

    def as_dict(self) -> dict[str, Any]:
        completeness = (
            self.completeness.as_dict() if self.completeness is not None else None
        )
        return {
            "source": self.source_name,
            "source_name": self.source_name,
            "status": self.status,
            "windows": list(self.windows),
            "windows_processed": self.windows_processed,
            "records_fetched": self.records_fetched,
            "diff": self.diff,
            "completeness": completeness,
            "retries_used": self.retries_used,
            "rate_limit_waits": self.rate_limit_waits,
            "warnings": list(self.warnings),
            "source_digest": self.source_digest,
            "kit_version": self.kit_version,
        }

    def save_json(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.as_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "IngestReport":
        completeness_raw = d.get("completeness", {})
        return cls(
            source_name=d.get("source_name", d.get("source", "")),
            status=d.get("status", "ok"),
            windows=d.get("windows", []),
            windows_processed=d.get("windows_processed", 0),
            records_fetched=d.get("records_fetched", 0),
            diff=d.get("diff", {}),
            completeness=(
                CompletenessReport.from_dict(completeness_raw)
                if completeness_raw is not None
                else None
            ),
            retries_used=d.get("retries_used", 0),
            rate_limit_waits=d.get("rate_limit_waits", 0),
            warnings=d.get("warnings", []),
            source_digest=d.get("source_digest", ""),
            kit_version=d.get("kit_version", _KIT_VERSION),
        )

    @classmethod
    def load_json(cls, path: str | Path) -> "IngestReport":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_dict(raw)
