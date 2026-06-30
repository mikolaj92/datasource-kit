"""Discovery ledger shapes and crash-safe JSONL persistence."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from .errors import ValidationError

__all__ = [
    "DiscoveredItem",
    "DiscoveryLedgerStore",
    "Evidence",
    "LedgerSummary",
]


@dataclass(slots=True, frozen=True)
class DiscoveredItem:
    """One discovered item with consumer-supplied status."""

    source_id: str
    status: str
    url: str | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    @classmethod
    def from_json(cls, raw: Mapping[str, object]) -> "DiscoveredItem":
        source_id = raw.get("source_id")
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValidationError("DiscoveredItem requires a non-empty source_id")
        status = raw.get("status")
        if not isinstance(status, str) or not status:
            raise ValidationError("DiscoveredItem requires a status string")
        url = raw.get("url")
        metadata = raw.get("metadata") or {}
        return cls(
            source_id=source_id,
            status=status,
            url=url if isinstance(url, str) else None,
            metadata=dict(metadata) if isinstance(metadata, Mapping) else {},
        )

    def as_dict(self) -> dict[str, object]:
        data: dict[str, object] = {
            "source_id": self.source_id,
            "status": self.status,
            "metadata": dict(self.metadata),
        }
        if self.url is not None:
            data["url"] = self.url
        return data


@dataclass(slots=True, frozen=True)
class Evidence:
    """Opaque evidence payload captured during a run."""

    run_id: str
    captured_at: str
    payload: Mapping[str, object]

    @classmethod
    def capture(cls, *, run_id: str, payload: Mapping[str, object]) -> "Evidence":
        return cls(
            run_id=run_id,
            captured_at=datetime.now(UTC).isoformat(),
            payload=dict(payload),
        )

    @classmethod
    def from_json(cls, raw: Mapping[str, object]) -> "Evidence":
        run_id = raw.get("run_id")
        captured_at = raw.get("captured_at")
        payload = raw.get("payload") or {}
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValidationError("Evidence requires a non-empty run_id")
        if not isinstance(captured_at, str) or not captured_at.strip():
            raise ValidationError("Evidence requires a captured_at timestamp")
        if not isinstance(payload, Mapping):
            raise ValidationError("Evidence payload must be a mapping")
        return cls(run_id=run_id, captured_at=captured_at, payload=dict(payload))

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "captured_at": self.captured_at,
            "payload": dict(self.payload),
        }


@dataclass(slots=True)
class LedgerSummary:
    """Mechanical counts only; no classifier or verdict."""

    discovered: int = 0
    fetched: int = 0
    merged: int = 0
    failed: int = 0
    skipped: int = 0
    pending: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "discovered": self.discovered,
            "fetched": self.fetched,
            "merged": self.merged,
            "failed": self.failed,
            "skipped": self.skipped,
            "pending": self.pending,
        }


class DiscoveryLedgerStore:
    """File-backed discovery ledger using atomic tmp-then-replace writes."""

    def __init__(self, root: str | os.PathLike[str]) -> None:
        self._root = Path(root)

    def write_window(
        self,
        *,
        source: str,
        window_key: str,
        run_id: str,
        items: Iterable[DiscoveredItem],
    ) -> LedgerSummary:
        item_list = list(items)
        folder = self._root / source / window_key
        folder.mkdir(parents=True, exist_ok=True)

        jsonl = "\n".join(
            json.dumps(item.as_dict(), sort_keys=True, ensure_ascii=False)
            for item in item_list
        )
        if jsonl:
            jsonl += "\n"

        summary = self._summary(item_list)
        self._atomic_write(folder / f"{run_id}.jsonl", jsonl.encode())
        self._atomic_write(
            folder / "summary.json",
            json.dumps(summary.as_dict(), sort_keys=True, indent=2).encode(),
        )
        return summary

    @staticmethod
    def _summary(items: list[DiscoveredItem]) -> LedgerSummary:
        summary = LedgerSummary(discovered=len(items))
        for item in items:
            if item.status in {"fetched", "merged", "failed", "skipped", "pending"}:
                setattr(summary, item.status, getattr(summary, item.status) + 1)
        return summary

    @staticmethod
    def _atomic_write(path: str | os.PathLike[str], data: bytes) -> None:
        target = Path(path)
        tmp = target.with_name(f"{target.name}.{os.getpid()}.tmp")
        try:
            with tmp.open("wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                tmp.unlink()
