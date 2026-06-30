"""DiscoveryLedger and Evidence primitive.

Counts only — no count-to-verdict classifier, no lifecycle_state.
Consumer names completeness buckets; the kit carries no default taxonomy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

__all__ = ["Evidence", "DiscoveryLedger"]


@dataclass(slots=True)
class Evidence:
    """A single measurement captured during a run.

    ``label`` is a consumer-chosen bucket name (e.g. "fetched", "persisted").
    ``count`` is how many items were seen. ``meta`` holds any extra data.
    """

    label: str
    count: int
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveryLedger:
    """Accumulates :class:`Evidence` entries produced during an ingest run.

    Only tracks counts. The consumer decides what the counts mean.
    """

    entries: list[Evidence] = field(default_factory=list)

    def record(self, label: str, count: int, **meta: Any) -> None:
        self.entries.append(Evidence(label=label, count=count, meta=meta))

    def totals(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for e in self.entries:
            out[e.label] = out.get(e.label, 0) + e.count
        return out

    def __len__(self) -> int:
        return len(self.entries)
