"""Completeness report: per-layer counts from an ingest run."""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["CompletenessReport"]


@dataclass
class CompletenessReport:
    """Aggregate counts produced by one ingest run, per consumer-named layer."""

    layers: dict[str, int] = field(default_factory=dict)
    present: int = 0
    truth: int = 0

    @property
    def ratio(self) -> float | None:
        if self.truth == 0:
            return None
        return self.present / self.truth

    def as_dict(self) -> dict:
        return {
            "layers": dict(self.layers),
            "present": self.present,
            "truth": self.truth,
            "ratio": self.ratio,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CompletenessReport":
        return cls(
            layers=d.get("layers", {}),
            present=d.get("present", 0),
            truth=d.get("truth", 0),
        )
