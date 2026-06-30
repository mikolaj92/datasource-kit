"""Completeness report: consumer-named counting buckets."""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import ValidationError

__all__ = ["CompletenessReport", "LayerCoverage", "layers_from_names"]


@dataclass(slots=True)
class LayerCoverage:
    """Counts for one consumer-named layer."""

    layer: str
    truth_count: int = 0
    present_count: int = 0

    @property
    def missing_count(self) -> int:
        return max(self.truth_count - self.present_count, 0)

    def as_dict(self) -> dict[str, int | str]:
        return {
            "layer": self.layer,
            "truth_count": self.truth_count,
            "present_count": self.present_count,
            "missing_count": self.missing_count,
        }


@dataclass(slots=True)
class CompletenessReport:
    """Aggregate counts produced by one ingest run.

    ``layers`` names come from the consumer/profile. ``present`` and ``truth``
    remain as compatibility fields for the existing CLI/report output.
    """

    layers: dict[str, LayerCoverage] = field(default_factory=dict)
    present: int = 0
    truth: int = 0

    def __post_init__(self) -> None:
        normalized: dict[str, LayerCoverage] = {}
        for name, value in self.layers.items():
            if isinstance(value, LayerCoverage):
                normalized[name] = value
            elif isinstance(value, int):
                normalized[name] = LayerCoverage(
                    layer=name,
                    truth_count=value,
                    present_count=value,
                )
            elif isinstance(value, dict):
                normalized[name] = LayerCoverage(
                    layer=str(value.get("layer", name)),
                    truth_count=int(value.get("truth_count", value.get("truth", 0))),
                    present_count=int(
                        value.get("present_count", value.get("present", 0))
                    ),
                )
            else:
                raise ValidationError(f"invalid layer coverage for {name!r}")
        self.layers = normalized

    @property
    def ratio(self) -> float | None:
        if self.truth == 0:
            return None
        return self.present / self.truth

    def fraction(self, layer: str) -> float:
        if layer not in self.layers:
            raise ValidationError(f"unknown completeness layer: {layer}")
        coverage = self.layers[layer]
        if coverage.truth_count == 0:
            return 0.0
        return coverage.present_count / coverage.truth_count

    def as_dict(self) -> dict:
        return {
            "layers": {name: value.as_dict() for name, value in self.layers.items()},
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


def layers_from_names(names: list[str] | tuple[str, ...]) -> dict[str, LayerCoverage]:
    """Build zeroed coverage buckets from consumer-supplied names."""

    return {name: LayerCoverage(layer=name) for name in names}
