"""Declarative descriptors for a datasource: *what* it is, not *how* it runs.

These dataclasses let a project describe each source as data (source of truth,
identity strategy, rate limits, ...) instead of scattering the same facts across
imperative code. Generalized from the contract/manifest shapes used by the
Temida/hermes scraper workers, but free of any legal-domain coupling.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["SourceContract", "Manifest"]


@dataclass(slots=True)
class SourceContract:
    """The operating contract for an autonomous (scraper) datasource.

    Captures the durable facts an operator needs to reason about coverage and
    correctness without inspecting runtime state.
    """

    source_truth: str
    enumeration_method: str
    evidence: tuple[str, ...]
    identity_strategy: str
    diff_target: str
    coverage_unit: str = "source_defined"
    notes: str = ""


@dataclass(slots=True)
class Manifest:
    """Static description of a datasource.

    ``rate_limit`` is a free-form mapping (e.g. ``{"rps": 1.0, "burst": 2.0}``)
    so batch sources can leave it empty and scraper sources can populate it.
    An autonomous source must declare a :class:`SourceContract`.
    """

    name: str
    source_type: str
    priority: int = 50
    jurisdiction: str = ""
    implementation_status: str = "active"
    supports_autonomous: bool = False
    rate_limit: dict[str, float] = field(default_factory=dict)
    contract: SourceContract | None = None

    def __post_init__(self) -> None:
        if self.supports_autonomous and self.contract is None:
            raise ValueError(
                f"Autonomous datasource '{self.name}' must declare a contract"
            )
