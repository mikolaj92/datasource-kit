"""Declarative descriptors for a datasource: *what* it is, not *how* it runs.

These dataclasses let a project describe each source as data (source of truth,
identity strategy, rate limits, ...) instead of scattering the same facts across
imperative code. Generalized from the contract/manifest shapes used by the
Temida/hermes scraper workers, but free of any legal-domain coupling.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["EXECUTION_AUTONOMOUS", "ExecutionModel", "SourceContract", "Manifest"]

#: Recognized execution model that makes a datasource a long-lived autonomous
#: worker. It is the one value the manifest ties to the contract requirement;
#: any other model string is consumer-defined and carries no built-in meaning.
EXECUTION_AUTONOMOUS = "autonomous"


@dataclass(slots=True, frozen=True)
class ExecutionModel:
    """How a datasource runs: its execution model plus an optional step ref.

    ``model`` is a consumer-chosen label (e.g. ``"autonomous"`` for a long-lived
    worker process, ``"batch"`` for a one-shot updater). ``step_ref`` is an
    optional dotted reference to the callable/module a runtime should drive for
    this source; the kit stores it verbatim and never imports or resolves it.
    """

    model: str
    step_ref: str = ""

    def __post_init__(self) -> None:
        if not self.model:
            raise ValueError("ExecutionModel.model must be a non-empty label")


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

    ``execution`` is the first-class way to state how the source runs (its
    :class:`ExecutionModel`). The older boolean ``supports_autonomous`` is kept
    for backward compatibility but is superseded by
    ``execution=ExecutionModel("autonomous", ...)``; :attr:`is_autonomous`
    honours either signal, and the contract requirement applies to both.
    """

    name: str
    source_type: str
    priority: int = 50
    jurisdiction: str = ""
    implementation_status: str = "active"
    supports_autonomous: bool = False
    rate_limit: dict[str, float] = field(default_factory=dict)
    contract: SourceContract | None = None
    execution: ExecutionModel | None = None

    def __post_init__(self) -> None:
        if self.is_autonomous and self.contract is None:
            raise ValueError(
                f"Autonomous datasource '{self.name}' must declare a contract"
            )

    @property
    def is_autonomous(self) -> bool:
        """True if the source runs as an autonomous worker, by either signal.

        Honours both the legacy ``supports_autonomous`` boolean and a first-class
        ``execution`` whose model is :data:`EXECUTION_AUTONOMOUS`.
        """

        if self.supports_autonomous:
            return True
        return self.execution is not None and self.execution.model == EXECUTION_AUTONOMOUS

    @property
    def execution_model(self) -> str:
        """The declared execution-model label, or ``""`` if none is set."""

        return self.execution.model if self.execution is not None else ""
