"""Aggregate fleet observation and desired-state controls."""
from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .process import _pid_path, liveness
from .state import (
    _CORE_STATE_KEYS,
    DesiredStateReconciler,
)


@dataclass(slots=True, frozen=True)
class UnitObservation:
    """A single unit's control-plane snapshot.

    ``heartbeat`` carries every non-core key a worker merged into its state as
    an opaque payload: the kit never interprets it, keeping the control plane
    health-blind.  ``pid`` is reported only while the unit is observed running.
    """

    unit: str
    desired: str
    actual: str
    generation: int
    pid: int | None
    heartbeat: Mapping[str, Any]

class WorkerControlPlane:
    """Aggregate observe / pause / resume surface over a declared fleet.

    Wraps a :class:`DesiredStateReconciler` and a fixed tuple of unit ids to
    give a consumer -- or an adapter such as the optional FastAPI router -- one
    place to read fleet state and flip desired states.  ``pause`` writes the
    warm keep-alive :data:`DESIRED_PAUSED` state (the reconciler keeps the
    process running -- spawning it if down, never killing it -- while the
    consumer's drive loop idles); ``resume`` writes :data:`DESIRED_ENABLED`.

    It stays domain-blind and health-blind: unit ids only, heartbeat passed
    through opaque.  Unit-scoped calls fail closed with :class:`KeyError` for a
    unit outside the declared fleet.
    """

    def __init__(
        self, reconciler: DesiredStateReconciler, units: Iterable[str]
    ) -> None:
        self._reconciler = reconciler
        self._units = tuple(units)

    @property
    def units(self) -> tuple[str, ...]:
        return self._units

    def _require(self, unit: str) -> None:
        if unit not in self._units:
            raise KeyError(unit)

    def observe_unit(self, unit: str) -> UnitObservation:
        """Snapshot a single declared unit (fails closed on unknown units).

        This is a **read-only** view: it conservatively observes the presence of
        ``pid.json`` and never cleans a stale pid file.  Pruning stale metadata
        is the supervisor's job, done under the supervisor lock during reconcile
        -- an out-of-process observer (e.g. the FastAPI adapter) must not race
        it by unlinking a pid file the supervisor may have just refreshed.  The
        live pid from ``pid.json`` is authoritative for the view, not the
        possibly-lagging ``state["pid"]`` record.
        """
        self._require(unit)
        state = self._reconciler.load_state(unit)
        path = _pid_path(self._reconciler.unit_dir(unit))
        try:
            os.lstat(path)
        except FileNotFoundError:
            actual, pid = "stopped", None
        except OSError:
            actual, pid = "unknown", None
        else:
            # A tombstone never proves that the whole workload is gone. Expose
            # a live leader when observable, otherwise preserve uncertainty.
            try:
                live = liveness(self._reconciler.unit_dir(unit))
            except (FileNotFoundError, OSError):
                live = None
            if live is not None and live.state == "running":
                actual, pid = "running", live.pid
            else:
                actual, pid = "unknown", None
        heartbeat = {
            key: value for key, value in state.items() if key not in _CORE_STATE_KEYS
        }
        return UnitObservation(
            unit=unit,
            desired=str(state["desired"]),
            actual=actual,
            generation=int(state["generation"]),
            pid=pid,
            heartbeat=heartbeat,
        )

    def observe(self) -> list[UnitObservation]:
        """Whole-fleet snapshot in declared order."""
        return [self.observe_unit(unit) for unit in self._units]

    def pause(self, unit: str) -> dict[str, Any]:
        """Warm-pause a unit: write the keep-alive :data:`DESIRED_PAUSED` state."""
        self._require(unit)
        return self._reconciler.pause(unit)

    def resume(self, unit: str) -> dict[str, Any]:
        """Resume a unit: write :data:`DESIRED_ENABLED`."""
        self._require(unit)
        return self._reconciler.enable(unit)

    def enable(self, unit: str) -> dict[str, Any]:
        """Enable a unit (alias of :meth:`resume`, for vocabulary parity)."""
        self._require(unit)
        return self._reconciler.enable(unit)

    def disable(self, unit: str) -> dict[str, Any]:
        """Disable a unit: write :data:`DESIRED_DISABLED` (stop on reconcile)."""
        self._require(unit)
        return self._reconciler.disable(unit)
