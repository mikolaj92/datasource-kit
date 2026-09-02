"""Consumer-driven fleet pass host."""
from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

from .state import acquire_lock, release_lock

T = TypeVar("T")
R = TypeVar("R")
A = TypeVar("A")

@dataclass(slots=True, frozen=True)
class FleetPass(Generic[R, A]):  # noqa: UP046 - Python 3.12 floor
    """Opaque result of one admitted, ordered fleet pass."""

    admission: A
    results: tuple[R, ...]

class FleetHost(Generic[T, R, A]):  # noqa: UP046 - Python 3.12 floor
    """Run consumer callbacks over a fixed, ordered fleet under one lock.

    The host interprets none of the unit, admission, or reconciliation values.
    Admission runs first on every pass and exceptions propagate unchanged, so a
    failed admission reconciles no units.  ``reconcile_pass`` is deliberately
    unlocked; ``run_once`` and ``serve`` provide the two lock lifetimes.
    """

    def __init__(
        self,
        *,
        lock_path: str | Path,
        units: Iterable[T],
        admit: Callable[[tuple[T, ...]], A],
        reconcile: Callable[[T], R],
        lock_payload: Callable[[], Mapping[str, Any]] | None = None,
        sleep: Callable[[float], object] = time.sleep,
    ) -> None:
        self.lock_path = Path(lock_path)
        self.units = tuple(units)
        self._admit = admit
        self._reconcile = reconcile
        self._lock_payload = lock_payload
        self._sleep = sleep

    def reconcile_pass(self) -> FleetPass[R, A]:
        """Admit and reconcile the fixed fleet in order, without locking."""
        admission = self._admit(self.units)
        results = tuple(self._reconcile(unit) for unit in self.units)
        return FleetPass(admission=admission, results=results)

    def _acquire(self) -> None:
        payload = self._lock_payload() if self._lock_payload is not None else None
        acquire_lock(self.lock_path, payload=payload)

    def run_once(self) -> FleetPass[R, A]:
        """Acquire once, run one pass, and always release the lock."""
        self._acquire()
        try:
            return self.reconcile_pass()
        finally:
            release_lock(self.lock_path)

    def serve(
        self,
        *,
        interval: float,
        on_pass: Callable[[FleetPass[R, A]], None] | None = None,
        stop_condition: Callable[[], bool] | None = None,
    ) -> None:
        """Hold one lock while running passes until the consumer stops it.

        A successful pass is reported through ``on_pass`` before the stop check
        and sleep.  Admission is repeated for every pass.  Every exception is
        propagated and releases the lock; there is no per-unit best effort.
        """
        if interval <= 0:
            raise ValueError("interval must be positive")
        self._acquire()
        try:
            while True:
                completed = self.reconcile_pass()
                if on_pass is not None:
                    on_pass(completed)
                if stop_condition is not None and stop_condition():
                    return
                self._sleep(interval)
        finally:
            release_lock(self.lock_path)
