"""Desired-state persistence, locks, and reconciliation."""
from __future__ import annotations

import json
import os
import socket
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .lock import unit_lock as _unit_lock
from .process import (
    ProcessSpec,
    ProcessTombstoneError,
    SpawnResult,
    StopResult,
    _pid_alive,
    _pid_path,
    _validate_unit,
    read_json,
    spawn,
    stop,
    write_json_atomic,
)

_STATE_FILE = "state.json"
_LOCK_FILE = "supervisor.lock"
_STATE_SCHEMA_VERSION = 1

#: Desired-state vocabulary persisted in ``state.json``.
DESIRED_ENABLED = "enabled"
DESIRED_DISABLED = "disabled"
#: Warm keep-alive desired state.  Like :data:`DESIRED_ENABLED`, the reconciler
#: wants a paused unit's process *running* -- it spawns one that is down
#: (reboot-warm: a paused unit comes back up after a supervisor restart) and
#: never kills one that is up (only :data:`DESIRED_DISABLED` stops a process).
#: The difference from ``enabled`` is purely the carried label: the consumer's
#: own drive loop reads ``paused`` and idles (holds no lease, acquires nothing),
#: keeping the process warm for instant resume.  Domain-blind: the kit only
#: keeps the process alive and carries the opaque label; what "paused" means for
#: the loop is the consumer's decision.
DESIRED_PAUSED = "paused"

#: State keys the reconciler owns.  ``merge_heartbeat`` refuses to let a worker
#: heartbeat overwrite any of these, and :class:`WorkerControlPlane` treats
#: every *other* key a worker merged into its state as opaque heartbeat payload
#: -- the kit never interprets it, staying health-blind.
_CORE_STATE_KEYS = frozenset(
    {"schema_version", "unit", "desired", "actual", "generation", "pid"}
)


class SupervisorLockError(RuntimeError):
    """Raised when the exclusive supervisor lock is held by a live owner."""

@dataclass(slots=True, frozen=True)
class ReconcileOutcome:
    """Result of converging a single unit toward its desired state."""

    unit: str
    action: str  # "spawned", "stopped", or "noop"
    desired: str  # "enabled", "disabled", or "paused"
    actual: str  # "running" or "stopped"
    generation: int
    pid: int | None
    alive: bool
    degraded_reason: str | None = None

def lock_is_live(owner: Mapping[str, Any] | None) -> bool:
    """Whether an existing lock is held by a live, foreign owner.

    ``None`` (corrupt/empty) and this process's own stale lock are reclaimable.
    A lock owned by a different host cannot be liveness-probed here, so it fails
    closed (treated as held).
    """
    if owner is None:
        return False  # corrupt / empty -> stealable
    pid = owner.get("pid")
    if not isinstance(pid, int) or pid == os.getpid():
        return False  # our own stale lock -> reclaimable
    if owner.get("hostname") != socket.gethostname():
        # Different host: we cannot verify liveness -> fail closed (held).
        return True
    return _pid_alive(pid)

def acquire_lock(
    path: str | Path, *, payload: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Acquire the exclusive lock at *path*, stealing a dead owner's.

    *payload* is written into the lock body verbatim, except ``pid`` and
    ``hostname``, which this function always stamps itself (a caller cannot
    override them, so the dead-owner-steal logic stays sound); ``started_at``
    defaults to the current time when the caller omits it.  Returns the written
    body.  Raises :class:`SupervisorLockError` when a live, foreign owner
    already holds the lock.
    """
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    body = dict(payload or {})
    body.setdefault("started_at", time.time())
    body["pid"] = os.getpid()
    body["hostname"] = socket.gethostname()
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            owner = read_json(lock_path)
            if lock_is_live(owner):
                raise SupervisorLockError(
                    f"supervisor lock {lock_path} held by live pid "
                    f"{owner.get('pid') if owner else '?'}"
                )
            # Dead / corrupt / our own stale owner -> steal and retry.
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(body, handle)
        return body

def release_lock(path: str | Path) -> bool:
    """Release the lock at *path* iff this process owns it.

    Returns whether the lock was freed.
    """
    lock_path = Path(path)
    owner = read_json(lock_path)
    if owner is None or owner.get("pid") != os.getpid():
        return False
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return False
    return True

ReconcilePolicy = Callable[[Mapping[str, Any]], bool]

#: Injected spawn action: ``(spec, generation, unit_dir) -> SpawnResult``.
#: The reconciler owns the generation counter and hands the new generation to
#: the action, which is responsible for making the child aware of it (the
#: default injects :data:`GENERATION_ENV`).
SpawnAction = Callable[["ProcessSpec", int, Path], SpawnResult]

#: Injected stop action: ``(unit_dir) -> StopResult``.
StopAction = Callable[[Path], StopResult]


def honor_desired_state(state: Mapping[str, Any]) -> bool:
    """Default policy: run a unit iff its desired state is enabled or paused.

    ``paused`` is a *warm* keep-alive (:data:`DESIRED_PAUSED`): its process
    should be running-but-idle, so the policy wants it running exactly like
    ``enabled``.  The two differ only in the label the consumer's drive loop
    reads; only :data:`DESIRED_DISABLED` keeps a unit stopped.
    """
    return state.get("desired") in (DESIRED_ENABLED, DESIRED_PAUSED)

def _default_spawn_action(
    spec: ProcessSpec, generation: int, unit_dir: Path
) -> SpawnResult:
    """Spawn via :func:`spawn`, injecting the generation into the child env."""
    return spawn(spec, unit_dir=unit_dir, generation=generation)

def _default_stop_action(unit_dir: Path) -> StopResult:
    """Stop via :func:`stop`."""
    return stop(unit_dir)

class DesiredStateReconciler:
    """Converge a fleet of units toward a consumer-declared desired state.

    State layout under *root*::

        <root>/<unit>/state.json   # desired/actual/generation, atomic
        <root>/<unit>/pid.json     # written by the spawn action
        <root>/supervisor.lock     # exclusive supervisor lock

    The reconciler owns the generic mechanics (atomic state files, generation
    fencing, the supervisor lock with dead-owner steal, the converge loop) and
    delegates process launch/stop to injected actions so a consumer can keep a
    richer launch (log redirection, its own env, richer pid metadata) while the
    kit stays domain-blind.  The defaults wrap :func:`spawn`/:func:`stop`.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        spawn_action: SpawnAction | None = None,
        stop_action: StopAction | None = None,
        schema_version: int = _STATE_SCHEMA_VERSION,
    ) -> None:
        self._root = Path(root)
        self._spawn = spawn_action or _default_spawn_action
        self._stop = stop_action or _default_stop_action
        self._schema_version = schema_version

    # -- paths -------------------------------------------------------------

    @property
    def root(self) -> Path:
        return self._root

    def unit_dir(self, unit: str) -> Path:
        return self._root / _validate_unit(unit)

    def state_file(self, unit: str) -> Path:
        return self.unit_dir(unit) / _STATE_FILE

    def _lock_path(self) -> Path:
        return self._root / _LOCK_FILE

    # -- state -------------------------------------------------------------

    def _default_state(self, unit: str) -> dict[str, Any]:
        return {
            "schema_version": self._schema_version,
            "unit": unit,
            "desired": DESIRED_DISABLED,
            "actual": "unknown",
            "generation": 0,
            "pid": None,
        }

    def load_state(self, unit: str) -> dict[str, Any]:
        """Load a unit's persisted state, backfilling missing core keys."""
        stored = read_json(self.state_file(unit))
        state = self._default_state(unit)
        if stored is not None:
            state.update(stored)
        state["unit"] = unit
        return state

    def _save_state(self, state: Mapping[str, Any]) -> None:
        write_json_atomic(self.state_file(str(state["unit"])), state)

    def set_desired(self, unit: str, desired: str) -> dict[str, Any]:
        """Persist a unit's desired state (``enabled``/``disabled``/``paused``)."""
        if desired not in (DESIRED_ENABLED, DESIRED_DISABLED, DESIRED_PAUSED):
            raise ValueError(
                f"desired must be one of {DESIRED_ENABLED!r}, "
                f"{DESIRED_DISABLED!r}, {DESIRED_PAUSED!r}, got {desired!r}"
            )
        state = self.load_state(unit)
        state["desired"] = desired
        self._save_state(state)
        return state

    def enable(self, unit: str) -> dict[str, Any]:
        return self.set_desired(unit, DESIRED_ENABLED)

    def disable(self, unit: str) -> dict[str, Any]:
        return self.set_desired(unit, DESIRED_DISABLED)

    def pause(self, unit: str) -> dict[str, Any]:
        """Warm keep-alive: keep the process running-but-idle (reboot-warm).

        The reconciler wants a paused unit running just like ``enabled`` (it
        spawns one that is down, never kills one that is up); the consumer's
        drive loop reads ``paused`` and idles.
        """
        return self.set_desired(unit, DESIRED_PAUSED)

    def merge_heartbeat(
        self, unit: str, heartbeat: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Merge a worker heartbeat into state under generation fencing.

        A heartbeat is accepted only when its ``generation`` matches the
        current one, so a zombie from a previous generation can never overwrite
        current state.  The reconciler-owned core keys (:data:`_CORE_STATE_KEYS`
        -- ``schema_version``, ``unit``, ``desired``, ``actual``, ``generation``,
        ``pid``) are never taken from a heartbeat: a worker cannot forge its own
        liveness (``actual``/``pid``) or reassign its desired state.  Every other
        key is stored verbatim as opaque heartbeat payload.
        """
        state = self.load_state(unit)
        reported = heartbeat.get("generation")
        if not isinstance(reported, int) or reported != int(state["generation"]):
            return state
        merged = dict(state)
        for key, value in heartbeat.items():
            if key in _CORE_STATE_KEYS:
                continue
            merged[key] = value
        self._save_state(merged)
        return merged

    def observe_actual(self, unit: str) -> str:
        """Observe conservatively; every tombstone means running-or-unknown."""
        path = _pid_path(self.unit_dir(unit))
        try:
            os.lstat(path)
        except FileNotFoundError:
            return "stopped"
        except OSError:
            return "unknown"
        return "running"  # never use PID liveness to authorize replacement

    # -- reconcile ---------------------------------------------------------

    def reconcile_unit(self, spec: ProcessSpec, policy: ReconcilePolicy) -> ReconcileOutcome:
        """Reconcile without restart/adoption; tombstones permanently fail closed."""
        unit = spec.unit
        with _unit_lock(self.unit_dir(unit)):
            state = self.load_state(unit)
            want_running = policy(state)
            actual = self.observe_actual(unit)
            state["actual"] = actual
            reason: str | None = None
            if actual != "stopped":
                reason = "process_tombstone_operator_verification_required"
                state["degraded_reason"] = reason
                state["operator_verification_required"] = True
                if not want_running:
                    try:
                        outcome = stop(self.unit_dir(unit))
                        if outcome.signalled:
                            state["stop_requested"] = True
                    except ProcessTombstoneError:
                        pass
                self._save_state(state)
                pid = state.get("pid")
                return ReconcileOutcome(unit, "noop", str(state["desired"]),
                    actual, int(state["generation"]), pid if isinstance(pid, int) else None,
                    actual == "running", reason)
            if want_running:
                generation = int(state["generation"]) + 1
                try:
                    result = self._spawn(spec, generation, self.unit_dir(unit))
                except ProcessTombstoneError:
                    reason = "process_tombstone_operator_verification_required"
                    state.update(actual="unknown", degraded_reason=reason,
                                 operator_verification_required=True)
                    self._save_state(state)
                    return ReconcileOutcome(unit, "noop", str(state["desired"]),
                        "unknown", int(state["generation"]), None, False, reason)
                # Metadata is durable before exec; never erase it on early exit.
                state.update(generation=generation, pid=result.pid,
                             actual="running" if result.alive else "unknown",
                             process_token=result.token,
                             operator_verification_required=not result.alive)
                if not result.alive:
                    reason = "leader_exited_operator_verification_required"
                    state["degraded_reason"] = reason
                self._save_state(state)
                return ReconcileOutcome(unit, "spawned", str(state["desired"]),
                    str(state["actual"]), generation, result.pid, result.alive, reason)
            # Disable with no provenance is only a state label; it deletes nothing.
            state["actual"] = "stopped"
            self._save_state(state)
            return ReconcileOutcome(unit, "noop", str(state["desired"]),
                "stopped", int(state["generation"]), None, False)

    def _reconcile_all(
        self, specs: Iterable[ProcessSpec], policy: ReconcilePolicy
    ) -> list[ReconcileOutcome]:
        return [self.reconcile_unit(spec, policy) for spec in specs]

    def reconcile_once(
        self, specs: Iterable[ProcessSpec], policy: ReconcilePolicy
    ) -> list[ReconcileOutcome]:
        """Acquire the supervisor lock, converge every unit once, release."""
        with self.hold_lock():
            return self._reconcile_all(specs, policy)

    def serve(
        self,
        specs: Iterable[ProcessSpec],
        policy: ReconcilePolicy,
        *,
        interval: float = 5.0,
        stop_condition: Callable[[], bool] | None = None,
    ) -> None:
        """Hold the lock and reconcile on a fixed interval until stopped.

        *specs* is materialized once so the same fleet is reconciled each pass.
        *stop_condition*, when supplied, is checked after each pass; returning
        ``True`` ends the loop (primarily a test / graceful-shutdown seam).
        """
        fleet = list(specs)
        with self.hold_lock():
            while True:
                self._reconcile_all(fleet, policy)
                if stop_condition is not None and stop_condition():
                    return
                time.sleep(interval)

    # -- supervisor lock ---------------------------------------------------

    def _lock_is_live(self, owner: dict[str, Any] | None) -> bool:
        """Whether an existing lock is held by a live, foreign owner."""
        return lock_is_live(owner)

    def acquire_lock(self) -> dict[str, Any]:
        """Acquire the exclusive supervisor lock, stealing a dead owner's.

        Raises :class:`SupervisorLockError` when a live, foreign supervisor
        already holds the lock.
        """
        return acquire_lock(self._lock_path())

    def release_lock(self) -> bool:
        """Release the lock iff this process owns it.  Returns whether freed."""
        return release_lock(self._lock_path())

    @contextmanager
    def hold_lock(self) -> Iterator[None]:
        """Context manager wrapping :meth:`acquire_lock`/:meth:`release_lock`."""
        self.acquire_lock()
        try:
            yield
        finally:
            self.release_lock()
