"""Domain-blind fleet-process supervision primitives.

A consumer runs a fleet of long-lived source-worker OS processes
(one per datasource unit) and needs generic, stdlib-only process
supervision.  This module provides those primitives.

Boundaries
----------
- No scheduler, no cron, no daemon: these are primitives;
  policy stays in the consumer.
- No knowledge of what the worker does -- the kit never sees
  consumer vocabulary, storage, or business state.
- POSIX only (uses ``os.kill``, ``signal``, ``start_new_session``).
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import IO, Any, Callable, Iterable, Iterator, Mapping, Sequence

__all__ = [
    "Liveness",
    "ProcessSpec",
    "SpawnResult",
    "StopOutcome",
    "StopResult",
    "liveness",
    "spawn",
    "spawn_process",
    "stop",
    "stop_process",
    # DesiredStateReconciler face
    "DesiredStateReconciler",
    "ReconcileOutcome",
    "ReconcilePolicy",
    "SpawnAction",
    "StopAction",
    "SupervisorLockError",
    "acquire_lock",
    "honor_desired_state",
    "lock_is_live",
    "read_json",
    "release_lock",
    "write_json_atomic",
    "DESIRED_ENABLED",
    "DESIRED_DISABLED",
    "DESIRED_HELD",
    "GENERATION_ENV",
    # WorkerControlPlane face
    "WorkerControlPlane",
    "UnitObservation",
]

# ---------------------------------------------------------------------------
# Pure-data shapes
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class ProcessSpec:
    """Declarative description of a worker process to spawn.

    Parameters
    ----------
    unit:
        Opaque unit identifier (typically a datasource name, consumer-chosen).
    command:
        Executable path and arguments (passed to ``subprocess.Popen``).
    cwd:
        Working directory for the child process, or ``None`` to inherit.
    env:
        Environment variables for the child, or ``None`` to inherit.
    label:
        Human-readable label for logging / dashboards (consumer-chosen).
    """

    unit: str
    command: tuple[str, ...]
    cwd: str | None = None
    env: dict[str, str] | None = None
    label: str = ""


@dataclass(slots=True, frozen=True)
class SpawnResult:
    """Outcome of a :func:`spawn` call."""

    pid: int
    started_at: float
    alive: bool


@dataclass(slots=True, frozen=True)
class StopResult:
    """Outcome of a :func:`stop` call."""

    pid: int
    signalled: bool
    killed: bool
    cleaned: bool


@dataclass(slots=True, frozen=True)
class StopOutcome:
    """Outcome of a :func:`stop_process` call.

    Layout-agnostic sibling of :class:`StopResult`: it carries no ``cleaned``
    flag because :func:`stop_process` performs no pid-file I/O -- pid metadata
    is the caller's concern.
    """

    pid: int
    signalled: bool
    killed: bool


@dataclass(slots=True, frozen=True)
class Liveness:
    """Process liveness state from :func:`liveness`."""

    pid: int
    state: str  # "running", "stopped", or "stale"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PID_FILE = "pid.json"
_PROBE_SLEEP = 0.25  # seconds to wait before the first exit-probe
_PROBE_WINDOW = 1.5  # total seconds for the immediate-exit probe window
_STOP_SIGNAL = signal.SIGTERM
_KILL_SIGNAL = signal.SIGKILL
_DEFAULT_TIMEOUT = 5.0  # seconds before escalating to SIGKILL


def _pid_path(unit_dir: str | Path) -> Path:
    return Path(unit_dir) / _PID_FILE


def _read_pid(unit_dir: str | Path) -> dict[str, Any] | None:
    """Read *pid.json* from *unit_dir*, return parsed dict or ``None``."""
    path = _pid_path(unit_dir)
    try:
        raw = path.read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError):
        return None
    try:
        data: dict[str, Any] = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data


def _write_pid(unit_dir: str | Path, pid: int, command: tuple[str, ...], label: str) -> None:
    """Atomically write *pid.json* to *unit_dir*."""
    path = _pid_path(unit_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": pid,
        "command": list(command),
        "started_at": time.time(),
        "label": label,
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def _clean_pid(unit_dir: str | Path) -> bool:
    """Remove *pid.json* from *unit_dir*.  Returns ``True`` if removed."""
    path = _pid_path(unit_dir)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return False
    else:
        return True


def _pid_alive(pid: int) -> bool:
    """Return ``True`` if *pid* refers to a live process owned by us."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but owned by another user -- treat as alive
        # (the consumer's *pid.json* is our provenance, not a correctness
        # guarantee about ownership).
        return True
    except OSError:
        return False
    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def spawn_process(
    command: Sequence[str],
    *,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    stdout: int | IO[str] | None = None,
    stderr: int | IO[str] | None = None,
    probe_window: float = _PROBE_WINDOW,
    probe_sleep: float = _PROBE_SLEEP,
) -> SpawnResult:
    """Spawn a detached child process and probe for immediate exit.

    Layout-agnostic core of :func:`spawn`: it starts the process in its own
    session (so a later :func:`stop_process` can signal the whole group), runs
    the fail-closed immediate-exit probe, and returns the outcome. It writes
    NO ``pid.json`` -- pid metadata and its on-disk layout are the caller's
    concern.

    Parameters
    ----------
    command:
        Executable path and arguments (passed to ``subprocess.Popen``).
    cwd:
        Working directory for the child, or ``None`` to inherit.
    env:
        Environment for the child, or ``None`` to inherit the parent's.
    stdout, stderr:
        Destinations for the child's streams -- a file descriptor, an open
        file object, or ``None`` (defaults to ``subprocess.DEVNULL``). Lets a
        consumer redirect the child to its own log without the kit owning
        log paths.
    probe_window:
        Total seconds to watch for an immediate exit.
    probe_sleep:
        Seconds to sleep between exit probes.

    Returns
    -------
    A :class:`SpawnResult`. ``alive=False`` means the child exited within the
    probe window (a failed spawn), never "running and then crashed".
    """
    child_env = dict(os.environ) if env is None else dict(env)
    resolved_out: int | IO[str] = subprocess.DEVNULL if stdout is None else stdout
    resolved_err: int | IO[str] = subprocess.DEVNULL if stderr is None else stderr

    proc = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=child_env,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=resolved_out,
        stderr=resolved_err,
    )

    started_at = time.time()
    deadline = started_at + probe_window
    alive = True
    while time.time() < deadline:
        ret = proc.poll()
        if ret is not None:
            alive = False
            break
        time.sleep(probe_sleep)

    return SpawnResult(pid=proc.pid, started_at=started_at, alive=alive)


def spawn(spec: ProcessSpec, *, unit_dir: str | Path | None = None) -> SpawnResult:
    """Start a worker process described by *spec*.

    Writes ``pid.json`` metadata atomically to *unit_dir* (or
    ``spec.unit`` as a directory name when *unit_dir* is ``None``).

    Delegates the raw spawn + immediate-exit probe to :func:`spawn_process`
    and only persists ``pid.json`` once the child survives the probe, so a
    failed spawn (``alive=False``) leaves no stale pid metadata behind.
    """
    resolved = Path(unit_dir) if unit_dir is not None else Path(spec.unit)
    resolved.mkdir(parents=True, exist_ok=True)

    result = spawn_process(spec.command, cwd=spec.cwd, env=spec.env)

    if result.alive:
        _write_pid(resolved, result.pid, spec.command, spec.label)

    return result


def stop_process(pid: int, *, timeout: float = _DEFAULT_TIMEOUT) -> StopOutcome:
    """Stop a process group identified by *pid*, escalating SIGTERM -> SIGKILL.

    Layout-agnostic core of :func:`stop`: it signals the process group, waits
    up to *timeout* seconds for graceful exit, then escalates to SIGKILL. It
    performs NO pid-file I/O -- reading the pid and cleaning up its on-disk
    record are the caller's concern.

    Returns a :class:`StopOutcome`:

    - ``signalled=False, killed=False`` -- the pid was already dead, or it
      vanished before SIGTERM could land (nothing to signal).
    - ``signalled=True, killed=False`` -- SIGTERM was delivered and the
      process exited within *timeout*.
    - ``signalled=True, killed=True`` -- SIGTERM did not suffice, so SIGKILL
      was delivered.
    """
    if not _pid_alive(pid):
        return StopOutcome(pid=pid, signalled=False, killed=False)

    try:
        os.killpg(os.getpgid(pid), _STOP_SIGNAL)
    except (ProcessLookupError, PermissionError, OSError):
        # Process already gone; nothing to signal.
        return StopOutcome(pid=pid, signalled=False, killed=False)

    # Wait for graceful exit.
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            return StopOutcome(pid=pid, signalled=True, killed=False)
        time.sleep(0.1)

    # Escalate to SIGKILL.
    killed = False
    try:
        os.killpg(os.getpgid(pid), _KILL_SIGNAL)
        killed = True
    except (ProcessLookupError, PermissionError, OSError):
        pass

    return StopOutcome(pid=pid, signalled=True, killed=killed)


def stop(
    unit_dir: str | Path,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> StopResult:
    """Stop a supervised process identified by *unit_dir*.

    Reads the pid from *unit_dir*'s ``pid.json``, delegates the SIGTERM ->
    SIGKILL escalation to :func:`stop_process`, then cleans up the pid file.
    A stale ``pid.json`` (referencing a dead or recycled pid) is cleaned up
    silently and reported via ``cleaned=True`` -- which mirrors the
    "nothing was signalled" outcome.
    """
    data = _read_pid(unit_dir)
    if data is None:
        raise FileNotFoundError(f"no pid.json in {unit_dir}")

    pid = int(data["pid"])

    outcome = stop_process(pid, timeout=timeout)

    _clean_pid(unit_dir)
    return StopResult(
        pid=outcome.pid,
        signalled=outcome.signalled,
        killed=outcome.killed,
        cleaned=not outcome.signalled,
    )


def liveness(unit_dir: str | Path) -> Liveness:
    """Check the liveness of a supervised process by *unit_dir*.

    Returns ``"running"``, ``"stopped"``, or ``"stale"``.
    ``"stale"`` means *pid.json* exists but the referenced pid is not alive,
    or the file is corrupt (a valid JSON object whose ``pid`` is missing or
    non-integer) -- either way the consumer should clean it up.  Raises
    :class:`FileNotFoundError` only when *pid.json* is absent, so an
    out-of-process observer can classify any surviving pid file without
    crashing on a torn or hand-edited one.
    """
    data = _read_pid(unit_dir)
    if data is None:
        raise FileNotFoundError(f"no pid.json in {unit_dir}")

    try:
        pid = int(data["pid"])
    except (KeyError, TypeError, ValueError):
        # Corrupt pid.json (missing or non-integer ``pid``): treat as stale
        # per the contract above.  There is no usable pid, so report 0 -- the
        # stale branch is never signalled, so the sentinel is never used.
        return Liveness(pid=0, state="stale")
    if _pid_alive(pid):
        return Liveness(pid=pid, state="running")

    # Stale: pid.json exists but process is gone.  The consumer
    # is expected to call ``stop()`` to clean up stale metadata.
    return Liveness(pid=pid, state="stale")


# ---------------------------------------------------------------------------
# DesiredStateReconciler
# ---------------------------------------------------------------------------
#
# A generic desired-state reconciler built on the process primitives above.
# The consumer declares which units should be running; a supervisor converges
# reality to that declaration without each consumer re-implementing the
# lock / generation-fencing / atomic-write mechanics.
#
# Boundaries: no health semantics beyond process liveness -- health
# interpretation stays in the consumer.  Stdlib-only.

_STATE_FILE = "state.json"
_LOCK_FILE = "supervisor.lock"
_STATE_SCHEMA_VERSION = 1

#: Desired-state vocabulary persisted in ``state.json``.
DESIRED_ENABLED = "enabled"
DESIRED_DISABLED = "disabled"
#: Keep-alive desired state: the reconciler neither spawns nor kills a held
#: unit -- a running process stays running (warm-parked by the consumer's own
#: drive loop), a stopped one stays stopped.  Domain-blind: what "held" means
#: for the loop is the consumer's decision; the kit only declines to change
#: process topology.
DESIRED_HELD = "held"

#: Environment variable the default spawn action injects into each child so a
#: worker can stamp its heartbeats with the generation it was spawned under.
#: Consumers that inject their own spawn action pick their own variable name.
GENERATION_ENV = "DATASOURCE_KIT_GENERATION"

#: State keys the reconciler owns.  ``merge_heartbeat`` refuses to let a worker
#: heartbeat overwrite any of these, and :class:`WorkerControlPlane` treats
#: every *other* key a worker merged into its state as opaque heartbeat payload
#: -- the kit never interprets it, staying health-blind.
_CORE_STATE_KEYS = frozenset(
    {"schema_version", "unit", "desired", "actual", "generation", "pid"}
)


class SupervisorLockError(RuntimeError):
    """Raised when the exclusive supervisor lock is held by a live owner."""


#: A policy is a consumer hook that decides, given a unit's persisted state,
#: whether that unit should be running.  The kit never decides which units run;
#: it converges to the consumer's declaration.  Return ``True`` to run the unit.
ReconcilePolicy = Callable[[Mapping[str, Any]], bool]

#: Injected spawn action: ``(spec, generation, unit_dir) -> SpawnResult``.
#: The reconciler owns the generation counter and hands the new generation to
#: the action, which is responsible for making the child aware of it (the
#: default injects :data:`GENERATION_ENV`).
SpawnAction = Callable[["ProcessSpec", int, Path], SpawnResult]

#: Injected stop action: ``(unit_dir) -> StopResult``.
StopAction = Callable[[Path], StopResult]


@dataclass(slots=True, frozen=True)
class ReconcileOutcome:
    """Result of converging a single unit toward its desired state."""

    unit: str
    action: str  # "spawned", "stopped", "held", or "noop"
    desired: str  # "enabled", "disabled", or "held"
    actual: str  # "running" or "stopped"
    generation: int
    pid: int | None
    alive: bool


def write_json_atomic(path: str | Path, payload: Mapping[str, Any]) -> None:
    """Atomically write *payload* as JSON to *path* (tmp file + ``replace``)."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    tmp.replace(target)


def read_json(path: str | Path) -> dict[str, Any] | None:
    """Read a JSON object from *path*; return ``None`` if absent or corrupt."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except (FileNotFoundError, PermissionError, IsADirectoryError):
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


# ---------------------------------------------------------------------------
# Standalone supervisor-lock primitive
# ---------------------------------------------------------------------------
#
# A consumer that drives its own converge loop (rather than the
# DesiredStateReconciler face) still needs the exact same exclusive
# single-supervisor lock: create-exclusive, steal a dead or corrupt owner,
# fail closed on a live foreign owner.  These module-level functions are that
# primitive; :class:`DesiredStateReconciler` delegates to them.  A consumer may
# pass its own ``payload`` (operator provenance, schema version, ...) which is
# written into the lock file verbatim except for ``pid``/``hostname``, which
# the primitive always stamps itself so the dead-owner-steal logic stays sound.


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


def honor_desired_state(state: Mapping[str, Any]) -> bool:
    """Default policy: run a unit iff its persisted desired state is enabled."""
    return state.get("desired") == DESIRED_ENABLED


def _default_spawn_action(
    spec: ProcessSpec, generation: int, unit_dir: Path
) -> SpawnResult:
    """Spawn via :func:`spawn`, injecting the generation into the child env."""
    env = dict(os.environ) if spec.env is None else dict(spec.env)
    env[GENERATION_ENV] = str(generation)
    return spawn(replace(spec, env=env), unit_dir=unit_dir)


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
        return self._root / unit

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
        """Persist a unit's desired state (``enabled``/``disabled``/``held``)."""
        if desired not in (DESIRED_ENABLED, DESIRED_DISABLED, DESIRED_HELD):
            raise ValueError(
                f"desired must be one of {DESIRED_ENABLED!r}, "
                f"{DESIRED_DISABLED!r}, {DESIRED_HELD!r}, got {desired!r}"
            )
        state = self.load_state(unit)
        state["desired"] = desired
        self._save_state(state)
        return state

    def enable(self, unit: str) -> dict[str, Any]:
        return self.set_desired(unit, DESIRED_ENABLED)

    def disable(self, unit: str) -> dict[str, Any]:
        return self.set_desired(unit, DESIRED_DISABLED)

    def hold(self, unit: str) -> dict[str, Any]:
        """Keep-alive: leave a running process running, never force-restart."""
        return self.set_desired(unit, DESIRED_HELD)

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
        """Return ``"running"`` or ``"stopped"``; clean up a stale pid file."""
        try:
            live = liveness(self.unit_dir(unit))
        except FileNotFoundError:
            return "stopped"
        if live.state == "running":
            return "running"
        # Stale pid.json (process gone) -> clean so the unit reads as stopped.
        _clean_pid(self.unit_dir(unit))
        return "stopped"

    # -- reconcile ---------------------------------------------------------

    def reconcile_unit(
        self, spec: ProcessSpec, policy: ReconcilePolicy
    ) -> ReconcileOutcome:
        """Converge a single unit toward the desired state (no lock)."""
        unit = spec.unit
        state = self.load_state(unit)

        if state["desired"] == DESIRED_HELD:
            # Keep-alive: never spawn, never kill.  A running process stays
            # running (warm-parked by the consumer's own drive loop); a
            # stopped one stays stopped.  The kit only declines to change
            # process topology -- what "held" means for the drive loop is the
            # consumer's decision.  Short-circuit before the policy so a held
            # unit is inert regardless of what the policy would return, and
            # never invoke ``policy`` with a mutated ``state`` -- the non-held
            # path below keeps its HEAD input contract untouched.
            actual = self.observe_actual(unit)
            state["actual"] = actual
            self._save_state(state)
            pid = state.get("pid")
            return ReconcileOutcome(
                unit=unit,
                action="held",
                desired=DESIRED_HELD,
                actual=actual,
                generation=int(state["generation"]),
                pid=pid if (isinstance(pid, int) and actual == "running") else None,
                alive=actual == "running",
            )

        want_running = policy(state)
        actual = self.observe_actual(unit)
        state["actual"] = actual
        if want_running and actual != "running":
            generation = int(state["generation"]) + 1
            result = self._spawn(spec, generation, self.unit_dir(unit))
            state["generation"] = generation
            state["pid"] = result.pid if result.alive else None
            state["actual"] = "running" if result.alive else "stopped"
            self._save_state(state)
            return ReconcileOutcome(
                unit=unit,
                action="spawned",
                desired=str(state["desired"]),
                actual=str(state["actual"]),
                generation=generation,
                pid=state["pid"],
                alive=result.alive,
            )

        if not want_running and actual == "running":
            self._stop(self.unit_dir(unit))
            state["pid"] = None
            state["actual"] = "stopped"
            self._save_state(state)
            return ReconcileOutcome(
                unit=unit,
                action="stopped",
                desired=str(state["desired"]),
                actual="stopped",
                generation=int(state["generation"]),
                pid=None,
                alive=False,
            )

        # Already converged: persist the observed actual and report no-op.
        self._save_state(state)
        pid = state.get("pid")
        return ReconcileOutcome(
            unit=unit,
            action="noop",
            desired=str(state["desired"]),
            actual=actual,
            generation=int(state["generation"]),
            pid=pid if isinstance(pid, int) else None,
            alive=actual == "running",
        )

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


# ---------------------------------------------------------------------------
# WorkerControlPlane -- aggregate observe / pause / resume over a fleet
# ---------------------------------------------------------------------------


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
    keep-alive :data:`DESIRED_HELD` state (warm pause: the reconciler leaves a
    running process alive); ``resume`` writes :data:`DESIRED_ENABLED`.

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

        This is a **read-only** view: it classifies liveness straight from
        ``pid.json`` and never cleans a stale pid file.  Pruning stale metadata
        is the supervisor's job, done under the supervisor lock during reconcile
        -- an out-of-process observer (e.g. the FastAPI adapter) must not race
        it by unlinking a pid file the supervisor may have just refreshed.  The
        live pid from ``pid.json`` is authoritative for the view, not the
        possibly-lagging ``state["pid"]`` record.
        """
        self._require(unit)
        state = self._reconciler.load_state(unit)
        try:
            live = liveness(self._reconciler.unit_dir(unit))
        except FileNotFoundError:
            live = None
        if live is not None and live.state == "running":
            actual, pid = "running", live.pid
        else:
            # No pid file, or a stale one (process gone): report stopped
            # without touching disk.
            actual, pid = "stopped", None
        heartbeat = {
            key: value
            for key, value in state.items()
            if key not in _CORE_STATE_KEYS
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
        """Warm-pause a unit: write the keep-alive :data:`DESIRED_HELD` state."""
        self._require(unit)
        return self._reconciler.hold(unit)

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
