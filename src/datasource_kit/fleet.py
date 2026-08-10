"""Domain-blind fleet-process supervision primitives.

A consumer runs a fleet of long-lived source-worker OS processes
(one per datasource unit) and needs generic, stdlib-only process
supervision.  This module provides those primitives.

Boundaries
----------
- No scheduler or cron. ``FleetHost`` only owns a consumer-driven pass loop;
  fleet membership, admission, reconciliation, and policy stay in the consumer.
- No knowledge of what the worker does -- the kit never sees
  consumer vocabulary, storage, or business state.
- POSIX only (uses ``os.kill``, ``signal``, ``start_new_session``).
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, Generic, TypeVar

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
    "clear_process_tombstone",
    "ProcessTombstoneError",
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
    # FleetHost face
    "FleetHost",
    "FleetPass",
    "DESIRED_ENABLED",
    "DESIRED_DISABLED",
    "DESIRED_PAUSED",
    "GENERATION_ENV",
    # WorkerControlPlane face
    "WorkerControlPlane",
    "UnitObservation",
]

# ---------------------------------------------------------------------------
# Pure-data shapes
# ---------------------------------------------------------------------------

#: Default child environment key used for reconciler generations.
GENERATION_ENV = "DATASOURCE_KIT_GENERATION"
_PROBE_SLEEP = 0.25
_PROBE_WINDOW = 1.5

StreamOpener = Callable[[Path], IO[Any]]


@dataclass(slots=True, frozen=True)
class ProcessSpec:
    """Declarative description of a worker process to spawn.

    ``env`` is an optional complete base environment (``None`` inherits the
    parent); ``env_overlay`` is then applied without ever being written to pid
    metadata.  When :func:`spawn` receives a generation, it is injected last
    under ``generation_env`` (set it to ``None`` to disable injection).

    ``stdout_path`` and ``stderr_path`` are consumer-owned paths opened in
    append mode.  The kit chooses neither their layout nor names.  ``opener``
    may replace the default append opener (it is called once per configured
    path); every returned descriptor is closed in the parent after spawning,
    including on errors.

    ``pid_metadata`` is JSON-compatible, opaque consumer metadata added to the
    top level of ``pid.json``.  It may not replace the standard ``pid``,
    ``command``, ``started_at``, or ``label`` fields.
    """

    unit: str
    command: tuple[str, ...]
    cwd: str | None = None
    env: Mapping[str, str] | None = None
    label: str = ""
    stdout_path: str | Path | None = None
    stderr_path: str | Path | None = None
    opener: StreamOpener | None = None
    probe_window: float = _PROBE_WINDOW
    probe_sleep: float = _PROBE_SLEEP
    env_overlay: Mapping[str, str] = field(default_factory=dict)
    generation_env: str | None = GENERATION_ENV
    pid_metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _validate_unit(self.unit)


@dataclass(slots=True, frozen=True)
class SpawnResult:
    """Outcome of a :func:`spawn` call."""

    pid: int
    started_at: float
    alive: bool
    token: str | None = None
    generation: int | None = None


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
_STOP_SIGNAL = signal.SIGTERM
_KILL_SIGNAL = signal.SIGKILL
_DEFAULT_TIMEOUT = 5.0
_OWNED_HANDLES: dict[str, subprocess.Popen[Any]] = {}


def _validate_unit(unit: str) -> str:
    """Reject unit ids that could escape the reconciler's root."""
    if (not isinstance(unit, str) or not unit or unit in {".", ".."}
            or Path(unit).name != unit or "\x00" in unit):
        raise ValueError(f"unit must be one safe path component, got {unit!r}")
    return unit


def _ensure_unit_dir(unit_dir: str | Path) -> Path:
    """Create a real unit directory, refusing a symlink at the trust boundary."""
    directory = Path(unit_dir)
    directory.mkdir(parents=True, exist_ok=True)
    import stat
    mode = os.lstat(directory).st_mode
    if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
        raise ProcessTombstoneError("unit directory must be a real directory")
    return directory


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


_STANDARD_PID_KEYS = frozenset({"pid", "command", "started_at", "label"})


def _pid_payload(spec: ProcessSpec, result: SpawnResult) -> dict[str, object]:
    metadata = dict(spec.pid_metadata)
    collisions = _STANDARD_PID_KEYS.intersection(metadata)
    if collisions:
        joined = ", ".join(sorted(collisions))
        raise ValueError(f"pid_metadata may not replace standard fields: {joined}")
    payload: dict[str, object] = {
        "pid": result.pid,
        "command": list(spec.command),
        "started_at": result.started_at,
        "label": spec.label,
        **metadata,
    }
    # Fail before starting a child when opaque metadata cannot be persisted.
    json.dumps(payload)
    return payload


def _write_pid(unit_dir: str | Path, payload: Mapping[str, object]) -> None:
    """Atomically write *pid.json* to *unit_dir*."""
    path = _pid_path(unit_dir)
    write_json_atomic(path, payload)


def _append_opener(path: Path) -> IO[Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.open("a", encoding="utf-8")


def _clean_pid(unit_dir: str | Path) -> bool:
    """Remove *pid.json* from *unit_dir*.  Returns ``True`` if removed."""
    path = _pid_path(unit_dir)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return False
    else:
        return True


def _pid_is_zombie(pid: int) -> bool:
    """Return ``True`` if *pid* is a zombie (exited but not yet reaped).

    A process that has exited but whose parent has not ``wait()``-ed on it
    still occupies its PID slot, so ``os.kill(pid, 0)`` keeps succeeding even
    though the process is dead. When the supervisor is the worker's own parent
    (the reconciler spawns and never reaps), a crashed worker becomes exactly
    such a zombie -- and without this check it would read "running" forever and
    never be respawned. Detected via POSIX ``ps -o stat=``: the state column's
    zombie marker is ``Z``. Any inability to inspect (no ``ps``, timeout, race)
    is reported as "not a zombie", so a transient probe failure never masks a
    genuinely live process.
    """
    try:
        result = subprocess.run(
            ["ps", "-o", "stat=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=1,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0 and "Z" in result.stdout.strip()


def _pid_alive(pid: int) -> bool:
    """Return ``True`` if *pid* refers to a live, non-zombie process.

    ``os.kill(pid, 0)`` succeeding only proves the PID slot is occupied; a
    zombie occupies its slot too. Excluding zombies is what lets the reconciler
    respawn a crashed worker whose parent is the supervisor itself (see
    :func:`_pid_is_zombie`).
    """
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but is owned by another user (a recycled PID slot, or
        # a worker that dropped privileges). ``os.kill`` cannot signal it, but
        # ``ps`` can still read its state, so a foreign zombie is excluded the
        # same way as one of our own. The consumer's *pid.json* is provenance,
        # not an ownership guarantee.
        return not _pid_is_zombie(pid)
    except OSError:
        return False
    return not _pid_is_zombie(pid)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def spawn_process(
    command: Sequence[str], *, cwd: str | None = None,
    env: Mapping[str, str] | None = None, stdout: int | IO[str] | None = None,
    stderr: int | IO[str] | None = None, probe_window: float = _PROBE_WINDOW,
    probe_sleep: float = _PROBE_SLEEP,
    _persist: Callable[[int, float, str], None] | None = None,
    _generation: int | None = None, _token: str | None = None,
) -> SpawnResult:
    """Launch through an ACK gate; the consumer cannot exec before provenance.

    The private ``_persist`` hook is intentionally only used by :func:`spawn`.
    Without it this remains a compatibility primitive and ACKs immediately.
    """
    child_env = dict(os.environ) if env is None else dict(env)
    token = _token or uuid.uuid4().hex
    parent_fd, child_fd = socket.socketpair()
    child_env["DATASOURCE_KIT_ACK_FD"] = str(child_fd.fileno())
    child_env["DATASOURCE_KIT_EXEC_COMMAND"] = json.dumps(list(command))
    proc = subprocess.Popen(
        [sys.executable, "-m", "datasource_kit._exec_gate"], cwd=cwd,
        env=child_env, start_new_session=True, stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL if stdout is None else stdout,
        stderr=subprocess.DEVNULL if stderr is None else stderr,
        pass_fds=(child_fd.fileno(),),
    )
    child_fd.close()
    started_at = time.time()
    try:
        parent_fd.settimeout(max(probe_window, 1.0))
        if parent_fd.recv(6) != b"READY\n":
            return SpawnResult(proc.pid, started_at, False)
        if _persist is not None:
            _persist(proc.pid, started_at, token)
            _OWNED_HANDLES[token] = proc
        parent_fd.sendall(b"ACK\n")
    except BaseException:
        # Closing the channel makes a pre-ACK wrapper exit harmlessly.
        parent_fd.close()
        try: proc.wait(timeout=1)
        except subprocess.TimeoutExpired: pass
        raise
    finally:
        parent_fd.close()
    deadline = started_at + probe_window
    while time.time() < deadline:
        if proc.poll() is not None:
            if _OWNED_HANDLES.get(token) is proc:
                _OWNED_HANDLES.pop(token, None)
            return SpawnResult(proc.pid, started_at, False, token, _generation)
        time.sleep(probe_sleep)
    return SpawnResult(proc.pid, started_at, True, token, _generation)


def spawn(spec: ProcessSpec, *, unit_dir: str | Path | None = None,
          generation: int | None = None) -> SpawnResult:
    """First-launch-only spawn with durable, ACK-gated process provenance."""
    resolved = _ensure_unit_dir(unit_dir if unit_dir is not None else spec.unit)
    pid_path = _pid_path(resolved)
    # Presence is the fence: never parse, probe, clean, adopt, or infer safety.
    try:
        os.lstat(pid_path)
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise ProcessTombstoneError("pid metadata unreadable; operator verification required") from exc
    else:
        raise ProcessTombstoneError("process tombstone exists; operator verification required")
    template = _pid_payload(spec, SpawnResult(0, 0.0, False))
    token = uuid.uuid4().hex
    # Establish the durable fence before Popen. A crash at any later boundary
    # leaves launch intent, so a restarted supervisor cannot launch again.
    intent = dict(template)
    intent.update(pid=None, started_at=None, unit=spec.unit,
                  generation=generation, token=token, incarnation=token,
                  status="launch_intent")
    try:
        fd = os.open(pid_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(intent, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        directory_fd = os.open(resolved, os.O_RDONLY)
        try: os.fsync(directory_fd)
        finally: os.close(directory_fd)
    except BaseException:
        # Presence (including a partial file) remains a conservative fence.
        raise
    child_env = dict(os.environ) if spec.env is None else dict(spec.env)
    child_env.update(spec.env_overlay)
    if generation is not None and spec.generation_env is not None:
        child_env[spec.generation_env] = str(generation)
    open_stream = spec.opener or _append_opener
    with ExitStack() as stack:
        out = stack.enter_context(open_stream(Path(spec.stdout_path))) if spec.stdout_path else None
        err = stack.enter_context(open_stream(Path(spec.stderr_path))) if spec.stderr_path else None
        def persist(pid: int, started: float, token: str) -> None:
            payload = dict(template)
            payload.update(pid=pid, started_at=started, unit=spec.unit,
                           generation=generation, token=token,
                           incarnation=token, status="running_or_unknown")
            _write_pid(resolved, payload)
        return spawn_process(spec.command, cwd=spec.cwd, env=child_env,
            stdout=out, stderr=err, probe_window=spec.probe_window,
            probe_sleep=spec.probe_sleep, _persist=persist,
            _generation=generation, _token=token)


def stop_process(pid: int, *, timeout: float = _DEFAULT_TIMEOUT) -> StopOutcome:
    """Refuse numeric-PID signalling; identity cannot be proven by a PID."""
    raise ProcessTombstoneError(
        "numeric PID signalling is disabled; operator verification required"
    )


def stop(unit_dir: str | Path, *, timeout: float = _DEFAULT_TIMEOUT) -> StopResult:
    """Request cooperative TERM only through this supervisor's live handle.

    Provenance is retained regardless of the outcome. There is no escalation.
    """
    path = _pid_path(unit_dir)
    data = read_json(path)
    if data is None:
        raise ProcessTombstoneError("pid metadata absent or unreadable")
    token = data.get("token")
    pid = data.get("pid")
    handle = _OWNED_HANDLES.get(token) if isinstance(token, str) else None
    signalled = False
    if handle is not None and handle.pid == pid and handle.poll() is None:
        handle.terminate()  # Popen capability, never a reconstructed numeric PID
        signalled = True
    elif handle is not None:
        # Retire dead or mismatched capabilities; they must never accumulate or
        # later authorize a PID-reuse signal.
        _OWNED_HANDLES.pop(token, None)
    data["stop_requested"] = True
    data["operator_verification_required"] = True
    data["status"] = "stop_requested_or_unknown"
    write_json_atomic(path, data)
    return StopResult(int(pid or 0), signalled, False, False)


def clear_process_tombstone(
    unit_dir: str | Path, *, unit: str, generation: int, token: str,
    workload_fully_gone_asserted: bool, operator: str = "operator",
) -> Path:
    """Operator-only clearance after external proof the whole workload is gone."""
    if not workload_fully_gone_asserted:
        raise ProcessTombstoneError("explicit workload-gone assertion is required")
    directory = Path(unit_dir)
    with _unit_lock(directory):
        path = _pid_path(directory)
        try:
            mode = os.lstat(path).st_mode
        except OSError as exc:
            raise ProcessTombstoneError("tombstone absent or unreadable") from exc
        import stat
        if not stat.S_ISREG(mode):
            raise ProcessTombstoneError("tombstone is not a regular file")
        data = read_json(path)
        if data is None:
            raise ProcessTombstoneError("tombstone absent or unreadable")
        if (data.get("unit"), data.get("generation"), data.get("token")) != (unit, generation, token):
            raise ProcessTombstoneError("unit/generation/token do not exactly match")
        audit = directory / "process-clearance.audit.jsonl"
        record = {"unit": unit, "generation": generation, "token": token,
                  "operator": operator, "asserted_workload_fully_gone": True,
                  "cleared_at": time.time()}
        audit_fd = os.open(audit, os.O_WRONLY | os.O_APPEND | os.O_CREAT |
                           getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(audit_fd, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        path.unlink()
        directory_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        _OWNED_HANDLES.pop(token, None)
        return audit


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


class ProcessTombstoneError(RuntimeError):
    """An existing process provenance record requires operator clearance."""


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
    action: str  # "spawned", "stopped", or "noop"
    desired: str  # "enabled", "disabled", or "paused"
    actual: str  # "running" or "stopped"
    generation: int
    pid: int | None
    alive: bool
    degraded_reason: str | None = None


def write_json_atomic(path: str | Path, payload: Mapping[str, Any]) -> None:
    """Durably replace JSON without following a predictable temp symlink."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, indent=2, sort_keys=True).encode()
    tmp = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(tmp, flags, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


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


T = TypeVar("T")
R = TypeVar("R")
A = TypeVar("A")


@dataclass(slots=True, frozen=True)
class FleetPass(Generic[R, A]):
    """Opaque result of one admitted, ordered fleet pass."""

    admission: A
    results: tuple[R, ...]


class FleetHost(Generic[T, R, A]):
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


@contextmanager
def _unit_lock(unit_dir: Path) -> Iterator[None]:
    """Serialize state/tombstone transitions for one unit."""
    unit_dir = _ensure_unit_dir(unit_dir)
    path = unit_dir / ".process.lock"
    flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "a+") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try: yield
        finally: fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


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
