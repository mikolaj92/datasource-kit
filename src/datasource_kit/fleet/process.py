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
- POSIX only (uses ``os.kill``, ``start_new_session``).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

__all__ = [
    "LegacyProcessTombstoneInspection",
    "Liveness",
    "ProcessSpec",
    "ProcessTombstoneError",
    "SpawnResult",
    "StopOutcome",
    "StopResult",
    "clear_legacy_process_tombstone",
    "clear_process_tombstone",
    "inspect_legacy_process_tombstone",
    "liveness",
    "spawn",
    "spawn_process",
    "stop",
    "stop_process",
]

@dataclass(slots=True, frozen=True)
class LegacyProcessTombstoneInspection:
    """Read-only identity of a process tombstone opened in a trusted namespace."""

    path: Path
    sha256: str
    generation: int | None
    token_absent: bool

class ProcessTombstoneError(RuntimeError):
    """An existing process provenance record requires operator clearance."""

# ---------------------------------------------------------------------------
# Pure-data shapes
# ---------------------------------------------------------------------------

#: Default child environment key used for reconciler generations.
GENERATION_ENV = "DATASOURCE_KIT_GENERATION"
_PROBE_SLEEP = 0.25
_PROBE_WINDOW = 1.5
_STARTUP_TIMEOUT = 30.0


def _validate_startup_timeout(value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError("startup_timeout must be positive and finite")

StreamOpener = Callable[[Path], IO[Any]]


@dataclass(slots=True, frozen=True)
class ProcessSpec:
    """Declarative description of a worker process to spawn.

    ``env`` is an optional complete base environment (``None`` inherits the
    parent); ``env_overlay`` is then applied without ever being written to pid
    metadata.  When :func:`spawn` receives a generation, it is injected last
    under ``generation_env`` (set it to ``None`` to disable injection).

    ``startup_timeout`` bounds wrapper readiness and ACK transport (30 seconds
    by default), independently from ``probe_window`` after exec. It must be
    positive and finite. Timeout retains durable launch intent; operators still
    must verify the whole workload before clearing it.

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
    startup_timeout: float = _STARTUP_TIMEOUT

    def __post_init__(self) -> None:
        _validate_unit(self.unit)
        _validate_startup_timeout(self.startup_timeout)


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
    """Outcome of a :func:`stop` call.

    ``killed`` and ``cleaned`` stay ``False``: there is no SIGKILL
    escalation and the pid.json tombstone is retained.
    """

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
    state: str  # "running" or "stale"; never "stopped"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


_PID_FILE = "pid.json"
_OWNED_HANDLES: dict[str, subprocess.Popen[Any]] = {}
# POSIX record locks do not serialize threads reliably (and macOS flock locks
# are process-associated), so protect descriptor ownership within this process.
_LEGACY_CLEARANCE_THREAD_LOCK = threading.RLock()


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
    startup_timeout: float = _STARTUP_TIMEOUT,
    _persist: Callable[[int, float, str], None] | None = None,
    _generation: int | None = None, _token: str | None = None,
) -> SpawnResult:
    """Launch through an ACK gate; the consumer cannot exec before provenance.

    The private ``_persist`` hook is intentionally only used by :func:`spawn`.
    Without it this remains a compatibility primitive and ACKs immediately.
    """
    _validate_startup_timeout(startup_timeout)
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
        parent_fd.settimeout(startup_timeout)
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
    # Probe after ACK only: handshake time must not consume the crash window.
    deadline = time.time() + probe_window
    while time.time() < deadline:
        if proc.poll() is not None:
            if _OWNED_HANDLES.get(token) is proc:
                _OWNED_HANDLES.pop(token, None)
            return SpawnResult(proc.pid, started_at, False, token, _generation)
        time.sleep(probe_sleep)
    return SpawnResult(proc.pid, started_at, True, token, _generation)


def spawn(
    spec: ProcessSpec,
    *,
    unit_dir: str | Path | None = None,
    generation: int | None = None,
    _spawn_process: Callable[..., SpawnResult] = spawn_process,
) -> SpawnResult:
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
    fd = os.open(
        pid_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(intent, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(resolved, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
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
        return _spawn_process(spec.command, cwd=spec.cwd, env=child_env,
            stdout=out, stderr=err, probe_window=spec.probe_window,
            probe_sleep=spec.probe_sleep, startup_timeout=spec.startup_timeout,
            _persist=persist,
            _generation=generation, _token=token)


def stop_process(pid: int) -> StopOutcome:
    """Refuse numeric-PID signalling; identity cannot be proven by a PID."""
    raise ProcessTombstoneError(
        "numeric PID signalling is disabled; operator verification required"
    )


def stop(unit_dir: str | Path) -> StopResult:
    """Request cooperative TERM only through this supervisor's live handle.

    Provenance is retained regardless of the outcome. There is no
    escalation to SIGKILL, no numeric-PID signalling, and the pid.json
    tombstone is not cleared.
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



def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _validate_trusted_directory(st: os.stat_result, *, unit: bool) -> None:
    """Validate the clearance namespace threat boundary.

    The caller's euid is trusted.  Same-euid code is deliberately out of scope:
    it can replace this package and its state.  Root-owned ancestors are trusted;
    a writable root-owned ancestor is accepted only with the sticky bit (the
    normal /tmp-style namespace protection).  Group/other-writable unit and
    non-root ancestor directories are rejected.
    """
    import stat
    euid = os.geteuid()
    if not stat.S_ISDIR(st.st_mode):
        raise ProcessTombstoneError("clearance namespace component is not a directory")
    if unit:
        if st.st_uid != euid or st.st_mode & 0o022:
            raise ProcessTombstoneError("unit directory must be euid-owned and not group/world writable")
    elif st.st_uid not in {0, euid}:
        raise ProcessTombstoneError("clearance ancestor has an untrusted owner")
    elif st.st_mode & 0o022 and not (st.st_uid == 0 and st.st_mode & stat.S_ISVTX):
        raise ProcessTombstoneError("clearance ancestor is group/world writable")


def _open_trusted_unit_directory(directory: Path) -> tuple[Path, int, os.stat_result]:
    """Open every absolute pathname component with NOFOLLOW and retain unit FD."""
    absolute = Path(os.path.abspath(os.fspath(directory)))
    parts = absolute.parts
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(parts[0], flags)
    try:
        _validate_trusted_directory(os.fstat(fd), unit=False)
        for index, part in enumerate(parts[1:], 1):
            child = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = child
            _validate_trusted_directory(os.fstat(fd), unit=index == len(parts) - 1)
        st = os.fstat(fd)
        named = os.stat(absolute, follow_symlinks=False)
        if not _same_inode(st, named):
            raise ProcessTombstoneError("unit directory pathname identity changed")
        return absolute, fd, st
    except BaseException:
        os.close(fd)
        raise


def _bind_named_directory(dir_fd: int, opened: os.stat_result, name: str | Path,
                          *, message: str) -> None:
    try:
        named = os.stat(name, dir_fd=dir_fd if isinstance(name, str) else None,
                        follow_symlinks=False)
    except FileNotFoundError as exc:
        raise ProcessTombstoneError(message) from exc
    if not _same_inode(opened, named):
        raise ProcessTombstoneError(message)


def _bind_clearance_namespace(directory: Path, dir_fd: int, unit_st: os.stat_result,
                              lock_st: os.stat_result, audit_fd: int,
                              audit_st: os.stat_result, audit_name: str) -> None:
    _bind_named_directory(dir_fd, unit_st, directory, message="unit directory pathname identity changed")
    _bind_named_directory(dir_fd, lock_st, ".process.lock", message="unit lock pathname identity changed")
    _bind_named_directory(dir_fd, audit_st, audit_name, message="audit directory pathname identity changed")
    if not _same_inode(audit_st, os.fstat(audit_fd)):
        raise ProcessTombstoneError("audit directory descriptor identity changed")

def _read_regular_fd(fd: int, *, limit: int = 16 * 1024 * 1024) -> bytes:
    """Read a small regular file from its already validated descriptor."""
    os.lseek(fd, 0, os.SEEK_SET)
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = os.read(fd, min(1024 * 1024, limit + 1 - size))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        size += len(chunk)
        if size > limit:
            raise ProcessTombstoneError("tombstone is unreasonably large")


def _safe_internal_basename(name: object, *, prefix: str, suffix: str) -> str:
    """Accept only the exact single-component names this module generates."""
    if (not isinstance(name, str) or len(name) > 255 or not name.startswith(prefix)
            or not name.endswith(suffix) or name in {".", ".."}
            or "/" in name or (os.altsep is not None and os.altsep in name)):
        raise ProcessTombstoneError("invalid internal clearance artifact name")
    return name


def _open_private_named(name: str, *, dir_fd: int, directory: bool = False,
                        allowed_nlinks: tuple[int, ...] = (1,)) -> tuple[int, os.stat_result]:
    import stat
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    if directory:
        flags |= getattr(os, "O_DIRECTORY", 0)
    fd = os.open(name, flags, dir_fd=dir_fd)
    st = os.fstat(fd)
    named = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    kind_ok = stat.S_ISDIR(st.st_mode) if directory else stat.S_ISREG(st.st_mode)
    # A directory normally has two links; only regular audit/lock files must be nlink 1.
    if (not kind_ok or st.st_uid != os.geteuid()
            or (not directory and st.st_nlink not in allowed_nlinks) or st.st_mode & 0o077
            or (st.st_dev, st.st_ino) != (named.st_dev, named.st_ino)):
        os.close(fd)
        raise ProcessTombstoneError("clearance artifact must be private and named by its opened inode")
    return fd, st


def _atomic_private_json(dir_fd: int, final: str, value: Mapping[str, Any]) -> None:
    """Publish one immutable audit record; abandoned dot-tmp files are ignored."""
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if len(raw) > 8192:
        raise ProcessTombstoneError("clearance audit record is too large")
    tmp = f".tmp-{uuid.uuid4().hex}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                 0o600, dir_fd=dir_fd)
    try:
        offset = 0
        while offset < len(raw):
            wrote = os.write(fd, raw[offset:])
            if wrote <= 0:
                raise OSError("short write to clearance audit")
            offset += wrote
        os.fsync(fd)
        os.fchmod(fd, 0o600)
        os.fsync(fd)
        # link is the portable, atomic NOREPLACE publication primitive.
        os.link(tmp, final, src_dir_fd=dir_fd, dst_dir_fd=dir_fd, follow_symlinks=False)
        os.fsync(dir_fd)
        published = os.stat(final, dir_fd=dir_fd, follow_symlinks=False)
        opened = os.fstat(fd)
        if (published.st_dev, published.st_ino) != (opened.st_dev, opened.st_ino):
            raise ProcessTombstoneError("published audit pathname identity changed")
    finally:
        os.close(fd)
        try:
            os.unlink(tmp, dir_fd=dir_fd)
            os.fsync(dir_fd)
        except FileNotFoundError:
            pass



def _recover_audit_link(fd: int, opened: os.stat_result, dir_fd: int) -> None:
    if opened.st_nlink != 2:
        return
    candidates: list[str] = []
    for entry in os.listdir(dir_fd):
        if (len(entry) == 37 and entry.startswith(".tmp-")
                and all(c in "0123456789abcdef" for c in entry[5:])):
            est = os.stat(entry, dir_fd=dir_fd, follow_symlinks=False)
            if _same_inode(opened, est):
                candidates.append(entry)
    if len(candidates) != 1:
        raise ProcessTombstoneError("published audit has unexpected extra links")
    os.unlink(candidates[0], dir_fd=dir_fd)
    os.fsync(dir_fd)
    if os.fstat(fd).st_nlink != 1:
        raise ProcessTombstoneError("published audit has unexpected extra links")

def _read_exact_audit(dir_fd: int, name: str, expected: Mapping[str, Any]) -> bool:
    try:
        fd, opened = _open_private_named(name, dir_fd=dir_fd, allowed_nlinks=(1, 2))
    except FileNotFoundError:
        return False
    try:
        _recover_audit_link(fd, opened, dir_fd)
        raw = _read_regular_fd(fd, limit=8192)
        try:
            actual = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProcessTombstoneError("clearance audit record is malformed") from exc
        if actual != dict(expected):
            raise ProcessTombstoneError("clearance audit record does not match request")
        # Bind the official directory entry to the descriptor used above.
        named = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        opened = os.fstat(fd)
        if (named.st_dev, named.st_ino) != (opened.st_dev, opened.st_ino):
            raise ProcessTombstoneError("clearance audit pathname identity changed")
        return True
    finally:
        os.close(fd)


def _inspect_legacy_process_tombstone(
    unit_dir: str | Path,
) -> LegacyProcessTombstoneInspection:
    """Inspect a tombstone through the same trusted namespace used for clearance.

    This is deliberately read-only and never performs PID liveness inspection.
    The SHA-256 covers the exact bounded bytes read from the validated regular,
    single-link ``pid.json`` descriptor.  Malformed metadata is represented by
    ``generation=None`` and ``token_absent=False`` so callers cannot mistake it
    for a clearable tokenless legacy record.
    """
    import stat

    directory, dir_fd, unit_st = _open_trusted_unit_directory(Path(unit_dir))
    try:
        try:
            pid_fd = os.open(
                _PID_FILE, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=dir_fd
            )
        except (FileNotFoundError, PermissionError) as exc:
            raise ProcessTombstoneError("tombstone absent or unreadable") from exc
        try:
            opened = os.fstat(pid_fd)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise ProcessTombstoneError("tombstone must be a regular single-link file")
            raw = _read_regular_fd(pid_fd)
            named = os.stat(_PID_FILE, dir_fd=dir_fd, follow_symlinks=False)
            if not _same_inode(opened, named):
                raise ProcessTombstoneError("tombstone pathname identity changed")
            _bind_named_directory(
                dir_fd, unit_st, directory,
                message="unit directory pathname identity changed",
            )
            try:
                metadata = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                metadata = None
            generation = metadata.get("generation") if isinstance(metadata, dict) else None
            if not isinstance(generation, int) or isinstance(generation, bool):
                generation = None
            return LegacyProcessTombstoneInspection(
                path=directory / _PID_FILE,
                sha256=hashlib.sha256(raw).hexdigest(),
                generation=generation,
                token_absent=isinstance(metadata, dict) and "token" not in metadata,
            )
        finally:
            os.close(pid_fd)
    finally:
        os.close(dir_fd)


def inspect_legacy_process_tombstone(
    unit_dir: str | Path,
) -> LegacyProcessTombstoneInspection:
    """Inspect legacy metadata, exposing only stable domain failures."""
    try:
        return _inspect_legacy_process_tombstone(unit_dir)
    except ProcessTombstoneError:
        raise
    except OSError as exc:
        raise ProcessTombstoneError("tombstone namespace is absent or unreadable") from exc


def _clear_legacy_process_tombstone_locked(
    unit_dir: str | Path, *, unit: str, generation: int,
    expected_sha256: str, workload_fully_gone_asserted: bool,
    operator: str, ticket: str,
) -> Path:
    """Durably quarantine a tokenless legacy tombstone, without signalling.

    Audit is an append-only *set* of individually atomic files in private
    ``legacy-process-clearance.audit.d``.  JSONL is deliberately not used:
    incomplete temporary files are unreferenced and harmless after a crash.
    """
    import stat

    _validate_unit(unit)
    if not isinstance(generation, int) or isinstance(generation, bool):
        raise ProcessTombstoneError("generation must be an integer")
    if workload_fully_gone_asserted is not True:
        raise ProcessTombstoneError("explicit workload-gone assertion is required")
    for label, value, limit in (("operator", operator, 256), ("ticket", ticket, 512)):
        if not isinstance(value, str) or not value.strip() or len(value.encode()) > limit:
            raise ProcessTombstoneError(f"nonempty bounded {label} is required")
    if (not isinstance(expected_sha256, str) or len(expected_sha256) != 64
            or any(c not in "0123456789abcdef" for c in expected_sha256)):
        raise ProcessTombstoneError("expected_sha256 must be a lowercase SHA-256 digest")

    directory = Path(unit_dir)
    directory, dir_fd, unit_st = _open_trusted_unit_directory(directory)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    try:
        # Exactly one absent-lock creator publishes a private, durable inode.
        # A failed publication deliberately leaves the artifact in place: later
        # callers validate it and fail closed rather than silently replacing it.
        lock_flags = os.O_RDWR | nofollow
        created = False
        try:
            lock_fd = os.open(
                ".process.lock", lock_flags | os.O_CREAT | os.O_EXCL,
                0o600, dir_fd=dir_fd,
            )
            created = True
        except FileExistsError:
            try:
                lock_fd = os.open(".process.lock", lock_flags, dir_fd=dir_fd)
            except OSError as exc:
                raise ProcessTombstoneError("unit lock is absent or unreadable") from exc
        except OSError as exc:
            raise ProcessTombstoneError("unit lock could not be created") from exc
        if created:
            try:
                os.fchmod(lock_fd, 0o600)
                os.fsync(lock_fd)
                os.fsync(dir_fd)
            except OSError as exc:
                os.close(lock_fd)
                raise ProcessTombstoneError("unit lock could not be durably published") from exc
        try:
            lst = os.fstat(lock_fd)
            named = os.stat(".process.lock", dir_fd=dir_fd, follow_symlinks=False)
            if (not stat.S_ISREG(lst.st_mode) or lst.st_uid != os.geteuid()
                    or lst.st_nlink != 1 or lst.st_mode & 0o077
                    or (lst.st_dev, lst.st_ino) != (named.st_dev, named.st_ino)):
                raise ProcessTombstoneError("unit lock must be a private regular named file")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)

            audit_dir_name = "legacy-process-clearance.audit.d"
            try:
                os.mkdir(audit_dir_name, 0o700, dir_fd=dir_fd)
                os.fsync(dir_fd)
            except FileExistsError:
                pass
            audit_fd, audit_st = _open_private_named(audit_dir_name, dir_fd=dir_fd, directory=True)
            try:
                _bind_clearance_namespace(directory, dir_fd, unit_st, lst,
                                          audit_fd, audit_st, audit_dir_name)
                index_fields = {"version": 2, "unit": unit, "generation": generation,
                    "operator": operator, "ticket": ticket,
                    "tombstone_sha256": expected_sha256,
                    "asserted_workload_fully_gone": True}
                canonical = json.dumps(index_fields, sort_keys=True, separators=(",", ":")).encode()
                op_id = hashlib.sha256(canonical).hexdigest()
                intent_name = f"intent-{op_id}.json"
                complete_name = f"complete-{op_id}.json"
                quarantine = f".legacy-pid.{expected_sha256}.{uuid.uuid4().hex}.quarantine"
                _safe_internal_basename(quarantine, prefix=".legacy-pid.", suffix=".quarantine")

                # An existing intent is authoritative only after exact validation.
                try:
                    ifd, ist = _open_private_named(intent_name, dir_fd=audit_fd,
                                                       allowed_nlinks=(1, 2))
                    _recover_audit_link(ifd, ist, audit_fd)
                except FileNotFoundError:
                    intent = dict(index_fields, event="legacy_tombstone_clearance_intent",
                                  operation_id=op_id, quarantine=quarantine)
                    existing_intent = False
                else:
                    try:
                        try: intent = json.loads(_read_regular_fd(ifd, limit=8192))
                        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                            raise ProcessTombstoneError("clearance intent is malformed") from exc
                        ist = os.fstat(ifd)
                        inamed = os.stat(intent_name, dir_fd=audit_fd, follow_symlinks=False)
                        if (ist.st_dev, ist.st_ino) != (inamed.st_dev, inamed.st_ino):
                            raise ProcessTombstoneError("clearance intent pathname identity changed")
                    finally: os.close(ifd)
                    if not isinstance(intent, dict):
                        raise ProcessTombstoneError("clearance intent is malformed")
                    expected_part = dict(index_fields, event="legacy_tombstone_clearance_intent",
                                         operation_id=op_id, quarantine=intent.get("quarantine"))
                    if intent != expected_part:
                        raise ProcessTombstoneError("clearance intent does not match request")
                    quarantine = _safe_internal_basename(intent.get("quarantine"),
                        prefix=".legacy-pid.", suffix=".quarantine")
                    existing_intent = True
                completion = dict(intent, event="legacy_tombstone_clearance_complete")
                if _read_exact_audit(audit_fd, complete_name, completion):
                    # A completion can never authorize ignoring a live pid.json.
                    # Such a state is inconsistent (or forged), so fail closed.
                    try:
                        os.stat(_PID_FILE, dir_fd=dir_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        pass
                    else:
                        raise ProcessTombstoneError("completion exists while tombstone is present")
                    qfd, qst = _open_private_named(quarantine, dir_fd=dir_fd)
                    try:
                        if hashlib.sha256(_read_regular_fd(qfd)).hexdigest() != expected_sha256:
                            raise ProcessTombstoneError("completed quarantine does not match")
                    finally: os.close(qfd)
                    _bind_clearance_namespace(directory, dir_fd, unit_st, lst,
                                              audit_fd, audit_st, audit_dir_name)
                    cst = os.stat(complete_name, dir_fd=audit_fd, follow_symlinks=False)
                    path = directory / audit_dir_name / complete_name
                    _bind_named_directory(audit_fd, cst, complete_name,
                                          message="completion pathname identity changed")
                    if not _same_inode(cst, os.stat(path, follow_symlinks=False)):
                        raise ProcessTombstoneError("returned audit pathname identity changed")
                    return path

                try:
                    pid_fd = os.open(_PID_FILE, os.O_RDONLY | nofollow, dir_fd=dir_fd)
                except FileNotFoundError:
                    if not existing_intent:
                        raise ProcessTombstoneError("tombstone absent and no matching intent")
                    qfd, _ = _open_private_named(quarantine, dir_fd=dir_fd)
                    try:
                        if hashlib.sha256(_read_regular_fd(qfd)).hexdigest() != expected_sha256:
                            raise ProcessTombstoneError("intended quarantine does not match")
                    finally: os.close(qfd)
                else:
                    try:
                        pst = os.fstat(pid_fd)
                        raw = _read_regular_fd(pid_fd)
                        # nlink 2 is accepted only for recovery of our already-published intent/link.
                        if (not stat.S_ISREG(pst.st_mode) or pst.st_nlink not in ({1, 2} if existing_intent else {1})
                                or hashlib.sha256(raw).hexdigest() != expected_sha256):
                            raise ProcessTombstoneError("tombstone identity or SHA-256 does not match")
                        if pst.st_nlink == 2:
                            try:
                                recovery_q = os.stat(quarantine, dir_fd=dir_fd, follow_symlinks=False)
                            except FileNotFoundError as exc:
                                raise ProcessTombstoneError("shared tombstone is not our recovery link") from exc
                            if (recovery_q.st_dev, recovery_q.st_ino) != (pst.st_dev, pst.st_ino):
                                raise ProcessTombstoneError("shared tombstone is not our recovery link")
                        try: data = json.loads(raw)
                        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                            raise ProcessTombstoneError("legacy tombstone is malformed") from exc
                        if (not isinstance(data, dict) or "token" in data
                                or type(data.get("unit")) is not str or data.get("unit") != unit
                                or type(data.get("generation")) is not int or data.get("generation") != generation):
                            raise ProcessTombstoneError("tombstone is not tokenless exact legacy metadata")
                        pathst = os.stat(_PID_FILE, dir_fd=dir_fd, follow_symlinks=False)
                        if (pathst.st_dev, pathst.st_ino) != (pst.st_dev, pst.st_ino):
                            raise ProcessTombstoneError("tombstone pathname identity changed")
                        os.fchmod(pid_fd, 0o600)
                        os.fsync(pid_fd)
                        if not existing_intent:
                            _atomic_private_json(audit_fd, intent_name, intent)
                            existing_intent = True
                        try:
                            os.link(_PID_FILE, quarantine, src_dir_fd=dir_fd,
                                    dst_dir_fd=dir_fd, follow_symlinks=False)
                            os.fsync(dir_fd)
                        except FileExistsError:
                            qst = os.stat(quarantine, dir_fd=dir_fd, follow_symlinks=False)
                            if (qst.st_dev, qst.st_ino) != (pst.st_dev, pst.st_ino):
                                raise ProcessTombstoneError("quarantine collision")
                        qfd, qst = _open_private_named(quarantine, dir_fd=dir_fd, allowed_nlinks=(1, 2))
                        try:
                            if (qst.st_dev, qst.st_ino) != (pst.st_dev, pst.st_ino):
                                raise ProcessTombstoneError("quarantine inode mismatch")
                            os.fsync(qfd)
                        finally: os.close(qfd)
                        # Last authorization check before the irreversible retirement.
                        _bind_clearance_namespace(directory, dir_fd, unit_st, lst,
                                                  audit_fd, audit_st, audit_dir_name)
                        current = os.stat(_PID_FILE, dir_fd=dir_fd, follow_symlinks=False)
                        if not _same_inode(pst, current):
                            raise ProcessTombstoneError("tombstone pathname identity changed")
                        os.unlink(_PID_FILE, dir_fd=dir_fd)
                        os.fsync(dir_fd)
                    finally: os.close(pid_fd)

                # Verify post-retirement invariant before publishing completion.
                qfd, qst = _open_private_named(quarantine, dir_fd=dir_fd)
                try:
                    if qst.st_nlink != 1 or hashlib.sha256(_read_regular_fd(qfd)).hexdigest() != expected_sha256:
                        raise ProcessTombstoneError("retired quarantine invariant failed")
                    os.fsync(qfd)
                finally: os.close(qfd)
                _atomic_private_json(audit_fd, complete_name, completion)
                # Completion is valid only after source absence and exact quarantine,
                # with all public namespace entries still bound to opened inodes.
                try:
                    os.stat(_PID_FILE, dir_fd=dir_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise ProcessTombstoneError("completion published while tombstone is present")
                _bind_clearance_namespace(directory, dir_fd, unit_st, lst,
                                          audit_fd, audit_st, audit_dir_name)
                cfd, cst = _open_private_named(complete_name, dir_fd=audit_fd)
                try:
                    path = directory / audit_dir_name / complete_name
                    if not _same_inode(cst, os.stat(path, follow_symlinks=False)):
                        raise ProcessTombstoneError("returned audit pathname identity changed")
                finally:
                    os.close(cfd)
                return path
            finally:
                os.close(audit_fd)
        finally:
            try: fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally: os.close(lock_fd)
    finally:
        os.close(dir_fd)

def clear_legacy_process_tombstone(
    unit_dir: str | Path, *, unit: str, generation: int,
    expected_sha256: str, workload_fully_gone_asserted: bool,
    operator: str, ticket: str,
) -> Path:
    """Serialize same-process callers, then use the file lock across processes."""
    with _LEGACY_CLEARANCE_THREAD_LOCK:
        try:
            return _clear_legacy_process_tombstone_locked(
                unit_dir, unit=unit, generation=generation,
                expected_sha256=expected_sha256,
                workload_fully_gone_asserted=workload_fully_gone_asserted,
                operator=operator, ticket=ticket,
            )
        except ProcessTombstoneError:
            raise
        except OSError as exc:
            raise ProcessTombstoneError("legacy tombstone namespace operation failed") from exc


def _unit_lock(unit_dir: Path):
    from .lock import unit_lock

    return unit_lock(unit_dir)

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


def liveness(
    unit_dir: str | Path,
    *,
    _pid_probe: Callable[[int], bool] = _pid_alive,
) -> Liveness:
    """Check the liveness of a supervised process by *unit_dir*.

    Returns ``"running"`` when *pid.json* names a live pid, or ``"stale"``
    when the file exists but the pid is missing, non-integer, or not alive.
    Raises :class:`FileNotFoundError` when *pid.json* is absent.  This
    primitive never returns ``"stopped"``; that label belongs to desired-state
    observation / the control plane, not to :func:`liveness`.
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
    if _pid_probe(pid):
        return Liveness(pid=pid, state="running")

    # Stale: pid.json exists but the process is gone. That file remains a
    # tombstone; :func:`stop` will not remove it.
    return Liveness(pid=pid, state="stale")
