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
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "Liveness",
    "ProcessSpec",
    "SpawnResult",
    "StopResult",
    "liveness",
    "spawn",
    "stop",
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


def spawn(spec: ProcessSpec, *, unit_dir: str | Path | None = None) -> SpawnResult:
    """Start a worker process described by *spec*.

    Writes ``pid.json`` metadata atomically to *unit_dir* (or
    ``spec.unit`` as a directory name when *unit_dir* is ``None``).

    Performs a fail-closed immediate-exit probe: if the child dies
    within the probe window the spawn is reported as failed
    (``alive=False``), never as "running and then crashed".
    """
    resolved = Path(unit_dir) if unit_dir is not None else Path(spec.unit)
    resolved.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ) if spec.env is None else spec.env

    proc = subprocess.Popen(
        list(spec.command),
        cwd=spec.cwd,
        env=env,
        start_new_session=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    started_at = time.time()
    _write_pid(resolved, proc.pid, spec.command, spec.label)

    # Immediate-exit probe: poll the process for a short window.
    # A process that dies within this window is reported as a failed spawn.
    deadline = started_at + _PROBE_WINDOW
    alive = True
    while time.time() < deadline:
        ret = proc.poll()
        if ret is not None:
            alive = False
            _clean_pid(resolved)
            break
        time.sleep(_PROBE_SLEEP)

    return SpawnResult(pid=proc.pid, started_at=started_at, alive=alive)


def stop(
    unit_dir: str | Path,
    *,
    timeout: float = _DEFAULT_TIMEOUT,
) -> StopResult:
    """Stop a supervised process identified by *unit_dir*.

    Sends SIGTERM to the process group, then escalates to SIGKILL
    after *timeout* seconds.  Stale *pid.json* (referencing a dead
    or recycled pid) is cleaned up silently and reported via
    ``cleaned=True``.
    """
    data = _read_pid(unit_dir)
    if data is None:
        raise FileNotFoundError(f"no pid.json in {unit_dir}")

    pid = int(data["pid"])
    cleaned = False

    # Stale-pid detection: if the pid is not alive, clean up and report.
    if not _pid_alive(pid):
        _clean_pid(unit_dir)
        return StopResult(pid=pid, signalled=False, killed=False, cleaned=True)

    signalled = False
    killed = False

    try:
        os.killpg(os.getpgid(pid), _STOP_SIGNAL)
        signalled = True
    except (ProcessLookupError, PermissionError, OSError):
        # Process already gone; nothing to signal.
        _clean_pid(unit_dir)
        return StopResult(pid=pid, signalled=False, killed=False, cleaned=True)

    # Wait for graceful exit.
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            _clean_pid(unit_dir)
            return StopResult(pid=pid, signalled=True, killed=False, cleaned=False)
        time.sleep(0.1)

    # Escalate to SIGKILL.
    try:
        os.killpg(os.getpgid(pid), _KILL_SIGNAL)
        killed = True
    except (ProcessLookupError, PermissionError, OSError):
        pass

    _clean_pid(unit_dir)
    return StopResult(pid=pid, signalled=True, killed=killed, cleaned=False)


def liveness(unit_dir: str | Path) -> Liveness:
    """Check the liveness of a supervised process by *unit_dir*.

    Returns ``"running"``, ``"stopped"``, or ``"stale"``.
    ``"stale"`` means *pid.json* exists but the referenced pid is not
    alive (or the pid file is corrupt) -- the consumer should clean up.
    """
    data = _read_pid(unit_dir)
    if data is None:
        raise FileNotFoundError(f"no pid.json in {unit_dir}")

    pid = int(data["pid"])
    if _pid_alive(pid):
        return Liveness(pid=pid, state="running")

    # Stale: pid.json exists but process is gone.  The consumer
    # is expected to call ``stop()`` to clean up stale metadata.
    return Liveness(pid=pid, state="stale")
