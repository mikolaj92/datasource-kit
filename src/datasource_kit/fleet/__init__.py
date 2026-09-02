"""Domain-blind fleet process composition facade."""
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import IO

from . import process as _process
from . import state as _state
from .control import UnitObservation, WorkerControlPlane
from .host import FleetHost, FleetPass
from .process import *
from .process import GENERATION_ENV
from .state import (
    DESIRED_DISABLED,
    DESIRED_ENABLED,
    DESIRED_PAUSED,
    ReconcileOutcome,
    ReconcilePolicy,
    SpawnAction,
    StopAction,
    SupervisorLockError,
    acquire_lock,
    honor_desired_state,
    lock_is_live,
    read_json,
    release_lock,
    write_json_atomic,
)


# Public facade wrappers keep dependency injection stable without mutating
# private module globals, so concurrent fleet calls remain independent.
def spawn_process(
    command: Sequence[str],
    *,
    cwd: str | None = None,
    env: Mapping[str, str] | None = None,
    stdout: int | IO[str] | None = None,
    stderr: int | IO[str] | None = None,
    probe_window: float = 1.5,
    probe_sleep: float = 0.25,
    _persist: Callable[[int, float, str], None] | None = None,
    _generation: int | None = None,
    _token: str | None = None,
) -> SpawnResult:
    """Launch through the stable facade seam."""
    return _process.spawn_process(
        command,
        cwd=cwd,
        env=env,
        stdout=stdout,
        stderr=stderr,
        probe_window=probe_window,
        probe_sleep=probe_sleep,
        _persist=_persist,
        _generation=_generation,
        _token=_token,
    )


def spawn(
    spec: ProcessSpec,
    *,
    unit_dir: str | Path | None = None,
    generation: int | None = None,
) -> SpawnResult:
    """Spawn while honoring a facade-level ``spawn_process`` injection."""
    return _process.spawn(
        spec,
        unit_dir=unit_dir,
        generation=generation,
        _spawn_process=spawn_process,
    )


def liveness(unit_dir: str | Path) -> Liveness:
    """Probe while honoring the stable facade-level PID seam."""
    return _process.liveness(unit_dir, _pid_probe=_pid_alive)


def _default_spawn_action(
    spec: ProcessSpec, generation: int, unit_dir: Path
) -> SpawnResult:
    return spawn(spec, unit_dir=unit_dir, generation=generation)


def _default_stop_action(unit_dir: Path) -> StopResult:
    return stop(unit_dir)


class DesiredStateReconciler(_state.DesiredStateReconciler):
    """Reconciler whose defaults resolve through the stable fleet facade."""

    def __init__(
        self,
        root: str | Path,
        *,
        spawn_action: SpawnAction | None = None,
        stop_action: StopAction | None = None,
        schema_version: int = 1,
    ) -> None:
        super().__init__(
            root,
            spawn_action=spawn_action or _default_spawn_action,
            stop_action=stop_action or _default_stop_action,
            schema_version=schema_version,
        )


# Stable diagnostic/monkeypatch seams retained for existing consumers/tests.
os = _process.os
subprocess = _process.subprocess
fcntl = _process.fcntl
_OWNED_HANDLES = _process._OWNED_HANDLES
_pid_alive = _process._pid_alive
_clear_legacy_process_tombstone_locked = _process._clear_legacy_process_tombstone_locked

# Compatibility wrapper keeps the former module-level monkeypatch seam useful.
def clear_legacy_process_tombstone(*args, **kwargs):
    with _process._LEGACY_CLEARANCE_THREAD_LOCK:
        try:
            return _clear_legacy_process_tombstone_locked(*args, **kwargs)
        except _process.ProcessTombstoneError:
            raise
        except OSError as exc:
            raise _process.ProcessTombstoneError(
                "legacy tombstone namespace operation failed"
            ) from exc

__all__ = [
    "DESIRED_DISABLED", "DESIRED_ENABLED", "DESIRED_PAUSED", "GENERATION_ENV",
    "DesiredStateReconciler", "FleetHost", "FleetPass", "LegacyProcessTombstoneInspection",
    "Liveness", "ProcessSpec", "ProcessTombstoneError", "ReconcileOutcome",
    "ReconcilePolicy", "SpawnAction", "SpawnResult", "StopAction", "StopOutcome",
    "StopResult", "SupervisorLockError", "UnitObservation", "WorkerControlPlane",
    "acquire_lock", "clear_legacy_process_tombstone", "clear_process_tombstone",
    "honor_desired_state", "inspect_legacy_process_tombstone", "liveness",
    "lock_is_live", "read_json", "release_lock", "spawn", "spawn_process", "stop",
    "stop_process", "write_json_atomic",
]
