"""Domain-blind fleet process composition facade."""
from . import process as _process
from .control import UnitObservation, WorkerControlPlane
from .host import FleetHost, FleetPass
from .process import *
from .process import GENERATION_ENV
from .state import (
    DESIRED_DISABLED,
    DESIRED_ENABLED,
    DESIRED_PAUSED,
    DesiredStateReconciler,
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

# Stable diagnostic/monkeypatch seams retained for existing consumers/tests.
os = _process.os
subprocess = _process.subprocess
fcntl = _process.fcntl
_OWNED_HANDLES = _process._OWNED_HANDLES
_pid_alive = _process._pid_alive
_clear_legacy_process_tombstone_locked = _process._clear_legacy_process_tombstone_locked

# Process/state internals resolve the facade exports above at call time.  This
# keeps these aliases as authoritative injection seams after the module split.

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
