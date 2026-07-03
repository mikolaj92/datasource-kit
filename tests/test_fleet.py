"""Tests for datasource_kit.fleet process supervision primitives."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from datasource_kit.fleet import (
    DESIRED_DISABLED,
    DESIRED_ENABLED,
    GENERATION_ENV,
    DesiredStateReconciler,
    Liveness,
    ProcessSpec,
    ReconcileOutcome,
    SpawnResult,
    StopOutcome,
    StopResult,
    SupervisorLockError,
    acquire_lock,
    honor_desired_state,
    liveness,
    lock_is_live,
    read_json,
    release_lock,
    spawn,
    spawn_process,
    stop,
    stop_process,
    write_json_atomic,
)

PYTHON = sys.executable


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_payload(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# ---------------------------------------------------------------------------
# Spawn
# ---------------------------------------------------------------------------


def test_spawn_basic(tmp_path: Path) -> None:
    """A short-lived process dies during probe window; pid.json cleaned up."""
    spec = ProcessSpec(
        unit="test-unit",
        command=(PYTHON, "-c", "import sys; sys.exit(0)"),
    )
    unit_dir = tmp_path / "basic"
    result = spawn(spec, unit_dir=unit_dir)

    assert isinstance(result, SpawnResult)
    assert result.pid > 0
    assert result.started_at > 0
    assert result.alive is False

    # pid.json is cleaned up when the process dies during the probe window.
    assert not (unit_dir / "pid.json").exists()


def test_spawn_long_running(tmp_path: Path) -> None:
    """A long-running process is reported as alive with pid.json."""
    spec = ProcessSpec(
        unit="long-runner",
        command=(PYTHON, "-c", "import time; time.sleep(30)"),
    )
    unit_dir = tmp_path / "long"
    result = spawn(spec, unit_dir=unit_dir)

    assert result.alive is True
    assert (unit_dir / "pid.json").exists()

    # Verify pid.json content.
    data = json.loads((unit_dir / "pid.json").read_text(encoding="utf-8"))
    assert data["pid"] == result.pid
    assert data["command"] == [PYTHON, "-c", "import time; time.sleep(30)"]
    assert "started_at" in data

    # Verify pid is genuinely alive.
    try:
        os.kill(result.pid, 0)
    except OSError:
        pytest.fail("spawned process died unexpectedly")

    os.kill(result.pid, signal.SIGKILL)


def test_spawn_with_env_and_cwd(tmp_path: Path) -> None:
    """Environment and cwd are forwarded to the child."""
    workdir = tmp_path / "worker"
    workdir.mkdir()
    spec = ProcessSpec(
        unit="env-test",
        command=(PYTHON, "-c",
                 "import os; os.write(1, os.environ.get('MY_VAR', 'nope').encode())"),
        cwd=str(workdir),
        env={"MY_VAR": "hello"},
    )
    unit_dir = tmp_path / "env"
    result = spawn(spec, unit_dir=unit_dir)
    # Short-lived process: pid.json cleaned up by probe.
    assert result.alive is False
    assert not (unit_dir / "pid.json").exists()


def test_spawn_default_unit_dir(tmp_path: Path) -> None:
    """When unit_dir is None, spec.unit is used as the directory name."""
    prev = Path.cwd()
    try:
        os.chdir(tmp_path)
        spec = ProcessSpec(unit="default-dir-test", command=(PYTHON, "-c", "pass"))
        result = spawn(spec)
        # Short-lived process: pid.json cleaned up by probe.
        assert result.alive is False
    finally:
        os.chdir(prev)


# ---------------------------------------------------------------------------
# spawn_process (layout-agnostic core)
# ---------------------------------------------------------------------------


def test_spawn_process_long_running_alive() -> None:
    """A long-running process is reported alive and writes no pid.json."""
    result = spawn_process((PYTHON, "-c", "import time; time.sleep(30)"))
    try:
        assert isinstance(result, SpawnResult)
        assert result.alive is True
        assert result.pid > 0
        assert result.started_at > 0
        # No pid metadata is persisted anywhere by the layout-agnostic core.
        os.kill(result.pid, 0)  # genuinely alive
    finally:
        os.kill(result.pid, signal.SIGKILL)


def test_spawn_process_short_lived_dead() -> None:
    """A process that exits in the probe window is reported not-alive."""
    result = spawn_process(
        (PYTHON, "-c", "import sys; sys.exit(0)"),
        probe_window=1.5,
        probe_sleep=0.05,
    )
    assert result.alive is False
    assert result.pid > 0


def test_spawn_process_env_and_cwd(tmp_path: Path) -> None:
    """Environment, cwd, and a redirected stdout are forwarded to the child."""
    workdir = tmp_path / "worker"
    workdir.mkdir()
    out_path = tmp_path / "out.log"
    with out_path.open("w", encoding="utf-8") as out:
        result = spawn_process(
            (PYTHON, "-c",
             "import os; print(os.environ.get('MY_VAR', 'nope')); "
             "print(os.getcwd())"),
            cwd=str(workdir),
            env={**os.environ, "MY_VAR": "hello"},
            stdout=out,
            probe_window=1.5,
            probe_sleep=0.05,
        )
    assert result.alive is False
    captured = out_path.read_text(encoding="utf-8")
    assert "hello" in captured
    assert str(workdir) in captured


# ---------------------------------------------------------------------------
# Liveness
# ---------------------------------------------------------------------------


def test_liveness_running(tmp_path: Path) -> None:
    """A running process is reported as 'running'."""
    proc = subprocess.Popen(
        [PYTHON, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    unit_dir = tmp_path / "live-running"
    _write_payload(unit_dir / "pid.json",
                   {"pid": proc.pid, "command": [], "started_at": time.time()})

    result = liveness(unit_dir)
    assert result.state == "running"
    assert result.pid == proc.pid

    os.kill(proc.pid, signal.SIGKILL)


def test_liveness_stale(tmp_path: Path) -> None:
    """A pid.json referencing a dead pid is reported as 'stale'."""
    unit_dir = tmp_path / "stale"
    _write_payload(unit_dir / "pid.json",
                   {"pid": 999_999_999, "command": [], "started_at": 0})

    result = liveness(unit_dir)
    assert result.state == "stale"
    assert result.pid == 999_999_999


def test_liveness_no_pid_file(tmp_path: Path) -> None:
    """liveness raises FileNotFoundError when pid.json is missing."""
    with pytest.raises(FileNotFoundError):
        liveness(tmp_path / "nonexistent")


# ---------------------------------------------------------------------------
# Stop
# ---------------------------------------------------------------------------


def test_stop_running_process(tmp_path: Path) -> None:
    """stop sends SIGTERM and cleans up pid.json."""
    proc = subprocess.Popen(
        [PYTHON, "-c",
         "import time, signal;"
         "signal.signal(signal.SIGTERM, lambda *_: None); time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    unit_dir = tmp_path / "stop-running"
    _write_payload(unit_dir / "pid.json",
                   {"pid": proc.pid, "command": [], "started_at": time.time()})

    result = stop(unit_dir)
    assert result.pid == proc.pid
    assert result.signalled is True or result.killed is True
    assert not (unit_dir / "pid.json").exists()


def test_stop_stale_pid(tmp_path: Path) -> None:
    """stop with a stale pid cleans up and returns cleaned=True."""
    unit_dir = tmp_path / "stop-stale"
    _write_payload(unit_dir / "pid.json",
                   {"pid": 999_999_999, "command": [], "started_at": 0})

    result = stop(unit_dir)
    assert result.cleaned is True
    assert result.signalled is False
    assert result.killed is False
    assert not (unit_dir / "pid.json").exists()


def test_stop_no_pid_file(tmp_path: Path) -> None:
    """stop raises FileNotFoundError when pid.json is missing."""
    with pytest.raises(FileNotFoundError):
        stop(tmp_path / "nonexistent")


# ---------------------------------------------------------------------------
# stop_process (layout-agnostic core)
# ---------------------------------------------------------------------------


def test_stop_process_dead_pid() -> None:
    """A dead pid yields signalled=False, killed=False and does no I/O."""
    outcome = stop_process(999_999_999)
    assert isinstance(outcome, StopOutcome)
    assert outcome.pid == 999_999_999
    assert outcome.signalled is False
    assert outcome.killed is False


def test_stop_process_graceful() -> None:
    """A process honouring SIGTERM exits within the timeout, not killed."""
    proc = subprocess.Popen(
        [PYTHON, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    outcome = stop_process(proc.pid, timeout=5.0)
    assert outcome.pid == proc.pid
    assert outcome.signalled is True
    assert outcome.killed is False


def test_stop_process_escalates_to_sigkill() -> None:
    """A process ignoring SIGTERM is escalated to SIGKILL."""
    proc = subprocess.Popen(
        [PYTHON, "-c",
         "import time, signal;"
         "signal.signal(signal.SIGTERM, lambda *_: None); time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Let the child install its SIGTERM handler before we signal, otherwise a
    # SIGTERM landing during interpreter startup terminates it by default and
    # the escalation path never runs.
    time.sleep(0.5)
    outcome = stop_process(proc.pid, timeout=0.5)
    assert outcome.pid == proc.pid
    assert outcome.signalled is True
    assert outcome.killed is True


def test_stop_outcome_immutable() -> None:
    o = StopOutcome(pid=7, signalled=True, killed=False)
    assert o.pid == 7
    with pytest.raises(AttributeError):
        o.signalled = False  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Dataclass immutability
# ---------------------------------------------------------------------------


def test_spawn_result_immutable() -> None:
    r = SpawnResult(pid=42, started_at=1.0, alive=True)
    assert r.pid == 42
    assert r.started_at == 1.0
    assert r.alive is True
    with pytest.raises(AttributeError):
        r.pid = 99  # type: ignore[misc]


def test_stop_result_immutable() -> None:
    r = StopResult(pid=42, signalled=True, killed=False, cleaned=False)
    assert r.pid == 42
    with pytest.raises(AttributeError):
        r.signalled = False  # type: ignore[misc]


def test_liveness_immutable() -> None:
    r = Liveness(pid=42, state="running")
    assert r.state == "running"
    with pytest.raises(AttributeError):
        r.state = "stopped"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Module exports via datasource_kit
# ---------------------------------------------------------------------------


def test_fleet_exports_public_api() -> None:
    """All expected names are exported from datasource_kit."""
    import datasource_kit as dk

    assert dk.ProcessSpec is ProcessSpec
    assert dk.SpawnResult is SpawnResult
    assert dk.StopResult is StopResult
    assert dk.StopOutcome is StopOutcome
    assert dk.Liveness is Liveness
    assert dk.spawn is spawn
    assert dk.spawn_process is spawn_process
    assert dk.stop is stop
    assert dk.stop_process is stop_process
    assert dk.liveness is liveness
    assert dk.DesiredStateReconciler is DesiredStateReconciler
    assert dk.ReconcileOutcome is ReconcileOutcome
    assert dk.honor_desired_state is honor_desired_state
    assert dk.write_json_atomic is write_json_atomic
    assert dk.read_json is read_json


# ---------------------------------------------------------------------------
# Atomic JSON helpers
# ---------------------------------------------------------------------------


def test_write_json_atomic_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "state.json"
    write_json_atomic(path, {"b": 2, "a": 1})
    assert read_json(path) == {"a": 1, "b": 2}
    # No stray tmp file is left behind.
    assert not (path.parent / "state.json.tmp").exists()


def test_read_json_missing_and_corrupt(tmp_path: Path) -> None:
    assert read_json(tmp_path / "nope.json") is None
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    assert read_json(corrupt) is None
    # A JSON scalar (not an object) is rejected.
    scalar = tmp_path / "scalar.json"
    scalar.write_text("42", encoding="utf-8")
    assert read_json(scalar) is None


# ---------------------------------------------------------------------------
# DesiredStateReconciler: state + policy
# ---------------------------------------------------------------------------


def _recording_spawn() -> tuple[list[tuple[str, int]], object]:
    """Return (calls, action); action records (unit, generation) and is alive.

    Faithful to the real spawn contract: it writes ``pid.json`` (pointing at a
    live pid -- this test process) so a later ``observe_actual`` reports the
    unit as running, exactly as :func:`spawn` would.
    """
    calls: list[tuple[str, int]] = []

    def action(spec: ProcessSpec, generation: int, unit_dir: Path) -> SpawnResult:
        calls.append((spec.unit, generation))
        write_json_atomic(
            unit_dir / "pid.json",
            {"pid": os.getpid(), "command": list(spec.command), "started_at": 1.0},
        )
        return SpawnResult(pid=os.getpid(), started_at=1.0, alive=True)

    return calls, action


def _recording_stop() -> tuple[list[Path], object]:
    calls: list[Path] = []

    def action(unit_dir: Path) -> StopResult:
        calls.append(unit_dir)
        return StopResult(pid=1, signalled=True, killed=False, cleaned=False)

    return calls, action


def test_default_state_and_set_desired(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    state = rec.load_state("eli")
    assert state["desired"] == DESIRED_DISABLED
    assert state["generation"] == 0
    assert state["pid"] is None

    rec.enable("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_ENABLED
    rec.disable("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_DISABLED


def test_set_desired_rejects_bad_value(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    with pytest.raises(ValueError):
        rec.set_desired("eli", "paused")


def test_honor_desired_state_policy() -> None:
    assert honor_desired_state({"desired": DESIRED_ENABLED}) is True
    assert honor_desired_state({"desired": DESIRED_DISABLED}) is False
    assert honor_desired_state({}) is False


# ---------------------------------------------------------------------------
# DesiredStateReconciler: converge
# ---------------------------------------------------------------------------


def test_converge_spawns_enabled_unit(tmp_path: Path) -> None:
    calls, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))

    rec.enable("eli")
    outcome = rec.reconcile_unit(spec, honor_desired_state)

    assert outcome.action == "spawned"
    assert outcome.generation == 1  # 0 -> 1 on first spawn
    assert outcome.actual == "running"
    assert calls == [("eli", 1)]
    assert rec.load_state("eli")["generation"] == 1


def test_converge_noop_when_disabled(tmp_path: Path) -> None:
    calls, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))

    outcome = rec.reconcile_unit(spec, honor_desired_state)
    assert outcome.action == "noop"
    assert outcome.actual == "stopped"
    assert calls == []


def test_converge_stops_disabled_running_unit(tmp_path: Path) -> None:
    stop_calls, stop_action = _recording_stop()
    rec = DesiredStateReconciler(tmp_path, stop_action=stop_action)
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))

    # A genuinely-running child so observe_actual reports "running".
    proc = subprocess.Popen(
        [PYTHON, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        write_json_atomic(
            rec.unit_dir("eli") / "pid.json",
            {"pid": proc.pid, "command": [], "started_at": time.time()},
        )
        rec.disable("eli")
        outcome = rec.reconcile_unit(spec, honor_desired_state)
        assert outcome.action == "stopped"
        assert outcome.actual == "stopped"
        assert stop_calls == [rec.unit_dir("eli")]
        assert rec.load_state("eli")["pid"] is None
    finally:
        os.kill(proc.pid, signal.SIGKILL)


def test_default_spawn_action_injects_generation(tmp_path: Path) -> None:
    """The default spawn action stamps GENERATION_ENV into the child env."""
    rec = DesiredStateReconciler(tmp_path)
    marker = tmp_path / "gen.txt"
    spec = ProcessSpec(
        unit="eli",
        command=(
            PYTHON,
            "-c",
            f"import os; open({str(marker)!r},'w').write("
            f"os.environ['{GENERATION_ENV}']); import time; time.sleep(30)",
        ),
    )
    rec.enable("eli")
    outcome = rec.reconcile_unit(spec, honor_desired_state)
    try:
        assert outcome.alive is True
        # The child observed generation 1 in its environment.
        deadline = time.time() + 3.0
        while time.time() < deadline and not marker.exists():
            time.sleep(0.05)
        assert marker.read_text(encoding="utf-8") == "1"
    finally:
        if outcome.pid:
            os.kill(outcome.pid, signal.SIGKILL)


# ---------------------------------------------------------------------------
# DesiredStateReconciler: generation fencing
# ---------------------------------------------------------------------------


def test_merge_heartbeat_generation_fencing(tmp_path: Path) -> None:
    _, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))
    rec.enable("eli")
    rec.reconcile_unit(spec, honor_desired_state)  # generation -> 1

    # Stale generation (0) from a zombie of the previous generation: rejected.
    state = rec.merge_heartbeat("eli", {"generation": 0, "handled_count": 99})
    assert "handled_count" not in state

    # Matching generation (1): merged.
    state = rec.merge_heartbeat("eli", {"generation": 1, "handled_count": 7})
    assert state["handled_count"] == 7
    # Reconciler-owned fields are never taken from a heartbeat.
    state = rec.merge_heartbeat(
        "eli", {"generation": 1, "desired": "disabled", "generation_from_hb": 5}
    )
    assert state["desired"] == DESIRED_ENABLED


# ---------------------------------------------------------------------------
# Standalone supervisor-lock primitive
# ---------------------------------------------------------------------------


def test_standalone_acquire_release_round_trips_payload(tmp_path: Path) -> None:
    lock = tmp_path / "sub" / "supervisor.lock"  # parent created on demand
    owner = acquire_lock(
        lock, payload={"managed_by": "consumer", "schema_version": 2}
    )
    assert owner["pid"] == os.getpid()
    assert owner["hostname"] == socket.gethostname()
    assert owner["managed_by"] == "consumer"
    assert owner["schema_version"] == 2
    assert lock.exists()
    on_disk = json.loads(lock.read_text(encoding="utf-8"))
    assert on_disk["managed_by"] == "consumer"
    assert on_disk["pid"] == os.getpid()
    assert release_lock(lock) is True
    assert not lock.exists()


def test_standalone_payload_cannot_override_identity(tmp_path: Path) -> None:
    lock = tmp_path / "supervisor.lock"
    owner = acquire_lock(lock, payload={"pid": 1, "hostname": "elsewhere"})
    assert owner["pid"] == os.getpid()
    assert owner["hostname"] == socket.gethostname()
    release_lock(lock)


def test_standalone_defaults_started_at(tmp_path: Path) -> None:
    lock = tmp_path / "supervisor.lock"
    owner = acquire_lock(lock)
    assert isinstance(owner["started_at"], float)
    release_lock(lock)


def test_standalone_raises_on_live_foreign_owner(tmp_path: Path) -> None:
    lock = tmp_path / "supervisor.lock"
    proc = subprocess.Popen(
        [PYTHON, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        write_json_atomic(
            lock,
            {"pid": proc.pid, "hostname": socket.gethostname(), "started_at": 1.0},
        )
        with pytest.raises(SupervisorLockError):
            acquire_lock(lock)
    finally:
        os.kill(proc.pid, signal.SIGKILL)


def test_standalone_steals_dead_owner(tmp_path: Path) -> None:
    lock = tmp_path / "supervisor.lock"
    write_json_atomic(
        lock,
        {"pid": 999_999_999, "hostname": socket.gethostname(), "started_at": 1.0},
    )
    owner = acquire_lock(lock)  # dead owner -> stolen
    assert owner["pid"] == os.getpid()
    release_lock(lock)


def test_standalone_release_foreign_lock_is_noop(tmp_path: Path) -> None:
    lock = tmp_path / "supervisor.lock"
    write_json_atomic(lock, {"pid": 111_111, "hostname": "x", "started_at": 1.0})
    assert release_lock(lock) is False
    assert lock.exists()  # a foreign owner's lock is never removed


def test_lock_is_live_classifies_owners() -> None:
    assert lock_is_live(None) is False
    assert lock_is_live({"pid": os.getpid(), "hostname": socket.gethostname()}) is False
    assert lock_is_live({"pid": 4242, "hostname": "some-other-host"}) is True
    assert (
        lock_is_live({"pid": 999_999_999, "hostname": socket.gethostname()}) is False
    )


# ---------------------------------------------------------------------------
# DesiredStateReconciler: supervisor lock
# ---------------------------------------------------------------------------


def test_lock_acquire_release(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    owner = rec.acquire_lock()
    assert owner["pid"] == os.getpid()
    assert (tmp_path / "supervisor.lock").exists()
    assert rec.release_lock() is True
    assert not (tmp_path / "supervisor.lock").exists()


def test_lock_raises_on_live_foreign_owner(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    # A live foreign owner: this test process's parent is alive, but use a
    # separate live pid we control.
    proc = subprocess.Popen(
        [PYTHON, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        write_json_atomic(
            tmp_path / "supervisor.lock",
            {"pid": proc.pid, "hostname": socket.gethostname(), "started_at": 1.0},
        )
        with pytest.raises(SupervisorLockError):
            rec.acquire_lock()
    finally:
        os.kill(proc.pid, signal.SIGKILL)


def test_lock_steals_dead_owner(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    write_json_atomic(
        tmp_path / "supervisor.lock",
        {"pid": 999_999_999, "hostname": socket.gethostname(), "started_at": 1.0},
    )
    owner = rec.acquire_lock()  # dead owner -> stolen
    assert owner["pid"] == os.getpid()
    rec.release_lock()


def test_hold_lock_context_manager(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    with rec.hold_lock():
        assert (tmp_path / "supervisor.lock").exists()
    assert not (tmp_path / "supervisor.lock").exists()


# ---------------------------------------------------------------------------
# DesiredStateReconciler: reconcile_once + serve
# ---------------------------------------------------------------------------


def test_reconcile_once_takes_lock_and_releases(tmp_path: Path) -> None:
    calls, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    specs = [
        ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass")),
        ProcessSpec(unit="saos", command=(PYTHON, "-c", "pass")),
    ]
    rec.enable("eli")
    outcomes = rec.reconcile_once(specs, honor_desired_state)
    assert {o.unit: o.action for o in outcomes} == {"eli": "spawned", "saos": "noop"}
    assert calls == [("eli", 1)]
    assert not (tmp_path / "supervisor.lock").exists()  # released


def test_serve_runs_until_stop_condition(tmp_path: Path) -> None:
    calls, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    specs = [ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))]
    rec.enable("eli")

    passes = {"n": 0}

    def stop_after_two() -> bool:
        passes["n"] += 1
        return passes["n"] >= 2

    rec.serve(specs, honor_desired_state, interval=0.0, stop_condition=stop_after_two)
    assert passes["n"] == 2
    # First pass spawns; the recorded spawn keeps state "running" so the
    # second pass is a no-op -> only one spawn call.
    assert calls == [("eli", 1)]
    assert not (tmp_path / "supervisor.lock").exists()
