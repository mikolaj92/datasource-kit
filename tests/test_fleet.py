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
    DESIRED_PAUSED,
    GENERATION_ENV,
    DesiredStateReconciler,
    Liveness,
    ProcessSpec,
    ReconcileOutcome,
    SpawnResult,
    StopOutcome,
    StopResult,
    SupervisorLockError,
    UnitObservation,
    WorkerControlPlane,
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


def test_spawn_rich_spec_logs_env_generation_and_metadata(tmp_path: Path) -> None:
    """Rich declarative launch stays domain-blind and persists no secrets."""
    stdout_path = tmp_path / "consumer" / "worker.log"
    stderr_path = tmp_path / "consumer" / "worker.err"
    spec = ProcessSpec(
        unit="opaque-unit",
        command=(
            PYTHON,
            "-c",
            "import os,sys,time; print(os.environ['OVERLAY'], flush=True); "
            f"print(os.environ['{GENERATION_ENV}'], file=sys.stderr, flush=True); "
            "time.sleep(30)",
        ),
        env={"BASE": "base", "SECRET": "do-not-persist"},
        env_overlay={"OVERLAY": "overlaid"},
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        probe_window=0.15,
        probe_sleep=0.02,
        pid_metadata={"consumer": {"session": "abc"}, "attempt": 4},
    )
    unit_dir = tmp_path / "state"
    result = spawn(spec, unit_dir=unit_dir, generation=7)
    try:
        assert result.alive is True
        assert stdout_path.read_text(encoding="utf-8").strip() == "overlaid"
        assert stderr_path.read_text(encoding="utf-8").strip() == "7"
        raw = (unit_dir / "pid.json").read_text(encoding="utf-8")
        data = json.loads(raw)
        assert data["consumer"] == {"session": "abc"}
        assert data["attempt"] == 4
        assert data["started_at"] == result.started_at
        assert "SECRET" not in raw
        assert "do-not-persist" not in raw
    finally:
        os.kill(result.pid, signal.SIGKILL)
        stop(unit_dir, timeout=0.05)
    assert not (unit_dir / "pid.json").exists()


def test_spawn_appends_logs(tmp_path: Path) -> None:
    log = tmp_path / "worker.log"
    log.write_text("existing\n", encoding="utf-8")
    spec = ProcessSpec(
        unit="append",
        command=(PYTHON, "-c", "print('new', flush=True)"),
        stdout_path=log,
        probe_window=0.5,
        probe_sleep=0.02,
    )
    result = spawn(spec, unit_dir=tmp_path / "state")
    assert result.alive is False
    assert log.read_text(encoding="utf-8") == "existing\nnew\n"


def test_spawn_injected_opener_descriptors_close_in_parent(tmp_path: Path) -> None:
    opened: list[object] = []

    def opener(path: Path):
        handle = path.open("a", encoding="utf-8")
        opened.append(handle)
        return handle

    spec = ProcessSpec(
        unit="fds",
        command=(PYTHON, "-c", "import time; time.sleep(30)"),
        stdout_path=tmp_path / "out.log",
        stderr_path=tmp_path / "err.log",
        opener=opener,
        probe_window=0.1,
        probe_sleep=0.02,
    )
    result = spawn(spec, unit_dir=tmp_path / "state")
    try:
        assert result.alive
        assert len(opened) == 2
        assert all(handle.closed for handle in opened)  # type: ignore[attr-defined]
    finally:
        os.kill(result.pid, signal.SIGKILL)


def test_spawn_immediate_exit_removes_preexisting_pid_metadata(tmp_path: Path) -> None:
    unit_dir = tmp_path / "state"
    _write_payload(unit_dir / "pid.json", {"pid": 999_999_999, "old": True})
    result = spawn(
        ProcessSpec(
            unit="dies",
            command=(PYTHON, "-c", "raise SystemExit(2)"),
            probe_window=0.5,
            probe_sleep=0.02,
            pid_metadata={"opaque": "never-written"},
        ),
        unit_dir=unit_dir,
    )
    assert result.alive is False
    assert not (unit_dir / "pid.json").exists()


def test_spawn_rejects_pid_metadata_collision_before_launch(tmp_path: Path) -> None:
    marker = tmp_path / "launched"
    spec = ProcessSpec(
        unit="bad-metadata",
        command=(PYTHON, "-c", f"open({str(marker)!r}, 'w').close()"),
        pid_metadata={"pid": 123},
    )
    with pytest.raises(ValueError, match="standard fields: pid"):
        spawn(spec, unit_dir=tmp_path / "state")
    assert not marker.exists()



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


def test_liveness_pidless_pid_json_is_stale(tmp_path: Path) -> None:
    """A valid-JSON pid.json missing the 'pid' key is stale, not a crash."""
    unit_dir = tmp_path / "pidless"
    _write_payload(unit_dir / "pid.json", {"label": "pidless", "started_at": 0.0})

    result = liveness(unit_dir)
    assert result.state == "stale"


def test_liveness_non_integer_pid_is_stale(tmp_path: Path) -> None:
    """A pid.json whose 'pid' is not an integer is stale, not a crash."""
    unit_dir = tmp_path / "badpid"
    _write_payload(unit_dir / "pid.json", {"pid": "not-a-number"})

    result = liveness(unit_dir)
    assert result.state == "stale"


def test_liveness_zombie_child_is_stale(tmp_path: Path) -> None:
    """A zombie child (exited but unreaped) reads as 'stale', not 'running'.

    ``os.kill(pid, 0)`` keeps succeeding for a zombie because the PID slot is
    still occupied. If ``liveness`` reported that as 'running', the reconciler
    would never respawn a crashed worker whose parent is the supervisor itself.
    """
    from datasource_kit.fleet import _pid_alive, _pid_is_zombie, stop_process

    pid = os.fork()
    if pid == 0:  # pragma: no cover - child hard-exits, never returns to pytest
        os._exit(0)

    try:
        # The child has exited but this process has not reaped it, so it is now
        # a zombie. Poll (rather than fixed-sleep) for the kernel transition.
        deadline = time.time() + 5.0
        while time.time() < deadline and not _pid_is_zombie(pid):
            time.sleep(0.02)

        assert _pid_is_zombie(pid) is True
        assert _pid_alive(pid) is False

        unit_dir = tmp_path / "zombie"
        _write_payload(unit_dir / "pid.json", {"pid": pid, "command": []})
        assert liveness(unit_dir).state == "stale"

        # stop_process on a zombie must return "already dead" at once, not wait
        # out its timeout on a SIGTERM that can never be acknowledged.
        outcome = stop_process(pid, timeout=5.0)
        assert outcome.signalled is False
        assert outcome.killed is False
    finally:
        os.waitpid(pid, 0)


def test_pid_alive_zombie_via_ps_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """_pid_alive excludes a Z-state process deterministically (no real fork).

    Keeps the zombie-exclusion logic under test on hosts where forking a real
    zombie is undesirable: ``os.kill`` succeeds (slot occupied) but ``ps``
    reports the state column.
    """
    from datasource_kit import fleet

    monkeypatch.setattr(fleet.os, "kill", lambda pid, sig: None)

    def _ps(state: str):
        def run(cmd: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(cmd, 0, stdout=state, stderr="")

        return run

    monkeypatch.setattr(fleet.subprocess, "run", _ps("Z+\n"))
    assert fleet._pid_alive(4242) is False

    monkeypatch.setattr(fleet.subprocess, "run", _ps("S\n"))
    assert fleet._pid_alive(4242) is True


def test_pid_alive_nonpositive_pid_is_dead() -> None:
    """A zero/negative pid is never alive (guards os.kill(0)/os.kill(-1))."""
    from datasource_kit.fleet import _pid_alive

    assert _pid_alive(0) is False
    assert _pid_alive(-1) is False


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
    assert dk.DESIRED_PAUSED is DESIRED_PAUSED
    assert dk.WorkerControlPlane is WorkerControlPlane
    assert dk.UnitObservation is UnitObservation


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
        rec.set_desired("eli", "frozen")


def test_honor_desired_state_policy() -> None:
    assert honor_desired_state({"desired": DESIRED_ENABLED}) is True
    assert honor_desired_state({"desired": DESIRED_DISABLED}) is False
    # Warm-paused wants the process running (reboot-warm), just like enabled.
    assert honor_desired_state({"desired": DESIRED_PAUSED}) is True
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


def test_merge_heartbeat_refuses_all_core_keys(tmp_path: Path) -> None:
    """A worker cannot forge its own liveness (actual/pid) via a heartbeat.

    ``actual`` and ``pid`` are reconciler-owned observations; if a heartbeat
    could overwrite them, a parked worker could lie itself into 'running' and
    the control-plane view would trust the lie.  Only non-core keys are stored.
    """
    rec = DesiredStateReconciler(tmp_path)
    rec.enable("eli")  # desired=enabled, generation 0, actual="unknown", pid=None
    before = rec.load_state("eli")

    merged = rec.merge_heartbeat(
        "eli",
        {
            "generation": 0,  # matches -> heartbeat accepted
            "actual": "running",  # forged liveness
            "pid": 4242,  # forged pid
            "desired": DESIRED_DISABLED,  # forged desired-state flip
            "schema_version": 99,  # forged schema
            "unit": "not-eli",  # forged identity
            "worker_status": "parked",  # the only non-core key -> stored
        },
    )
    # Every core key keeps its reconciler-owned value.
    assert merged["actual"] == before["actual"]
    assert merged["pid"] == before["pid"]
    assert merged["desired"] == DESIRED_ENABLED
    assert merged["schema_version"] == before["schema_version"]
    assert merged["unit"] == "eli"
    # Only the non-core key lands, and disk agrees with the returned merge.
    assert merged["worker_status"] == "parked"
    assert rec.load_state("eli") == merged


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


# ---------------------------------------------------------------------------
# DesiredStateReconciler: paused (warm keep-alive)
# ---------------------------------------------------------------------------


def test_set_desired_accepts_paused(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    rec.pause("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_PAUSED
    # The literal is accepted through set_desired too.
    rec.set_desired("saos", DESIRED_PAUSED)
    assert rec.load_state("saos")["desired"] == DESIRED_PAUSED


def test_pause_leaves_running_process_alive(tmp_path: Path) -> None:
    """A paused unit that is running is neither stopped nor re-spawned."""
    spawn_calls, spawn_action = _recording_spawn()
    stop_calls, stop_action = _recording_stop()
    rec = DesiredStateReconciler(
        tmp_path, spawn_action=spawn_action, stop_action=stop_action
    )
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))

    # Bring the unit up through the reconciler.  The recorded spawn points
    # pid.json at this live test process, so observe_actual reads it running
    # and the spawn branch records state["pid"].
    rec.enable("eli")
    up = rec.reconcile_unit(spec, honor_desired_state)
    assert up.action == "spawned"
    assert spawn_calls == [("eli", 1)]

    # Now pause it and reconcile again: warm keep-alive -- an up process is
    # left resident (noop), exactly like enabled.  Only the label differs.
    rec.pause("eli")
    outcome = rec.reconcile_unit(spec, honor_desired_state)
    assert outcome.action == "noop"
    assert outcome.desired == DESIRED_PAUSED
    assert outcome.actual == "running"
    assert outcome.alive is True
    assert outcome.pid == os.getpid()
    # Neither re-spawned nor stopped -- the process stays resident.
    assert spawn_calls == [("eli", 1)]  # unchanged: no second spawn
    assert stop_calls == []


def test_pause_respawns_stopped_unit(tmp_path: Path) -> None:
    """A paused unit that is down is re-spawned into an idle park (reboot-warm).

    This is the crux of warm-pause: after a supervisor/host restart a paused
    unit's process comes back *up* (want_running like enabled), and the
    consumer's own drive loop reads the ``paused`` label and self-idles.  The
    kit keeps the process warm; it does not know or care what idling means.
    """
    spawn_calls, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))

    rec.pause("eli")
    outcome = rec.reconcile_unit(spec, honor_desired_state)
    assert outcome.action == "spawned"
    assert outcome.desired == DESIRED_PAUSED
    assert spawn_calls == [("eli", 1)]


def test_set_desired_vocabulary_stays_narrow(tmp_path: Path) -> None:
    """Adding 'paused' does not widen the vocabulary to arbitrary values."""
    rec = DesiredStateReconciler(tmp_path)
    with pytest.raises(ValueError):
        rec.set_desired("eli", "held")


# ---------------------------------------------------------------------------
# WorkerControlPlane
# ---------------------------------------------------------------------------


def test_control_plane_pause_resume_writes_desired(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    cp = WorkerControlPlane(rec, ["eli", "saos"])

    cp.pause("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_PAUSED
    cp.resume("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_ENABLED
    cp.disable("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_DISABLED
    cp.enable("eli")
    assert rec.load_state("eli")["desired"] == DESIRED_ENABLED


def test_control_plane_observe_whole_fleet(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    cp = WorkerControlPlane(rec, ["eli", "saos"])
    rec.enable("eli")
    rec.pause("saos")

    snap = cp.observe()
    assert [u.unit for u in snap] == ["eli", "saos"]  # declared order
    by_unit = {u.unit: u for u in snap}
    assert by_unit["eli"].desired == DESIRED_ENABLED
    assert by_unit["saos"].desired == DESIRED_PAUSED
    # No live process -> stopped, pid None, empty (opaque) heartbeat.
    assert by_unit["eli"].actual == "stopped"
    assert by_unit["eli"].pid is None
    assert by_unit["eli"].heartbeat == {}


def test_control_plane_heartbeat_passthrough_opaque(tmp_path: Path) -> None:
    """Non-core state keys surface as an opaque heartbeat payload."""
    rec = DesiredStateReconciler(tmp_path)
    cp = WorkerControlPlane(rec, ["eli"])
    rec.enable("eli")
    # Generation 0 matches the never-spawned unit, so the heartbeat is merged.
    rec.merge_heartbeat(
        "eli",
        {
            "generation": 0,
            "worker_status": "parked",
            "last_window": "2026-07-01",
            "actual": "lies",  # a core key must never leak through
        },
    )
    obs = cp.observe_unit("eli")
    assert obs.heartbeat == {"worker_status": "parked", "last_window": "2026-07-01"}
    # Core keys never appear in the opaque payload.
    for core in ("schema_version", "unit", "desired", "actual", "generation", "pid"):
        assert core not in obs.heartbeat
    # And the reconciler-owned actual wins over any heartbeat-reported value.
    assert obs.actual == "stopped"


def test_control_plane_pid_reported_only_when_running(tmp_path: Path) -> None:
    spawn_calls, spawn_action = _recording_spawn()
    rec = DesiredStateReconciler(tmp_path, spawn_action=spawn_action)
    cp = WorkerControlPlane(rec, ["eli"])
    spec = ProcessSpec(unit="eli", command=(PYTHON, "-c", "pass"))
    rec.enable("eli")
    rec.reconcile_unit(spec, honor_desired_state)  # recorded spawn -> running

    obs = cp.observe_unit("eli")
    assert obs.actual == "running"
    assert obs.pid == os.getpid()


def test_control_plane_observe_is_read_only_on_stale_pid(tmp_path: Path) -> None:
    """Observe must classify a stale pid file without deleting it.

    Cleaning stale metadata is the supervisor's job, done under its lock during
    reconcile.  An out-of-process observer (e.g. the FastAPI adapter) that
    unlinked pid.json could race a pid the supervisor just refreshed and cause a
    double-spawn -- so observe reports 'stopped' and leaves the filesystem
    untouched.
    """
    rec = DesiredStateReconciler(tmp_path)
    cp = WorkerControlPlane(rec, ["eli"])
    rec.enable("eli")
    pid_file = rec.unit_dir("eli") / "pid.json"
    _write_payload(pid_file, {"pid": 999_999_999, "command": [], "started_at": 0})

    obs = cp.observe_unit("eli")
    assert obs.actual == "stopped"
    assert obs.pid is None
    # The stale pid file survives -- observe did not mutate the filesystem.
    assert pid_file.exists()


def test_control_plane_fails_closed_on_unknown_unit(tmp_path: Path) -> None:
    rec = DesiredStateReconciler(tmp_path)
    cp = WorkerControlPlane(rec, ["eli"])
    assert cp.units == ("eli",)
    for op in (cp.observe_unit, cp.pause, cp.resume, cp.enable, cp.disable):
        with pytest.raises(KeyError):
            op("saos")


def test_unit_observation_immutable() -> None:
    obs = UnitObservation(
        unit="eli",
        desired=DESIRED_ENABLED,
        actual="running",
        generation=1,
        pid=42,
        heartbeat={},
    )
    assert obs.pid == 42
    with pytest.raises(AttributeError):
        obs.pid = 7  # type: ignore[misc]
