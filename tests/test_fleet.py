"""Tests for datasource_kit.fleet process supervision primitives."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from datasource_kit.fleet import (
    Liveness,
    ProcessSpec,
    SpawnResult,
    StopResult,
    liveness,
    spawn,
    stop,
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
    assert dk.Liveness is Liveness
    assert dk.spawn is spawn
    assert dk.stop is stop
    assert dk.liveness is liveness
