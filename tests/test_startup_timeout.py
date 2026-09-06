"""Startup readiness and post-ACK crash probing have distinct time budgets (#78)."""

from __future__ import annotations

import math
from pathlib import Path

import pytest

from datasource_kit.fleet import ProcessSpec, process, spawn, spawn_process


@pytest.mark.parametrize("timeout", [0, -1, math.inf, math.nan])
def test_invalid_startup_timeout_creates_no_provenance(tmp_path: Path, timeout: float):
    with pytest.raises(ValueError, match="startup_timeout"):
        spec = ProcessSpec(unit="invalid", command=("unused",), startup_timeout=timeout)
        spawn(spec, unit_dir=tmp_path / "unit")
    assert not (tmp_path / "unit").exists()


@pytest.mark.parametrize("configured", [False, True])
def test_delayed_ready_uses_startup_budget_not_crash_probe(monkeypatch, configured):
    class Channel:
        timeout = 0.0

        def settimeout(self, timeout):
            self.timeout = timeout

        def recv(self, _count):
            # Simulate a READY arriving at t=2s without a real wall-clock wait.
            if self.timeout < 2:
                raise TimeoutError("wrapper startup exceeded crash-probe budget")
            return b"READY\n"

        def sendall(self, value):
            assert value == b"ACK\n"

        def close(self):
            pass

        def fileno(self):
            return 99

    class Child:
        pid = 12345

        def poll(self):
            return None

        def wait(self, timeout):
            return 0

    parent, child = Channel(), Channel()
    monkeypatch.setattr(process.socket, "socketpair", lambda: (parent, child))
    monkeypatch.setattr(process.subprocess, "Popen", lambda *args, **kwargs: Child())
    options = {"startup_timeout": 5} if configured else {}
    result = spawn_process(("unused",), probe_window=0, **options)
    assert result.alive
    assert parent.timeout == (5 if configured else 30)


@pytest.mark.parametrize("timeout", [0, -1, math.inf, math.nan])
def test_invalid_direct_timeout_does_not_open_channel(monkeypatch, timeout):
    def unexpected_channel():
        pytest.fail("invalid timeout must fail before socket creation")

    monkeypatch.setattr(process.socket, "socketpair", unexpected_channel)
    with pytest.raises(ValueError, match="startup_timeout"):
        spawn_process(("unused",), startup_timeout=timeout)


def test_timeout_retains_launch_intent_without_ack(monkeypatch, tmp_path):
    channels = []

    class Channel:
        closed = False

        def settimeout(self, value):
            assert value == 0.1

        def recv(self, count):
            raise TimeoutError("delayed wrapper")

        def sendall(self, value):
            pytest.fail("must not ACK after timeout")

        def close(self):
            self.closed = True

        def fileno(self):
            return 99

    class Child:
        pid = 12345

        def wait(self, timeout):
            return 0

    channels.extend([Channel(), Channel()])
    monkeypatch.setattr(process.socket, "socketpair", lambda: tuple(channels))
    monkeypatch.setattr(process.subprocess, "Popen", lambda *a, **kw: Child())
    unit_dir = tmp_path / "unit"
    spec = ProcessSpec(unit="timeout", command=("unused",), startup_timeout=0.1)
    with pytest.raises(TimeoutError):
        spawn(spec, unit_dir=unit_dir)
    import json

    intent = json.loads((unit_dir / "pid.json").read_text())
    assert intent["status"] == "launch_intent"
    assert intent["pid"] is None
    assert intent["token"]
    assert all(channel.closed for channel in channels)
