"""Tests for the generic, domain-blind FleetHost orchestration face."""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

from datasource_kit.fleet import (
    FleetHost,
    FleetPass,
    SupervisorLockError,
    read_json,
    write_json_atomic,
)


def host(tmp_path: Path, **overrides: object) -> FleetHost[str, str, object]:
    values = {
        "lock_path": tmp_path / "fleet.lock",
        "units": ("second", "first"),
        "admit": lambda units: {"count": len(units)},
        "reconcile": lambda unit: unit.upper(),
    }
    values.update(overrides)
    return FleetHost(**values)  # type: ignore[arg-type]


def test_reconcile_pass_is_unlocked_admits_first_and_preserves_order(
    tmp_path: Path,
) -> None:
    calls: list[object] = []
    fleet = host(
        tmp_path,
        admit=lambda units: calls.append(("admit", units)) or "allowed",
        reconcile=lambda unit: calls.append(("reconcile", unit)) or unit.upper(),
    )

    result = fleet.reconcile_pass()

    assert result == FleetPass(admission="allowed", results=("SECOND", "FIRST"))
    assert calls == [
        ("admit", ("second", "first")),
        ("reconcile", "second"),
        ("reconcile", "first"),
    ]
    assert not fleet.lock_path.exists()


def test_units_are_materialized_once_and_remain_opaque(tmp_path: Path) -> None:
    source = iter(({"id": 2}, {"id": 1}))
    seen: list[dict[str, int]] = []
    fleet = FleetHost(
        lock_path=tmp_path / "fleet.lock",
        units=source,
        admit=lambda units: tuple(unit["id"] for unit in units),
        reconcile=lambda unit: seen.append(unit) or unit["id"],
    )

    assert fleet.reconcile_pass() == FleetPass(admission=(2, 1), results=(2, 1))
    assert fleet.reconcile_pass() == FleetPass(admission=(2, 1), results=(2, 1))
    assert [unit["id"] for unit in seen] == [2, 1, 2, 1]


def test_admission_failure_is_fail_closed_and_run_once_releases(tmp_path: Path) -> None:
    reconciled: list[str] = []
    fleet = host(
        tmp_path,
        admit=lambda units: (_ for _ in ()).throw(RuntimeError("denied")),
        reconcile=lambda unit: reconciled.append(unit),
    )

    with pytest.raises(RuntimeError, match="denied"):
        fleet.run_once()

    assert reconciled == []
    assert not fleet.lock_path.exists()


def test_run_once_releases_when_reconcile_raises(tmp_path: Path) -> None:
    fleet = host(
        tmp_path,
        reconcile=lambda unit: (_ for _ in ()).throw(LookupError(unit)),
    )

    with pytest.raises(LookupError, match="second"):
        fleet.run_once()

    assert not fleet.lock_path.exists()


def test_serve_holds_one_lock_admits_every_pass_and_reports_before_sleep(
    tmp_path: Path,
) -> None:
    events: list[object] = []
    passes = 0
    lock_path = tmp_path / "fleet.lock"

    def admit(units: tuple[str, ...]) -> int:
        nonlocal passes
        assert lock_path.exists()
        passes += 1
        events.append(("admit", passes))
        return passes

    def on_pass(result: FleetPass[str, int]) -> None:
        assert read_json(lock_path)["pid"] == os.getpid()  # type: ignore[index]
        events.append(("callback", result.admission))

    def sleep(interval: float) -> None:
        assert lock_path.exists()
        events.append(("sleep", interval))

    fleet = host(tmp_path, units=("unit",), admit=admit, sleep=sleep)
    fleet.serve(
        interval=0.25,
        on_pass=on_pass,
        stop_condition=lambda: passes == 2,
    )

    assert events == [
        ("admit", 1),
        ("callback", 1),
        ("sleep", 0.25),
        ("admit", 2),
        ("callback", 2),
    ]
    assert not lock_path.exists()


def test_serve_callback_failure_propagates_and_releases_before_sleep(
    tmp_path: Path,
) -> None:
    sleeps: list[float] = []
    fleet = host(tmp_path, sleep=sleeps.append)

    with pytest.raises(ZeroDivisionError):
        fleet.serve(
            interval=1,
            on_pass=lambda result: 1 / 0,
        )

    assert sleeps == []
    assert not fleet.lock_path.exists()


def test_serve_rejects_nonpositive_interval_before_lock_or_payload(
    tmp_path: Path,
) -> None:
    payload_calls: list[bool] = []
    fleet = host(tmp_path, lock_payload=lambda: payload_calls.append(True) or {})

    for interval in (0, -1):
        with pytest.raises(ValueError, match="positive"):
            fleet.serve(interval=interval)

    assert payload_calls == []
    assert not fleet.lock_path.exists()


def test_lock_payload_is_opaque_but_pid_and_hostname_are_trustworthy(
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}
    fleet = host(
        tmp_path,
        units=(),
        lock_payload=lambda: {
            "managed_by": "consumer",
            "nested": {"arbitrary": True},
            "pid": -1,
            "hostname": "forged",
        },
    )

    fleet.serve(
        interval=1,
        on_pass=lambda result: observed.update(read_json(fleet.lock_path) or {}),
        stop_condition=lambda: True,
    )

    assert observed["managed_by"] == "consumer"
    assert observed["nested"] == {"arbitrary": True}
    assert observed["pid"] == os.getpid()
    assert observed["hostname"] == socket.gethostname()


def test_live_foreign_lock_fails_closed_without_callbacks(tmp_path: Path) -> None:
    lock_path = tmp_path / "fleet.lock"
    write_json_atomic(
        lock_path,
        {"pid": 12345, "hostname": "unprobeable-foreign-host"},
    )
    calls: list[str] = []
    fleet = host(
        tmp_path,
        admit=lambda units: calls.append("admit"),
        reconcile=lambda unit: calls.append(unit),
    )

    with pytest.raises(SupervisorLockError):
        fleet.run_once()

    assert calls == []
    assert lock_path.exists()


def test_dead_same_host_lock_is_stolen(tmp_path: Path) -> None:
    lock_path = tmp_path / "fleet.lock"
    write_json_atomic(lock_path, {"pid": 999_999_999, "hostname": socket.gethostname()})

    result = host(tmp_path, units=("opaque",)).run_once()

    assert result.results == ("OPAQUE",)
    assert not lock_path.exists()


def test_root_exports_are_canonical() -> None:
    from datasource_kit import FleetHost as RootFleetHost
    from datasource_kit import FleetPass as RootFleetPass

    assert RootFleetHost is FleetHost
    assert RootFleetPass is FleetPass


def test_serve_second_admission_failure_stops_before_second_reconcile(
    tmp_path: Path,
) -> None:
    admissions = 0
    reconciled: list[str] = []

    def admit(units: tuple[str, ...]) -> None:
        nonlocal admissions
        admissions += 1
        if admissions == 2:
            raise RuntimeError("inventory changed")

    fleet = host(
        tmp_path,
        units=("unit",),
        admit=admit,
        reconcile=lambda unit: reconciled.append(unit) or unit,
        sleep=lambda interval: None,
    )

    with pytest.raises(RuntimeError, match="inventory changed"):
        fleet.serve(interval=1)

    assert admissions == 2
    assert reconciled == ["unit"]
    assert not fleet.lock_path.exists()


@pytest.mark.parametrize("failure_site", ["stop", "sleep"])
def test_serve_stop_and_sleep_failures_propagate_and_release(
    tmp_path: Path, failure_site: str
) -> None:
    def fail() -> bool:
        raise OSError(failure_site)

    fleet = host(
        tmp_path,
        sleep=(lambda interval: fail())
        if failure_site == "sleep"
        else lambda interval: None,
    )

    with pytest.raises(OSError, match=failure_site):
        fleet.serve(
            interval=1,
            stop_condition=fail if failure_site == "stop" else None,
        )

    assert not fleet.lock_path.exists()
