from __future__ import annotations

import builtins
import json
import sys
from pathlib import Path
from typing import Final

import pytest

from datasource_kit.fleet import DesiredStateReconciler, WorkerControlPlane


_INSTALL_HINT: Final = (
    "build_control_plane_router requires the 'fastapi' extra: "
    "pip install datasource-kit[fastapi]"
)


def _block_fastapi_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "fastapi", raising=False)
    real_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "fastapi" or name.startswith("fastapi."):
            raise ModuleNotFoundError("No module named 'fastapi'")
        return real_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def _control_plane(tmp_path: Path) -> WorkerControlPlane:
    # A real reconciler on a tmp root: observe reads nonexistent pid files
    # (=> "stopped") and pause/resume only flip desired state on disk. Nothing
    # spawns a process, so the router is exercised honestly without side effects.
    reconciler = DesiredStateReconciler(tmp_path)
    return WorkerControlPlane(reconciler, ["eli", "saos"])


def test_importing_fastapi_adapter_when_fastapi_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the optional fastapi dependency is not importable and the adapter
    # module is not already cached.
    monkeypatch.delitem(sys.modules, "datasource_kit.adapters.fastapi", raising=False)
    _block_fastapi_imports(monkeypatch)

    # When: the core package and the adapter module are imported.
    import datasource_kit
    import datasource_kit.adapters as adapters
    import datasource_kit.adapters.fastapi as fastapi_adapter

    # Then: import succeeds without touching fastapi.
    assert datasource_kit.__name__ == "datasource_kit"
    assert adapters.__name__ == "datasource_kit.adapters"
    assert hasattr(fastapi_adapter, "build_control_plane_router")


def test_build_router_requires_fastapi_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Given: the adapter module is importable, but fastapi itself is not.
    from datasource_kit.adapters.fastapi import build_control_plane_router

    _block_fastapi_imports(monkeypatch)

    # When / Then: building the router raises the install hint before it ever
    # touches the control plane.
    with pytest.raises(ImportError) as exc_info:
        build_control_plane_router(_control_plane(tmp_path))
    assert str(exc_info.value) == _INSTALL_HINT


def test_router_observe_and_control_roundtrip(tmp_path: Path) -> None:
    # Given: a router mounted over a real control plane for two units.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from datasource_kit.adapters.fastapi import build_control_plane_router

    app = FastAPI()
    app.include_router(build_control_plane_router(_control_plane(tmp_path)))
    client = TestClient(app)

    # When: the whole fleet is observed before any control action.
    fleet = client.get("/workers")

    # Then: both declared units report stopped/disabled with no process.
    assert fleet.status_code == 200
    units = {row["unit"]: row for row in fleet.json()}
    assert set(units) == {"eli", "saos"}
    for row in units.values():
        assert row["desired"] == "disabled"
        assert row["actual"] == "stopped"
        assert row["pid"] is None
        assert row["generation"] == 0
        assert row["heartbeat"] == {}

    # When: eli is warm-paused, then observed on its own.
    paused = client.post("/workers/eli/pause")
    observed = client.get("/workers/eli")

    # Then: the keep-alive state flips to paused and the read-back agrees.
    assert paused.status_code == 200
    assert paused.json()["desired"] == "paused"
    assert observed.status_code == 200
    assert observed.json()["desired"] == "paused"
    # The control plane in this fixture has no spawn action, so warm-pause only
    # flips desired; nothing is spawned here and actual stays stopped.
    assert observed.json()["actual"] == "stopped"

    # When / Then: the remaining control verbs flip desired state as declared.
    assert client.post("/workers/eli/resume").json()["desired"] == "enabled"
    assert client.post("/workers/eli/disable").json()["desired"] == "disabled"
    assert client.post("/workers/eli/enable").json()["desired"] == "enabled"

    # And: saas (the other unit) is untouched by eli's transitions.
    assert client.get("/workers/saos").json()["desired"] == "disabled"


def test_router_unknown_unit_fails_closed_with_404(tmp_path: Path) -> None:
    # Given: a router over a fleet that declares only eli and saos.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from datasource_kit.adapters.fastapi import build_control_plane_router

    app = FastAPI()
    app.include_router(build_control_plane_router(_control_plane(tmp_path)))
    client = TestClient(app)

    # When / Then: both reads and writes for an undeclared unit fail closed.
    assert client.get("/workers/ghost").status_code == 404
    assert client.post("/workers/ghost/pause").status_code == 404
    assert client.post("/workers/ghost/resume").status_code == 404
    assert client.post("/workers/ghost/disable").status_code == 404


def test_router_declared_unit_with_corrupt_pid_reads_as_stopped(
    tmp_path: Path,
) -> None:
    # Given: a DECLARED unit whose pid.json is valid JSON but lacks a usable
    # pid (torn/partial write, custom spawn action, or manual edit).
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from datasource_kit.adapters.fastapi import build_control_plane_router

    unit_dir = tmp_path / "eli"
    unit_dir.mkdir()
    (unit_dir / "pid.json").write_text(
        json.dumps({"label": "eli", "started_at": 0.0}), encoding="utf-8"
    )

    app = FastAPI()
    app.include_router(build_control_plane_router(_control_plane(tmp_path)))
    client = TestClient(app)

    # When: the corrupt declared unit is read both ways.
    single = client.get("/workers/eli")
    fleet = client.get("/workers")

    # Then: corrupt runtime state on a declared unit is neither "unknown"
    # (no 404) nor a crash (no 500) -- it reads as a normal stopped
    # observation, and the single-unit and whole-fleet reads agree.
    assert single.status_code == 200
    assert single.json()["actual"] == "unknown"
    assert single.json()["pid"] is None
    assert fleet.status_code == 200
    eli = next(row for row in fleet.json() if row["unit"] == "eli")
    assert eli["actual"] == "unknown"
