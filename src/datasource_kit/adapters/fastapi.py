"""Optional FastAPI adapter: an ``APIRouter`` over :class:`WorkerControlPlane`.

Mirrors the ``fala`` artifact-store adapter (and, in the consuming host, the
my-auth / my-usermanager pattern): the third-party dependency is imported
lazily inside the factory, so importing this module never pulls in FastAPI.
Install the extra to use it::

    pip install datasource-kit[fastapi]

The factory returns a bare :class:`~fastapi.APIRouter` that the host mounts
under its own prefix and security dependencies -- the adapter itself carries
no authentication and never touches worker processes directly. Every route
delegates to the control plane, which is the only thing that reads liveness or
flips desired state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, TypeVar

from datasource_kit.fleet import UnitObservation, WorkerControlPlane

if TYPE_CHECKING:
    from fastapi import APIRouter

__all__ = ["build_control_plane_router"]

_INSTALL_HINT = (
    "build_control_plane_router requires the 'fastapi' extra: "
    "pip install datasource-kit[fastapi]"
)

_T = TypeVar("_T")


def _observation_as_dict(obs: UnitObservation) -> dict[str, Any]:
    return {
        "unit": obs.unit,
        "desired": obs.desired,
        "actual": obs.actual,
        "generation": obs.generation,
        "pid": obs.pid,
        "heartbeat": dict(obs.heartbeat),
    }


def build_control_plane_router(control_plane: WorkerControlPlane) -> APIRouter:
    """Build an :class:`~fastapi.APIRouter` over *control_plane*.

    Routes (mount under any prefix via ``include_router``):

    * ``GET  /workers`` -- observe the whole declared fleet (read-only)
    * ``GET  /workers/{unit}`` -- observe one unit (read-only)
    * ``POST /workers/{unit}/pause`` -- warm-pause (keep-alive ``paused``)
    * ``POST /workers/{unit}/resume`` -- resume (keep-alive ``enabled``)
    * ``POST /workers/{unit}/enable`` -- enable (keep-alive ``enabled``)
    * ``POST /workers/{unit}/disable`` -- disable (keep-alive ``disabled``)

    Read routes are side-effect-free (they classify liveness without cleaning
    stale pid files). Write routes flip the desired keep-alive state through
    the control plane and never spawn or kill worker processes themselves. An
    unknown unit fails closed with HTTP 404. No authentication is applied
    here -- the host secures the router when it mounts it.
    """
    try:
        from fastapi import APIRouter, HTTPException
    except ImportError as exc:  # pragma: no cover - exercised via import block
        raise ImportError(_INSTALL_HINT) from exc

    router = APIRouter(tags=["workers"])

    def _guard(action: Callable[[str], _T], unit: str) -> _T:
        # 404 means "not in the declared fleet" -- decided by membership, not
        # by catching KeyError.  A deeper KeyError (e.g. corrupt runtime state
        # on a *declared* unit) must surface as 500, never a misleading
        # "unknown unit" 404.
        if unit not in control_plane.units:
            raise HTTPException(
                status_code=404, detail=f"unknown worker unit: {unit!r}"
            )
        return action(unit)

    @router.get("/workers")
    def observe_fleet() -> list[dict[str, Any]]:
        return [_observation_as_dict(obs) for obs in control_plane.observe()]

    @router.get("/workers/{unit}")
    def observe_unit(unit: str) -> dict[str, Any]:
        return _observation_as_dict(_guard(control_plane.observe_unit, unit))

    @router.post("/workers/{unit}/pause")
    def pause_unit(unit: str) -> dict[str, Any]:
        return _guard(control_plane.pause, unit)

    @router.post("/workers/{unit}/resume")
    def resume_unit(unit: str) -> dict[str, Any]:
        return _guard(control_plane.resume, unit)

    @router.post("/workers/{unit}/enable")
    def enable_unit(unit: str) -> dict[str, Any]:
        return _guard(control_plane.enable, unit)

    @router.post("/workers/{unit}/disable")
    def disable_unit(unit: str) -> dict[str, Any]:
        return _guard(control_plane.disable, unit)

    return router
