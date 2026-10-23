from __future__ import annotations

from collections.abc import Callable, Mapping
from os import PathLike
from typing import Any, Protocol, TypeVar

from datasource_kit.execution import ExecutionRequest

__all__ = ["FalaExecutionBackend"]

_T = TypeVar("_T")

_EXECUTION_INSTALL_HINT = (
    "FalaExecutionBackend requires the 'fala' extra: pip install datasource-kit[fala]"
)
_EXECUTION_CAPABILITY_ERROR = (
    "FalaExecutionBackend requires Fala 0.7.28 or newer with the public "
    "record_in_process API"
)


class _RecordInProcess(Protocol):
    def __call__(
        self,
        *,
        db_path: str | PathLike[str],
        run_id: str,
        process_id: str,
        operation: Callable[[], _T],
        inputs: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> _T: ...


class FalaExecutionBackend:
    """Record one synchronous callback through Fala's durable journal API.

    The referenced Fala run must already exist. Run lifecycle, identifiers,
    retries, and result-lifetime policy remain the caller's responsibility.
    """

    def __init__(self, db_path: str | PathLike[str]) -> None:
        try:
            import fala
        except ImportError as exc:
            raise ImportError(_EXECUTION_INSTALL_HINT) from exc

        try:
            record_in_process = fala.record_in_process
        except AttributeError as exc:
            raise RuntimeError(_EXECUTION_CAPABILITY_ERROR) from exc

        if not callable(record_in_process):
            raise RuntimeError(_EXECUTION_CAPABILITY_ERROR)  # noqa: TRY004
        self._db_path = db_path
        self._record_in_process: _RecordInProcess = record_in_process

    def execute(
        self,
        request: ExecutionRequest,
        operation: Callable[[], _T],
    ) -> _T:
        """Delegate the request and callback unchanged to Fala."""
        return self._record_in_process(
            db_path=self._db_path,
            run_id=request.run_id,
            process_id=request.execution_id,
            operation=operation,
            inputs=request.inputs,
            metadata=request.metadata,
        )
