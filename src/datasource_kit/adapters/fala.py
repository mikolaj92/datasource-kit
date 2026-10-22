"""Optional Fala execution and artifact adapters."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import Any, BinaryIO, Protocol, TypeVar

from datasource_kit.execution import ExecutionRequest
from datasource_kit.protocols import ArtifactStore

__all__ = ["FalaArtifactStore", "FalaExecutionBackend"]

_T = TypeVar("_T")

_INSTALL_HINT = (
    "FalaArtifactStore requires the 'fala' extra: pip install datasource-kit[fala]"
)
_EXECUTION_INSTALL_HINT = (
    "FalaExecutionBackend requires the 'fala' extra: pip install datasource-kit[fala]"
)
_EXECUTION_CAPABILITY_ERROR = (
    "FalaExecutionBackend requires Fala 0.7.28 or newer with the public "
    "record_in_process API"
)
_BLOB_KIND = "blob"
_PAYLOAD_FILENAME = "payload.bin"


class _FalaArtifactRef(Protocol):
    uri: str


class _FalaArtifactRefFactory(Protocol):
    def __call__(self, *, kind: str, uri: str) -> object: ...


class _FalaFileArtifactStore(Protocol):
    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
    ) -> _FalaArtifactRef: ...

    def resolve(self, artifact: object) -> Path: ...


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


class FalaArtifactStore(ArtifactStore):
    """Thin ``ArtifactStore`` adapter over Fala's file artifact store."""

    def __init__(self, root: str | PathLike[str]) -> None:
        try:
            from fala.artifacts import ArtifactRef, FileArtifactStore
        except ImportError as exc:
            raise ImportError(_INSTALL_HINT) from exc

        self._artifact_ref: _FalaArtifactRefFactory = ArtifactRef
        self._store: _FalaFileArtifactStore = FileArtifactStore(Path(root))

    def store(self, payload: bytes) -> str:
        """Store bytes in Fala and return the content-addressed artifact URI."""
        artifact = self._store.put_fileobj(
            kind=_BLOB_KIND,
            fileobj=BytesIO(payload),
            filename=_PAYLOAD_FILENAME,
        )
        return artifact.uri

    def resolve(self, ref: str) -> bytes:
        """Resolve a Fala artifact URI and return the payload bytes."""
        artifact = self._artifact_ref(kind=_BLOB_KIND, uri=ref)
        return self._store.resolve(artifact).read_bytes()
