"""Fala artifact backend adapter."""

from __future__ import annotations

from io import BytesIO
from os import PathLike
from pathlib import Path
from typing import BinaryIO, Protocol

from datasource_kit.protocols import ArtifactStore

__all__ = ["FalaArtifactStore"]

_INSTALL_HINT = (
    "FalaArtifactStore requires the 'fala' extra: "
    "pip install datasource-kit[fala]"
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
