"""ArtifactStore protocol and in-memory default.

ArtifactStore is the seam between the ingest runtime and persistence.
InMemoryArtifactStore is the zero-dependency default for tests and demos.
"""

from __future__ import annotations

from typing import Any, Iterable, Protocol, runtime_checkable

__all__ = ["ArtifactStore", "InMemoryArtifactStore"]


@runtime_checkable
class ArtifactStore(Protocol):
    """Persistence seam for ingested artifacts."""

    def store_one(self, artifact: Any) -> None: ...

    def full_replace(self, artifacts: Iterable[Any]) -> int:
        """Replace the entire store with ``artifacts``; return the count stored."""
        ...

    def list_ids(self) -> list[str]: ...

    def get(self, artifact_id: str) -> Any | None: ...


class InMemoryArtifactStore:
    """Zero-dependency in-memory store for demos and unit tests."""

    def __init__(self, *, id_field: str = "id") -> None:
        self._id_field = id_field
        self._data: dict[str, Any] = {}

    def _artifact_id(self, artifact: Any) -> str:
        if isinstance(artifact, dict):
            return str(artifact[self._id_field])
        return str(getattr(artifact, self._id_field))

    def store_one(self, artifact: Any) -> None:
        self._data[self._artifact_id(artifact)] = artifact

    def full_replace(self, artifacts: Iterable[Any]) -> int:
        items = list(artifacts)
        self._data = {self._artifact_id(a): a for a in items}
        return len(self._data)

    def list_ids(self) -> list[str]:
        return sorted(self._data.keys())

    def get(self, artifact_id: str) -> Any | None:
        return self._data.get(artifact_id)

    def __len__(self) -> int:
        return len(self._data)
