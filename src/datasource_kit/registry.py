"""Name-keyed registry for datasource objects.

Generic over whatever a project registers: batch ``DataSource`` lookups,
scraper ``IngestActor`` instances, or plain manifests. The registry only relies
on each entry exposing a ``name`` attribute, or on an explicit name passed to
``register``.
"""

from __future__ import annotations

from typing import Generic, Iterator, Protocol, TypeVar, runtime_checkable


@runtime_checkable
class _Named(Protocol):
    name: str


T = TypeVar("T")

__all__ = ["Registry"]


class Registry(Generic[T]):
    """A simple ``name -> entry`` registry with duplicate protection."""

    def __init__(self) -> None:
        self._entries: dict[str, T] = {}

    def register(self, entry: T, *, name: str | None = None) -> T:
        """Register ``entry`` under ``name`` (or ``entry.name`` if omitted)."""
        key = name if name is not None else getattr(entry, "name", None)
        if not key:
            raise ValueError(
                "entry has no 'name' attribute and no explicit name was provided"
            )
        if key in self._entries:
            raise KeyError(f"Datasource '{key}' is already registered")
        self._entries[key] = entry
        return entry

    def get(self, name: str) -> T:
        if name not in self._entries:
            raise KeyError(f"Datasource '{name}' is not registered")
        return self._entries[name]

    def __contains__(self, name: object) -> bool:
        return name in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def names(self) -> list[str]:
        return sorted(self._entries.keys())

    def items(self) -> list[tuple[str, T]]:
        return sorted(self._entries.items(), key=lambda kv: kv[0])

    def __iter__(self) -> Iterator[T]:
        for _, entry in self.items():
            yield entry
