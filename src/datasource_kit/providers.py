"""Built-in provider implementations and safe-name registry."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

from .errors import RegistryError
from .window import split_range_into_days

__all__ = ["ProviderRegistry", "builtin_registry"]

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")


class ProviderRegistry:
    """Safe-named allowlist of provider callables grouped by category."""

    def __init__(self) -> None:
        self._by_name: dict[str, Callable[..., Any]] = {}
        self._by_category: dict[str, list[str]] = {}

    def register(
        self,
        name: str,
        fn: Callable[..., Any],
        *,
        category: str,
    ) -> None:
        if not name or not _NAME_RE.match(name):
            raise RegistryError(f"invalid provider name: {name!r}")
        if not category or not category.strip():
            raise RegistryError("provider category must be non-empty")
        if name in self._by_name:
            raise RegistryError(f"duplicate provider: {name}")
        self._by_name[name] = fn
        self._by_category.setdefault(category, []).append(name)

    def has(self, name: str) -> bool:
        return name in self._by_name

    def get(self, name: str) -> Callable[..., Any]:
        try:
            return self._by_name[name]
        except KeyError as exc:
            raise RegistryError(f"unknown provider: {name}") from exc

    def names_by_category(self, category: str) -> list[str]:
        return sorted(self._by_category.get(category, []))

    def keys(self) -> list[str]:
        return sorted(self._by_name)

    def items(self) -> list[tuple[str, Callable[..., Any]]]:
        return [(name, self._by_name[name]) for name in self.keys()]

    def values(self) -> list[Callable[..., Any]]:
        return [self._by_name[name] for name in self.keys()]

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and self.has(name)

    def __getitem__(self, name: str) -> Callable[..., Any]:
        return self.get(name)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self._by_name)


def _window_by_day(config: dict[str, Any]) -> list[dict[str, str]]:
    start = date.fromisoformat(str(config.get("start", "2024-01-01")))
    end = date.fromisoformat(str(config.get("end", "2024-01-03")))
    return [
        {"date": window.start.isoformat()}
        for window in split_range_into_days(start, end)
    ]


def _fetch_mock(window: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"id": f"rec-{window.get('date', 'x')}-1", "value": 42}]


def _records_passthrough(raw: list[dict[str, Any]], config: dict[str, Any]) -> list[dict[str, Any]]:
    return list(raw)


def _record_id(record: dict[str, Any], config: dict[str, Any]) -> str:
    field = str(config.get("field", "id"))
    return str(record[field])


def _diff_by_id(
    fetched: list[dict[str, Any]],
    stored: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    fetched_ids = {_record_id(record, config) for record in fetched if "id" in record}
    stored_ids = {_record_id(record, config) for record in stored if "id" in record}
    return {
        "added": sorted(fetched_ids - stored_ids),
        "removed": sorted(stored_ids - fetched_ids),
        "unchanged": sorted(fetched_ids & stored_ids),
    }


def _diff_full_replace(
    fetched: list[dict[str, Any]],
    stored: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    return {"replaced": len(fetched), "previous": len(stored)}


def _assess_passthrough(
    records: list[dict[str, Any]],
    window: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    return {"count": len(records), "status": str(config.get("status", "ok"))}


def _identity_by_field(record: dict[str, Any], config: dict[str, Any]) -> str:
    return _record_id(record, config)


class _InMemoryStoreDriver:
    """Simple in-memory store that accumulates records across windows."""

    def __init__(self) -> None:
        self._data: list[dict[str, Any]] = []

    def load(self) -> list[dict[str, Any]]:
        return list(self._data)

    def save(self, records: list[dict[str, Any]]) -> None:
        self._data.extend(records)

    def upsert(self, records: list[dict[str, Any]]) -> dict[str, int]:
        self.save(records)
        return {"upserted": len(records)}

    def replace_all(self, records: list[dict[str, Any]]) -> dict[str, int]:
        self._data = list(records)
        return {"replaced": len(records)}

    def existing_ids(self) -> set[str]:
        return {str(record["id"]) for record in self._data if "id" in record}


class _SQLiteStoreDriver:
    """Tiny stdlib sqlite store for demo/runtime use."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self._con = sqlite3.connect(path)
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS records (id TEXT PRIMARY KEY, payload TEXT)"
        )

    def load(self) -> list[dict[str, Any]]:
        rows = self._con.execute("SELECT id, payload FROM records").fetchall()
        return [{"id": row[0], "payload": row[1]} for row in rows]

    def save(self, records: list[dict[str, Any]]) -> None:
        for record in records:
            self._con.execute(
                "INSERT OR REPLACE INTO records(id, payload) VALUES (?, ?)",
                (str(record["id"]), str(record)),
            )
        self._con.commit()

    def upsert(self, records: list[dict[str, Any]]) -> dict[str, int]:
        self.save(records)
        return {"upserted": len(records)}

    def replace_all(self, records: list[dict[str, Any]]) -> dict[str, int]:
        self._con.execute("DELETE FROM records")
        self.save(records)
        return {"replaced": len(records)}

    def existing_ids(self) -> set[str]:
        return {str(row[0]) for row in self._con.execute("SELECT id FROM records")}


def builtin_registry() -> ProviderRegistry:
    """Return a registry with a complete zero-network provider chain."""

    registry = ProviderRegistry()
    registry.register("window.by_day", _window_by_day, category="enumerator")
    registry.register("fetch.mock", _fetch_mock, category="fetcher")
    registry.register("records.passthrough", _records_passthrough, category="mapper")
    registry.register("diff.by_id", _diff_by_id, category="diff")
    registry.register("diff.full_replace", _diff_full_replace, category="diff")
    registry.register("assess.passthrough", _assess_passthrough, category="assess")
    registry.register("identity.by_field", _identity_by_field, category="identity")
    registry.register("store.in_memory", _InMemoryStoreDriver, category="store")
    registry.register("store.sqlite", _SQLiteStoreDriver, category="store")
    return registry
