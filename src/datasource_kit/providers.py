"""Built-in provider implementations and registry.

All providers are stdlib-only and side-effect-free (no network, no filesystem).
The registry maps dotted names (e.g. ``"fetch.mock"``) to callables or objects
that satisfy the provider protocols used by the runtime.
"""

from __future__ import annotations

from typing import Any, Callable

__all__ = ["builtin_registry"]

# ---------------------------------------------------------------------------
# Enumerators
# ---------------------------------------------------------------------------


def _window_by_day(config: dict) -> list[dict]:
    """Yield day-sized window dicts from ``start`` to ``end`` (ISO dates)."""
    from datetime import date, timedelta

    start_raw = config.get("start", "2024-01-01")
    end_raw = config.get("end", "2024-01-03")
    start = date.fromisoformat(str(start_raw))
    end = date.fromisoformat(str(end_raw))
    windows: list[dict] = []
    cur = start
    while cur <= end:
        windows.append({"date": cur.isoformat()})
        cur += timedelta(days=1)
    return windows


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------


def _fetch_mock(window: dict, config: dict) -> list[dict]:
    """Return a tiny synthetic payload; no network."""
    return [{"id": f"rec-{window.get('date','x')}-1", "value": 42}]


# ---------------------------------------------------------------------------
# Mappers
# ---------------------------------------------------------------------------


def _records_passthrough(raw: list[dict], config: dict) -> list[dict]:
    return raw


# ---------------------------------------------------------------------------
# Diff strategies
# ---------------------------------------------------------------------------


def _diff_by_id(fetched: list[dict], stored: list[dict], config: dict) -> dict:
    fetched_ids = {r["id"] for r in fetched if "id" in r}
    stored_ids = {r["id"] for r in stored if "id" in r}
    return {
        "added": list(fetched_ids - stored_ids),
        "removed": list(stored_ids - fetched_ids),
        "unchanged": list(fetched_ids & stored_ids),
    }


def _diff_full_replace(fetched: list[dict], stored: list[dict], config: dict) -> dict:
    return {"replaced": len(fetched), "previous": len(stored)}


# ---------------------------------------------------------------------------
# Assess strategies
# ---------------------------------------------------------------------------


def _assess_passthrough(records: list[dict], window: dict, config: dict) -> dict:
    return {"count": len(records)}


# ---------------------------------------------------------------------------
# Store drivers
# ---------------------------------------------------------------------------


class _InMemoryStoreDriver:
    """Simple in-memory store that accumulates records across windows."""

    def __init__(self) -> None:
        self._data: list[dict] = []

    def load(self) -> list[dict]:
        return list(self._data)

    def save(self, records: list[dict]) -> None:
        self._data.extend(records)

    def replace_all(self, records: list[dict]) -> None:
        self._data = list(records)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_ENUMERATORS: dict[str, Callable] = {
    "window.by_day": _window_by_day,
}

_FETCHERS: dict[str, Callable] = {
    "fetch.mock": _fetch_mock,
}

_MAPPERS: dict[str, Callable] = {
    "records.passthrough": _records_passthrough,
}

_DIFFS: dict[str, Callable] = {
    "diff.by_id": _diff_by_id,
    "diff.full_replace": _diff_full_replace,
}

_ASSESSORS: dict[str, Callable] = {
    "assess.passthrough": _assess_passthrough,
}

_STORE_FACTORIES: dict[str, Callable] = {
    "store.in_memory": _InMemoryStoreDriver,
}

_ALL: dict[str, Any] = {
    **_ENUMERATORS,
    **_FETCHERS,
    **_MAPPERS,
    **_DIFFS,
    **_ASSESSORS,
    **_STORE_FACTORIES,
}


def builtin_registry() -> dict[str, Any]:
    """Return a mapping of all built-in provider names to their implementations."""
    return dict(_ALL)
