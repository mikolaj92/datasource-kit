"""Provider registry and builtin_registry().

Providers are callables registered by safe name. The registry maps
provider-type:name to callables. builtin_registry() ships a complete
chain including diff.by_id, diff.full_replace, and assess.passthrough.
"""

from __future__ import annotations

from typing import Any, Callable

__all__ = ["ProviderRegistry", "builtin_registry"]


class ProviderRegistry:
    """Maps ``kind:name`` keys to provider callables."""

    def __init__(self) -> None:
        self._providers: dict[str, Callable[..., Any]] = {}

    def register(self, kind: str, name: str, fn: Callable[..., Any]) -> None:
        key = f"{kind}:{name}"
        if key in self._providers:
            raise KeyError(f"Provider '{key}' is already registered")
        self._providers[key] = fn

    def get(self, kind: str, name: str) -> Callable[..., Any]:
        key = f"{kind}:{name}"
        if key not in self._providers:
            raise KeyError(f"Provider '{key}' is not registered")
        return self._providers[key]

    def __contains__(self, item: object) -> bool:
        return item in self._providers

    def keys(self) -> list[str]:
        return sorted(self._providers.keys())


# ---------------------------------------------------------------------------
# Builtin providers
# ---------------------------------------------------------------------------

def _diff_by_id(existing_ids: list[str], fetched: list[Any], *, id_field: str = "id") -> dict[str, list[Any]]:
    """Return {'new': [...], 'unchanged_ids': [...]} by comparing ids."""
    existing = set(existing_ids)
    new_items = []
    seen = []
    for item in fetched:
        item_id = str(item[id_field]) if isinstance(item, dict) else str(getattr(item, id_field))
        if item_id in existing:
            seen.append(item_id)
        else:
            new_items.append(item)
    return {"new": new_items, "unchanged_ids": seen}


def _diff_full_replace(existing_ids: list[str], fetched: list[Any], **_: Any) -> dict[str, list[Any]]:
    """Treat all fetched items as new; ignore existing state entirely."""
    return {"new": fetched, "unchanged_ids": []}


def _assess_passthrough(evidence: dict[str, int]) -> str:
    """Return 'ok' unconditionally — assessment policy lives in the consumer."""
    return "ok"


def builtin_registry() -> ProviderRegistry:
    """Return a :class:`ProviderRegistry` loaded with the complete builtin chain."""
    r = ProviderRegistry()
    r.register("diff", "by_id", _diff_by_id)
    r.register("diff", "full_replace", _diff_full_replace)
    r.register("assess", "passthrough", _assess_passthrough)
    return r
