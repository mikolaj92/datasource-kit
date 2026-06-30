"""Provider registry and built-in mock providers.

A ProviderRegistry maps provider names to callables.  ``builtin_registry()``
returns a pre-populated instance with the mock providers used in examples and
tests so consumers can validate a profile without standing up real providers.
"""

from __future__ import annotations

__all__ = ["ProviderRegistry", "builtin_registry"]

_BUILTINS = frozenset(
    [
        "window.by_day",
        "fetch.mock",
        "records.passthrough",
        "diff.by_id",
        "diff.full_replace",
        "assess.passthrough",
        "store.in_memory",
    ]
)


class ProviderRegistry:
    """Name-keyed registry for provider callables."""

    def __init__(self) -> None:
        self._names: set[str] = set()

    def register(self, name: str) -> None:
        self._names.add(name)

    def has(self, name: str) -> bool:
        return name in self._names


def builtin_registry() -> ProviderRegistry:
    """Return a registry pre-populated with the built-in mock providers."""
    reg = ProviderRegistry()
    for name in _BUILTINS:
        reg.register(name)
    return reg
