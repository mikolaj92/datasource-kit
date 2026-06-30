"""Profile-folder loader and provider-only validate_source.

A profile is a JSON file (source.json by default) that names safe registered
providers and carries policies-as-data. validate_source checks only provider
registration — no business logic.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .providers import ProviderRegistry

__all__ = ["SourceProfile", "load_profile", "validate_source"]

_PROVIDER_KEYS = ("enumerator", "fetcher", "mapper", "diff", "assess", "store")


@dataclass
class SourceProfile:
    """Loaded profile for one source.

    ``providers`` maps role to provider name (e.g. ``{"diff": "by_id"}``).
    ``policies`` carries arbitrary policy data from the profile file.
    """

    name: str
    providers: dict[str, str] = field(default_factory=dict)
    policies: dict[str, Any] = field(default_factory=dict)


def load_profile(path: str | Path) -> SourceProfile:
    """Load a source profile from a JSON file."""
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    return SourceProfile(
        name=data.get("name", p.stem),
        providers={k: data["providers"][k] for k in _PROVIDER_KEYS if k in data.get("providers", {})},
        policies=data.get("policies", {}),
    )


def validate_source(profile: SourceProfile, registry: ProviderRegistry) -> list[str]:
    """Return a list of error strings for any provider not registered.

    Empty list means the profile is valid against the given registry.
    """
    errors: list[str] = []
    for role, name in profile.providers.items():
        # roles with a known kind map directly; others assumed by name only
        kind = role if role in ("diff", "assess") else role
        key = f"{kind}:{name}"
        if key not in registry:
            errors.append(f"Provider '{key}' required by profile '{profile.name}' is not registered")
    return errors
