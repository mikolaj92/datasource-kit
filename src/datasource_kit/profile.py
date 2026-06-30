"""Profile loading and validation for source folders.

A source folder contains at minimum a ``source.json`` file. Optional
``coverage.md`` / ``identity.md`` files may also be present.

``load_profile`` reads and parses the JSON. ``validate_source`` checks that
every provider name referenced in the profile is present in the supplied
registry.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import ProfileError

__all__ = ["load_profile", "validate_source"]

_PROVIDER_KEYS = (
    "enumerator",
    "fetcher",
    "mapper",
    "diff",
    "assess",
    "store",
)


def load_profile(source: str | Path) -> dict[str, Any]:
    """Load and return the ``source.json`` from *source* folder or file."""
    p = Path(source)
    if p.is_dir():
        p = p / "source.json"
    if not p.exists():
        raise ProfileError(f"source.json not found: {p}")
    try:
        with p.open(encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError as exc:
        raise ProfileError(f"invalid JSON in {p}: {exc}") from exc


def validate_source(profile: dict[str, Any], registry: dict[str, Any]) -> None:
    """Raise :exc:`ProfileError` if any provider in *profile* is not in *registry*."""
    providers: dict[str, str] = profile.get("providers", {})
    missing = [
        f"{key}={name!r}"
        for key, name in providers.items()
        if name not in registry
    ]
    if missing:
        raise ProfileError(
            "unknown providers: " + ", ".join(missing)
        )
