"""Profile-folder injection seam.

A consumer hands the framework a folder; the kit reads ``source.json`` (stdlib,
the dep-free default) or ``source.yaml`` (lazy pyyaml, behind the ``[profiles]``
extra) into a frozen :class:`SourceProfile`.

``validate_source`` is the allowlist check: it fails closed only when a named
provider is not registered.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from .errors import ProfileError
from .providers import ProviderRegistry

__all__ = ["SourceProfile", "load_profile", "validate_source"]


@dataclass(slots=True, frozen=True)
class SourceProfile:
    name: str
    source_type: str
    providers: dict[str, str]
    policies: dict[str, object] = field(default_factory=dict)
    status_vocabulary: list[str] = field(default_factory=list)
    completeness_layers: list[str] = field(default_factory=list)
    markdown: dict[str, str] = field(default_factory=dict)


def load_profile(folder: str | os.PathLike[str]) -> SourceProfile:
    """Load <folder>/source.json or <folder>/source.yaml into a SourceProfile.

    Fails closed (raises ProfileError) on: neither file present, malformed
    document, or an empty load-bearing field (name / source_type / providers).
    Optional coverage.md / identity.md are read verbatim into .markdown.
    """
    root = Path(folder)
    json_path = root / "source.json"
    yaml_path = root / "source.yaml"
    if json_path.exists():
        try:
            raw = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise ProfileError(f"malformed source.json in {root}: {e}") from e
    elif yaml_path.exists():
        try:
            import yaml  # noqa: PLC0415 — lazy: only [profiles] extra pulls pyyaml
        except ImportError as e:  # pragma: no cover
            raise ProfileError(
                "YAML profiles require the 'profiles' extra: "
                "pip install datasource-kit[profiles]"
            ) from e
        try:
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        except Exception as e:
            raise ProfileError(f"malformed source.yaml in {root}: {e}") from e
    else:
        raise ProfileError(f"no source.json or source.yaml in {root}")
    return _build_profile(raw, root)


def validate_source(profile: SourceProfile, registry: ProviderRegistry) -> None:
    """Raise ProfileError if any named provider is not registered.

    No worker-contract check, no status/layer vocabulary validation — only the
    allowlist membership test: is every named provider registered?
    """
    missing = [
        f"{cat}={name}"
        for cat, name in profile.providers.items()
        if not registry.has(name)
    ]
    if missing:
        raise ProfileError(f"unregistered provider(s): {', '.join(sorted(missing))}")


def _build_profile(raw: object, root: Path) -> SourceProfile:
    if not isinstance(raw, dict):
        raise ProfileError("profile document must be a mapping")

    name = raw.get("name", "")
    if not name or not isinstance(name, str):
        raise ProfileError("profile 'name' must be a non-empty string")

    source_type = raw.get("source_type", "")
    if not source_type or not isinstance(source_type, str):
        raise ProfileError("profile 'source_type' must be a non-empty string")

    providers = raw.get("providers")
    if not providers or not isinstance(providers, dict):
        raise ProfileError("profile 'providers' must be a non-empty mapping")
    for cat, pname in providers.items():
        if not pname or not isinstance(pname, str):
            raise ProfileError(
                f"profile 'providers.{cat}' must be a non-empty string"
            )

    markdown: dict[str, str] = {}
    for key, filename in (("coverage", "coverage.md"), ("identity", "identity.md")):
        p = root / filename
        if p.exists():
            markdown[key] = p.read_text(encoding="utf-8")

    return SourceProfile(
        name=name,
        source_type=source_type,
        providers=dict(providers),
        policies=dict(raw.get("policies") or {}),
        status_vocabulary=list(raw.get("status_vocabulary") or []),
        completeness_layers=list(raw.get("completeness_layers") or []),
        markdown=markdown,
    )
