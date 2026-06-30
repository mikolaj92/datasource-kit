"""Profile-folder loading and provider-name validation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ProfileError

__all__ = ["SourceProfile", "load_profile", "validate_source"]


@dataclass(slots=True, frozen=True)
class SourceProfile:
    """Data-only source profile.

    Provider values are safe-name strings resolved by ``ProviderRegistry``.
    Vocabularies and layer names are consumer-supplied strings.
    """

    name: str
    source_type: str
    providers: dict[str, str]
    policies: dict[str, Any] = field(default_factory=dict)
    status_vocabulary: list[str] = field(default_factory=list)
    completeness_layers: list[str] = field(default_factory=list)
    markdown: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ProfileError("profile name must be non-empty")
        if not self.source_type.strip():
            raise ProfileError("source_type must be non-empty")
        if not self.providers:
            raise ProfileError("providers must be non-empty")
        for key, value in self.providers.items():
            if not key.strip() or not isinstance(value, str) or not value.strip():
                raise ProfileError("provider names must be non-empty strings")

    def digest_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_type": self.source_type,
            "providers": self.providers,
            "policies": self.policies,
            "status_vocabulary": self.status_vocabulary,
            "completeness_layers": self.completeness_layers,
        }


def load_profile(source: str | Path) -> SourceProfile:
    """Load ``source.json`` or lazy ``source.yaml`` from a profile folder."""

    root = Path(source)
    if root.is_file():
        profile_path = root
        root = root.parent
    else:
        json_path = root / "source.json"
        yaml_path = root / "source.yaml"
        if json_path.exists():
            profile_path = json_path
        elif yaml_path.exists():
            profile_path = yaml_path
        else:
            raise ProfileError(f"no source.json or source.yaml in {root}")

    try:
        if profile_path.suffix in {".yaml", ".yml"}:
            raw = _load_yaml(profile_path)
        else:
            raw = json.loads(profile_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ProfileError(f"invalid JSON in {profile_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ProfileError("profile document must be an object")

    markdown: dict[str, str] = {}
    for name in ("coverage", "identity"):
        path = root / f"{name}.md"
        if path.exists():
            markdown[name] = path.read_text(encoding="utf-8")

    return SourceProfile(
        name=_string(raw, "name"),
        source_type=_string(raw, "source_type"),
        providers=_providers(raw.get("providers")),
        policies=dict(raw.get("policies") or {}),
        status_vocabulary=list(raw.get("status_vocabulary") or []),
        completeness_layers=list(raw.get("completeness_layers") or []),
        markdown=markdown,
    )


def validate_source(profile: SourceProfile, registry: Any) -> None:
    """Raise when a named provider is not registered.

    No status, layer, source-type, or governance rules live here.
    """

    missing: list[str] = []
    for category, name in profile.providers.items():
        has = registry.has(name) if hasattr(registry, "has") else name in registry
        if not has:
            missing.append(f"{category}={name}")
    if missing:
        raise ProfileError("unknown providers: " + ", ".join(sorted(missing)))


def _load_yaml(path: Path) -> Any:
    try:
        import yaml
    except ImportError as exc:
        raise ProfileError(
            "YAML profiles require the 'profiles' extra: "
            "pip install datasource-kit[profiles]"
        ) from exc
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ProfileError(f"{key} must be a non-empty string")
    return value


def _providers(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict) or not raw:
        raise ProfileError("providers must be a non-empty object")
    return {str(key): str(value) for key, value in raw.items()}
