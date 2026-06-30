"""Tests for profile.py: load_profile + validate_source."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from datasource_kit.errors import ProfileError
from datasource_kit.profile import SourceProfile, load_profile, validate_source
from datasource_kit.providers import ProviderRegistry, builtin_registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL = {
    "name": "test-src",
    "source_type": "scraper",
    "providers": {"enumerator": "window.by_day"},
}

_FULL = {
    "name": "demo-scraper",
    "source_type": "scraper",
    "providers": {
        "enumerator": "window.by_day",
        "fetcher": "fetch.mock",
        "mapper": "records.passthrough",
        "diff": "diff.by_id",
        "assess": "assess.passthrough",
        "store": "store.in_memory",
    },
    "policies": {"rate_limit": {"rate": 5.0}},
    "status_vocabulary": ["working", "blocked", "completed"],
    "completeness_layers": ["records"],
}


def _write_json(tmp_path: Path, data: dict) -> Path:
    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "source.json").write_text(json.dumps(data), encoding="utf-8")
    return folder


# ---------------------------------------------------------------------------
# JSON happy path
# ---------------------------------------------------------------------------


def test_load_profile_json_minimal(tmp_path: Path) -> None:
    folder = _write_json(tmp_path, _MINIMAL)
    p = load_profile(folder)
    assert p.name == "test-src"
    assert p.source_type == "scraper"
    assert p.providers == {"enumerator": "window.by_day"}
    assert p.policies == {}
    assert p.status_vocabulary == []
    assert p.completeness_layers == []
    assert p.markdown == {}


def test_load_profile_json_full(tmp_path: Path) -> None:
    folder = _write_json(tmp_path, _FULL)
    p = load_profile(folder)
    assert p.name == "demo-scraper"
    assert p.policies["rate_limit"] == {"rate": 5.0}
    assert p.status_vocabulary == ["working", "blocked", "completed"]
    assert p.completeness_layers == ["records"]


def test_load_profile_markdown_captured(tmp_path: Path) -> None:
    folder = _write_json(tmp_path, _MINIMAL)
    (folder / "coverage.md").write_text("## coverage", encoding="utf-8")
    (folder / "identity.md").write_text("## identity", encoding="utf-8")
    p = load_profile(folder)
    assert p.markdown["coverage"] == "## coverage"
    assert p.markdown["identity"] == "## identity"


def test_load_profile_no_optional_markdown(tmp_path: Path) -> None:
    folder = _write_json(tmp_path, _MINIMAL)
    p = load_profile(folder)
    assert p.markdown == {}


# ---------------------------------------------------------------------------
# YAML happy path (skipped when pyyaml absent)
# ---------------------------------------------------------------------------


def test_load_profile_yaml_happy(tmp_path: Path) -> None:
    pytest.importorskip("yaml", reason="pyyaml not installed")
    folder = tmp_path / "yaml-src"
    folder.mkdir()
    (folder / "source.yaml").write_text(
        "name: yaml-src\nsource_type: batch\nproviders:\n  store: store.in_memory\n",
        encoding="utf-8",
    )
    p = load_profile(folder)
    assert p.name == "yaml-src"
    assert p.providers == {"store": "store.in_memory"}


# ---------------------------------------------------------------------------
# Fail-closed: missing files
# ---------------------------------------------------------------------------


def test_load_profile_no_file_raises(tmp_path: Path) -> None:
    folder = tmp_path / "empty"
    folder.mkdir()
    with pytest.raises(ProfileError, match="no source.json or source.yaml"):
        load_profile(folder)


# ---------------------------------------------------------------------------
# Fail-closed: malformed document
# ---------------------------------------------------------------------------


def test_load_profile_invalid_json(tmp_path: Path) -> None:
    folder = tmp_path / "bad"
    folder.mkdir()
    (folder / "source.json").write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ProfileError, match="malformed source.json"):
        load_profile(folder)


def test_load_profile_non_mapping(tmp_path: Path) -> None:
    folder = tmp_path / "list"
    folder.mkdir()
    (folder / "source.json").write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(ProfileError, match="mapping"):
        load_profile(folder)


# ---------------------------------------------------------------------------
# Fail-closed: empty load-bearing fields
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "patch",
    [
        {"name": ""},
        {"name": None},
        {"source_type": ""},
        {"source_type": None},
        {"providers": {}},
        {"providers": None},
    ],
)
def test_load_profile_empty_field_raises(tmp_path: Path, patch: dict) -> None:
    data = {**_MINIMAL, **patch}
    folder = tmp_path / "bad-field"
    folder.mkdir()
    (folder / "source.json").write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ProfileError):
        load_profile(folder)


def test_load_profile_empty_provider_value_raises(tmp_path: Path) -> None:
    data = {**_MINIMAL, "providers": {"enumerator": ""}}
    folder = tmp_path / "empty-prov"
    folder.mkdir()
    (folder / "source.json").write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ProfileError):
        load_profile(folder)


# ---------------------------------------------------------------------------
# validate_source
# ---------------------------------------------------------------------------


def test_validate_source_passes_all_registered(tmp_path: Path) -> None:
    folder = _write_json(tmp_path, _FULL)
    p = load_profile(folder)
    validate_source(p, builtin_registry())  # must not raise


def test_validate_source_raises_on_unknown(tmp_path: Path) -> None:
    data = {**_MINIMAL, "providers": {"enumerator": "nope.unknown"}}
    folder = _write_json(tmp_path, data)
    p = load_profile(folder)
    with pytest.raises(ProfileError, match="unregistered provider"):
        validate_source(p, builtin_registry())


def test_validate_source_lists_all_missing(tmp_path: Path) -> None:
    data = {
        **_MINIMAL,
        "providers": {"a": "missing.a", "b": "missing.b"},
    }
    folder = _write_json(tmp_path, data)
    p = load_profile(folder)
    with pytest.raises(ProfileError) as exc_info:
        validate_source(p, builtin_registry())
    msg = str(exc_info.value)
    assert "missing.a" in msg
    assert "missing.b" in msg


def test_validate_source_empty_providers_dict_registered(tmp_path: Path) -> None:
    # validate_source with all providers registered passes regardless of count
    reg = ProviderRegistry()
    reg.register("my.provider")
    data = {**_MINIMAL, "providers": {"enumerator": "my.provider"}}
    folder = _write_json(tmp_path, data)
    p = load_profile(folder)
    validate_source(p, reg)  # must not raise


# ---------------------------------------------------------------------------
# Demo-scraper example validates green with stdlib only
# ---------------------------------------------------------------------------


def test_demo_scraper_example_loads_and_validates() -> None:
    examples_dir = Path(__file__).parent.parent / "examples" / "sources" / "demo-scraper"
    p = load_profile(examples_dir)
    assert p.name == "demo-scraper"
    validate_source(p, builtin_registry())  # must not raise


# ---------------------------------------------------------------------------
# SourceProfile is frozen
# ---------------------------------------------------------------------------


def test_source_profile_frozen() -> None:
    p = SourceProfile(
        name="x", source_type="batch", providers={"store": "store.in_memory"}
    )
    with pytest.raises((AttributeError, TypeError)):
        p.name = "y"  # type: ignore[misc]
