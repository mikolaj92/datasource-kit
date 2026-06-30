from __future__ import annotations

import json
import tempfile
from pathlib import Path

from datasource_kit import builtin_registry, load_profile, validate_source
from datasource_kit.profile import SourceProfile


def _write_profile(tmp: Path, data: dict) -> Path:
    p = tmp / "source.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def test_load_profile_basic():
    with tempfile.TemporaryDirectory() as td:
        p = _write_profile(
            Path(td),
            {"name": "demo", "providers": {"diff": "by_id", "assess": "passthrough"}, "policies": {"k": 1}},
        )
        profile = load_profile(p)
        assert profile.name == "demo"
        assert profile.providers == {"diff": "by_id", "assess": "passthrough"}
        assert profile.policies == {"k": 1}


def test_validate_source_ok():
    with tempfile.TemporaryDirectory() as td:
        p = _write_profile(
            Path(td),
            {"name": "demo", "providers": {"diff": "by_id", "assess": "passthrough"}},
        )
        profile = load_profile(p)
        errors = validate_source(profile, builtin_registry())
        assert errors == []


def test_validate_source_missing_provider():
    profile = SourceProfile(name="x", providers={"diff": "nonexistent"})
    errors = validate_source(profile, builtin_registry())
    assert len(errors) == 1
    assert "nonexistent" in errors[0]


def test_demo_scraper_profile_validates():
    """The shipped examples/demo-scraper/source.json must pass validation."""
    profile_path = Path(__file__).parent.parent / "examples" / "demo-scraper" / "source.json"
    profile = load_profile(profile_path)
    errors = validate_source(profile, builtin_registry())
    assert errors == []
