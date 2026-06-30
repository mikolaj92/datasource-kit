from __future__ import annotations

import pytest

from datasource_kit import ProviderRegistry, builtin_registry


def test_builtin_registry_has_expected_providers():
    r = builtin_registry()
    assert "diff:by_id" in r
    assert "diff:full_replace" in r
    assert "assess:passthrough" in r


def test_diff_by_id_surfaces_new():
    r = builtin_registry()
    diff = r.get("diff", "by_id")
    result = diff(["a", "b"], [{"id": "b"}, {"id": "c"}])
    assert len(result["new"]) == 1
    assert result["new"][0]["id"] == "c"
    assert "b" in result["unchanged_ids"]


def test_diff_full_replace_ignores_existing():
    r = builtin_registry()
    diff = r.get("diff", "full_replace")
    result = diff(["a", "b", "c"], [{"id": "x"}])
    assert result["new"] == [{"id": "x"}]
    assert result["unchanged_ids"] == []


def test_assess_passthrough_returns_ok():
    r = builtin_registry()
    assess = r.get("assess", "passthrough")
    assert assess({"fetched": 10, "new": 2}) == "ok"


def test_duplicate_registration_rejected():
    r = ProviderRegistry()
    r.register("diff", "x", lambda: None)
    with pytest.raises(KeyError):
        r.register("diff", "x", lambda: None)


def test_get_unknown_raises():
    r = ProviderRegistry()
    with pytest.raises(KeyError):
        r.get("diff", "nonexistent")
