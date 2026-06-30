"""Smoke tests for every CLI verb."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datasource_kit.cli import main, EXAMPLE_ROOT
from datasource_kit.providers import builtin_registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def demo_scraper(tmp_path: Path) -> Path:
    """A minimal scraper profile in a temp folder."""
    profile = {
        "name": "test-scraper",
        "source_type": "scraper",
        "providers": {
            "enumerator": "window.by_day",
            "fetcher": "fetch.mock",
            "mapper": "records.passthrough",
            "diff": "diff.by_id",
            "assess": "assess.passthrough",
            "store": "store.in_memory",
        },
        "policies": {},
        "completeness_layers": ["records"],
    }
    (tmp_path / "source.json").write_text(json.dumps(profile), encoding="utf-8")
    return tmp_path


@pytest.fixture()
def demo_batch(tmp_path: Path) -> Path:
    """A minimal batch profile in a temp folder."""
    profile = {
        "name": "test-batch",
        "source_type": "batch",
        "providers": {
            "enumerator": "window.by_day",
            "fetcher": "fetch.mock",
            "mapper": "records.passthrough",
            "diff": "diff.full_replace",
            "assess": "assess.passthrough",
            "store": "store.in_memory",
        },
        "policies": {},
        "completeness_layers": [],
    }
    (tmp_path / "source.json").write_text(json.dumps(profile), encoding="utf-8")
    return tmp_path


# ---------------------------------------------------------------------------
# validate verb
# ---------------------------------------------------------------------------


def test_validate_demo_scraper_ok(demo_scraper: Path) -> None:
    assert main(["validate", str(demo_scraper)]) == 0


def test_validate_demo_batch_ok(demo_batch: Path) -> None:
    assert main(["validate", str(demo_batch)]) == 0


def test_validate_unknown_provider_nonzero(tmp_path: Path) -> None:
    profile = {
        "name": "bad",
        "source_type": "scraper",
        "providers": {
            "enumerator": "window.by_day",
            "fetcher": "fetch.NONEXISTENT",
            "mapper": "records.passthrough",
            "diff": "diff.by_id",
            "assess": "assess.passthrough",
            "store": "store.in_memory",
        },
        "policies": {},
        "completeness_layers": [],
    }
    (tmp_path / "source.json").write_text(json.dumps(profile), encoding="utf-8")
    assert main(["validate", str(tmp_path)]) != 0


# ---------------------------------------------------------------------------
# run verb
# ---------------------------------------------------------------------------


def test_run_writes_json(demo_scraper: Path, tmp_path: Path) -> None:
    out = tmp_path / "report.json"
    rc = main(["run", str(demo_scraper), "--out", str(out)])
    assert rc == 0
    assert out.exists()
    data = json.loads(out.read_text())
    assert data["source_name"] == "test-scraper"
    assert data["windows_processed"] > 0
    assert data["records_fetched"] > 0


def test_run_stdout(demo_scraper: Path, capsys: pytest.CaptureFixture) -> None:
    rc = main(["run", str(demo_scraper)])
    assert rc == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert "source_name" in data


# ---------------------------------------------------------------------------
# coverage report verb
# ---------------------------------------------------------------------------


def test_coverage_report(demo_scraper: Path, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    out = tmp_path / "report.json"
    main(["run", str(demo_scraper), "--out", str(out)])
    rc = main(["coverage", "report", str(out)])
    assert rc == 0
    captured = capsys.readouterr()
    assert "windows_processed" in captured.out
    assert "records_fetched" in captured.out


# ---------------------------------------------------------------------------
# explain verb
# ---------------------------------------------------------------------------


def test_explain(demo_scraper: Path, tmp_path: Path, capsys: pytest.CaptureFixture) -> None:
    out = tmp_path / "report.json"
    main(["run", str(demo_scraper), "--out", str(out)])
    rc = main(["explain", str(out)])
    assert rc == 0
    captured = capsys.readouterr()
    assert "source_name" in captured.out
    assert "kit_version" in captured.out


# ---------------------------------------------------------------------------
# examples run verb (shipped demo profiles)
# ---------------------------------------------------------------------------


def test_examples_run_demo_scraper() -> None:
    assert EXAMPLE_ROOT.exists(), f"EXAMPLE_ROOT missing: {EXAMPLE_ROOT}"
    rc = main(["examples", "run", "demo-scraper"])
    assert rc == 0


def test_examples_run_demo_scraper_report(tmp_path: Path) -> None:
    out = tmp_path / "demo-scraper.json"
    rc = main(["examples", "run", "demo-scraper", "--out", str(out)])
    assert rc == 0
    data = json.loads(out.read_text())
    assert data["source_name"] == "demo-scraper"
    assert data["windows_processed"] > 0
    assert data["records_fetched"] > 0
    # diff populated (by_id produces added/removed/unchanged)
    assert "added" in data["diff"] or "replaced" in data["diff"]
    # per-layer count populated
    assert "records" in data["completeness"]["layers"]


def test_examples_run_demo_batch(tmp_path: Path) -> None:
    out = tmp_path / "demo-batch.json"
    rc = main(["examples", "run", "demo-batch", "--out", str(out)])
    assert rc == 0
    data = json.loads(out.read_text())
    assert data["source_name"] == "demo-batch"
    # full_replace diff
    assert "replaced" in data["diff"]


def test_examples_run_unknown_name_nonzero() -> None:
    rc = main(["examples", "run", "does-not-exist"])
    assert rc != 0


# ---------------------------------------------------------------------------
# Dep-free standalone assertion
# ---------------------------------------------------------------------------


def test_builtin_registry_stdlib_only() -> None:
    """builtin_registry() must not require any third-party package."""
    reg = builtin_registry()
    required = {
        "window.by_day", "fetch.mock", "records.passthrough",
        "diff.by_id", "diff.full_replace", "assess.passthrough", "store.in_memory",
    }
    assert required <= set(reg.keys())
