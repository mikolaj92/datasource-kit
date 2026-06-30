from __future__ import annotations

import json
import tomllib
from datetime import date
from pathlib import Path

import pytest

import datasource_kit as dk
from datasource_kit.ledger import DiscoveredItem, DiscoveryLedgerStore, Evidence

ROOT = Path(__file__).resolve().parent.parent


def test_error_hierarchy_is_typed() -> None:
    for typ in (
        dk.ProfileError,
        dk.ProviderError,
        dk.RegistryError,
        dk.RuntimeStepError,
        dk.ValidationError,
    ):
        assert issubclass(typ, dk.DatasourceKitError)


def test_result_builders_fail_closed() -> None:
    result = dk.working_result(
        status="working",
        cursor_kind="page",
        cursor_value="1",
        objects=({"id": "one"},),
    )
    assert result.cursor == dk.Cursor("page", "1")
    assert dk.completed_result(status="done").status == "done"
    assert dk.blocked_result(status="blocked", reason="rate limit").reason == "rate limit"

    with pytest.raises(dk.ValidationError):
        dk.working_result(status="", cursor_kind="page", cursor_value="1")
    with pytest.raises(dk.ValidationError):
        dk.working_result(status="working", cursor_kind="", cursor_value="1")
    with pytest.raises(dk.ValidationError):
        dk.blocked_result(status="blocked", reason="")


def test_completeness_layers_are_consumer_named_counts_only() -> None:
    layers = dk.layers_from_names(["records"])
    report = dk.CompletenessReport(layers=layers)
    layers["records"].truth_count = 4
    layers["records"].present_count = 3

    assert report.fraction("records") == 0.75
    assert layers["records"].missing_count == 1
    with pytest.raises(dk.ValidationError):
        report.fraction("missing")


def test_window_split_is_inclusive_and_reversed_empty() -> None:
    assert list(dk.split_range_into_days(date(2024, 1, 2), date(2024, 1, 1))) == []
    assert list(dk.split_range_into_days(date(2024, 1, 1), date(2024, 1, 2))) == [
        dk.DayWindow(date(2024, 1, 1), date(2024, 1, 1)),
        dk.DayWindow(date(2024, 1, 2), date(2024, 1, 2)),
    ]


def test_token_bucket_acquire_and_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    now = {"value": 0.0}

    def monotonic() -> float:
        return now["value"]

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        now["value"] += seconds

    import datasource_kit.ratelimit as ratelimit

    monkeypatch.setattr(ratelimit.time, "monotonic", monotonic)
    monkeypatch.setattr(ratelimit.time, "sleep", sleep)

    bucket = dk.TokenBucket(rate=2.0, capacity=1.0)
    assert bucket.acquire() == 0.0
    assert bucket.acquire() == 0.5

    calls = {"count": 0}

    def flaky() -> str:
        calls["count"] += 1
        if calls["count"] < 3:
            raise ValueError("not yet")
        return "ok"

    assert ratelimit.with_retry(
        flaky,
        attempts=3,
        base_delay=0.1,
        max_delay=0.2,
        retry_on=(ValueError,),
    ) == "ok"
    assert calls["count"] == 3


def test_ledger_roundtrip_and_counts(tmp_path: Path) -> None:
    arbitrary = DiscoveredItem.from_json(
        {"source_id": "a", "status": "whatever-consumer-said"}
    )
    assert arbitrary.status == "whatever-consumer-said"
    with pytest.raises(dk.ValidationError):
        DiscoveredItem.from_json({"source_id": "", "status": "fetched"})

    evidence = Evidence.capture(run_id="run", payload={"count": 1})
    assert Evidence.from_json(evidence.as_dict()) == evidence

    store = DiscoveryLedgerStore(tmp_path)
    summary = store.write_window(
        source="demo",
        window_key="w1",
        run_id="run",
        items=[
            DiscoveredItem(source_id="one", status="fetched"),
            DiscoveredItem(source_id="two", status="merged"),
            arbitrary,
        ],
    )
    assert summary.as_dict() == {
        "discovered": 3,
        "fetched": 1,
        "merged": 1,
        "failed": 0,
        "skipped": 0,
        "pending": 0,
    }
    assert not list(tmp_path.rglob("*.tmp"))
    assert "lifecycle_state" not in dir(summary)
    assert "completeness_state" not in json.loads(
        (tmp_path / "demo" / "w1" / "summary.json").read_text()
    )


def test_profile_loader_and_provider_validation(tmp_path: Path) -> None:
    (tmp_path / "coverage.md").write_text("coverage notes", encoding="utf-8")
    (tmp_path / "source.json").write_text(
        json.dumps(
            {
                "name": "demo",
                "source_type": "scraper",
                "providers": {
                    "enumerator": "window.by_day",
                    "fetcher": "fetch.mock",
                    "mapper": "records.passthrough",
                    "diff": "diff.by_id",
                    "assess": "assess.passthrough",
                    "store": "store.in_memory",
                },
                "policies": {"coverage_unit": "day"},
                "status_vocabulary": ["ok"],
                "completeness_layers": ["records"],
            }
        ),
        encoding="utf-8",
    )

    profile = dk.load_profile(tmp_path)
    assert profile.name == "demo"
    assert profile.markdown == {"coverage": "coverage notes"}
    dk.validate_source(profile, dk.builtin_registry())

    bad = dk.SourceProfile(
        name="bad",
        source_type="scraper",
        providers={"fetcher": "fetch.nope"},
    )
    with pytest.raises(dk.ProfileError, match="fetch.nope"):
        dk.validate_source(bad, dk.builtin_registry())


def test_provider_registry_complete_chain() -> None:
    registry = dk.builtin_registry()
    required = {
        "window.by_day",
        "fetch.mock",
        "records.passthrough",
        "diff.by_id",
        "diff.full_replace",
        "assess.passthrough",
        "identity.by_field",
        "store.in_memory",
        "store.sqlite",
    }
    assert required <= set(registry.keys())
    assert registry.names_by_category("diff") == ["diff.by_id", "diff.full_replace"]
    with pytest.raises(dk.RegistryError):
        registry.get("missing.provider")


def test_packaging_and_readme_framing() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert data["project"]["requires-python"] == ">=3.12"
    assert data["project"]["dependencies"] == []

    text = (ROOT / "README.md").read_text()
    for token in ("enumerate", "fetch", "persist", "diff", "assess", "report"):
        assert token in text
    assert "It is NOT" in text
