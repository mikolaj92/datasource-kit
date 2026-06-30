from __future__ import annotations

import pytest

from datasource_kit import (
    InMemoryArtifactStore,
    builtin_registry,
    run_ingest,
)
from datasource_kit.providers import ProviderRegistry


def _fake_items(n: int = 5) -> list[dict]:
    return [{"id": str(i), "v": i} for i in range(n)]


def test_run_ingest_persists_new_items():
    store = InMemoryArtifactStore()
    report = run_ingest(
        enumerator=lambda: _fake_items(3),
        fetcher=lambda item: item,
        store=store,
        registry=builtin_registry(),
    )
    assert report.status == "ok"
    assert len(store) == 3
    assert report.totals["new"] == 3
    assert report.assessment == "ok"


def test_run_ingest_full_replace_provider():
    store = InMemoryArtifactStore()
    store.store_one({"id": "old"})
    report = run_ingest(
        enumerator=lambda: _fake_items(2),
        fetcher=lambda item: item,
        store=store,
        registry=builtin_registry(),
        diff_provider="full_replace",
    )
    # full_replace treats all fetched as new; old item survives because we only
    # store_one new items (not full_replace on the store in the pipeline)
    assert report.totals["new"] == 2


def test_run_ingest_fails_closed_on_missing_provider():
    r = ProviderRegistry()
    with pytest.raises(KeyError):
        run_ingest(
            enumerator=lambda: [],
            fetcher=lambda item: item,
            store=InMemoryArtifactStore(),
            registry=r,
            diff_provider="by_id",
        )


def test_run_ingest_partial_on_fetch_errors():
    def bad_fetcher(item):
        raise RuntimeError("network error")

    store = InMemoryArtifactStore()
    report = run_ingest(
        enumerator=lambda: _fake_items(2),
        fetcher=bad_fetcher,
        store=store,
        registry=builtin_registry(),
        max_retries=1,
    )
    assert report.status == "partial"
    assert len(report.errors) == 2


def test_ingest_report_summary_contains_key_fields():
    store = InMemoryArtifactStore()
    report = run_ingest(
        enumerator=lambda: _fake_items(2),
        fetcher=lambda item: item,
        store=store,
        registry=builtin_registry(),
    )
    summary = report.summary()
    assert "status=ok" in summary
    assert "assessment=ok" in summary


def test_demo_scraper_end_to_end():
    """Mirrors the CLI demo: no network, no extras, no consumer."""
    fake_records = [{"id": str(i), "value": f"item-{i}"} for i in range(5)]
    store = InMemoryArtifactStore()
    registry = builtin_registry()

    report = run_ingest(
        enumerator=lambda: fake_records,
        fetcher=lambda item: item,
        store=store,
        registry=registry,
        diff_provider="by_id",
        assess_provider="passthrough",
        max_retries=1,
    )
    assert report.status == "ok"
    assert len(store) == 5
    assert report.assessment == "ok"
    assert report.totals["enumerated"] == 5
    assert report.totals["persisted"] == 5
