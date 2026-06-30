from __future__ import annotations

import hashlib

import pytest

import datasource_kit as dk
from datasource_kit import DataSource, IngestActor


class _BatchSource:
    def lookup(self, identifier: str) -> list:
        return [identifier]

    def refresh(self) -> dict:
        return {"rows_loaded": 0}


class _Scraper:
    name = "x"

    def handle_job(self, job: dict):
        return []


def test_batch_source_satisfies_datasource_protocol():
    assert isinstance(_BatchSource(), DataSource)


def test_scraper_satisfies_ingest_actor_protocol():
    assert isinstance(_Scraper(), IngestActor)


def test_cross_model_negative():
    assert not isinstance(_BatchSource(), IngestActor)
    assert not isinstance(_Scraper(), DataSource)


class _Enumerator:
    def enumerate(self, window: object) -> list[object]:
        return [window]


class _Fetcher:
    def fetch(self, ref: object) -> object:
        return ref


class _FullReplaceOnlyStore:
    def __init__(self) -> None:
        self.records: list[object] = []

    def upsert(self, records: list[object]) -> dict[str, int]:
        self.records.extend(records)
        return {"upserted": len(records)}

    def replace_all(self, records: list[object]) -> dict[str, int]:
        self.records = list(records)
        return {"replaced": len(self.records)}


class _ArtifactStore:
    def store(self, payload: bytes) -> str:
        return payload.hex()

    def resolve(self, ref: str) -> bytes:
        return bytes.fromhex(ref)


def test_protocol_surface_is_public():
    # Given: the package top-level is the import surface consumers use.
    public_names = set(dk.__all__)

    # When/Then: the new protocol seams and defaults are eagerly re-exported.
    assert {
        "ArtifactStore",
        "Enumerator",
        "Fetcher",
        "InMemoryArtifactStore",
        "InMemoryStore",
        "MockEnumerator",
        "MockFetcher",
        "StoragePort",
        "SupportsExistingIds",
    } <= public_names


def test_injected_protocols_are_runtime_checkable():
    # Given: conforming objects expose each required structural method.
    enumerator = _Enumerator()
    fetcher = _Fetcher()
    store = _FullReplaceOnlyStore()
    artifacts = _ArtifactStore()

    # When/Then: runtime Protocol checks accept conforming implementations.
    assert isinstance(enumerator, dk.Enumerator)
    assert isinstance(fetcher, dk.Fetcher)
    assert isinstance(store, dk.StoragePort)
    assert isinstance(artifacts, dk.ArtifactStore)


def test_injected_protocols_reject_nonconforming_objects():
    # Given: a plain object has none of the seam methods.
    value = object()

    # When/Then: runtime Protocol checks reject it for every new seam.
    assert not isinstance(value, dk.Enumerator)
    assert not isinstance(value, dk.Fetcher)
    assert not isinstance(value, dk.StoragePort)
    assert not isinstance(value, dk.ArtifactStore)


def test_existing_ids_is_optional_for_full_replace_stores():
    # Given: a store supports upsert and replace_all but no existing_ids method.
    store = _FullReplaceOnlyStore()

    # When: a full-replace path receives it through the base StoragePort seam.
    summary = store.replace_all([{"id": "one"}])

    # Then: the base protocol accepts it, while the optional by-id seam does not.
    assert isinstance(store, dk.StoragePort)
    assert not isinstance(store, dk.SupportsExistingIds)
    assert summary == {"replaced": 1}
    assert store.records == [{"id": "one"}]


def test_mock_enumerator_yields_deterministic_refs():
    # Given: a fixed count and window.
    enumerator = dk.MockEnumerator(count=3)

    # When: enumeration is repeated.
    first = list(enumerator.enumerate("window"))
    second = list(enumerator.enumerate("window"))

    # Then: refs are stable and window-derived.
    assert first == ["window:0", "window:1", "window:2"]
    assert second == first


def test_mock_fetcher_returns_deterministic_bytes():
    # Given: a mock fetcher with no network dependencies.
    fetcher = dk.MockFetcher()

    # When: the same ref is fetched twice.
    first = fetcher.fetch("window:0")
    second = fetcher.fetch("window:0")

    # Then: the fake payload is stable bytes derived from the ref.
    assert first == second
    assert first == b"mock:window:0"
    assert fetcher.fetch("window:1") != first


def test_in_memory_artifact_store_is_content_addressed():
    # Given: a content-addressed in-memory artifact store.
    store = dk.InMemoryArtifactStore()
    payload = b"payload"

    # When: the same payload is stored more than once.
    first_ref = store.store(payload)
    second_ref = store.store(payload)

    # Then: the SHA-256 digest is reused and resolves back to the payload.
    assert first_ref == hashlib.sha256(payload).hexdigest()
    assert second_ref == first_ref
    assert store.resolve(first_ref) == payload
    with pytest.raises(KeyError):
        store.resolve("missing")


def test_in_memory_store_upsert_replace_and_existing_ids():
    # Given: an in-memory store keyed by the default id field.
    store = dk.InMemoryStore()

    # When: records are upserted, merged by id, then fully replaced.
    assert store.upsert([{"id": "a", "value": 1}, {"id": "b", "value": 2}]) == {
        "upserted": 2
    }
    assert store.upsert([{"id": "a", "value": 3}]) == {"upserted": 1}

    # Then: existing_ids reflects current keys and all() exposes current records.
    assert store.existing_ids() == {"a", "b"}
    assert store.all() == [{"id": "a", "value": 3}, {"id": "b", "value": 2}]

    # When/Then: replace_all swaps the whole set.
    assert store.replace_all([{"id": "c", "value": 4}]) == {"replaced": 1}
    assert store.existing_ids() == {"c"}
    assert store.all() == [{"id": "c", "value": 4}]


def test_in_memory_store_supports_custom_id_key():
    # Given: records with a source-defined identifier field.
    store = dk.InMemoryStore(id_key="source_id")

    # When: the records are upserted.
    store.upsert([{"source_id": "x", "value": 1}])

    # Then: the custom id key controls identity.
    assert store.existing_ids() == {"x"}


def test_mock_chain_runs_without_network_or_dependencies():
    # Given: the stdlib-only mock seams for enumerate -> fetch -> persist.
    enumerator = dk.MockEnumerator(count=2)
    fetcher = dk.MockFetcher()
    artifacts = dk.InMemoryArtifactStore()
    records = dk.InMemoryStore()

    # When: refs are enumerated, fetched, stored as artifacts, then recorded.
    fetched_records: list[dict[str, str]] = []
    for ref in enumerator.enumerate("window"):
        payload = fetcher.fetch(ref)
        artifact_ref = artifacts.store(payload)
        fetched_records.append({"id": str(ref), "artifact_ref": artifact_ref})
    summary = records.upsert(fetched_records)

    # Then: the chain lands records and artifacts deterministically.
    assert summary == {"upserted": 2}
    assert records.existing_ids() == {"window:0", "window:1"}
    for record in records.all():
        assert artifacts.resolve(record["artifact_ref"]) == b"mock:" + record["id"].encode()
