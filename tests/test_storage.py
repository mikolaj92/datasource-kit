from __future__ import annotations

from datasource_kit import ArtifactStore, InMemoryArtifactStore


def test_store_and_get():
    store = InMemoryArtifactStore()
    store.store_one({"id": "a", "v": 1})
    assert store.get("a") == {"id": "a", "v": 1}
    assert store.get("missing") is None


def test_list_ids_sorted():
    store = InMemoryArtifactStore()
    store.store_one({"id": "b"})
    store.store_one({"id": "a"})
    assert store.list_ids() == ["a", "b"]


def test_full_replace_replaces_all():
    store = InMemoryArtifactStore()
    store.store_one({"id": "old"})
    count = store.full_replace([{"id": "x"}, {"id": "y"}])
    assert count == 2
    assert "old" not in store.list_ids()
    assert store.list_ids() == ["x", "y"]


def test_len():
    store = InMemoryArtifactStore()
    assert len(store) == 0
    store.store_one({"id": "1"})
    assert len(store) == 1


def test_satisfies_protocol():
    assert isinstance(InMemoryArtifactStore(), ArtifactStore)
