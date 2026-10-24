from __future__ import annotations

import builtins
import hashlib
import sys

import pytest


def _block_fala_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "fala", raising=False)
    real_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "fala" or name.startswith("fala."):
            raise ModuleNotFoundError("No module named 'fala'")
        return real_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def test_importing_core_and_adapters_when_fala_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the optional fala dependency is not importable.
    _block_fala_imports(monkeypatch)

    # When: the core package and adapters package are imported.
    import datasource_kit
    import datasource_kit.adapters as adapters

    # Then: import succeeds without touching fala.
    assert datasource_kit.__name__ == "datasource_kit"
    assert adapters.__name__ == "datasource_kit.adapters"


def test_fala_adapter_does_not_export_artifact_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _block_fala_imports(monkeypatch)
    from datasource_kit.adapters import fala

    with pytest.raises(ImportError):
        from datasource_kit.adapters.fala import FalaArtifactStore
    assert "FalaArtifactStore" not in fala.__all__


def test_artifact_store_round_trips_bytes_without_fala(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _block_fala_imports(monkeypatch)
    from datasource_kit import ArtifactStore, InMemoryArtifactStore

    store = InMemoryArtifactStore()
    ref = store.store(b"payload")
    resolved = store.resolve(ref)

    assert isinstance(store, ArtifactStore)
    assert ref == hashlib.sha256(b"payload").hexdigest()
    assert resolved == b"payload"
