from __future__ import annotations

import builtins
import hashlib
import sys
import types
from pathlib import Path
from typing import BinaryIO, Final

import pytest


_INSTALL_HINT: Final = (
    "FalaArtifactStore requires the 'fala' extra: "
    "pip install datasource-kit[fala]"
)


def _block_fala_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "fala", raising=False)
    monkeypatch.delitem(sys.modules, "fala.artifacts", raising=False)
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


class _FakeArtifactRef:
    def __init__(self, *, kind: str, uri: str) -> None:
        self.kind = kind
        self.uri = uri


class _FakeFileArtifactStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self._payloads: dict[str, bytes] = {}

    def put_fileobj(
        self,
        *,
        kind: str,
        fileobj: BinaryIO,
        filename: str,
        artifact_id: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> _FakeArtifactRef:
        payload = fileobj.read()
        digest = hashlib.sha256(payload).hexdigest()
        uri = f"fala-artifact://sha256/{digest}"
        self._payloads[uri] = payload
        return _FakeArtifactRef(kind=kind, uri=uri)

    def resolve(self, artifact: _FakeArtifactRef) -> Path:
        digest = artifact.uri.removeprefix("fala-artifact://sha256/")
        target = self.root / "blobs" / "sha256" / digest[:2] / digest
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self._payloads[artifact.uri])
        return target


def _install_fake_fala(monkeypatch: pytest.MonkeyPatch) -> None:
    package = types.ModuleType("fala")
    artifacts = types.ModuleType("fala.artifacts")
    artifacts.ArtifactRef = _FakeArtifactRef
    artifacts.FileArtifactStore = _FakeFileArtifactStore
    package.artifacts = artifacts
    monkeypatch.setitem(sys.modules, "fala", package)
    monkeypatch.setitem(sys.modules, "fala.artifacts", artifacts)


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


def test_fala_artifact_store_requires_fala_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the adapter module is importable, but fala itself is not.
    from datasource_kit.adapters.fala import FalaArtifactStore

    _block_fala_imports(monkeypatch)

    # When / Then: constructing the adapter raises the install hint.
    with pytest.raises(ImportError) as exc_info:
        FalaArtifactStore("unused")
    assert str(exc_info.value) == _INSTALL_HINT


def test_fala_artifact_store_round_trips_bytes_through_fala(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # Given: a fala-compatible FileArtifactStore implementation is importable.
    _install_fake_fala(monkeypatch)
    from datasource_kit import ArtifactStore
    from datasource_kit.adapters.fala import FalaArtifactStore

    store = FalaArtifactStore(tmp_path)

    # When: payload bytes are stored and resolved through the adapter.
    ref = store.store(b"payload")
    resolved = store.resolve(ref)

    # Then: the adapter satisfies the kit protocol and returns the original bytes.
    assert isinstance(store, ArtifactStore)
    assert ref.startswith("fala-artifact://sha256/")
    assert resolved == b"payload"
