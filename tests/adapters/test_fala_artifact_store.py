from __future__ import annotations

import builtins
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
