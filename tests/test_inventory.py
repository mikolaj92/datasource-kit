from __future__ import annotations

import sys
from pathlib import Path

import pytest

from datasource_kit import (
    EXECUTION_AUTONOMOUS,
    InventoryEntry,
    InventoryError,
    fleet_inventory,
)


def _write(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _make_plugin_package(root: Path, package: str) -> None:
    """Create an importable plugin package with three datasource manifests."""

    pkg_dir = root / package
    _write(pkg_dir / "__init__.py", "")

    # Autonomous scraper declared via the first-class execution model.
    _write(
        pkg_dir / "eli" / "__init__.py",
        "",
    )
    _write(
        pkg_dir / "eli" / "manifest.py",
        (
            "from datasource_kit import EXECUTION_AUTONOMOUS, ExecutionModel, Manifest, SourceContract\n"
            "MANIFEST = Manifest(\n"
            "    name='eli', source_type='acts',\n"
            "    execution=ExecutionModel(EXECUTION_AUTONOMOUS),\n"
            "    rate_limit={'rps': 2.0},\n"
            "    contract=SourceContract('s', 'e', ('p',), 'id', 'corpus'),\n"
            ")\n"
        ),
    )

    # Autonomous scraper declared via the first-class execution model.
    _write(pkg_dir / "saos" / "__init__.py", "")
    _write(
        pkg_dir / "saos" / "manifest.py",
        (
            "from datasource_kit import EXECUTION_AUTONOMOUS, ExecutionModel, Manifest, SourceContract\n"
            "MANIFEST = Manifest(\n"
            "    name='saos', source_type='judgments',\n"
            "    execution=ExecutionModel(EXECUTION_AUTONOMOUS, 'saos.worker:run'),\n"
            "    contract=SourceContract('s', 'e', ('p',), 'id', 'corpus'),\n"
            ")\n"
        ),
    )

    # Plain batch source.
    _write(pkg_dir / "clp" / "__init__.py", "")
    _write(
        pkg_dir / "clp" / "manifest.py",
        (
            "from datasource_kit import Manifest\n"
            "MANIFEST = Manifest(name='clp', source_type='batch')\n"
        ),
    )


@pytest.fixture
def plugin_pkg(tmp_path: Path):
    package = "kit_inv_fixture"
    _make_plugin_package(tmp_path, package)
    sys.path.insert(0, str(tmp_path))
    try:
        yield package
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(package + "."):
                del sys.modules[name]


def test_fleet_inventory_reads_capability_flags(plugin_pkg: str):
    entries = fleet_inventory(plugin_pkg, ["eli", "saos", "clp"])
    assert [e.name for e in entries] == ["eli", "saos", "clp"]
    assert all(isinstance(e, InventoryEntry) for e in entries)

    eli, saos, clp = entries
    assert eli.is_autonomous is True
    assert eli.has_contract is True
    assert eli.execution_model == EXECUTION_AUTONOMOUS
    assert eli.rate_limit == {"rps": 2.0}

    assert saos.is_autonomous is True
    assert saos.has_contract is True
    assert saos.execution_model == "autonomous"
    assert saos.manifest.execution is not None
    assert saos.manifest.execution.step_ref == "saos.worker:run"

    assert clp.is_autonomous is False
    assert clp.has_contract is False
    assert clp.execution_model == ""


def test_fleet_inventory_preserves_requested_order(plugin_pkg: str):
    entries = fleet_inventory(plugin_pkg, ["clp", "eli"])
    assert [e.name for e in entries] == ["clp", "eli"]


def test_rate_limit_is_copied_not_shared(plugin_pkg: str):
    (entry,) = fleet_inventory(plugin_pkg, ["eli"])
    entry.rate_limit["rps"] = 999.0
    assert entry.manifest.rate_limit == {"rps": 2.0}


def test_missing_datasource_fails_closed(plugin_pkg: str):
    with pytest.raises(InventoryError) as exc:
        fleet_inventory(plugin_pkg, ["eli", "does_not_exist"])
    assert "does_not_exist" in str(exc.value)


def test_missing_manifest_attr_fails_closed(tmp_path: Path):
    package = "kit_inv_noattr"
    pkg_dir = tmp_path / package
    _write(pkg_dir / "__init__.py", "")
    _write(pkg_dir / "empty" / "__init__.py", "")
    _write(pkg_dir / "empty" / "manifest.py", "X = 1\n")
    sys.path.insert(0, str(tmp_path))
    try:
        with pytest.raises(InventoryError) as exc:
            fleet_inventory(package, ["empty"])
        assert "MANIFEST" in str(exc.value)
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(package + "."):
                del sys.modules[name]


def test_non_manifest_value_fails_closed(tmp_path: Path):
    package = "kit_inv_wrongtype"
    pkg_dir = tmp_path / package
    _write(pkg_dir / "__init__.py", "")
    _write(pkg_dir / "bad" / "__init__.py", "")
    _write(pkg_dir / "bad" / "manifest.py", "MANIFEST = {'name': 'bad'}\n")
    sys.path.insert(0, str(tmp_path))
    try:
        with pytest.raises(InventoryError) as exc:
            fleet_inventory(package, ["bad"])
        assert "not a Manifest" in str(exc.value)
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(package + "."):
                del sys.modules[name]


def test_custom_submodule_and_attr(tmp_path: Path):
    package = "kit_inv_custom"
    pkg_dir = tmp_path / package
    _write(pkg_dir / "__init__.py", "")
    _write(pkg_dir / "src" / "__init__.py", "")
    _write(
        pkg_dir / "src" / "descriptor.py",
        (
            "from datasource_kit import Manifest\n"
            "SOURCE = Manifest(name='src', source_type='batch')\n"
        ),
    )
    sys.path.insert(0, str(tmp_path))
    try:
        entries = fleet_inventory(
            package, ["src"], submodule="descriptor", attr="SOURCE"
        )
        assert entries[0].manifest.name == "src"
    finally:
        sys.path.remove(str(tmp_path))
        for name in list(sys.modules):
            if name == package or name.startswith(package + "."):
                del sys.modules[name]
