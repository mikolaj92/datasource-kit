"""Build a fleet inventory by importing each datasource's manifest module.

A consumer names a plugin package (e.g. ``myproj.datasources``) and the
datasource keys living under it; :func:`fleet_inventory` imports each source's
manifest module and reads its module-level :class:`~datasource_kit.manifest.Manifest`.

It is import-based on purpose. The capability flags come from the very object
the runtime uses, so the inventory can never disagree with what actually runs.
A manifest module that is missing, fails to import, or exposes no ``Manifest``
of the expected shape is a fail-closed :class:`InventoryError` -- never an AST
scan of source text, never a filesystem heuristic, and never a silently skipped
unit. This replaces the older pattern of static-parsing manifest source files
to guess capability flags.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Iterable

from .errors import InventoryError
from .manifest import Manifest

__all__ = ["InventoryEntry", "fleet_inventory"]


@dataclass(slots=True, frozen=True)
class InventoryEntry:
    """One datasource's capability flags, sourced from its loaded manifest.

    ``name`` is the datasource key the caller passed (the one used to locate the
    module), which may differ from ``manifest.name``. The flag fields are
    conveniences derived from ``manifest`` so a consumer can build a fleet view
    without re-deriving them.
    """

    name: str
    manifest: Manifest
    is_autonomous: bool
    has_contract: bool
    execution_model: str
    rate_limit: dict[str, float]

    @classmethod
    def from_manifest(cls, name: str, manifest: Manifest) -> InventoryEntry:
        return cls(
            name=name,
            manifest=manifest,
            is_autonomous=manifest.is_autonomous,
            has_contract=manifest.contract is not None,
            execution_model=manifest.execution_model,
            rate_limit=dict(manifest.rate_limit),
        )


def _load_manifest(module_path: str, attr: str) -> Manifest:
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise InventoryError(
            f"cannot import manifest module '{module_path}': {exc}"
        ) from exc
    try:
        candidate = getattr(module, attr)
    except AttributeError as exc:
        raise InventoryError(
            f"manifest module '{module_path}' has no attribute '{attr}'"
        ) from exc
    if not isinstance(candidate, Manifest):
        raise InventoryError(
            f"'{module_path}.{attr}' is {type(candidate).__name__}, not a Manifest"
        )
    return candidate


def fleet_inventory(
    package: str,
    datasources: Iterable[str],
    *,
    submodule: str = "manifest",
    attr: str = "MANIFEST",
) -> list[InventoryEntry]:
    """Import each datasource's manifest and return its capability flags.

    For each key ``ds`` in ``datasources`` the module ``package.ds.submodule``
    is imported (or ``package.ds`` when ``submodule`` is empty) and its ``attr``
    is read as a :class:`Manifest`. Order follows ``datasources``.

    Raises :class:`InventoryError` (fail-closed) if any module is missing, fails
    to import, lacks ``attr``, or exposes a non-``Manifest`` value. No unit is
    ever skipped and no source text is parsed.
    """

    entries: list[InventoryEntry] = []
    for ds in datasources:
        module_path = f"{package}.{ds}.{submodule}" if submodule else f"{package}.{ds}"
        manifest = _load_manifest(module_path, attr)
        entries.append(InventoryEntry.from_manifest(ds, manifest))
    return entries
