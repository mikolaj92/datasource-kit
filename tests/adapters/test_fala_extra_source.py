from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Final

_ROOT: Final = Path(__file__).resolve().parents[2]
_FALA_GIT: Final = "https://github.com/mikolaj92/Fala"
_FALA_TAG: Final = "v0.7.28"
_FALA_EXTRA: Final = f"fala @ git+{_FALA_GIT}.git@{_FALA_TAG}"


def _pyproject() -> dict[str, object]:
    return tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def test_fala_extra_installs_mikolaj92_fala_from_git() -> None:
    extras = _pyproject()["project"]["optional-dependencies"]["fala"]

    assert extras == [_FALA_EXTRA]


def test_uv_sources_pin_fala_to_the_same_git_tag() -> None:
    sources = _pyproject()["tool"]["uv"]["sources"]["fala"]

    assert sources == {"git": _FALA_GIT, "tag": _FALA_TAG}
