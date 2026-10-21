from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Final

_ROOT: Final = Path(__file__).resolve().parents[2]
_FALA_URL: Final = "https://github.com/mikolaj92/Fala/archive/27c179aee96daf6953ef36d6b1a007214d14a0db.tar.gz"
_FALA_EXTRA: Final = f"fala @ {_FALA_URL}"


def _pyproject() -> dict[str, object]:
    return tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def test_fala_extra_installs_mikolaj92_fala_from_archive() -> None:
    extras = _pyproject()["project"]["optional-dependencies"]["fala"]

    assert extras == [_FALA_EXTRA]


def test_uv_sources_pin_fala_to_the_same_archive() -> None:
    lock = tomllib.loads((_ROOT / "uv.lock").read_text(encoding="utf-8"))
    package = next(p for p in lock["package"] if p["name"] == "fala")
    assert package["version"] == "0.8.1"
    assert package["source"] == {"url": _FALA_URL}
