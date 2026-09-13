from __future__ import annotations

import subprocess
import tomllib
from pathlib import Path
from typing import Final

_ROOT: Final = Path(__file__).resolve().parents[2]
_FASTAPI: Final = "fastapi>=0.141.1"
_HTTPX2: Final = "httpx2>=2.12.0"
_DEV: Final = ["pytest>=8.0", _FASTAPI, _HTTPX2]


def _pyproject() -> dict[str, object]:
    return tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def _lock_packages() -> dict[str, str]:
    lock = tomllib.loads((_ROOT / "uv.lock").read_text(encoding="utf-8"))
    return {package["name"]: package["version"] for package in lock["package"]}


def test_fastapi_extra_pins_current_fastapi() -> None:
    extras = _pyproject()["project"]["optional-dependencies"]["fastapi"]

    assert extras == [_FASTAPI]


def test_dev_extra_and_group_pin_current_fastapi_and_httpx2() -> None:
    extras = _pyproject()["project"]["optional-dependencies"]["dev"]
    group = _pyproject()["dependency-groups"]["dev"]

    assert extras == _DEV
    assert group == _DEV


def test_uv_lock_contains_current_fastapi_and_httpx2() -> None:
    packages = _lock_packages()

    assert packages["fastapi"] == "0.141.1"
    assert packages["httpx2"] == "2.12.0"


def test_uv_lock_matches_the_manifest() -> None:
    completed = subprocess.run(
        ["uv", "lock", "--check"],
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
