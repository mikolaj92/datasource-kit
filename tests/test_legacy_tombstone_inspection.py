from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from datasource_kit import ProcessTombstoneError, inspect_legacy_process_tombstone


def write_legacy(root: Path) -> tuple[bytes, str]:
    raw = json.dumps(
        {"pid": 123, "unit": "u", "generation": 4, "status": "old"},
        sort_keys=True,
    ).encode()
    (root / "pid.json").write_bytes(raw)
    return raw, hashlib.sha256(raw).hexdigest()


def test_inspects_exact_bytes_without_pid_liveness(tmp_path: Path) -> None:
    raw, digest = write_legacy(tmp_path)
    with patch("os.kill", side_effect=AssertionError("must not inspect PID liveness")):
        result = inspect_legacy_process_tombstone(tmp_path)
    assert result.path == tmp_path / "pid.json"
    assert result.sha256 == digest
    assert result.generation == 4
    assert result.token_absent is True
    assert (tmp_path / "pid.json").read_bytes() == raw


def test_token_key_is_reported_present_even_when_null(tmp_path: Path) -> None:
    (tmp_path / "pid.json").write_text('{"generation":4,"token":null}')
    assert inspect_legacy_process_tombstone(tmp_path).token_absent is False


def test_malformed_metadata_is_not_reported_clearable(tmp_path: Path) -> None:
    (tmp_path / "pid.json").write_bytes(b"{broken")
    result = inspect_legacy_process_tombstone(tmp_path)
    assert result.generation is None
    assert result.token_absent is False


def test_json_integer_over_interpreter_limit_is_not_reported_clearable(
    tmp_path: Path,
) -> None:
    (tmp_path / "pid.json").write_bytes(
        b'{"generation":' + (b"9" * 5_000) + b'}'
    )
    result = inspect_legacy_process_tombstone(tmp_path)
    assert result.generation is None
    assert result.token_absent is False


def test_symlink_and_hardlink_tombstones_fail_closed(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.write_text('{"generation":4}')
    (tmp_path / "pid.json").symlink_to(outside)
    with pytest.raises((ProcessTombstoneError, OSError)):
        inspect_legacy_process_tombstone(tmp_path)
    (tmp_path / "pid.json").unlink()
    os.link(outside, tmp_path / "pid.json")
    with pytest.raises(ProcessTombstoneError, match="single-link"):
        inspect_legacy_process_tombstone(tmp_path)


def test_tombstone_path_swap_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_legacy(tmp_path)
    replacement = b'{"unit":"u","generation":99}'
    real_stat = os.stat
    swapped = False

    def swapping_stat(path: object, *args: object, **kwargs: object):
        nonlocal swapped
        if path == "pid.json" and kwargs.get("dir_fd") is not None and not swapped:
            swapped = True
            os.replace(tmp_path / "pid.json", tmp_path / "original")
            (tmp_path / "pid.json").write_bytes(replacement)
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", swapping_stat)
    with pytest.raises(ProcessTombstoneError, match="pathname identity changed"):
        inspect_legacy_process_tombstone(tmp_path)
    assert swapped and (tmp_path / "pid.json").read_bytes() == replacement


def test_unit_namespace_swap_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    unit = tmp_path / "unit"
    unit.mkdir()
    write_legacy(unit)
    real_stat = os.stat
    named_unit_checks = 0

    def swapping_stat(path: object, *args: object, **kwargs: object):
        nonlocal named_unit_checks
        if path == unit and kwargs.get("follow_symlinks") is False:
            named_unit_checks += 1
            if named_unit_checks == 2:
                os.rename(unit, tmp_path / "preserved")
                unit.mkdir()
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", swapping_stat)
    with pytest.raises(ProcessTombstoneError, match="unit directory pathname identity changed"):
        inspect_legacy_process_tombstone(unit)
    assert named_unit_checks == 2
