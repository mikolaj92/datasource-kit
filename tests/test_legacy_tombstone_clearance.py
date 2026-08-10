from __future__ import annotations

import hashlib
import json
import os
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

from datasource_kit import ProcessTombstoneError, clear_legacy_process_tombstone


def legacy(path: Path, **changes: object) -> tuple[bytes, str]:
    data: dict[str, object] = {"unit": "u", "generation": 4, "pid": 123, "status": "old"}
    data.update(changes)
    raw = json.dumps(data, sort_keys=True).encode()
    path.write_bytes(raw)
    return raw, hashlib.sha256(raw).hexdigest()


def clear(root: Path, digest: str, **changes: object) -> Path:
    args: dict[str, object] = {
        "unit": "u", "generation": 4, "expected_sha256": digest,
        "workload_fully_gone_asserted": True, "operator": "alice", "ticket": "OPS-42",
    }
    args.update(changes)
    return clear_legacy_process_tombstone(root, **args)  # type: ignore[arg-type]


def test_clear_audits_then_quarantines_without_signalling(tmp_path: Path) -> None:
    raw, digest = legacy(tmp_path / "pid.json")
    with patch("os.kill", side_effect=AssertionError("must not inspect a PID")):
        audit = clear(tmp_path, digest)
    assert not (tmp_path / "pid.json").exists()
    record = json.loads(audit.read_text())
    assert record["tombstone_sha256"] == digest
    assert record["unit"] == "u" and record["generation"] == 4
    assert record["operator"] == "alice" and record["ticket"] == "OPS-42"
    assert record["asserted_workload_fully_gone"] is True
    quarantine = tmp_path / record["quarantine"]
    assert quarantine.read_bytes() == raw
    assert quarantine.stat().st_mode & 0o777 == 0o600
    assert audit.stat().st_mode & 0o777 == 0o600
    # Identical retry verifies both durable artifacts and returns the audit.
    assert clear(tmp_path, digest) == audit


@pytest.mark.parametrize("token", [None, "capability"])
def test_token_key_present_even_null_is_not_legacy(tmp_path: Path, token: object) -> None:
    _, digest = legacy(tmp_path / "pid.json", token=token)
    with pytest.raises(ProcessTombstoneError, match="not tokenless"):
        clear(tmp_path, digest)


@pytest.mark.parametrize("raw", [b"{broken", b"[]", b'{}'])
def test_malformed_or_incomplete_fails_closed(tmp_path: Path, raw: bytes) -> None:
    (tmp_path / "pid.json").write_bytes(raw)
    with pytest.raises(ProcessTombstoneError):
        clear(tmp_path, hashlib.sha256(raw).hexdigest())
    assert (tmp_path / "pid.json").exists()


def test_all_attestations_and_exact_identity_are_required(tmp_path: Path) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    bad = [
        {"workload_fully_gone_asserted": False}, {"operator": " "}, {"ticket": ""},
        {"unit": "v"}, {"generation": 5}, {"expected_sha256": "0" * 64},
    ]
    for kwargs in bad:
        with pytest.raises((ProcessTombstoneError, ValueError)):
            clear(tmp_path, digest, **kwargs)
    assert (tmp_path / "pid.json").exists()


def test_symlink_and_hardlink_fail_closed(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    _, digest = legacy(outside)
    (tmp_path / "pid.json").symlink_to(outside)
    with pytest.raises((ProcessTombstoneError, OSError)):
        clear(tmp_path, digest)
    (tmp_path / "pid.json").unlink()
    os.link(outside, tmp_path / "pid.json")
    with pytest.raises(ProcessTombstoneError):
        clear(tmp_path, digest)


def test_concurrent_calls_have_one_audit_and_are_idempotent(tmp_path: Path) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    results: list[Path] = []
    errors: list[BaseException] = []
    def run() -> None:
        try: results.append(clear(tmp_path, digest))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
    threads = [threading.Thread(target=run) for _ in range(8)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert not errors and len(results) == 8
    assert len(results[0].read_text().splitlines()) == 1


def test_crash_after_durable_audit_is_recoverable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    real_rename = os.rename
    calls = 0
    def crash(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise OSError("simulated crash before retirement")
    monkeypatch.setattr(os, "rename", crash)
    with pytest.raises(OSError): clear(tmp_path, digest)
    assert (tmp_path / "pid.json").exists()
    assert len((tmp_path / "legacy-process-clearance.audit.jsonl").read_text().splitlines()) == 1
    monkeypatch.setattr(os, "rename", real_rename)
    clear(tmp_path, digest)
    assert len((tmp_path / "legacy-process-clearance.audit.jsonl").read_text().splitlines()) == 1


def test_path_swap_before_cas_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    replacement = b'{"unit":"u","generation":4,"pid":999}'
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
    with pytest.raises(ProcessTombstoneError, match="identity changed"):
        clear(tmp_path, digest)
    assert (tmp_path / "pid.json").read_bytes() == replacement
