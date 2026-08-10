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


def test_crash_after_quarantine_link_is_recoverable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    real_unlink = os.unlink
    crashed = False
    def crash(path: object, *args: object, **kwargs: object) -> None:
        nonlocal crashed
        if path == "pid.json" and not crashed:
            crashed = True
            raise OSError("simulated crash before source retirement")
        real_unlink(path, *args, **kwargs)
    monkeypatch.setattr(os, "unlink", crash)
    with pytest.raises(OSError): clear(tmp_path, digest)
    assert (tmp_path / "pid.json").exists()
    intents = list((tmp_path / "legacy-process-clearance.audit.d").glob("intent-*.json"))
    assert len(intents) == 1
    quarantine = tmp_path / json.loads(intents[0].read_text())["quarantine"]
    assert quarantine.exists() and os.stat(quarantine).st_ino == os.stat(tmp_path / "pid.json").st_ino
    monkeypatch.setattr(os, "unlink", real_unlink)
    completion = clear(tmp_path, digest)
    assert completion.name.startswith("complete-")
    assert not (tmp_path / "pid.json").exists()
    assert quarantine.stat().st_nlink == 1


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


def test_bounds_operator_and_ticket_before_writing(tmp_path: Path) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    with pytest.raises(ProcessTombstoneError, match="bounded operator"):
        clear(tmp_path, digest, operator="x" * 257)
    with pytest.raises(ProcessTombstoneError, match="bounded ticket"):
        clear(tmp_path, digest, ticket="x" * 513)
    assert not (tmp_path / "legacy-process-clearance.audit.d").exists()


def test_private_regular_single_link_lock_required(tmp_path: Path) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    lock = tmp_path / ".process.lock"
    lock.write_text("")
    lock.chmod(0o644)
    with pytest.raises(ProcessTombstoneError, match="unit lock"):
        clear(tmp_path, digest)


def test_existing_quarantine_collision_is_noreplace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    real_link = os.link
    collision = b"do not replace"
    def colliding_link(src: object, dst: object, *args: object, **kwargs: object) -> None:
        if src == "pid.json":
            (tmp_path / str(dst)).write_bytes(collision)
        real_link(src, dst, *args, **kwargs)
    monkeypatch.setattr(os, "link", colliding_link)
    with pytest.raises(ProcessTombstoneError, match="collision"):
        clear(tmp_path, digest)
    assert (tmp_path / "pid.json").exists()
    assert any(path.read_bytes() == collision for path in tmp_path.glob("*.quarantine"))


def test_torn_unpublished_audit_temp_does_not_wedge_retry(tmp_path: Path) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    audit_dir = tmp_path / "legacy-process-clearance.audit.d"
    audit_dir.mkdir(mode=0o700)
    (audit_dir / ".tmp-deadbeef").write_bytes(b'{"torn"')
    assert clear(tmp_path, digest).name.startswith("complete-")


def test_audit_is_two_atomic_files_and_binds_all_request_fields(tmp_path: Path) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    completion = clear(tmp_path, digest)
    audit_dir = completion.parent
    intents = list(audit_dir.glob("intent-*.json"))
    completes = list(audit_dir.glob("complete-*.json"))
    assert len(intents) == len(completes) == 1
    intent, complete = json.loads(intents[0].read_text()), json.loads(completes[0].read_text())
    for key in ("unit", "generation", "operator", "ticket", "tombstone_sha256", "quarantine", "operation_id"):
        assert intent[key] == complete[key]
    assert intent["event"].endswith("intent") and complete["event"].endswith("complete")
    with pytest.raises(ProcessTombstoneError):
        clear(tmp_path, digest, ticket="different")


def test_forged_completion_with_live_pid_fails_without_retirement(tmp_path: Path) -> None:
    raw, digest = legacy(tmp_path / "pid.json")
    completion = clear(tmp_path, digest)
    # Model a forged/inconsistent completed namespace with a newly live tombstone.
    (tmp_path / "pid.json").write_bytes(raw)
    with pytest.raises(ProcessTombstoneError, match="completion exists while tombstone"):
        clear(tmp_path, digest)
    assert (tmp_path / "pid.json").read_bytes() == raw
    assert completion.exists()


def test_crash_after_audit_publication_before_temp_unlink_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    real_unlink = os.unlink
    crashed = False
    def crash(path: object, *args: object, **kwargs: object) -> None:
        nonlocal crashed
        if isinstance(path, str) and path.startswith(".tmp-") and not crashed:
            crashed = True
            raise OSError("simulated crash after audit link publication")
        real_unlink(path, *args, **kwargs)
    monkeypatch.setattr(os, "unlink", crash)
    with pytest.raises(OSError):
        clear(tmp_path, digest)
    audit_dir = tmp_path / "legacy-process-clearance.audit.d"
    intent = next(audit_dir.glob("intent-*.json"))
    temps = list(audit_dir.glob(".tmp-*"))
    assert len(temps) == 1 and intent.stat().st_ino == temps[0].stat().st_ino
    monkeypatch.setattr(os, "unlink", real_unlink)
    completion = clear(tmp_path, digest)
    assert completion.exists() and not temps[0].exists()
    assert intent.stat().st_nlink == 1


def test_audit_directory_swap_is_detected_before_retirement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    real_stat = os.stat
    checks = 0
    swapped = False
    def swapping_stat(path: object, *args: object, **kwargs: object):
        nonlocal checks, swapped
        if path == "legacy-process-clearance.audit.d" and kwargs.get("dir_fd") is not None:
            checks += 1
            # open validation, initial binding, then pre-retirement binding
            if checks == 3 and not swapped:
                swapped = True
                os.rename(tmp_path / str(path), tmp_path / "audit-preserved")
                (tmp_path / str(path)).mkdir(mode=0o700)
        return real_stat(path, *args, **kwargs)
    monkeypatch.setattr(os, "stat", swapping_stat)
    with pytest.raises(ProcessTombstoneError, match="audit directory pathname identity changed"):
        clear(tmp_path, digest)
    assert swapped
    assert (tmp_path / "pid.json").exists()  # pre-retirement identity gate
    assert list((tmp_path / "audit-preserved").glob("intent-*.json"))
    quarantine = list(tmp_path.glob("*.quarantine"))
    assert len(quarantine) == 1  # conservative hard-link publication remains auditable
