from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from datasource_kit import ProcessTombstoneError, clear_legacy_process_tombstone, fleet


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


def _mixed_clearance_process(root: str, digest: str, results: object) -> None:
    """Spawn-safe worker for the mixed process/thread regression test."""
    queue = results

    def run() -> None:
        try:
            result = clear(Path(root), digest)
        except BaseException as exc:  # noqa: BLE001 - crosses process boundary
            queue.put(("error", type(exc).__name__, str(exc)))  # type: ignore[attr-defined]
        else:
            queue.put(("ok", str(result)))  # type: ignore[attr-defined]

    threads = [threading.Thread(target=run) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


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


def test_threads_are_serialized_before_clearance_descriptor_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """flock is not a thread mutex on every supported POSIX platform."""
    active = 0
    maximum = 0
    guard = threading.Lock()

    def clearance(*args: object, **kwargs: object) -> Path:
        nonlocal active, maximum
        with guard:
            active += 1
            maximum = max(maximum, active)
        time.sleep(0.002)
        with guard:
            active -= 1
        return tmp_path / "complete.json"

    monkeypatch.setattr(fleet, "_clear_legacy_process_tombstone_locked", clearance)
    threads = [threading.Thread(
        target=lambda: clear_legacy_process_tombstone(
            tmp_path, unit="u", generation=4, expected_sha256="0" * 64,
            workload_fully_gone_asserted=True, operator="alice", ticket="OPS-42",
        )
    ) for _ in range(32)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert maximum == 1


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


def test_six_spawned_processes_with_eight_threads_are_idempotent(tmp_path: Path) -> None:
    """Regress Darwin's O_CREAT/O_NOFOLLOW race for an initially absent lock."""
    _, digest = legacy(tmp_path / "pid.json")
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    processes = [
        context.Process(target=_mixed_clearance_process,
                        args=(str(tmp_path.resolve()), digest, results))
        for _ in range(6)
    ]
    for process in processes:
        process.start()
    received = [results.get(timeout=30) for _ in range(48)]
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0

    errors = [result for result in received if result[0] == "error"]
    assert not errors
    returned = {result[1] for result in received}
    assert len(returned) == 1
    audit_dir = tmp_path / "legacy-process-clearance.audit.d"
    assert len(list(audit_dir.glob("intent-*.json"))) == 1
    assert len(list(audit_dir.glob("complete-*.json"))) == 1
    assert len(list(tmp_path.glob("*.quarantine"))) == 1


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
    with pytest.raises(ProcessTombstoneError): clear(tmp_path, digest)
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
    with pytest.raises(ProcessTombstoneError):
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


def test_new_lock_is_normalized_and_durable_before_flock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    events: list[tuple[str, int]] = []
    real_fchmod, real_fsync, real_flock = os.fchmod, os.fsync, fleet.fcntl.flock
    monkeypatch.setattr(os, "fchmod", lambda fd, mode: (events.append(("fchmod", mode)), real_fchmod(fd, mode))[1])
    monkeypatch.setattr(os, "fsync", lambda fd: (events.append(("fsync", fd)), real_fsync(fd))[1])
    monkeypatch.setattr(fleet.fcntl, "flock", lambda fd, op: (events.append(("flock", fd)), real_flock(fd, op))[1])
    clear(tmp_path, digest)
    assert events[0] == ("fchmod", 0o600)
    assert events[1][0] == events[2][0] == "fsync"
    assert events[1][1] != events[2][1]
    assert events[3][0] == "flock"
    assert (tmp_path / ".process.lock").stat().st_mode & 0o777 == 0o600


def test_existing_lock_is_validated_without_chmod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    (tmp_path / ".process.lock").write_bytes(b"")
    os.chmod(tmp_path / ".process.lock", 0o600)
    real_fchmod = os.fchmod
    lock_inode = (tmp_path / ".process.lock").stat().st_ino
    def guarded_fchmod(fd: int, mode: int) -> None:
        if os.fstat(fd).st_ino == lock_inode:
            raise AssertionError("must not chmod existing lock")
        real_fchmod(fd, mode)
    monkeypatch.setattr(os, "fchmod", guarded_fchmod)
    clear(tmp_path, digest)


def test_lock_publication_failure_is_chained_and_preserves_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, digest = legacy(tmp_path / "pid.json")
    failure = OSError("disk failure")
    monkeypatch.setattr(os, "fsync", lambda _fd: (_ for _ in ()).throw(failure))
    with pytest.raises(ProcessTombstoneError) as caught:
        clear(tmp_path, digest)
    assert caught.value.__cause__ is failure
    assert (tmp_path / ".process.lock").exists()
    assert (tmp_path / "pid.json").exists()
