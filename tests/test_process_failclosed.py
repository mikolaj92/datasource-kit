from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from datasource_kit.fleet import (
    ProcessSpec,
    ProcessTombstoneError,
    clear_process_tombstone,
    spawn,
    stop_process,
)


def spec() -> ProcessSpec:
    return ProcessSpec(unit="u", command=(sys.executable, "-c", "pass"), probe_window=.2, probe_sleep=.02)


def test_exited_leader_retains_tombstone_and_blocks_duplicate(tmp_path: Path) -> None:
    first = spawn(spec(), unit_dir=tmp_path, generation=1)
    assert not first.alive
    tombstone = json.loads((tmp_path / "pid.json").read_text())
    assert tombstone["token"] == first.token
    with pytest.raises(ProcessTombstoneError):
        spawn(spec(), unit_dir=tmp_path, generation=2)


def test_legacy_corrupt_and_reused_pid_metadata_all_block(tmp_path: Path) -> None:
    for raw in ('{"pid": 1}', '{broken', '{"pid": 999999}'):
        (tmp_path / "pid.json").write_text(raw)
        with pytest.raises(ProcessTombstoneError):
            spawn(spec(), unit_dir=tmp_path, generation=2)


def test_exact_asserted_clearance_audits_and_enables_replacement(tmp_path: Path) -> None:
    first = spawn(spec(), unit_dir=tmp_path, generation=7)
    with pytest.raises(ProcessTombstoneError):
        clear_process_tombstone(tmp_path, unit="u", generation=7, token=first.token or "", workload_fully_gone_asserted=False)
    with pytest.raises(ProcessTombstoneError):
        clear_process_tombstone(tmp_path, unit="u", generation=7, token="wrong", workload_fully_gone_asserted=True)
    audit = clear_process_tombstone(tmp_path, unit="u", generation=7, token=first.token or "", workload_fully_gone_asserted=True, operator="test")
    assert audit.exists() and '"asserted_workload_fully_gone": true' in audit.read_text()
    replacement = spawn(spec(), unit_dir=tmp_path, generation=8)
    assert replacement.token != first.token


def test_numeric_pid_signalling_is_disabled() -> None:
    with pytest.raises(ProcessTombstoneError):
        stop_process(1)
