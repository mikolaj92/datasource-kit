from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from datasource_kit import (
    BackoffPolicy,
    FileCheckpointStore,
    InMemoryCheckpointStore,
    WorkerHost,
    WorkerStep,
)


class Intent:
    def __init__(self, plans: list[WorkerStep | None]) -> None:
        self.plans = plans
        self.calls: list[tuple[str, object]] = []

    def plan(self, checkpoint: object | None) -> WorkerStep | None:
        self.calls.append(("plan", checkpoint))
        return self.plans.pop(0) if self.plans else None

    def fetch(self, plan: object) -> object:
        self.calls.append(("fetch", plan))
        return f"raw:{plan}"

    def transform(self, payload: object, plan: object) -> object:
        self.calls.append(("transform", payload))
        return f"mapped:{payload}"

    def persist(self, transformed: object, plan: object) -> None:
        self.calls.append(("persist", transformed))


def test_host_runs_consumer_pipeline_and_advances_checkpoint_after_persist() -> None:
    intent = Intent([WorkerStep("page-1", {"page": 1}), WorkerStep("page-2", {"page": 2})])
    checkpoints = InMemoryCheckpointStore({"page": 0})
    events = []

    result = WorkerHost(
        intent,
        checkpoints,
        heartbeat=events.append,
        now=lambda: datetime(2025, 1, 1, tzinfo=UTC),
    ).run(max_iterations=2)

    assert result.completed == 2
    assert result.failures == 0
    assert result.checkpoint == {"page": 2}
    assert checkpoints.load() == {"page": 2}
    assert intent.calls == [
        ("plan", {"page": 0}),
        ("fetch", "page-1"),
        ("transform", "raw:page-1"),
        ("persist", "mapped:raw:page-1"),
        ("plan", {"page": 1}),
        ("fetch", "page-2"),
        ("transform", "raw:page-2"),
        ("persist", "mapped:raw:page-2"),
    ]
    assert [event.state for event in events] == [
        "starting", "working", "checkpointed", "working", "checkpointed", "stopped"
    ]
    assert events[-2].timestamp == "2025-01-01T00:00:00+00:00"


def test_failure_keeps_checkpoint_and_uses_capped_exponential_backoff() -> None:
    class Flaky(Intent):
        failures = 0

        def fetch(self, plan: object) -> object:
            self.calls.append(("fetch", plan))
            if self.failures < 2:
                self.failures += 1
                raise OSError("offline")
            return "raw"

    # Each retry is replanned from the last durable checkpoint.
    intent = Flaky([WorkerStep("p", 1), WorkerStep("p", 1), WorkerStep("p", 1)])
    sleeps: list[float] = []
    events = []
    result = WorkerHost(
        intent,
        InMemoryCheckpointStore(0),
        backoff=BackoffPolicy(initial_seconds=2, maximum_seconds=3, multiplier=2),
        sleep=sleeps.append,
        heartbeat=events.append,
    ).run(max_iterations=3)

    assert sleeps == [2, 3]
    assert result == result.__class__(checkpoint=1, completed=1, failures=2, iterations=3)
    assert [call for call in intent.calls if call[0] == "plan"] == [("plan", 0)] * 3
    backoffs = [event for event in events if event.state == "backoff"]
    assert [event.retry_in for event in backoffs] == [2, 3]
    assert backoffs[0].error == "OSError: offline"


def test_checkpoint_is_not_advanced_when_persistence_fails() -> None:
    class Broken(Intent):
        def persist(self, transformed: object, plan: object) -> None:
            raise RuntimeError("database down")

    store = InMemoryCheckpointStore("old")
    result = WorkerHost(
        Broken([WorkerStep("p", "new")]), store,
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert result.failures == 1
    assert result.completed == 0
    assert store.load() == "old"


def test_idle_polling_stop_and_heartbeat_errors_are_safe() -> None:
    host: WorkerHost
    sleeps = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        host.request_stop()

    host = WorkerHost(
        Intent([None]),
        InMemoryCheckpointStore("cursor"),
        backoff=BackoffPolicy(idle_seconds=7),
        sleep=sleep,
        heartbeat=lambda event: (_ for _ in ()).throw(RuntimeError("monitor down")),
    )
    result = host.run()
    assert sleeps == [7]
    assert result.iterations == 1
    assert result.checkpoint == "cursor"


def test_terminal_failure_is_reraised_after_configured_limit() -> None:
    class Broken(Intent):
        def fetch(self, plan: object) -> object:
            raise LookupError("gone")

    events = []
    host = WorkerHost(
        Broken([WorkerStep("p", 1), WorkerStep("p", 1)]),
        InMemoryCheckpointStore(),
        backoff=BackoffPolicy(
            initial_seconds=0, maximum_seconds=0, max_consecutive_failures=2
        ),
        sleep=lambda _: None,
        heartbeat=events.append,
    )
    with pytest.raises(LookupError, match="gone"):
        host.run()
    assert [event.state for event in events][-2:] == ["failed", "stopped"]


def test_invalid_planner_result_is_a_normal_retriable_consumer_failure() -> None:
    class Bad(Intent):
        def plan(self, checkpoint: object | None) -> object:
            return "not a step"

    result = WorkerHost(
        Bad([]), InMemoryCheckpointStore(),
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert result.failures == 1


def test_file_checkpoint_store_roundtrip_atomic_replace_and_missing(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "checkpoint.json"
    store = FileCheckpointStore(path)
    assert store.load() is None
    store.save({"cursor": [1, "two"]})
    assert store.load() == {"cursor": [1, "two"]}
    assert json.loads(path.read_text()) == {"cursor": [1, "two"]}
    store.save("next")
    assert store.load() == "next"
    assert not list(path.parent.glob("*.tmp"))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"initial_seconds": -1},
        {"initial_seconds": 2, "maximum_seconds": 1},
        {"multiplier": 0.5},
        {"idle_seconds": -1},
        {"max_consecutive_failures": 0},
    ],
)
def test_backoff_policy_rejects_invalid_values(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        BackoffPolicy(**kwargs)


def test_iteration_bound_validation_and_zero_iteration_run() -> None:
    host = WorkerHost(Intent([]), InMemoryCheckpointStore("x"))
    with pytest.raises(ValueError):
        host.run(max_iterations=-1)
    result = host.run(max_iterations=0)
    assert result.iterations == 0
    assert result.checkpoint == "x"
