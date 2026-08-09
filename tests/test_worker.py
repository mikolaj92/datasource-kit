from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from datasource_kit import (
    BackoffPolicy,
    FileCheckpointStore,
    InMemoryCheckpointStore,
    SourceIntent,
    SourceOutput,
    StepDecision,
    WorkDirective,
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


class LateCheckpointIntent:
    def __init__(self, decisions: list[StepDecision]) -> None:
        self.decisions = decisions
        self.calls: list[tuple[str, object]] = []

    def plan(self, checkpoint: object | None) -> StepDecision:
        self.calls.append(("plan", checkpoint))
        return self.decisions.pop(0)

    def fetch(self, plan: object) -> object:
        self.calls.append(("fetch", plan))
        return {"records": [plan], "cursor": f"after-{plan}"}

    def transform(self, payload: object, plan: object) -> object:
        self.calls.append(("transform", payload))
        return payload

    def persist(self, transformed: object, plan: object) -> None:
        self.calls.append(("persist", transformed))

    def checkpoint(self, transformed: object, plan: object) -> object:
        self.calls.append(("checkpoint", transformed))
        return transformed["cursor"]


def test_checkpoint_can_be_derived_from_outcome_after_persistence() -> None:
    intent = LateCheckpointIntent([StepDecision(WorkDirective.CONTINUE, "page")])
    store = InMemoryCheckpointStore("old")

    result = WorkerHost(intent, store).run(max_iterations=1)

    assert result.checkpoint == "after-page"
    assert store.load() == "after-page"
    assert [name for name, _ in intent.calls] == [
        "plan",
        "fetch",
        "transform",
        "persist",
        "checkpoint",
    ]


def test_late_checkpoint_is_not_computed_when_persistence_fails() -> None:
    class Broken(LateCheckpointIntent):
        def persist(self, transformed: object, plan: object) -> None:
            self.calls.append(("persist", transformed))
            raise RuntimeError("database down")

    intent = Broken([StepDecision(WorkDirective.CONTINUE, "page")])
    store = InMemoryCheckpointStore("old")
    result = WorkerHost(
        intent,
        store,
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)

    assert result.failures == 1
    assert store.load() == "old"
    assert "checkpoint" not in [name for name, _ in intent.calls]


def test_idle_and_stop_directives_are_generic_and_stop_is_terminal() -> None:
    intent = LateCheckpointIntent(
        [
            StepDecision(WorkDirective.IDLE),
            StepDecision(WorkDirective.STOP),
        ]
    )
    events = []
    sleeps = []
    result = WorkerHost(
        intent,
        InMemoryCheckpointStore("cursor"),
        backoff=BackoffPolicy(idle_seconds=3),
        sleep=sleeps.append,
        heartbeat=events.append,
    ).run()

    assert result.iterations == 2
    assert result.completed == 0
    assert result.checkpoint == "cursor"
    assert sleeps == [3]
    assert [event.state for event in events] == ["starting", "idle", "stopped"]


def test_continue_requires_late_checkpoint_for_new_intent() -> None:
    class Missing(Intent):
        def plan(self, checkpoint: object | None) -> StepDecision:
            return StepDecision(WorkDirective.CONTINUE, "p")

    result = WorkerHost(
        Missing([]),
        InMemoryCheckpointStore("old"),
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert result.failures == 1
    assert result.checkpoint == "old"


class NarrowSource:
    def __init__(self, decisions: list[StepDecision]) -> None:
        self.decisions = decisions
        self.calls: list[tuple[str, object]] = []

    def plan(self, checkpoint: object | None) -> StepDecision:
        self.calls.append(("plan", checkpoint))
        return self.decisions.pop(0)

    def fetch(self, plan: object) -> object:
        self.calls.append(("fetch", plan))
        return {"raw": plan}

    def transform(self, payload: object, plan: object) -> object:
        self.calls.append(("transform", payload))
        return {"items": [payload], "next": f"after-{plan}"}

    def output(self, transformed: object, plan: object) -> SourceOutput:
        self.calls.append(("output", transformed))
        assert isinstance(transformed, dict)
        return SourceOutput(
            result={"items": transformed["items"]},
            checkpoint={"cursor": transformed["next"]},
        )

    def persist(self, result: object, plan: object) -> None:
        self.calls.append(("persist", result))


def test_source_intent_is_runtime_checkable_and_output_is_immutable() -> None:
    source = NarrowSource([StepDecision(WorkDirective.STOP)])
    assert isinstance(source, SourceIntent)
    output = SourceOutput(result={"items": []}, checkpoint={"cursor": 1})
    with pytest.raises((AttributeError, TypeError)):
        output.checkpoint = {"cursor": 2}  # type: ignore[misc]


def test_source_intent_exact_pipeline_order_and_json_checkpoint() -> None:
    source = NarrowSource([StepDecision(WorkDirective.CONTINUE, "page-1")])
    store = InMemoryCheckpointStore({"cursor": "old"})

    result = WorkerHost(source, store).run(max_iterations=1)

    assert result.checkpoint == {"cursor": "after-page-1"}
    assert store.load() == {"cursor": "after-page-1"}
    assert [name for name, _ in source.calls] == [
        "plan", "fetch", "transform", "output", "persist"
    ]
    # The produced checkpoint is accepted by the same JSON contract as the file store.
    json.dumps(result.checkpoint)


@pytest.mark.parametrize("directive", [WorkDirective.IDLE, WorkDirective.STOP])
def test_source_plan_without_work_never_fetches_and_plans_once_per_iteration(
    directive: WorkDirective,
) -> None:
    source = NarrowSource([StepDecision(directive, "ignored")])
    result = WorkerHost(
        source,
        InMemoryCheckpointStore("old"),
        backoff=BackoffPolicy(idle_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)

    assert result.iterations == 1
    assert source.calls == [("plan", "old")]
    assert result.completed == 0
    assert result.checkpoint == "old"


@pytest.mark.parametrize("failing_stage", ["fetch", "transform", "output", "persist"])
def test_source_failure_never_saves_checkpoint_and_replays_last_durable_plan(
    failing_stage: str,
) -> None:
    class Failing(NarrowSource):
        failed = False

        def _fail_once(self, stage: str) -> None:
            if failing_stage == stage and not self.failed:
                self.failed = True
                raise RuntimeError(stage)

        def fetch(self, plan: object) -> object:
            self._fail_once("fetch")
            return super().fetch(plan)

        def transform(self, payload: object, plan: object) -> object:
            self._fail_once("transform")
            return super().transform(payload, plan)

        def output(self, transformed: object, plan: object) -> SourceOutput:
            self._fail_once("output")
            return super().output(transformed, plan)

        def persist(self, result: object, plan: object) -> None:
            self._fail_once("persist")
            super().persist(result, plan)

    source = Failing([
        StepDecision(WorkDirective.CONTINUE, "same"),
        StepDecision(WorkDirective.CONTINUE, "same"),
    ])
    store = InMemoryCheckpointStore({"cursor": "durable"})
    result = WorkerHost(
        source,
        store,
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=2)

    assert result.failures == 1
    assert result.completed == 1
    assert [call for call in source.calls if call[0] == "plan"] == [
        ("plan", {"cursor": "durable"}),
        ("plan", {"cursor": "durable"}),
    ]
    assert store.load() == {"cursor": "after-same"}


def test_source_terminal_output_persists_before_checkpoint_and_stops() -> None:
    class Terminal(NarrowSource):
        def output(self, transformed: object, plan: object) -> SourceOutput:
            self.calls.append(("output", transformed))
            return SourceOutput(
                result={"final": True},
                checkpoint={"done": True},
                directive=WorkDirective.STOP,
            )

    source = Terminal([
        StepDecision(WorkDirective.CONTINUE, "final"),
        StepDecision(WorkDirective.CONTINUE, "must-not-plan"),
    ])
    store = InMemoryCheckpointStore({"done": False})
    result = WorkerHost(source, store).run()

    assert result.iterations == 1
    assert result.completed == 1
    assert [name for name, _ in source.calls] == [
        "plan", "fetch", "transform", "output", "persist"
    ]
    assert store.load() == {"done": True}


def test_source_crash_after_persist_before_checkpoint_save_is_at_least_once() -> None:
    class SaveFailsOnce(InMemoryCheckpointStore):
        failed = False

        def save(self, checkpoint: object) -> None:
            if not self.failed:
                self.failed = True
                raise OSError("simulated crash boundary")
            super().save(checkpoint)

    source = NarrowSource([
        StepDecision(WorkDirective.CONTINUE, "page"),
        StepDecision(WorkDirective.CONTINUE, "page"),
    ])
    store = SaveFailsOnce({"cursor": "old"})
    result = WorkerHost(
        source,
        store,
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=2)

    assert result.failures == 1
    assert result.completed == 1
    assert [call for call in source.calls if call[0] == "plan"] == [
        ("plan", {"cursor": "old"}),
        ("plan", {"cursor": "old"}),
    ]
    assert len([call for call in source.calls if call[0] == "persist"]) == 2
    assert store.load() == {"cursor": "after-page"}




def test_invalid_source_output_fails_before_persistence() -> None:
    class Invalid(NarrowSource):
        def output(self, transformed: object, plan: object) -> SourceOutput:
            return "invalid"  # type: ignore[return-value]

    source = Invalid([StepDecision(WorkDirective.CONTINUE, "page")])
    store = InMemoryCheckpointStore("old")
    result = WorkerHost(
        source,
        store,
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)

    assert result.failures == 1
    assert not [call for call in source.calls if call[0] == "persist"]
    assert store.load() == "old"
