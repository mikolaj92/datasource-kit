from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

import pytest

import datasource_kit as dk
from datasource_kit import (
    Cursor,
    FetchIntent,
    FetchPlan,
    FileCheckpointStore,
    InMemoryCheckpointStore,
    InMemoryStore,
    MockFetcher,
    ResumePlanner,
    SequenceOrder,
    SequencePlanner,
    SourceIntent,
    StepDecision,
    ValidationError,
    WindowOrder,
    WorkDirective,
    WorkerHost,
    split_range_into_days,
)
from datasource_kit.fetch import FetchIntent as ModuleFetchIntent
from datasource_kit.worker import BackoffPolicy


class RecordingFetcher:
    """Consumer-supplied fetcher: opaque pages keyed by a resume token."""

    def __init__(self, pages: dict[str, object]) -> None:
        self.pages = pages
        self.refs: list[object] = []

    def fetch(self, ref: object) -> object:
        self.refs.append(ref)
        return self.pages[str(ref)]


def test_public_fetch_surface_matches_module() -> None:
    assert dk.FetchIntent is ModuleFetchIntent
    public = set(dk.__all__)
    assert {
        "CursorPlanner",
        "FetchIntent",
        "FetchPlan",
        "ResumePlanner",
        "SequenceOrder",
        "SequencePlanner",
        "WindowOrder",
    } <= public


def test_custom_cursor_planner_is_structural() -> None:
    class Once:
        def plan(self, cursor: Cursor | None) -> StepDecision:
            if cursor is not None:
                return StepDecision(WorkDirective.STOP)
            return StepDecision(
                WorkDirective.CONTINUE, FetchPlan("ref", Cursor("token", "start"))
            )

        def checkpoint_after(self, payload: object, plan: object) -> Cursor | None:
            del payload, plan
            return None

    planner = Once()
    assert isinstance(planner, dk.CursorPlanner)
    stored: list[object] = []
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: stored.append(payload),
        planner=planner,
    )
    assert isinstance(intent, SourceIntent)
    result = WorkerHost(intent, InMemoryCheckpointStore()).run()
    assert stored == [b"mock:ref"]
    assert result.checkpoint == {"kind": "token", "value": "start"}


def test_plan_fetch_persist_with_consumer_fetcher_and_named_cursor() -> None:
    fetcher = RecordingFetcher(
        {
            "u1": b"alpha",
            "u2": b"beta",
        }
    )
    stored: list[tuple[str, object]] = []

    def persist(payload: object, plan: object) -> None:
        assert isinstance(plan, FetchPlan)
        stored.append((plan.cursor.value, payload))

    intent = FetchIntent(
        fetcher,
        persist,
        SequencePlanner(["u1", "u2"], kind="offset"),
    )
    store = InMemoryCheckpointStore()
    result = WorkerHost(intent, store).run()

    assert fetcher.refs == ["u1", "u2"]
    assert stored == [("u1", b"alpha"), ("u2", b"beta")]
    assert result.completed == 2
    assert store.load() == {"kind": "offset", "value": "u2"}
    assert result.checkpoint == store.load()


def test_timestamp_is_a_cursor_kind_not_a_record_id() -> None:
    pages = {
        "2024-01-01T00:00:00Z": {
            "items": [{"source_id": "alpha", "body": "one"}],
            "next": "2024-01-02T00:00:00Z",
        },
        "2024-01-02T00:00:00Z": {
            "items": [{"source_id": "beta", "body": "two"}],
            "next": None,
        },
    }
    records = InMemoryStore(id_key="source_id")

    def advance(payload: object, plan: FetchPlan) -> Cursor | None:
        assert isinstance(payload, dict)
        token = payload.get("next")
        if not isinstance(token, str):
            return None
        return Cursor("timestamp", token)

    def persist(payload: object, plan: object) -> None:
        assert isinstance(payload, dict)
        records.upsert(payload["items"])

    intent = FetchIntent(
        RecordingFetcher(pages),
        persist,
        ResumePlanner(
            kind="timestamp",
            start="2024-01-01T00:00:00Z",
            advance=advance,
        ),
    )
    result = WorkerHost(intent, InMemoryCheckpointStore()).run()

    assert result.completed == 2
    assert records.existing_ids() == {"alpha", "beta"}
    assert result.checkpoint == {"kind": "timestamp", "value": "2024-01-02T00:00:00Z"}
    assert "2024-01-01T00:00:00Z" not in records.existing_ids()
    assert "2024-01-02T00:00:00Z" not in records.existing_ids()


def test_sequence_order_reverse_is_first_class() -> None:
    seen: list[str] = []
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: seen.append(str(plan.cursor.value)),
        planner=SequencePlanner(
            ["oldest", "middle", "newest"],
            kind="slot",
            order=SequenceOrder.REVERSE,
        ),
    )
    WorkerHost(intent, InMemoryCheckpointStore()).run()
    assert seen == ["newest", "middle", "oldest"]


def test_day_windows_can_run_newest_first() -> None:
    windows = tuple(
        split_range_into_days(
            date(2024, 1, 1),
            date(2024, 1, 3),
            order=WindowOrder.NEWEST_FIRST,
        )
    )
    seen: list[str] = []
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: seen.append(plan.cursor.value),
        planner=SequencePlanner(
            windows,
            kind="day",
            encode=lambda window: window.start.isoformat(),
        ),
    )
    WorkerHost(intent, InMemoryCheckpointStore()).run()
    assert seen == ["2024-01-03", "2024-01-02", "2024-01-01"]


def test_fetch_intent_resumes_from_json_checkpoint(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"
    checkpoints = FileCheckpointStore(path)
    seen: list[str] = []
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: seen.append(plan.cursor.value),
        planner=SequencePlanner(["a", "b"], kind="page"),
    )

    first = WorkerHost(intent, checkpoints).run(max_iterations=1)
    assert seen == ["a"]
    assert first.checkpoint == {"kind": "page", "value": "a"}
    assert json.loads(path.read_text()) == {"kind": "page", "value": "a"}

    second = WorkerHost(intent, checkpoints).run()
    assert seen == ["a", "b"]
    assert second.checkpoint == {"kind": "page", "value": "b"}
    assert checkpoints.load() == {"kind": "page", "value": "b"}


def test_crash_after_persist_before_checkpoint_replays_same_cursor() -> None:
    class SaveFailsOnce(InMemoryCheckpointStore):
        failed = False

        def save(self, checkpoint: object) -> None:
            if not self.failed:
                self.failed = True
                raise OSError("simulated crash boundary")
            super().save(checkpoint)

    persisted: list[str] = []
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: persisted.append(plan.cursor.value),
        planner=SequencePlanner(["one", "two"], kind="page"),
    )
    store = SaveFailsOnce()
    result = WorkerHost(
        intent,
        store,
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run()

    assert persisted == ["one", "one", "two"]
    assert result.failures == 1
    assert result.completed == 2
    assert store.load() == {"kind": "page", "value": "two"}


def test_empty_sequence_stops_without_fetch() -> None:
    fetcher = RecordingFetcher({})
    intent = FetchIntent(
        fetcher,
        persist=lambda payload, plan: None,
        planner=SequencePlanner([], kind="page"),
    )
    result = WorkerHost(intent, InMemoryCheckpointStore()).run()
    assert result.completed == 0
    assert fetcher.refs == []
    assert result.checkpoint is None


def test_unknown_checkpoint_and_kind_mismatch_fail_closed() -> None:
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: None,
        planner=SequencePlanner(["one"], kind="page"),
    )
    result = WorkerHost(
        intent,
        InMemoryCheckpointStore({"kind": "page", "value": "missing"}),
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert result.failures == 1

    mismatched = WorkerHost(
        intent,
        InMemoryCheckpointStore({"kind": "offset", "value": "one"}),
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert mismatched.failures == 1


def test_duplicate_cursor_values_and_invalid_order_fail_closed() -> None:
    with pytest.raises(ValidationError, match="duplicate cursor value"):
        SequencePlanner(["same", "same"], kind="page")
    with pytest.raises(ValidationError, match="order must be one of"):
        SequencePlanner(["one"], kind="page", order="sideways")
    with pytest.raises(ValidationError, match="cursor kind"):
        SequencePlanner(["one"], kind="")


def test_resume_planner_rejects_advance_kind_mismatch() -> None:
    def advance(payload: object, plan: FetchPlan) -> Cursor:
        del payload
        return Cursor("other", "next")

    intent = FetchIntent(
        RecordingFetcher({"start": {"ok": True}}),
        persist=lambda payload, plan: None,
        planner=ResumePlanner(kind="timestamp", start="start", advance=advance),
    )
    result = WorkerHost(
        intent,
        InMemoryCheckpointStore(),
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert result.failures == 1
    assert result.completed == 0


def test_exhausted_idle_keeps_last_cursor() -> None:
    sleeps: list[float] = []
    intent = FetchIntent(
        MockFetcher(),
        persist=lambda payload, plan: None,
        planner=SequencePlanner(["only"], kind="page"),
        exhausted=WorkDirective.IDLE,
    )
    host = WorkerHost(
        intent,
        InMemoryCheckpointStore(),
        backoff=BackoffPolicy(idle_seconds=4),
        sleep=sleeps.append,
    )
    result = host.run(max_iterations=2)
    assert result.completed == 1
    assert result.checkpoint == {"kind": "page", "value": "only"}
    assert sleeps == [4]


def test_payload_stays_opaque_to_the_kit() -> None:
    marker = object()

    class OpaqueFetcher:
        def fetch(self, ref: object) -> object:
            del ref
            return marker

    captured: list[object] = []
    intent = FetchIntent(
        OpaqueFetcher(),
        persist=lambda payload, plan: captured.append(payload),
        planner=SequencePlanner(["ref"], kind="token"),
    )
    WorkerHost(intent, InMemoryCheckpointStore()).run()
    assert captured == [marker]


def test_string_checkpoint_and_continue_exhausted_fail_closed() -> None:
    with pytest.raises(ValueError, match="exhausted"):
        FetchIntent(
            MockFetcher(),
            persist=lambda payload, plan: None,
            planner=SequencePlanner(["one"], kind="page"),
            exhausted=WorkDirective.CONTINUE,
        )

    result = WorkerHost(
        FetchIntent(
            MockFetcher(),
            persist=lambda payload, plan: None,
            planner=SequencePlanner(["one"], kind="page"),
        ),
        InMemoryCheckpointStore("not-a-cursor"),
        backoff=BackoffPolicy(initial_seconds=0, maximum_seconds=0),
        sleep=lambda _: None,
    ).run(max_iterations=1)
    assert result.failures == 1


def test_general_fetch_module_has_no_legal_ontology() -> None:
    import datasource_kit.fetch as fetch_mod

    text = Path(fetch_mod.__file__).read_text(encoding="utf-8").lower()
    forbidden = ("eli", "dziennik", "sygnatura", "celex", "statute", "judgment")
    for token in forbidden:
        assert re.search(rf"\b{token}\b", text) is None, token
