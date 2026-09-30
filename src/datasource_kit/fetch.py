"""General plan → fetch → persist composition around a named cursor.

Consumers supply the fetcher, the persist callback, and the cursor *kind*.
The kit walks opaque refs, calls ``Fetcher.fetch``, and persists opaque
payloads.  It does not inspect payload identity, treat a timestamp as a
record id, or prefer oldest-to-newest over any other consumer-chosen order.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable

from .errors import ValidationError
from .protocols import Fetcher
from .results import Cursor
from .worker import SourceOutput, StepDecision, WorkDirective

__all__ = [
    "CursorPlanner",
    "FetchIntent",
    "FetchPlan",
    "ResumePlanner",
    "SequenceOrder",
    "SequencePlanner",
]


class SequenceOrder(str, Enum):
    """Walk direction for a consumer-supplied unit sequence.

    ``FORWARD`` keeps the caller's order.  ``REVERSE`` walks it backwards.
    Neither name implies time, rank, or identity.
    """

    FORWARD = "forward"
    REVERSE = "reverse"


@dataclass(frozen=True, slots=True)
class FetchPlan:
    """One planned fetch: an opaque ref plus the cursor that named it."""

    ref: object
    cursor: Cursor


@runtime_checkable
class CursorPlanner(Protocol):
    """Plan the next fetch from a consumer-named cursor.

    ``plan`` chooses work from the last durable cursor.  ``checkpoint_after``
    returns the cursor to store after a successful persist, or ``None`` when
    this step exhausted the source.  Payload contents stay opaque.
    """

    def plan(self, cursor: Cursor | None) -> StepDecision: ...

    def checkpoint_after(self, payload: object, plan: object) -> Cursor | None: ...


class SequencePlanner:
    """Walk opaque units under a consumer-named cursor kind.

    The checkpoint is the unit just completed.  The next plan is the following
    unit in the chosen order.  ``encode`` turns a unit into the cursor value;
    the kit never treats that value as a record identifier.
    """

    def __init__(
        self,
        units: Sequence[object] | Iterable[object],
        *,
        kind: str,
        encode: Callable[[object], str] = str,
        order: str = SequenceOrder.FORWARD,
    ) -> None:
        try:
            direction = SequenceOrder(order)
        except ValueError:
            allowed = ", ".join(item.value for item in SequenceOrder)
            raise ValidationError(f"order must be one of: {allowed}") from None
        ordered = tuple(units)
        if direction is SequenceOrder.REVERSE:
            ordered = tuple(reversed(ordered))
        if not isinstance(kind, str) or not kind.strip():
            raise ValidationError("cursor kind must be a non-empty string")
        self.kind = kind
        self._encode = encode
        self._units = ordered
        self._index: dict[str, int] = {}
        for index, unit in enumerate(ordered):
            value = encode(unit)
            Cursor(kind, value)
            if value in self._index:
                raise ValidationError(f"duplicate cursor value {value!r}")
            self._index[value] = index

    def plan(self, cursor: Cursor | None) -> StepDecision:
        if not self._units:
            return StepDecision(WorkDirective.STOP)
        if cursor is None:
            return self._continue(0)
        self._require_kind(cursor)
        index = self._index.get(cursor.value)
        if index is None:
            raise ValidationError(f"unknown cursor value {cursor.value!r}")
        next_index = index + 1
        if next_index >= len(self._units):
            return StepDecision(WorkDirective.STOP)
        return self._continue(next_index)

    def checkpoint_after(self, payload: object, plan: object) -> Cursor | None:
        del payload
        current = _require_plan(plan)
        self._require_kind(current.cursor)
        index = self._index.get(current.cursor.value)
        if index is None:
            raise ValidationError(f"unknown cursor value {current.cursor.value!r}")
        if index + 1 >= len(self._units):
            return None
        return current.cursor

    def _continue(self, index: int) -> StepDecision:
        unit = self._units[index]
        cursor = Cursor(self.kind, self._encode(unit))
        return StepDecision(WorkDirective.CONTINUE, FetchPlan(unit, cursor))

    def _require_kind(self, cursor: Cursor) -> None:
        if cursor.kind != self.kind:
            raise ValidationError(
                f"cursor kind {cursor.kind!r} does not match planner kind {self.kind!r}"
            )


class ResumePlanner:
    """Treat the cursor value as the next opaque fetch ref.

    Used when the consumer names a resume token — a page, offset, timestamp,
    or any other label — and reads the following token from the payload.
    The kit does not interpret ``kind`` or ``value``.
    """

    def __init__(
        self,
        *,
        kind: str,
        start: str,
        advance: Callable[[object, FetchPlan], Cursor | None],
    ) -> None:
        self._start = Cursor(kind, start)
        self._advance = advance

    def plan(self, cursor: Cursor | None) -> StepDecision:
        current = self._start if cursor is None else cursor
        if current.kind != self._start.kind:
            raise ValidationError(
                f"cursor kind {current.kind!r} does not match planner kind "
                f"{self._start.kind!r}"
            )
        return StepDecision(WorkDirective.CONTINUE, FetchPlan(current.value, current))

    def checkpoint_after(self, payload: object, plan: object) -> Cursor | None:
        current = _require_plan(plan)
        nxt = self._advance(payload, current)
        if nxt is None:
            return None
        if not isinstance(nxt, Cursor):
            raise TypeError("advance() must return Cursor or None")
        if nxt.kind != self._start.kind:
            raise ValidationError(
                f"cursor kind {nxt.kind!r} does not match planner kind "
                f"{self._start.kind!r}"
            )
        return nxt


class FetchIntent:
    """Concrete :class:`~datasource_kit.worker.SourceIntent` for general fetch.

    ``plan`` asks the planner, ``fetch`` calls the consumer fetcher, ``transform``
    is identity, and ``persist`` forwards the opaque payload.  Checkpoints are
    ``Cursor.as_dict()`` so file-backed stores round-trip without the kit
    knowing what a record is.
    """

    def __init__(
        self,
        fetcher: Fetcher,
        persist: Callable[[object, object], object | None],
        planner: CursorPlanner,
        *,
        exhausted: WorkDirective = WorkDirective.STOP,
    ) -> None:
        if exhausted not in (WorkDirective.STOP, WorkDirective.IDLE):
            raise ValueError("exhausted must be WorkDirective.STOP or WorkDirective.IDLE")
        self.fetcher = fetcher
        self._persist = persist
        self.planner = planner
        self._exhausted = exhausted

    def plan(self, checkpoint: object | None) -> StepDecision:
        cursor = _cursor_from_checkpoint(checkpoint)
        decision = self.planner.plan(cursor)
        if not isinstance(decision, StepDecision):
            raise TypeError("planner.plan() must return StepDecision")
        if decision.directive is WorkDirective.CONTINUE and not isinstance(
            decision.work, FetchPlan
        ):
            raise TypeError("CONTINUE work must be a FetchPlan")
        return decision

    def fetch(self, plan: object) -> object:
        return self.fetcher.fetch(_require_plan(plan).ref)

    def transform(self, payload: object, plan: object) -> object:
        del plan
        return payload

    def output(self, transformed: object, plan: object) -> SourceOutput:
        current = _require_plan(plan)
        nxt = self.planner.checkpoint_after(transformed, current)
        result: dict[str, object] = {"payload": transformed}
        if nxt is None:
            return SourceOutput(
                result=result,
                checkpoint=current.cursor.as_dict(),
                directive=self._exhausted,
            )
        if not isinstance(nxt, Cursor):
            raise TypeError("planner.checkpoint_after() must return Cursor or None")
        return SourceOutput(
            result=result,
            checkpoint=nxt.as_dict(),
            directive=WorkDirective.CONTINUE,
        )

    def persist(self, result: Mapping[str, object], plan: object) -> None:
        self._persist(result["payload"], plan)


def _require_plan(plan: object) -> FetchPlan:
    if not isinstance(plan, FetchPlan):
        raise TypeError("expected FetchPlan")
    return plan


def _cursor_from_checkpoint(checkpoint: object | None) -> Cursor | None:
    if checkpoint is None:
        return None
    if isinstance(checkpoint, Cursor):
        return checkpoint
    if isinstance(checkpoint, Mapping):
        return Cursor.from_mapping(checkpoint)
    raise ValidationError("checkpoint must be a Cursor, mapping, or None")
