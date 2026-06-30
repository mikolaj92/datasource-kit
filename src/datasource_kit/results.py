"""Pure result shapes used by datasource runtimes and batch consumers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from .errors import ValidationError

__all__ = [
    "Cursor",
    "WorkerResult",
    "blocked_result",
    "completed_result",
    "working_result",
]


def _require_text(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field} must be a non-empty string")
    return value


@dataclass(slots=True, frozen=True)
class Cursor:
    """Opaque cursor value named by the consumer."""

    kind: str
    value: str

    def __post_init__(self) -> None:
        _require_text(self.kind, "cursor kind")
        _require_text(self.value, "cursor value")


@dataclass(slots=True, frozen=True)
class WorkerResult:
    """Generic worker outcome; status is the consumer's vocabulary."""

    status: str
    checkpoint: object | None = None
    objects: tuple[object, ...] = ()
    cursor: Cursor | None = None
    follow_up_jobs: tuple[dict, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)
    reason: str = ""

    def __post_init__(self) -> None:
        _require_text(self.status, "status")


def working_result(
    *,
    status: str,
    cursor_kind: str,
    cursor_value: str,
    checkpoint: object | None = None,
    objects: tuple[object, ...] = (),
) -> WorkerResult:
    """Build a result that can continue from a non-empty cursor."""

    return WorkerResult(
        status=_require_text(status, "status"),
        checkpoint=checkpoint,
        objects=tuple(objects),
        cursor=Cursor(cursor_kind, cursor_value),
    )


def blocked_result(*, status: str, reason: str) -> WorkerResult:
    """Build a blocked result with a non-empty reason."""

    return WorkerResult(
        status=_require_text(status, "status"),
        reason=_require_text(reason, "reason"),
    )


def completed_result(
    *,
    status: str,
    checkpoint: object | None = None,
) -> WorkerResult:
    """Build a completed result."""

    return WorkerResult(status=_require_text(status, "status"), checkpoint=checkpoint)
