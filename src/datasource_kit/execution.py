"""Domain-blind execution boundary for one synchronous callback.

Request values are opaque diagnostics.  Callers must supply JSON-compatible
``inputs`` and ``metadata``; the core deliberately does not validate or
interpret them.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar

__all__ = ["ExecutionBackend", "ExecutionRequest", "InlineExecutionBackend"]

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    """Opaque identity and JSON-compatible diagnostics for one execution.

    The backend boundary does not create or finalize runs, mint identifiers,
    retry operations, manage checkpoints, or prescribe domain vocabulary.
    """

    run_id: str
    execution_id: str
    inputs: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


class ExecutionBackend(Protocol):
    """Execute and optionally observe one synchronous operation."""

    def execute(self, request: ExecutionRequest, operation: Callable[[], T]) -> T:
        """Call ``operation`` exactly once and return its result unchanged."""
        ...


class InlineExecutionBackend:
    """Dependency-free backend that executes the callback in the caller."""

    def execute(self, request: ExecutionRequest, operation: Callable[[], T]) -> T:
        del request
        return operation()
