"""Domain-blind hosting for a consumer-defined continuous step.

Unlike :mod:`datasource_kit.worker`, this module does not plan, persist, or
checkpoint work.  A consumer performs one opaque step and then returns a
post-step directive; the host owns only repetition, counters, waits, boundary
rotation, error mapping, and cooperative shutdown.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

__all__ = [
    "BoundaryAction",
    "ContinuousWorkerHost",
    "LoopAction",
    "LoopContext",
    "LoopDirective",
    "LoopRun",
]


class LoopAction(str, Enum):
    """What the continuous host should do after a callback."""

    CONTINUE = "continue"
    WAIT = "wait"
    STOP = "stop"


class BoundaryAction(str, Enum):
    """Whether the consumer's open lifecycle boundary should be replaced."""

    KEEP = "keep"
    ROTATE = "rotate"


@dataclass(frozen=True, slots=True)
class LoopDirective:
    """A domain-blind decision made after a step or control check.

    ``counted`` controls only the caller-visible work limit.  Every invocation
    of ``step`` advances ``sequence`` and ``attempts_in_boundary``, including
    invocations which raise.  ``delay`` is used only by :attr:`LoopAction.WAIT`;
    ``None`` means no delay.
    """

    action: LoopAction
    boundary: BoundaryAction = BoundaryAction.KEEP
    delay: float | None = None
    counted: bool = True
    reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.action, LoopAction):
            raise TypeError("action must be a LoopAction")
        if not isinstance(self.boundary, BoundaryAction):
            raise TypeError("boundary must be a BoundaryAction")
        if self.delay is not None and self.delay < 0:
            raise ValueError("delay must be >= 0")


@dataclass(frozen=True, slots=True)
class LoopContext:
    """Counters and prior-wait information visible to consumer callbacks.

    A step sees counters for the attempt which is about to run, so the first
    step receives ``sequence == attempts_in_boundary == 1``.  A control hook
    and a periodic rotation hook see the counters as they stand between steps.
    """

    sequence: int
    counted: int
    attempts_in_boundary: int
    repeated_wait: bool


@dataclass(frozen=True, slots=True)
class LoopRun:
    """Summary returned after normal termination or cooperative shutdown."""

    sequence: int
    counted: int
    attempts_in_boundary: int
    last_directive: LoopDirective | None


class ContinuousWorkerHost:
    """Repeat an opaque consumer step according to its post-step directives.

    The host is intentionally unaware of checkpoints, persistence, result
    shapes, and application status vocabulary.  ``run`` is non-reentrant; a
    second concurrent call raises :class:`RuntimeError`.
    """

    def __init__(
        self,
        step: Callable[[LoopContext], LoopDirective],
        *,
        control: Callable[[LoopContext], LoopDirective | None] | None = None,
        rotate: Callable[[LoopContext, str], None] | None = None,
        on_error: Callable[[BaseException, LoopContext], LoopDirective] | None = None,
        observe: Callable[[str, LoopContext, LoopDirective | None], None] | None = None,
        sleep: Callable[[float], object] = time.sleep,
        rotation_attempts: int | None = None,
    ) -> None:
        if rotation_attempts is not None and rotation_attempts < 1:
            raise ValueError("rotation_attempts must be >= 1")
        self.step = step
        self.control = control
        self.rotate = rotate
        self.on_error = on_error
        self.observe = observe
        self._sleep = sleep
        self.rotation_attempts = rotation_attempts
        self._stop = threading.Event()
        self._run_lock = threading.Lock()

    def request_stop(self) -> None:
        """Request shutdown at the next cooperative boundary (including waits)."""
        self._stop.set()

    def run(self, *, max_counted: int | None = None) -> LoopRun:
        """Run until STOP, a stop request, or ``max_counted`` is reached.

        The bound counts only directives whose ``counted`` flag is true.  Step
        attempts and their monotonically increasing sequence are tracked
        separately.  Exceptions are re-raised unless ``on_error`` maps them to
        a directive; an exception raised by the mapper is always propagated.
        """
        if max_counted is not None and max_counted < 0:
            raise ValueError("max_counted must be >= 0")
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("continuous worker host is already running")

        sequence = counted = attempts = 0
        repeated_wait = False
        last: LoopDirective | None = None
        try:
            while not self._stop.is_set() and (
                max_counted is None or counted < max_counted
            ):
                between = LoopContext(sequence, counted, attempts, repeated_wait)
                if self.control is not None:
                    directive = self.control(between)
                    if directive is not None:
                        self._require_directive(directive, "control")
                        last = directive
                        self._observe("control", between, directive)
                        counted, attempts, repeated_wait, should_stop = self._apply(
                            directive, between, counted, attempts, from_error=False
                        )
                        if should_stop:
                            break
                        continue

                if (
                    self.rotation_attempts is not None
                    and attempts >= self.rotation_attempts
                ):
                    self._rotate(between, "attempt-limit")
                    attempts = 0
                    between = LoopContext(sequence, counted, attempts, repeated_wait)

                sequence += 1
                attempts += 1
                context = LoopContext(sequence, counted, attempts, repeated_wait)
                from_error = False
                try:
                    directive = self.step(context)
                    self._require_directive(directive, "step")
                    self._observe("step", context, directive)
                except BaseException as exc:
                    if self.on_error is None:
                        raise
                    directive = self.on_error(exc, context)
                    self._require_directive(directive, "on_error")
                    from_error = True
                    self._observe("error", context, directive)

                last = directive
                counted, attempts, repeated_wait, should_stop = self._apply(
                    directive, context, counted, attempts, from_error=from_error
                )
                if should_stop:
                    break

            final = LoopContext(sequence, counted, attempts, repeated_wait)
            self._observe("stopped", final, last)
            return LoopRun(sequence, counted, attempts, last)
        finally:
            self._run_lock.release()

    @staticmethod
    def _require_directive(value: object, source: str) -> None:
        if not isinstance(value, LoopDirective):
            raise TypeError(f"{source} must return LoopDirective")

    def _apply(
        self,
        directive: LoopDirective,
        context: LoopContext,
        counted: int,
        attempts: int,
        *,
        from_error: bool,
    ) -> tuple[int, int, bool, bool]:
        if directive.counted:
            counted += 1
        if directive.boundary is BoundaryAction.ROTATE:
            self._rotate(context, directive.reason)
            attempts = 0
        repeated_wait = from_error or directive.action is LoopAction.WAIT
        if directive.action is LoopAction.STOP:
            return counted, attempts, repeated_wait, True
        if directive.action is LoopAction.WAIT:
            self._wait(0.0 if directive.delay is None else directive.delay)
        return counted, attempts, repeated_wait, False

    def _rotate(self, context: LoopContext, reason: str) -> None:
        if self.rotate is not None:
            self.rotate(context, reason)
        self._observe("rotate", context, None)

    def _wait(self, seconds: float) -> None:
        if self._sleep is time.sleep:
            self._stop.wait(seconds)
        elif not self._stop.is_set():
            self._sleep(seconds)

    def _observe(
        self, event: str, context: LoopContext, directive: LoopDirective | None
    ) -> None:
        if self.observe is not None:
            try:
                self.observe(event, context, directive)
            except Exception:
                # Observation is telemetry; lifecycle callbacks remain fail-closed.
                pass
