"""Dependency-free lifecycle host for autonomous datasource workers.

Consumers own source semantics through :class:`WorkerIntent`.  The host owns
iteration, durable checkpoint advancement, lifecycle heartbeats, failure
backoff, and cooperative shutdown.  Persistence must be idempotent: a crash
after ``persist`` but before checkpoint saving can replay the same plan, giving
the host deliberate at-least-once semantics.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "BackoffPolicy",
    "CheckpointStore",
    "FileCheckpointStore",
    "InMemoryCheckpointStore",
    "WorkerHeartbeat",
    "WorkerHost",
    "WorkerIntent",
    "WorkerRun",
    "WorkerStep",
]


@dataclass(frozen=True, slots=True)
class WorkerStep:
    """A consumer-planned unit of work and its checkpoint after success."""

    plan: object
    checkpoint: object


@runtime_checkable
class WorkerIntent(Protocol):
    """Source-specific work performed by :class:`WorkerHost`.

    ``plan`` returns ``None`` when currently idle.  ``persist`` should be
    idempotent because successful persistence and checkpoint storage cannot be
    made atomic across arbitrary consumer backends.
    """

    def plan(self, checkpoint: object | None) -> WorkerStep | None: ...

    def fetch(self, plan: object) -> object: ...

    def transform(self, payload: object, plan: object) -> object: ...

    def persist(self, transformed: object, plan: object) -> None: ...


@runtime_checkable
class CheckpointStore(Protocol):
    """Durable storage seam for an opaque, JSON-compatible checkpoint."""

    def load(self) -> object | None: ...

    def save(self, checkpoint: object) -> None: ...


class InMemoryCheckpointStore:
    """Small checkpoint fake useful for embedding and tests."""

    def __init__(self, checkpoint: object | None = None) -> None:
        self.checkpoint = checkpoint

    def load(self) -> object | None:
        return self.checkpoint

    def save(self, checkpoint: object) -> None:
        self.checkpoint = checkpoint


class FileCheckpointStore:
    """Atomically store one JSON-compatible checkpoint in a file."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)

    def load(self) -> object | None:
        try:
            with self.path.open("r", encoding="utf-8") as stream:
                return json.load(stream)
        except FileNotFoundError:
            return None

    def save(self, checkpoint: object) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                json.dump(checkpoint, stream, ensure_ascii=False, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Exponential failure backoff and idle polling policy."""

    initial_seconds: float = 1.0
    maximum_seconds: float = 60.0
    multiplier: float = 2.0
    idle_seconds: float = 5.0
    max_consecutive_failures: int | None = None

    def __post_init__(self) -> None:
        if self.initial_seconds < 0 or self.maximum_seconds < 0 or self.idle_seconds < 0:
            raise ValueError("backoff delays must be >= 0")
        if self.maximum_seconds < self.initial_seconds:
            raise ValueError("maximum_seconds must be >= initial_seconds")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if self.max_consecutive_failures is not None and self.max_consecutive_failures < 1:
            raise ValueError("max_consecutive_failures must be >= 1")

    def delay(self, consecutive_failures: int) -> float:
        if consecutive_failures < 1:
            raise ValueError("consecutive_failures must be >= 1")
        try:
            delay = self.initial_seconds * self.multiplier ** (consecutive_failures - 1)
        except OverflowError:
            delay = self.maximum_seconds
        return min(delay, self.maximum_seconds)


@dataclass(frozen=True, slots=True)
class WorkerHeartbeat:
    """A lifecycle observation emitted by the host."""

    state: str
    timestamp: str
    checkpoint: object | None
    completed: int
    failures: int
    consecutive_failures: int
    error: str = ""
    retry_in: float | None = None


@dataclass(frozen=True, slots=True)
class WorkerRun:
    """Summary returned when a worker host stops normally."""

    checkpoint: object | None
    completed: int
    failures: int
    iterations: int


class WorkerHost:
    """Run one source intent with at-least-once checkpoint semantics."""

    def __init__(
        self,
        intent: WorkerIntent,
        checkpoints: CheckpointStore,
        *,
        backoff: BackoffPolicy | None = None,
        heartbeat: Callable[[WorkerHeartbeat], object] | None = None,
        sleep: Callable[[float], object] = time.sleep,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.intent = intent
        self.checkpoints = checkpoints
        self.backoff = backoff or BackoffPolicy()
        self.heartbeat = heartbeat
        self._sleep = sleep
        self._now = now or (lambda: datetime.now(UTC))
        self._stop = threading.Event()
        self._run_lock = threading.Lock()

    def request_stop(self) -> None:
        """Request cooperative shutdown between lifecycle operations."""
        self._stop.set()

    def run(self, *, max_iterations: int | None = None) -> WorkerRun:
        """Run until stopped, terminal failure, or ``max_iterations``.

        The optional bound counts both work and idle planning iterations and is
        primarily useful for one-shot embedding and deterministic tests.
        """
        if max_iterations is not None and max_iterations < 0:
            raise ValueError("max_iterations must be >= 0")
        if not self._run_lock.acquire(blocking=False):
            raise RuntimeError("worker host is already running")

        checkpoint: object | None = None
        completed = failures = consecutive = iterations = 0
        try:
            checkpoint = self.checkpoints.load()
            self._emit("starting", checkpoint, completed, failures, consecutive)
            while not self._stop.is_set() and (
                max_iterations is None or iterations < max_iterations
            ):
                iterations += 1
                try:
                    step = self.intent.plan(checkpoint)
                    if step is None:
                        consecutive = 0
                        self._emit("idle", checkpoint, completed, failures, consecutive)
                        self._wait(self.backoff.idle_seconds)
                        continue
                    if not isinstance(step, WorkerStep):
                        raise TypeError("intent.plan() must return WorkerStep or None")
                    self._emit("working", checkpoint, completed, failures, consecutive)
                    payload = self.intent.fetch(step.plan)
                    transformed = self.intent.transform(payload, step.plan)
                    self.intent.persist(transformed, step.plan)
                    self.checkpoints.save(step.checkpoint)
                    checkpoint = step.checkpoint
                    completed += 1
                    consecutive = 0
                    self._emit("checkpointed", checkpoint, completed, failures, consecutive)
                except Exception as exc:  # consumer boundary: back off and retry
                    failures += 1
                    consecutive += 1
                    limit = self.backoff.max_consecutive_failures
                    if limit is not None and consecutive >= limit:
                        self._emit(
                            "failed", checkpoint, completed, failures, consecutive, error=exc
                        )
                        raise
                    delay = self.backoff.delay(consecutive)
                    self._emit(
                        "backoff", checkpoint, completed, failures, consecutive,
                        error=exc, retry_in=delay,
                    )
                    self._wait(delay)
            return WorkerRun(checkpoint, completed, failures, iterations)
        finally:
            self._emit("stopped", checkpoint, completed, failures, consecutive)
            self._run_lock.release()

    def _wait(self, seconds: float) -> None:
        # Small waits remain injectable while real waits react promptly to stop.
        if self._sleep is time.sleep:
            self._stop.wait(seconds)
        elif not self._stop.is_set():
            self._sleep(seconds)

    def _emit(
        self,
        state: str,
        checkpoint: object | None,
        completed: int,
        failures: int,
        consecutive: int,
        *,
        error: BaseException | None = None,
        retry_in: float | None = None,
    ) -> None:
        if self.heartbeat is None:
            return
        event = WorkerHeartbeat(
            state=state,
            timestamp=self._now().astimezone(UTC).isoformat(),
            checkpoint=checkpoint,
            completed=completed,
            failures=failures,
            consecutive_failures=consecutive,
            error="" if error is None else f"{type(error).__name__}: {error}",
            retry_in=retry_in,
        )
        try:
            self.heartbeat(event)
        except Exception:
            # Telemetry must never take down source work.
            pass
