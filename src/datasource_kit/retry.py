"""Tiny synchronous retry helper with linear or exponential backoff.

Kept dependency-free on purpose: datasource updaters frequently need to wrap a
flaky network read (or decorate a whole method) without pulling in tenacity or
similar.
"""

from __future__ import annotations

import functools
import time
from typing import Callable, Literal, TypeVar

__all__ = ["retry", "retry_decorator"]

T = TypeVar("T")

Backoff = Literal["linear", "exponential"]


def _delay_seconds(attempt: int, *, backoff: Backoff, backoff_seconds: float, max_backoff_seconds: float | None) -> float:
    delay = backoff_seconds * (attempt + 1) if backoff == "linear" else backoff_seconds * (2**attempt)
    if max_backoff_seconds is not None:
        delay = min(delay, max_backoff_seconds)
    return delay


def retry(
    fn: Callable[[], T],
    *,
    retries: int = 3,
    backoff_seconds: float = 1.2,
    backoff: Backoff = "linear",
    max_backoff_seconds: float | None = None,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] = Exception,
) -> T:
    """Call ``fn`` up to ``retries`` times, sleeping between attempts.

    Only exceptions matching ``retry_on`` are retried; anything else
    propagates immediately. Backoff before attempt ``i`` (0-indexed) is
    ``backoff_seconds * (i + 1)`` for ``"linear"`` or ``backoff_seconds * 2**i``
    for ``"exponential"``, optionally capped by ``max_backoff_seconds``. The
    final attempt is not caught here, so a failure on it propagates with its
    original type and traceback rather than being wrapped.
    """
    for i in range(retries):
        if i == retries - 1:
            return fn()
        try:
            return fn()
        except retry_on:
            time.sleep(_delay_seconds(i, backoff=backoff, backoff_seconds=backoff_seconds, max_backoff_seconds=max_backoff_seconds))
    raise AssertionError("unreachable")  # pragma: no cover - loop always returns or raises


def retry_decorator(
    *,
    retries: int = 3,
    backoff_seconds: float = 1.2,
    backoff: Backoff = "linear",
    max_backoff_seconds: float | None = None,
    retry_on: type[BaseException] | tuple[type[BaseException], ...] = Exception,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator form of :func:`retry`, for wrapping a whole method/function."""

    def decorate(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: object, **kwargs: object) -> T:
            return retry(
                lambda: fn(*args, **kwargs),
                retries=retries,
                backoff_seconds=backoff_seconds,
                backoff=backoff,
                max_backoff_seconds=max_backoff_seconds,
                retry_on=retry_on,
            )

        return wrapper

    return decorate
