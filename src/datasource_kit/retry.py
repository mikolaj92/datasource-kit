"""Tiny synchronous retry helper with linear backoff.

Kept dependency-free on purpose: datasource updaters frequently need to wrap a
single flaky network read without pulling in tenacity or similar.
"""

from __future__ import annotations

import time
from typing import Callable, TypeVar

__all__ = ["retry"]

T = TypeVar("T")


def retry(fn: Callable[[], T], *, retries: int = 3, backoff_seconds: float = 1.2) -> T:
    """Call ``fn`` up to ``retries`` times, sleeping with linear backoff between.

    Backoff before attempt ``i`` (0-indexed) is ``backoff_seconds * (i + 1)``.
    Raises ``RuntimeError`` chained to the last exception if all attempts fail.
    """
    last: Exception | None = None
    for i in range(retries):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - retry path is intentionally broad
            last = exc
            if i == retries - 1:
                break
            time.sleep(backoff_seconds * (i + 1))
    raise RuntimeError(f"operation failed after {retries} attempts") from last
