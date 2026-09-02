"""Reusable throttle and retry mechanics."""

from __future__ import annotations

import threading
import time

__all__ = ["TokenBucket"]


class TokenBucket:
    """Thread-safe token bucket.

    ``rate``/``capacity`` are the canonical names. ``rate_per_sec``/``burst``
    remain accepted for compatibility with the older ``rate_limit`` module.
    """

    def __init__(
        self,
        rate: float | None = None,
        capacity: float | None = None,
        *,
        rate_per_sec: float | None = None,
        burst: float | None = None,
    ) -> None:
        if rate is None:
            rate = rate_per_sec
        if capacity is None:
            capacity = burst
        if rate is None or capacity is None:
            raise ValueError("rate and capacity must be provided")
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate and capacity must be positive")
        self.rate = float(rate)
        self.capacity = float(capacity)
        self.rate_per_sec = self.rate
        self.burst = self.capacity
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> float:
        """Block until ``tokens`` are available; return seconds waited."""

        if tokens <= 0:
            raise ValueError("tokens must be positive")
        if tokens > self.capacity:
            raise ValueError("tokens cannot exceed capacity")

        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return waited
                wait = (tokens - self._tokens) / self.rate
            time.sleep(wait)
            waited += wait

    def wait(self, amount: float = 1.0) -> None:
        """Compatibility wrapper for the older API."""

        self.acquire(amount)
