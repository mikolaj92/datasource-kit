"""Thread-safe token-bucket rate limiter.

Used by scraper-style datasources that must throttle requests to an upstream
source. Batch updaters generally do not need this, which is why nothing else in
the kit imports it.
"""

from __future__ import annotations

import threading
import time

__all__ = ["TokenBucket"]


class TokenBucket:
    """Classic token bucket. ``wait`` blocks until ``amount`` tokens are free."""

    def __init__(self, rate_per_sec: float, burst: float) -> None:
        if rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be > 0")
        if burst <= 0:
            raise ValueError("burst must be > 0")
        self.rate_per_sec = rate_per_sec
        self.burst = burst
        self.tokens = burst
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def wait(self, amount: float = 1.0) -> None:
        """Block until ``amount`` tokens are available, then consume them."""
        if amount > self.burst:
            raise ValueError("amount cannot exceed burst")
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                self.last = now
                self.tokens = min(
                    self.burst, self.tokens + elapsed * self.rate_per_sec
                )
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
            time.sleep(0.05)
