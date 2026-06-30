"""Compatibility import for the token-bucket rate limiter."""

from __future__ import annotations

from .ratelimit import TokenBucket

__all__ = ["TokenBucket"]
