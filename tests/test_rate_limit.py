from __future__ import annotations

import time

import pytest

from datasource_kit import TokenBucket


def test_burst_tokens_are_immediate():
    bucket = TokenBucket(rate_per_sec=1.0, burst=3.0)
    start = time.monotonic()
    for _ in range(3):
        bucket.wait(1.0)
    assert time.monotonic() - start < 0.2


def test_wait_blocks_when_tokens_exhausted():
    bucket = TokenBucket(rate_per_sec=20.0, burst=1.0)
    bucket.wait(1.0)  # drain
    start = time.monotonic()
    bucket.wait(1.0)  # must wait ~1/20s for a refill
    assert time.monotonic() - start >= 0.03


def test_invalid_construction_rejected():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_sec=0, burst=1)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_sec=1, burst=0)


def test_amount_exceeding_burst_rejected():
    bucket = TokenBucket(rate_per_sec=1.0, burst=1.0)
    with pytest.raises(ValueError):
        bucket.wait(2.0)
