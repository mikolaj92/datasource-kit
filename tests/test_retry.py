from __future__ import annotations

import pytest

from datasource_kit import retry


def test_retry_returns_first_success():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        return "ok"

    assert retry(fn) == "ok"
    assert calls["n"] == 1


def test_retry_succeeds_after_transient_failures():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("transient")
        return calls["n"]

    assert retry(fn, retries=3, backoff_seconds=0) == 3
    assert calls["n"] == 3


def test_retry_raises_after_exhausting_attempts():
    def fn():
        raise ValueError("always")

    with pytest.raises(RuntimeError) as exc:
        retry(fn, retries=2, backoff_seconds=0)
    assert isinstance(exc.value.__cause__, ValueError)
