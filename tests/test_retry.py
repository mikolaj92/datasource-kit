from __future__ import annotations

import time

import pytest

from datasource_kit import retry, retry_decorator


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


def test_retry_raises_original_exception_after_exhausting_attempts():
    def fn():
        raise ValueError("always")

    with pytest.raises(ValueError, match="always"):
        retry(fn, retries=2, backoff_seconds=0)


def test_retry_propagates_non_matching_exception_immediately():
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise KeyError("not retryable")

    with pytest.raises(KeyError):
        retry(fn, retries=5, backoff_seconds=0, retry_on=ValueError)
    assert calls["n"] == 1


def test_retry_linear_backoff_delays(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)

    def fn():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        retry(fn, retries=3, backoff_seconds=1, backoff="linear")
    assert sleeps == [1, 2]


def test_retry_exponential_backoff_delays_with_cap(monkeypatch):
    sleeps: list[float] = []
    monkeypatch.setattr(time, "sleep", sleeps.append)

    def fn():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        retry(fn, retries=4, backoff_seconds=1, backoff="exponential", max_backoff_seconds=3)
    assert sleeps == [1, 2, 3]


def test_retry_decorator_wraps_a_method_transparently():
    calls = {"n": 0}

    class Client:
        @retry_decorator(retries=3, backoff_seconds=0)
        def fetch(self, value):
            calls["n"] += 1
            if calls["n"] < 2:
                raise ValueError("transient")
            return value

    assert Client().fetch("payload") == "payload"
    assert calls["n"] == 2


def test_retry_decorator_respects_retry_on():
    class Client:
        @retry_decorator(retries=5, backoff_seconds=0, retry_on=ValueError)
        def fetch(self):
            raise KeyError("not retryable")

    with pytest.raises(KeyError):
        Client().fetch()
