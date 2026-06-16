from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("apscheduler")

from datasource_kit.scheduler import WorkerScheduler


def test_schedule_runs_callable_repeatedly():
    calls: list[int] = []
    lock = threading.Lock()

    def tick():
        with lock:
            calls.append(1)

    sched = WorkerScheduler()
    sched.schedule(tick, interval_seconds=0.1)
    sched.start()
    try:
        time.sleep(0.45)
    finally:
        sched.stop()
    # ~0.45s at 0.1s interval -> at least 2 ticks fired
    with lock:
        assert len(calls) >= 2


def test_start_is_idempotent_and_stop_safe():
    sched = WorkerScheduler()
    sched.schedule(lambda: None, interval_seconds=1)
    sched.start()
    sched.start()  # second start must not raise
    sched.stop()
    sched.stop()  # stop when already stopped must not raise


def test_schedule_rejects_nonpositive_interval():
    sched = WorkerScheduler()
    with pytest.raises(ValueError):
        sched.schedule(lambda: None, interval_seconds=0)
