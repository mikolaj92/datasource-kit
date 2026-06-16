"""Optional periodic scheduler for driving a dispatcher/poll loop.

This is the one part of the kit that needs a third-party dependency
(APScheduler), so it lives behind the ``scheduler`` extra and is imported
lazily. The core package stays dependency-free; importing this module without
the extra installed raises a clear error.

Install with::

    pip install "datasource-kit[scheduler]"
"""

from __future__ import annotations

from typing import Callable

__all__ = ["WorkerScheduler"]


class WorkerScheduler:
    """Run a callable on a fixed interval via APScheduler's background scheduler.

    Typical use is to periodically poke an ingestion dispatcher so queued jobs
    get leased and executed without a hand-rolled ``while True: sleep`` loop.
    """

    def __init__(self) -> None:
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
        except ModuleNotFoundError as exc:  # pragma: no cover - import guard
            raise ModuleNotFoundError(
                "WorkerScheduler requires the optional 'scheduler' extra. "
                "Install with: pip install \"datasource-kit[scheduler]\""
            ) from exc
        self._scheduler = BackgroundScheduler()
        self._started = False

    def schedule(self, fn: Callable[[], object], *, interval_seconds: float) -> None:
        """Register ``fn`` to run every ``interval_seconds`` seconds."""
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")
        self._scheduler.add_job(fn, "interval", seconds=interval_seconds)

    def start(self) -> None:
        """Start the scheduler. Idempotent: a second call is a no-op."""
        if self._started:
            return
        self._scheduler.start()
        self._started = True

    def stop(self) -> None:
        """Stop the scheduler. Safe to call when not running."""
        if not self._started:
            return
        self._scheduler.shutdown(wait=False)
        self._started = False
