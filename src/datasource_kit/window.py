"""Pure window helpers."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Protocol, runtime_checkable

__all__ = ["DayWindow", "WindowIterator", "split_range_into_days"]


@dataclass(slots=True, frozen=True)
class DayWindow:
    """Inclusive one-day window."""

    start: date
    end: date


def split_range_into_days(start: date, end: date) -> Iterator[DayWindow]:
    """Yield inclusive day windows from ``start`` through ``end``."""

    current = start
    while current <= end:
        yield DayWindow(start=current, end=current)
        current += timedelta(days=1)


@runtime_checkable
class WindowIterator(Protocol):
    """Structural iterator yielding opaque window/checkpoint units."""

    def __next__(self) -> object:
        ...
