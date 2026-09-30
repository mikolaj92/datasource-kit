"""Pure window helpers."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Protocol, runtime_checkable

__all__ = ["DayWindow", "WindowIterator", "WindowOrder", "split_range_into_days"]


class WindowOrder(str, Enum):
    """Walk direction for an inclusive date range.

    Oldest-to-newest is the default, not the only order.  Bounds stay
    ``start <= end``; ``order`` only chooses which end is yielded first.
    """

    OLDEST_FIRST = "oldest_first"
    NEWEST_FIRST = "newest_first"


@dataclass(slots=True, frozen=True)
class DayWindow:
    """Inclusive one-day window."""

    start: date
    end: date


def split_range_into_days(
    start: date,
    end: date,
    *,
    order: str = WindowOrder.OLDEST_FIRST,
) -> Iterator[DayWindow]:
    """Yield inclusive day windows covering ``start`` through ``end``.

    An inverted range (``start > end``) yields nothing.  ``order`` selects
    oldest-to-newest or newest-to-oldest; it is not a domain ranking.
    """

    try:
        direction = WindowOrder(order)
    except ValueError:
        allowed = ", ".join(item.value for item in WindowOrder)
        raise ValueError(f"order must be one of: {allowed}") from None

    if start > end:
        return

    if direction is WindowOrder.NEWEST_FIRST:
        current = end
        while current >= start:
            yield DayWindow(start=current, end=current)
            current -= timedelta(days=1)
        return

    current = start
    while current <= end:
        yield DayWindow(start=current, end=current)
        current += timedelta(days=1)


@runtime_checkable
class WindowIterator(Protocol):
    """Structural iterator yielding opaque window/checkpoint units."""

    def __next__(self) -> object:
        ...
