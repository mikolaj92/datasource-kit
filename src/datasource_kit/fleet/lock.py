"""Unit-scoped transition lock."""
from __future__ import annotations

import fcntl
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .process import _ensure_unit_dir


@contextmanager
def unit_lock(unit_dir: Path) -> Iterator[None]:
    unit_dir = _ensure_unit_dir(unit_dir)
    path = unit_dir / ".process.lock"
    flags = os.O_RDWR | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    with os.fdopen(fd, "a+") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
