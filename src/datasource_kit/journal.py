"""Update/run journal primitives shared by datasource updaters.

A datasource that maintains a local SQLite store records every refresh in an
``update_log`` table. These helpers are domain-free: they only know about the
generic bookkeeping shape (which dataset, when, how many records, status).

The schema is intentionally identical to the one MSDS Portal datasources have
used in production so existing databases remain compatible.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

__all__ = ["now_utc", "ensure_update_log", "record_update"]


def now_utc() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def ensure_update_log(con: sqlite3.Connection) -> None:
    """Create the ``update_log`` table and its index if they do not exist."""
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS update_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset TEXT NOT NULL,
            last_update TEXT NOT NULL,
            records_loaded INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'ok',
            details TEXT
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_update_log_ds "
        "ON update_log(dataset, last_update DESC)"
    )
    con.commit()


def record_update(
    con: sqlite3.Connection,
    *,
    dataset: str,
    records_loaded: int,
    status: str = "ok",
    details: dict[str, Any] | None = None,
) -> None:
    """Append a single row describing the outcome of a dataset refresh."""
    ensure_update_log(con)
    con.execute(
        "INSERT INTO update_log(dataset,last_update,records_loaded,status,details) "
        "VALUES (?,?,?,?,?)",
        (
            dataset,
            now_utc(),
            int(records_loaded),
            status,
            json.dumps(details or {}, ensure_ascii=False),
        ),
    )
    con.commit()
