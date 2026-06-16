from __future__ import annotations

import sqlite3

from datasource_kit import ensure_update_log, now_utc, record_update


def test_now_utc_is_iso_with_offset():
    ts = now_utc()
    # ISO-8601 with timezone -> fromisoformat round-trips and is tz-aware
    from datetime import datetime

    parsed = datetime.fromisoformat(ts)
    assert parsed.tzinfo is not None


def test_ensure_update_log_idempotent():
    con = sqlite3.connect(":memory:")
    ensure_update_log(con)
    ensure_update_log(con)  # second call must not raise
    cols = {row[1] for row in con.execute("PRAGMA table_info(update_log)")}
    assert cols == {"id", "dataset", "last_update", "records_loaded", "status", "details"}


def test_record_update_writes_row_with_json_details():
    con = sqlite3.connect(":memory:")
    record_update(con, dataset="clp", records_loaded=42, details={"source": "x"})
    row = con.execute(
        "SELECT dataset, records_loaded, status, details FROM update_log"
    ).fetchone()
    assert row[0] == "clp"
    assert row[1] == 42
    assert row[2] == "ok"
    assert '"source": "x"' in row[3]


def test_record_update_defaults_empty_details_to_object():
    con = sqlite3.connect(":memory:")
    record_update(con, dataset="d", records_loaded=0)
    (details,) = con.execute("SELECT details FROM update_log").fetchone()
    assert details == "{}"
