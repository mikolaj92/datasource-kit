from __future__ import annotations

from datasource_kit import DiscoveryLedger, Evidence


def test_record_and_totals():
    ledger = DiscoveryLedger()
    ledger.record("fetched", 10)
    ledger.record("fetched", 5)
    ledger.record("persisted", 3)
    totals = ledger.totals()
    assert totals["fetched"] == 15
    assert totals["persisted"] == 3


def test_evidence_has_meta():
    ledger = DiscoveryLedger()
    ledger.record("fetched", 7, source="api")
    assert ledger.entries[0].meta == {"source": "api"}


def test_len():
    ledger = DiscoveryLedger()
    assert len(ledger) == 0
    ledger.record("x", 1)
    assert len(ledger) == 1


def test_evidence_dataclass():
    e = Evidence(label="x", count=3)
    assert e.meta == {}
