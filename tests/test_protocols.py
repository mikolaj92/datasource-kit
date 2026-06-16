from __future__ import annotations

from datasource_kit import DataSource, IngestActor


class _BatchSource:
    def lookup(self, identifier: str) -> list:
        return [identifier]

    def refresh(self) -> dict:
        return {"rows_loaded": 0}


class _Scraper:
    name = "x"

    def handle_job(self, job: dict):
        return []


def test_batch_source_satisfies_datasource_protocol():
    assert isinstance(_BatchSource(), DataSource)


def test_scraper_satisfies_ingest_actor_protocol():
    assert isinstance(_Scraper(), IngestActor)


def test_cross_model_negative():
    assert not isinstance(_BatchSource(), IngestActor)
    assert not isinstance(_Scraper(), DataSource)
