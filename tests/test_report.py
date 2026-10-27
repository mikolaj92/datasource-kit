"""Regression tests for IngestReport.kit_version (issue #90)."""

from importlib.metadata import version

from datasource_kit.report import IngestReport


def test_new_report_emits_package_version() -> None:
    report = IngestReport(source_name="demo")
    assert report.kit_version == version("datasource-kit")


def test_as_dict_carries_kit_version() -> None:
    d = IngestReport(source_name="demo").as_dict()
    assert d["kit_version"] == version("datasource-kit")


def test_from_dict_defaults_to_package_version() -> None:
    report = IngestReport.from_dict({"source_name": "demo"})
    assert report.kit_version == version("datasource-kit")


def test_from_dict_preserves_stored_kit_version() -> None:
    report = IngestReport.from_dict({"source_name": "demo", "kit_version": "0.1.0"})
    assert report.kit_version == "0.1.0"
