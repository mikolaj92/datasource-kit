from __future__ import annotations

import pytest

from datasource_kit import Manifest, SourceContract


def test_batch_manifest_minimal():
    m = Manifest(name="clp", source_type="batch")
    assert m.priority == 50
    assert m.supports_autonomous is False
    assert m.rate_limit == {}
    assert m.contract is None


def test_autonomous_manifest_requires_contract():
    with pytest.raises(ValueError):
        Manifest(name="saos", source_type="scraper", supports_autonomous=True)


def test_autonomous_manifest_with_contract_ok():
    contract = SourceContract(
        source_truth="Official surface.",
        enumeration_method="Worker-owned windows.",
        evidence=("pages",),
        identity_strategy="source id",
        diff_target="corpus",
    )
    m = Manifest(
        name="saos",
        source_type="scraper",
        supports_autonomous=True,
        rate_limit={"rps": 1.0, "burst": 2.0},
        contract=contract,
    )
    assert m.contract is contract
    assert m.contract.coverage_unit == "source_defined"
