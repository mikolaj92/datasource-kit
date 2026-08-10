from __future__ import annotations

import pytest

from datasource_kit import EXECUTION_AUTONOMOUS, ExecutionModel, Manifest, SourceContract


def _contract() -> SourceContract:
    return SourceContract(
        source_truth="Official surface.",
        enumeration_method="Worker-owned windows.",
        evidence=("pages",),
        identity_strategy="source id",
        diff_target="corpus",
    )


def test_batch_manifest_minimal():
    m = Manifest(name="clp", source_type="batch")
    assert m.priority == 50
    assert m.rate_limit == {}
    assert m.contract is None


def test_autonomous_manifest_requires_contract():
    with pytest.raises(ValueError):
        Manifest(
            name="saos",
            source_type="scraper",
            execution=ExecutionModel(model=EXECUTION_AUTONOMOUS),
        )


def test_autonomous_manifest_with_contract_ok():
    contract = _contract()
    m = Manifest(
        name="saos",
        source_type="scraper",
        execution=ExecutionModel(model=EXECUTION_AUTONOMOUS),
        rate_limit={"rps": 1.0, "burst": 2.0},
        contract=contract,
    )
    assert m.contract is contract
    assert m.contract.coverage_unit == "source_defined"


def test_batch_manifest_is_not_autonomous():
    m = Manifest(name="clp", source_type="batch")
    assert m.is_autonomous is False
    assert m.execution is None
    assert m.execution_model == ""


def test_execution_model_requires_label():
    with pytest.raises(ValueError):
        ExecutionModel(model="")


def test_execution_model_carries_step_ref():
    ex = ExecutionModel(model="autonomous", step_ref="pkg.mod:run")
    assert ex.model == EXECUTION_AUTONOMOUS
    assert ex.step_ref == "pkg.mod:run"


def test_execution_autonomous_requires_contract():
    with pytest.raises(ValueError):
        Manifest(
            name="saos",
            source_type="scraper",
            execution=ExecutionModel(model="autonomous"),
        )


def test_execution_autonomous_is_first_class():
    m = Manifest(
        name="saos",
        source_type="scraper",
        execution=ExecutionModel(model="autonomous", step_ref="saos.worker:run"),
        contract=_contract(),
    )
    assert m.is_autonomous is True
    assert m.execution_model == "autonomous"


def test_non_autonomous_execution_model_needs_no_contract():
    m = Manifest(
        name="clp",
        source_type="batch",
        execution=ExecutionModel(model="batch", step_ref="clp.update:run"),
    )
    assert m.is_autonomous is False
    assert m.execution_model == "batch"
    assert m.contract is None
