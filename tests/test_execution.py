from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from datasource_kit import ExecutionBackend, ExecutionRequest, InlineExecutionBackend


def request(
    *,
    run_id: str = "run-1",
    execution_id: str = "execution-1",
    inputs: Any = None,
    metadata: Any = None,
) -> ExecutionRequest:
    return ExecutionRequest(
        run_id=run_id,
        execution_id=execution_id,
        inputs={} if inputs is None else inputs,
        metadata={} if metadata is None else metadata,
    )


def test_request_defaults_are_independent() -> None:
    first = request()
    second = request(execution_id="execution-2")

    assert first.inputs == {}
    assert first.metadata == {}
    assert first.inputs is not second.inputs
    assert first.metadata is not second.metadata


def test_request_preserves_opaque_mapping_identity() -> None:
    inputs = {"checkpoint": {"cursor": 42}, "items": [1, 2]}
    metadata = {"consumer-defined": True, "nothing": None}

    value = request(inputs=inputs, metadata=metadata)

    assert value.inputs is inputs
    assert value.metadata is metadata


def test_request_is_frozen_and_slotted() -> None:
    value = request()

    with pytest.raises(FrozenInstanceError):
        value.run_id = "another"  # type: ignore[misc]
    assert not hasattr(value, "__dict__")


def test_request_does_not_overbuild_json_validation() -> None:
    marker = object()

    value = request(inputs={"opaque": marker})

    assert value.inputs["opaque"] is marker


def test_inline_backend_returns_exact_result_identity() -> None:
    expected = {"nested": [object()]}

    actual = InlineExecutionBackend().execute(request(), lambda: expected)

    assert actual is expected


def test_inline_backend_invokes_operation_exactly_once() -> None:
    calls = 0

    def operation() -> int:
        nonlocal calls
        calls += 1
        return calls

    result = InlineExecutionBackend().execute(request(), operation)

    assert result == 1
    assert calls == 1


def test_inline_backend_propagates_exact_exception_and_calls_once() -> None:
    calls = 0
    expected = RuntimeError("operation failed")

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise expected

    with pytest.raises(RuntimeError) as caught:
        InlineExecutionBackend().execute(request(), operation)

    assert caught.value is expected
    assert calls == 1


def test_inline_backend_does_not_interpret_request() -> None:
    class ExplodingMapping(dict[str, object]):
        def __iter__(self):
            raise AssertionError("request diagnostics must remain opaque")

        def __len__(self):
            raise AssertionError("request diagnostics must remain opaque")

        def __getitem__(self, key: str) -> object:
            raise AssertionError("request diagnostics must remain opaque")

    result = InlineExecutionBackend().execute(
        request(inputs=ExplodingMapping(), metadata=ExplodingMapping()),
        lambda: "done",
    )

    assert result == "done"


def test_structural_backend_satisfies_protocol_for_static_consumers() -> None:
    class RecordingBackend:
        def execute(self, request: ExecutionRequest, operation):
            return operation()

    backend: ExecutionBackend = RecordingBackend()

    assert backend.execute(request(), lambda: "result") == "result"


def test_root_exports_are_canonical() -> None:
    from datasource_kit.execution import (
        ExecutionBackend as ModuleExecutionBackend,
        ExecutionRequest as ModuleExecutionRequest,
        InlineExecutionBackend as ModuleInlineExecutionBackend,
    )

    assert ExecutionBackend is ModuleExecutionBackend
    assert ExecutionRequest is ModuleExecutionRequest
    assert InlineExecutionBackend is ModuleInlineExecutionBackend
