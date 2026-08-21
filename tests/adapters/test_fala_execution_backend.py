from __future__ import annotations

import builtins
import subprocess
import sys
import types
from pathlib import Path
from typing import Any, Final

import pytest

from datasource_kit import ExecutionBackend, ExecutionRequest

_INSTALL_HINT: Final = (
    "FalaExecutionBackend requires the 'fala' extra: pip install datasource-kit[fala]"
)
_CAPABILITY_ERROR: Final = (
    "FalaExecutionBackend requires Fala 0.7.28 or newer with the public "
    "record_in_process API"
)


def _block_fala_imports(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "fala", raising=False)
    real_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "fala" or name.startswith("fala."):
            raise ModuleNotFoundError("No module named 'fala'")
        return real_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def test_core_import_does_not_import_fala() -> None:
    script = "import sys; import datasource_kit; assert 'fala' not in sys.modules"
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_backend_requires_fala_extra_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datasource_kit.adapters.fala import FalaExecutionBackend

    _block_fala_imports(monkeypatch)

    with pytest.raises(ImportError) as caught:
        FalaExecutionBackend("journal.sqlite")

    assert str(caught.value) == _INSTALL_HINT


@pytest.mark.parametrize("record_in_process", [None, 42])
def test_backend_reports_missing_public_capability(
    monkeypatch: pytest.MonkeyPatch,
    record_in_process: object,
) -> None:
    from datasource_kit.adapters.fala import FalaExecutionBackend

    facade = types.ModuleType("fala")
    if record_in_process is not None:
        facade.record_in_process = record_in_process
    monkeypatch.setitem(sys.modules, "fala", facade)

    with pytest.raises(RuntimeError) as caught:
        FalaExecutionBackend("journal.sqlite")

    assert str(caught.value) == _CAPABILITY_ERROR


def test_backend_passes_exact_request_values_and_invokes_fala_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datasource_kit.adapters.fala import FalaExecutionBackend

    calls: list[dict[str, Any]] = []
    result = object()

    def record_in_process(**kwargs: Any) -> object:
        calls.append(kwargs)
        return result

    facade = types.ModuleType("fala")
    facade.record_in_process = record_in_process
    monkeypatch.setitem(sys.modules, "fala", facade)
    db_path = Path("relative/journal.sqlite")
    inputs = {"checkpoint": {"cursor": 7}}
    metadata = {"source": "consumer-defined"}
    request = ExecutionRequest(
        run_id="run-9",
        execution_id="attempt-3",
        inputs=inputs,
        metadata=metadata,
    )
    operation = lambda: object()

    backend: ExecutionBackend = FalaExecutionBackend(db_path)
    returned = backend.execute(request, operation)

    assert returned is result
    assert calls == [
        {
            "db_path": db_path,
            "run_id": request.run_id,
            "process_id": request.execution_id,
            "operation": operation,
            "inputs": inputs,
            "metadata": metadata,
        }
    ]
    assert calls[0]["db_path"] is db_path
    assert calls[0]["inputs"] is inputs
    assert calls[0]["metadata"] is metadata


def test_callback_return_and_invocation_are_delegated_to_fala(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datasource_kit.adapters.fala import FalaExecutionBackend

    expected = object()
    callback_calls = 0

    def record_in_process(*, operation, **kwargs):
        del kwargs
        return operation()

    def operation() -> object:
        nonlocal callback_calls
        callback_calls += 1
        return expected

    facade = types.ModuleType("fala")
    facade.record_in_process = record_in_process
    monkeypatch.setitem(sys.modules, "fala", facade)

    actual = FalaExecutionBackend("journal.sqlite").execute(
        ExecutionRequest("run", "process"), operation
    )

    assert actual is expected
    assert callback_calls == 1


def test_callback_exception_semantics_are_delegated_to_fala(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datasource_kit.adapters.fala import FalaExecutionBackend

    expected = LookupError("failed")
    callback_calls = 0

    def record_in_process(*, operation, **kwargs):
        del kwargs
        return operation()

    def operation() -> None:
        nonlocal callback_calls
        callback_calls += 1
        raise expected

    facade = types.ModuleType("fala")
    facade.record_in_process = record_in_process
    monkeypatch.setitem(sys.modules, "fala", facade)

    with pytest.raises(LookupError) as caught:
        FalaExecutionBackend("journal.sqlite").execute(
            ExecutionRequest("run", "process"), operation
        )

    assert caught.value is expected
    assert callback_calls == 1
