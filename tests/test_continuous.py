from __future__ import annotations

import threading
import time

import pytest

from datasource_kit import (
    BoundaryAction,
    ContinuousWorkerHost,
    LoopAction,
    LoopContext,
    LoopDirective,
    LoopRun,
)


def test_unbounded_continue_until_cooperative_stop() -> None:
    seen: list[LoopContext] = []
    host: ContinuousWorkerHost

    def step(context: LoopContext) -> LoopDirective:
        seen.append(context)
        if context.sequence == 3:
            host.request_stop()
        return LoopDirective(LoopAction.CONTINUE)

    host = ContinuousWorkerHost(step)
    result = host.run()

    assert [item.sequence for item in seen] == [1, 2, 3]
    assert result == LoopRun(3, 3, 3, LoopDirective(LoopAction.CONTINUE))


def test_stop_is_normal_termination() -> None:
    result = ContinuousWorkerHost(
        lambda _: LoopDirective(LoopAction.STOP, counted=False, reason="done")
    ).run()
    assert result.sequence == 1
    assert result.counted == 0
    assert result.last_directive == LoopDirective(
        LoopAction.STOP, counted=False, reason="done"
    )


def test_wait_uses_injected_delay_and_marks_next_context_repeated() -> None:
    sleeps: list[float] = []
    contexts: list[LoopContext] = []

    def step(context: LoopContext) -> LoopDirective:
        contexts.append(context)
        if context.sequence == 1:
            return LoopDirective(LoopAction.WAIT, delay=2.5, counted=False)
        return LoopDirective(LoopAction.STOP, counted=False)

    result = ContinuousWorkerHost(step, sleep=sleeps.append).run()
    assert sleeps == [2.5]
    assert contexts[0].repeated_wait is False
    assert contexts[1].repeated_wait is True
    assert result.sequence == 2
    assert result.counted == 0


def test_continue_clears_repeated_wait() -> None:
    flags: list[bool] = []

    def step(context: LoopContext) -> LoopDirective:
        flags.append(context.repeated_wait)
        return [
            LoopDirective(LoopAction.WAIT, counted=False),
            LoopDirective(LoopAction.CONTINUE, counted=False),
            LoopDirective(LoopAction.STOP, counted=False),
        ][context.sequence - 1]

    ContinuousWorkerHost(step, sleep=lambda _: None).run()
    assert flags == [False, True, False]


def test_max_counted_ignores_uncounted_attempts() -> None:
    directives = iter(
        [
            LoopDirective(LoopAction.CONTINUE, counted=False),
            LoopDirective(LoopAction.WAIT, counted=False),
            LoopDirective(LoopAction.CONTINUE),
            LoopDirective(LoopAction.CONTINUE),
        ]
    )
    contexts: list[LoopContext] = []

    def step(context: LoopContext) -> LoopDirective:
        contexts.append(context)
        return next(directives)

    result = ContinuousWorkerHost(step, sleep=lambda _: None).run(max_counted=2)
    assert result.sequence == 4
    assert result.counted == 2
    assert [context.counted for context in contexts] == [0, 0, 0, 1]


def test_zero_max_counted_does_not_invoke_callbacks() -> None:
    called = False

    def step(_: LoopContext) -> LoopDirective:
        nonlocal called
        called = True
        return LoopDirective(LoopAction.CONTINUE)

    assert ContinuousWorkerHost(step).run(max_counted=0) == LoopRun(0, 0, 0, None)
    assert called is False


def test_sequence_and_boundary_attempts_include_failures_and_uncounted_work() -> None:
    contexts: list[LoopContext] = []

    def step(context: LoopContext) -> LoopDirective:
        contexts.append(context)
        if context.sequence == 1:
            raise ValueError("retry")
        return LoopDirective(LoopAction.STOP, counted=False)

    result = ContinuousWorkerHost(
        step,
        on_error=lambda _error, _context: LoopDirective(
            LoopAction.CONTINUE, counted=False
        ),
    ).run()
    assert [(c.sequence, c.counted, c.attempts_in_boundary) for c in contexts] == [
        (1, 0, 1),
        (2, 0, 2),
    ]
    assert result.attempts_in_boundary == 2


def test_periodic_rotation_occurs_before_threshold_plus_one_attempt() -> None:
    events: list[tuple[str, int, int, str]] = []

    def step(context: LoopContext) -> LoopDirective:
        events.append(("step", context.sequence, context.attempts_in_boundary, ""))
        if context.sequence == 3:
            return LoopDirective(LoopAction.STOP, counted=False)
        return LoopDirective(LoopAction.CONTINUE)

    def rotate(context: LoopContext, reason: str) -> None:
        events.append(("rotate", context.sequence, context.attempts_in_boundary, reason))

    result = ContinuousWorkerHost(step, rotate=rotate, rotation_attempts=2).run()
    assert events == [
        ("step", 1, 1, ""),
        ("step", 2, 2, ""),
        ("rotate", 2, 2, "attempt-limit"),
        ("step", 3, 1, ""),
    ]
    assert result.attempts_in_boundary == 1


def test_directive_rotation_calls_hook_and_resets_attempts() -> None:
    rotations: list[tuple[LoopContext, str]] = []
    contexts: list[LoopContext] = []

    def step(context: LoopContext) -> LoopDirective:
        contexts.append(context)
        if context.sequence == 1:
            return LoopDirective(
                LoopAction.CONTINUE,
                boundary=BoundaryAction.ROTATE,
                counted=False,
                reason="transition",
            )
        return LoopDirective(LoopAction.STOP, counted=False)

    result = ContinuousWorkerHost(
        step, rotate=lambda context, reason: rotations.append((context, reason))
    ).run()
    assert rotations[0][0].attempts_in_boundary == 1
    assert rotations[0][1] == "transition"
    assert contexts[1].attempts_in_boundary == 1
    assert result.attempts_in_boundary == 1


def test_control_can_wait_without_driving_a_step() -> None:
    control_calls = 0
    sleeps: list[float] = []

    def control(context: LoopContext) -> LoopDirective | None:
        nonlocal control_calls
        control_calls += 1
        if control_calls == 1:
            assert context == LoopContext(0, 0, 0, False)
            return LoopDirective(LoopAction.WAIT, delay=4, counted=False)
        assert context.repeated_wait is True
        return None

    result = ContinuousWorkerHost(
        lambda _: LoopDirective(LoopAction.STOP, counted=False),
        control=control,
        sleep=sleeps.append,
    ).run()
    assert sleeps == [4]
    assert result.sequence == 1


def test_error_is_mapped_with_attempt_context_and_marks_repeated_wait() -> None:
    errors: list[tuple[BaseException, LoopContext]] = []
    flags: list[bool] = []

    def step(context: LoopContext) -> LoopDirective:
        flags.append(context.repeated_wait)
        if context.sequence == 1:
            raise LookupError("missing")
        return LoopDirective(LoopAction.STOP, counted=False)

    def on_error(error: BaseException, context: LoopContext) -> LoopDirective:
        errors.append((error, context))
        return LoopDirective(LoopAction.CONTINUE, counted=False)

    result = ContinuousWorkerHost(step, on_error=on_error).run()
    assert isinstance(errors[0][0], LookupError)
    assert errors[0][1] == LoopContext(1, 0, 1, False)
    assert flags == [False, True]
    assert result.sequence == 2


def test_unmapped_error_propagates() -> None:
    def step(_: LoopContext) -> LoopDirective:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        ContinuousWorkerHost(step).run()


def test_error_mapper_raising_propagates_mapper_error() -> None:
    def step(_: LoopContext) -> LoopDirective:
        raise ValueError("step")

    def mapper(_error: BaseException, _context: LoopContext) -> LoopDirective:
        raise RuntimeError("mapper")

    with pytest.raises(RuntimeError, match="mapper"):
        ContinuousWorkerHost(step, on_error=mapper).run()


def test_cooperative_stop_interrupts_real_wait() -> None:
    waiting = threading.Event()

    def step(_: LoopContext) -> LoopDirective:
        waiting.set()
        return LoopDirective(LoopAction.WAIT, delay=30, counted=False)

    host = ContinuousWorkerHost(step)
    thread = threading.Thread(target=host.run)
    thread.start()
    assert waiting.wait(1)
    host.request_stop()
    thread.join(1)
    assert not thread.is_alive()


def test_run_is_non_reentrant() -> None:
    entered = threading.Event()
    release = threading.Event()

    def step(_: LoopContext) -> LoopDirective:
        entered.set()
        release.wait(1)
        return LoopDirective(LoopAction.STOP, counted=False)

    host = ContinuousWorkerHost(step)
    thread = threading.Thread(target=host.run)
    thread.start()
    assert entered.wait(1)
    with pytest.raises(RuntimeError, match="already running"):
        host.run()
    release.set()
    thread.join(1)


@pytest.mark.parametrize("value", [-1, -0.1])
def test_negative_delay_is_rejected(value: float) -> None:
    with pytest.raises(ValueError, match="delay"):
        LoopDirective(LoopAction.WAIT, delay=value)


def test_invalid_bounds_are_rejected() -> None:
    with pytest.raises(ValueError, match="rotation_attempts"):
        ContinuousWorkerHost(lambda _: LoopDirective(LoopAction.STOP), rotation_attempts=0)
    host = ContinuousWorkerHost(lambda _: LoopDirective(LoopAction.STOP))
    with pytest.raises(ValueError, match="max_counted"):
        host.run(max_counted=-1)


def test_callbacks_must_return_directives() -> None:
    with pytest.raises(TypeError, match="step must return"):
        ContinuousWorkerHost(lambda _: None).run()  # type: ignore[arg-type,return-value]


def test_observation_is_best_effort() -> None:
    events: list[str] = []

    def observe(event: str, _context: LoopContext, _directive: LoopDirective | None) -> None:
        events.append(event)
        raise RuntimeError("telemetry")

    result = ContinuousWorkerHost(
        lambda _: LoopDirective(LoopAction.STOP, counted=False), observe=observe
    ).run()
    assert result.sequence == 1
    assert events == ["step", "stopped"]
