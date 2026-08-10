# datasource-kit

A domain-blind ingest framework. It owns the generic **how**: a named runtime,
window/checkpoint loop, rate-limit and retry mechanics, provider validation,
explainable reports, errors, and a CLI. It refuses the **what**: which source,
which endpoints, parsing, identity rules, completeness layer names, and grading
verdicts.

It has five honest faces:

- a **primitives library**: `DataSource`, `IngestActor`, `Registry`, `Manifest`,
  `journal`, `results`, `window`, `ledger`, `ratelimit`, `retry`,
  `completeness`, and structural storage/artifact protocols;
- an **opt-in runtime**: `run_ingest`, which is one composition of those
  primitives, never the only way to use the kit;
- a tiny **execution boundary**: `ExecutionBackend`, which wraps one opaque,
  synchronous in-process callback without owning run or retry policy;
- **autonomous worker hosts**: `WorkerHost`, which wraps consumer planning,
  fetch, transform, and persistence intent, and the domain-blind sibling
  `ContinuousWorkerHost`, which repeats an opaque, already-persisted step from
  its post-step lifecycle decision;
- a **fleet supervision face**: :mod:`datasource_kit.fleet` -- domain-blind
  process supervision primitives (spawn, stop, liveness) plus a
  `DesiredStateReconciler` (desired/actual state files, generation fencing, an
  exclusive supervisor lock, a converge loop) for long-lived worker OS
  processes.

## Core Archetype

```text
enumerate -> fetch -> persist -> diff -> assess -> report
   |          |         |         |         |         |
 window/   throttle  validated  by_id /  counts ->  IngestReport
 checkpoint + retry  records    full_    consumer   + optional per-layer
 loop      evidence  only       replace  status     CompletenessReport
```

`run_ingest()` wires `TokenBucket` throttling and `with_retry` around registered
provider hooks. The profile chooses provider names such as `diff.by_id`,
`diff.full_replace`, and `assess.passthrough`; the registry resolves them
fail-closed before the run starts.

## Quickstart

The shipped demos use JSON profiles, in-memory stores, mock fetchers, no
network, and no third-party dependency:

```bash
datasource-kit examples run demo-scraper
datasource-kit examples run demo-batch --out report.json
datasource-kit coverage report report.json
datasource-kit explain report.json
```

A consumer supplies two things:

- a profile folder, usually `source.json`, naming registered providers and
  carrying policy numbers plus its own `status_vocabulary` and
  `completeness_layers`;
- provider implementations registered by safe name. Providers satisfy
  structural protocols; consumers do not subclass kit internals.

## It is NOT

- Not a crawler or scraper for any specific source. The kit ships `fetch.mock`;
  real HTTP, browser, parsing, and identity logic are injected by the consumer.
- No domain vocabulary. Source names, record identity, status labels, and layer
  meanings live in the profile and providers.
- No default completeness taxonomy. `CompletenessReport.fraction()` is math, not
  a verdict.
- No grading classifier. Counts do not become `complete`, `partial`, or any
  other business status unless a consumer-registered `assess.*` provider says
  so.
- Not a mandatory orchestrator. Batch consumers can use `DataSource`, `journal`,
  `Registry`, and the pure-data shapes directly without `run_ingest`.
- Not a job queue. Scheduling and supervision remain in the consuming project.
- **Not a scheduler, cron, or daemon**. The `fleet` module provides process
  primitives only; policy stays in the consumer.

## Fleet Supervision

The `datasource_kit.fleet` module provides stdlib-only process supervision
primitives for long-lived worker OS processes:

- `ProcessSpec(...)` -- declarative unit description built by the consumer,
  including optional consumer-owned append paths for stdout/stderr (or an
  injected opener), immediate-exit probe timing, environment overlay and
  generation injection, and opaque JSON pid metadata.
- `spawn(spec) -> SpawnResult` -- starts a subprocess with `start_new_session`,
  writes pid.json atomically, and performs a fail-closed immediate-exit probe.
- `stop(unit_dir, timeout) -> StopResult` -- SIGTERM to the process group,
  escalates to SIGKILL after timeout; cleans up stale pid files.
- `liveness(unit_dir) -> Liveness` -- returns `"running"`, `"stopped"`, or
  `"stale"` from pid.json and OS-level checks.

POSIX only. No scheduler, no cron, no daemon -- these are primitives; policy
stays in the consuming project.

### Desired-state reconciliation

`DesiredStateReconciler` builds a generic supervisor on those primitives: the
consumer declares which units should be running and the reconciler converges
reality to that declaration, without each consumer re-implementing the
lock / generation-fencing / atomic-write mechanics.

- Per-unit `state.json` (desired / actual / generation), written atomically;
  `write_json_atomic` / `read_json` are exposed for reuse.
- **Generation fencing**: each (re)spawn increments a generation and the child
  is spawned aware of it (the default action injects `DATASOURCE_KIT_GENERATION`;
  a consumer that injects its own spawn action picks its own variable). A
  `merge_heartbeat` is accepted only when the reporting generation matches, so a
  zombie from a previous generation can never overwrite current state.
- **Exclusive supervisor lock with dead-owner steal**: a lock held by a dead
  pid (same host) is stolen; a live foreign owner raises `SupervisorLockError`;
  a foreign host is treated as held (fail closed).
- `reconcile_once(specs, policy)` converges every unit once under the lock;
  `serve(specs, policy, interval)` holds the lock and reconciles on a loop. The
  `policy` is a consumer hook (`honor_desired_state` is the default) deciding
  per unit whether it should run -- the kit never decides which units run.

The default launch handles rich, consumer-declared process specs directly;
launch/stop remain injectable (`spawn_action` / `stop_action`). For example:

```python
spec = ProcessSpec(
    unit="consumer-chosen",
    command=("python", "-m", "my_worker"),
    stdout_path="var/my-layout/current.log",  # append mode
    stderr_path="var/my-layout/current.err",
    env_overlay={"WORKER_MODE": "continuous"},
    generation_env="MY_GENERATION",          # injected by reconciler
    probe_window=2.0,
    probe_sleep=0.1,
    pid_metadata={"session": "opaque-consumer-value"},
)
```

Paths and names remain consumer-owned. Environment values are used only for
launch and are never serialized into `pid.json`; opaque pid metadata must be
JSON-compatible and cannot replace the standard pid fields. Parent-side log
descriptors are closed after spawning. A pid file is written atomically only
after the child survives its probe window, and immediate exit leaves none. No
health semantics beyond process liveness -- health interpretation stays in the
consumer.

### Generic fleet pass hosting

`FleetHost` is the minimal orchestration face for consumers that already own
fleet membership and reconciliation. It materializes an iterable of opaque
units once, preserving the consumer's order, then runs two injected callbacks:
admission once at the start of every pass and reconciliation once per unit.
`FleetPass` returns the opaque admission value and ordered result tuple.

```python
from datasource_kit import FleetHost

host = FleetHost(
    lock_path="var/supervisor.lock",
    units=("eli", "saos"),                 # consumer-chosen fixed order
    admit=lambda units: validate_inventory(units),
    reconcile=lambda unit: reconcile_unit(unit),
    lock_payload=lambda: {"managed_by": "my-supervisor"},
)

one_pass = host.reconcile_pass()            # intentionally unlocked
locked_pass = host.run_once()               # one lock for one pass
host.serve(
    interval=5.0,
    on_pass=lambda completed: log(completed),
    stop_condition=lambda: shutting_down(),
)                                            # one lock held for the whole loop
```

Admission is fail-closed: if it raises, no unit is reconciled. It is repeated
on every `serve` pass. Units and all callback values are opaque; the host knows
nothing about datasources, inventory meaning, desired state, or health.
Exceptions from admission, reconciliation, reporting, and sleeping propagate
unchanged, and the acquired lock is released in every case. In `serve`, the
pass callback runs while the lock is held and before the stop check and sleep.
The interval must be positive and is validated before lock acquisition.

### Fleet inventory

A fleet supervisor needs to know which units exist and what they can do. Rather
than static-parsing manifest source files, build the inventory by importing each
datasource's manifest and reading the same `Manifest` object the runtime uses:

```python
from datasource_kit import fleet_inventory

entries = fleet_inventory("myproj.datasources", ["eli", "saos", "clp"])
autonomous = [e.name for e in entries if e.is_autonomous]
```

`fleet_inventory(package, names)` imports `package.<name>.manifest` (submodule
and attribute name are overridable) and returns one `InventoryEntry` per name,
carrying the loaded `Manifest` plus convenience flags (`is_autonomous`,
`has_contract`, `execution_model`, `rate_limit`). It is fail-closed: a manifest
module that is missing, fails to import, or exposes no `Manifest` raises
`InventoryError` -- never a silently skipped unit, never an AST/filesystem guess.

A `Manifest` states how a source runs with a first-class `execution`
(`ExecutionModel(model, step_ref)`); `model="autonomous"` marks a long-lived
worker and requires a `SourceContract`. The older boolean `supports_autonomous`
is still honoured by `is_autonomous` but is superseded by `execution`.

## Autonomous Worker Host

`WorkerHost` is the minimal in-process lifecycle loop for sources whose work is
self-directed rather than queue-driven. The consumer implements the
runtime-checkable `SourceIntent` (`plan`, `fetch`, `transform`, `output`, and
idempotent `persist`); the kit owns checkpoint load/save, lifecycle heartbeats,
idle polling, exponential failure backoff, and cooperative shutdown. This is a
contract integrated into `WorkerHost`, not a second host:

```python
from datasource_kit import (
    FileCheckpointStore, SourceOutput, StepDecision, WorkDirective, WorkerHost,
)

class Source:
    def plan(self, checkpoint):
        if already_finished(checkpoint):
            return StepDecision(WorkDirective.STOP)
        return StepDecision(WorkDirective.CONTINUE, {"after": checkpoint})
    def fetch(self, plan): ...
    def transform(self, payload, plan): ...
    def output(self, records, plan):
        return SourceOutput(
            result={"records": records.items},
            checkpoint=records.next_cursor,
            directive=WorkDirective.STOP if records.final else WorkDirective.CONTINUE,
        )
    def persist(self, result, plan): ...  # must be idempotent

host = WorkerHost(Source(), FileCheckpointStore("state/checkpoint.json"))
host.run()  # another thread or signal handler may call host.request_stop()
```

The generic directives are `CONTINUE` (run the pipeline), `IDLE` (poll later),
and `STOP` (normal terminal completion). Their mapping to source-specific
statuses is entirely consumer-owned.

For `SourceIntent`, the order is strictly `plan -> fetch -> transform -> output
-> persist -> checkpoint store`; `output` is the final, side-effect-free shaping
stage of transformation. `STOP` and `IDLE` planning directives skip the
entire fetch pipeline, and only one plan is requested per iteration. The output
checkpoint never advances unless result persistence succeeds. An output may
also carry `STOP` or `IDLE`; its result is still persisted and checkpointed
before the directive takes effect. Since arbitrary storage
and checkpoint files cannot share a transaction, a crash between those actions
may replay a plan: the contract is deliberately **at least once**. The original `WorkerIntent`,
`WorkerStep(plan, checkpoint)`, late `checkpoint` method, and `None`-means-idle
planner forms remain supported for compatibility. New integrations should use
`SourceIntent` and immutable `SourceOutput`. `heartbeat` is an optional callback receiving
`WorkerHeartbeat`; callback failures are isolated from work. `max_iterations`
supports bounded/one-shot embedding.

### Continuous post-step host

`ContinuousWorkerHost` is a composition sibling, not a replacement for
`WorkerHost`. Use it when the consumer must interpret and persist an opaque
result before deciding whether to continue, wait, or stop. It owns only loop
mechanics and never inspects the result or checkpoint:

```python
from datasource_kit import ContinuousWorkerHost, LoopAction, LoopDirective

def step(context):
    result = obtain_and_persist(sequence=context.sequence)
    if result.is_finished:
        return LoopDirective(LoopAction.STOP, counted=False, reason="finished")
    if result.should_poll:
        return LoopDirective(
            LoopAction.WAIT, delay=5, counted=False, reason="polling"
        )
    return LoopDirective(LoopAction.CONTINUE)

run = ContinuousWorkerHost(step, rotation_attempts=200).run()
```

A step sees its current attempt number: the first `LoopContext` has `sequence=1`
and `attempts_in_boundary=1`. `sequence` increases for every step invocation,
including failures. `counted` increases only when the returned directive sets
`counted=True`; therefore `run(max_counted=N)` can bound productive work while
ignoring polls or error attempts. `attempts_in_boundary` resets after a requested
or periodic rotation. Periodic rotation happens before attempt *N+1*.

`WAIT` uses its non-negative `delay`; a real wait is interruptible through
`request_stop()`. `repeated_wait` tells callbacks whether the preceding outcome
was a wait or mapped error, enabling consumer-owned edge-trigger policy. The
optional `control` callback can return a directive between steps (for example,
a warm pause), and `rotate(context, reason)` performs consumer-owned boundary
replacement. An exception from `step` is propagated unless `on_error` maps it
to a `LoopDirective`; an exception from `on_error` is always propagated.
`observe` is best-effort telemetry and must not be used for durable effects.
Concurrent calls to the same host's `run()` are rejected.

## Execution Backend

`ExecutionBackend` is a structural, dependency-free seam for choosing where and
how one opaque callback executes and is observed. The built-in
`InlineExecutionBackend` calls it synchronously in the current process:

```python
from datasource_kit import ExecutionRequest, InlineExecutionBackend

backend = InlineExecutionBackend()
summary = backend.execute(
    ExecutionRequest(
        run_id="run-42",
        execution_id="run-42:step-3",
        inputs={"checkpoint": {"cursor": 10}},
        metadata={"step": 3, "source": "consumer-defined"},
    ),
    lambda: perform_step(),
)
```

The callback is invoked exactly once. Its return value is returned unchanged
(the same object), and its exception is propagated unchanged. `inputs` and
`metadata` are opaque, caller-owned JSON-compatible diagnostics: the kit neither
validates nor interprets their vocabulary. Install the optional Fala integration to
durably record a callback against an existing Fala run:

```python
from datasource_kit.adapters.fala import FalaExecutionBackend

backend = FalaExecutionBackend("journal.sqlite")
summary = backend.execute(request, perform_step)
```

This is a thin adapter over Fala 0.7.21's public `record_in_process` API:
`run_id`, `execution_id` (as Fala's `process_id`), `inputs`, `metadata`, and the
callback are forwarded unchanged. Fala owns validation, recording, exact-once
callback invocation, and return/exception semantics. The caller still creates
and finalizes the run and owns IDs, retries, and result lifetime.

A custom backend may add observation or durable recording, but concurrency and
recording policy are its own responsibility.

This boundary intentionally does **not** create or finalize runs, mint IDs,
retry, checkpoint, persist business data, clean results, or manage result
lifetimes. The optional Fala execution and artifact adapters are separate; neither
is imported by the dependency-free core.

## Install

```toml
[tool.uv.sources]
datasource-kit = { path = "../datasource-kit", editable = true }
```

Core has no third-party runtime dependencies. Optional integrations are lazy:

```bash
pip install "datasource-kit[profiles]"   # YAML profile loading
pip install "datasource-kit[fala]"       # Fala execution + artifact adapters
```

## Minimal Batch Usage

```python
import sqlite3
from datasource_kit import ensure_update_log, record_update, retry

def update_database(*, db_path) -> dict:
    con = sqlite3.connect(db_path)
    try:
        ensure_update_log(con)
        payload = retry(lambda: download())
        rows = load_rows(con, payload)
        record_update(con, dataset="records", records_loaded=rows)
        return {"rows_loaded": rows}
    finally:
        con.close()
```

## Profile Example

```json
{
  "name": "demo-scraper",
  "source_type": "scraper",
  "providers": {
    "enumerator": "window.by_day",
    "fetcher": "fetch.mock",
    "mapper": "records.passthrough",
    "diff": "diff.by_id",
    "assess": "assess.passthrough",
    "store": "store.in_memory"
  },
  "policies": {
    "rate_limit": {"rate": 5.0, "capacity": 5.0},
    "retry": {"attempts": 3, "base_delay": 0.0, "max_delay": 0.0}
  },
  "status_vocabulary": ["ok"],
  "completeness_layers": ["records"]
}
```

## Development

```bash
uv run pytest
```


### Fail-closed process lifecycle

Fleet worker launch is deliberately first-launch-only. The session-leader exec gate waits
for durable `pid.json` provenance (`unit`, `generation`, stable random `token`) before
executing consumer code. Any surviving metadata is a tombstone, regardless of PID
liveness or readability, and prevents automatic replacement. Disabling never removes it.
The owning in-memory supervisor may request cooperative TERM through its live `Popen`
handle; no numeric-PID signalling, escalation, adoption, guardian cleanup, or restart is
performed. After externally proving the entire workload is gone, an operator may call
`clear_process_tombstone` with the exact identity and explicit assertion; clearance is
audited before replacement becomes eligible.
