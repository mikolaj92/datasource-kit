# datasource-kit

A domain-blind ingest framework. It owns the generic **how**: a named runtime,
window/checkpoint loop, rate-limit and retry mechanics, provider validation,
explainable reports, errors, and a CLI. It refuses the **what**: which source,
which endpoints, parsing, identity rules, completeness layer names, and grading
verdicts.

It has four honest faces:

- a **primitives library**: `DataSource`, `IngestActor`, `Registry`, `Manifest`,
  `journal`, `results`, `window`, `ledger`, `ratelimit`, `retry`,
  `completeness`, and structural storage/artifact protocols;
- an **opt-in runtime**: `run_ingest`, which is one composition of those
  primitives, never the only way to use the kit;
- an **autonomous worker host**: `WorkerHost`, which wraps consumer planning,
  fetch, transform, and persistence intent in a common lifecycle loop;
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

- `ProcessSpec(unit, command, cwd, env, label)` -- declarative unit description
  built by the consumer.
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

Launch/stop are injectable (`spawn_action` / `stop_action`), defaulting to
`spawn` / `stop`, so a consumer can keep a richer launch (log redirection, its
own environment, richer pid metadata) while the kit stays domain-blind. No
health semantics beyond process liveness -- health interpretation stays in the
consumer.

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
self-directed rather than queue-driven. The consumer implements `WorkerIntent`
(`plan`, `fetch`, `transform`, and idempotent `persist`); the kit owns checkpoint
load/save, lifecycle heartbeats, idle polling, exponential failure backoff, and
cooperative shutdown:

```python
from datasource_kit import FileCheckpointStore, WorkerHost, WorkerStep

class Source:
    def plan(self, checkpoint):
        return WorkerStep(plan={"after": checkpoint}, checkpoint="next")
    def fetch(self, plan): ...
    def transform(self, payload, plan): ...
    def persist(self, records, plan): ...  # must be idempotent

host = WorkerHost(Source(), FileCheckpointStore("state/checkpoint.json"))
host.run()  # another thread or signal handler may call host.request_stop()
```

A checkpoint advances only after persistence succeeds. Since arbitrary storage
and checkpoint files cannot share a transaction, a crash between those actions
may replay a plan: the contract is deliberately **at least once**. `heartbeat`
is an optional callback receiving `WorkerHeartbeat`; callback failures are
isolated from work. `max_iterations` supports bounded/one-shot embedding.

## Install

```toml
[tool.uv.sources]
datasource-kit = { path = "../datasource-kit", editable = true }
```

Core has no third-party runtime dependencies. Optional integrations are lazy:

```bash
pip install "datasource-kit[profiles]"   # YAML profile loading
pip install "datasource-kit[fala]"       # Fala artifact adapter
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
