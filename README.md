# datasource-kit

A **domain-blind ingest FRAMEWORK** — sibling of reviewkit/splot/fala — that
also keeps its **primitives-library face**. It owns the generic HOW (a named
runtime driving a pipeline, policies-as-data, diff/assess providers, ledger,
report) and refuses the WHAT (which source, endpoints, parsing, identity rules,
completeness-layer taxonomy, grading verdict).

*"Gives us a generic way of doing something, but doesn't say what to do."*

---

## Core Archetype

```
enumerate -> fetch -> persist -> diff -> assess -> report
```

Driven over a window/checkpoint loop with rate-limit + retry wired automatically
around provider hooks. `diff` and `assess` are **profile-named providers** so
the batch full-replace model is expressible without window/id ceremony:

| Provider name        | Behaviour                                                   |
|----------------------|-------------------------------------------------------------|
| `diff.by_id`         | Compare existing ids against fetched; surface new items     |
| `diff.full_replace`  | Treat all fetched items as new; ignore existing state       |
| `assess.passthrough` | Return `"ok"` — assessment verdict lives in the consumer    |

`builtin_registry()` ships all three. The consumer plugs in by declaring a
**profile folder** (`source.json`) that names safe registered providers and
carries policies-as-data. Never by subclassing kit internals.

---

## It is NOT

- NOT a scraper/crawler for any specific source; knows nothing about
  ELI/SAOS/courts/legal/Polish anything.
- NOT a mandatory runtime — the primitives (`DataSource`, `IngestActor`,
  `Registry`, `Manifest`, journal helpers, `TokenBucket`, `retry`) are fully
  usable with no `run_ingest`.
- Does NOT own business logic, parsing, identity/dedup rules, a job queue, the
  runtime supervisor, the completeness-layer taxonomy, or the GRADING POLICY.
- The ledger ships **counts only** — no `lifecycle_state`, no
  count→verdict classifier. The verdict enters only as a consumer-registered
  `assess.*` provider mapping evidence to the consumer's own status strings.
- `validate_source` checks **only provider registration** — no business rules.

---

## What's in the box

### Primitives (zero dependencies — always available)

- `protocols.DataSource` / `protocols.IngestActor` — `runtime_checkable`
  structural protocols; existing classes satisfy them without importing the kit.
- `registry.Registry[T]` — name-keyed registry with duplicate protection.
- `manifest.Manifest` / `manifest.SourceContract` — declarative source
  descriptors (data, not code).
- `journal` — `now_utc`, `ensure_update_log`, `record_update`: the `update_log`
  bookkeeping table for batch updaters (schema-compatible with existing MSDS DBs).
- `retry` — linear-backoff synchronous retry around a single flaky call.
- `rate_limit.TokenBucket` — thread-safe throttle for scraper sources.
- `storage.ArtifactStore` (protocol) + `storage.InMemoryArtifactStore` — seam
  between the runtime and persistence; `full_replace` for the batch model.
- `ledger.DiscoveryLedger` + `ledger.Evidence` — counts-only accumulator; no
  lifecycle state, no classifier.

### Opt-in archetype runtime

- `providers.ProviderRegistry` + `providers.builtin_registry()` — maps
  `kind:name` to callables; ships `diff.by_id`, `diff.full_replace`,
  `assess.passthrough`.
- `ingest.run_ingest` — drives the Core Archetype pipeline; fails closed on
  partial provider coverage; returns `ingest.IngestReport`.
- `profile.SourceProfile` + `profile.load_profile` + `profile.validate_source`
  — profile-folder loader; validation checks provider registration only.

### Optional extras

- `scheduler.WorkerScheduler` — behind the `scheduler` extra (APScheduler);
  imported lazily so `import datasource_kit` never pulls in APScheduler.

---

## Install

```bash
pip install "datasource-kit"
# with the scheduler extra:
pip install "datasource-kit[scheduler]"
```

### Path dependency (editable)

```toml
# consumer pyproject.toml
[tool.uv.sources]
datasource-kit = { path = "../datasource-kit", editable = true }
```

---

## Standalone demo (no network, no extras, no consumer code)

```bash
datasource-kit examples run demo-scraper
```

This runs `builtin_registry()` + `InMemoryArtifactStore` + fake records through
the full `run_ingest` pipeline and prints a coverage report.

---

## Usage

### Batch reference-data source (primitives only, no `run_ingest`)

```python
import sqlite3
from datasource_kit import ensure_update_log, record_update, retry

def update_database(*, db_path) -> dict:
    con = sqlite3.connect(db_path)
    try:
        ensure_update_log(con)
        payload = retry(lambda: download(SOURCE_URL))
        rows = load_rows(con, payload)
        record_update(con, dataset="clp", records_loaded=rows, details={"source": SOURCE_URL})
        return {"dataset": "clp", "rows_loaded": rows}
    finally:
        con.close()
```

### Registering sources

```python
from datasource_kit import Registry

registry: Registry = Registry()
registry.register(CLPDataSource())          # uses .name if present
registry.register(PRTRDataSource(), name="prtr")
registry.get("prtr").lookup("7440-43-9")
```

### Running the archetype pipeline

```python
from datasource_kit import builtin_registry, run_ingest, InMemoryArtifactStore

store = InMemoryArtifactStore()
report = run_ingest(
    enumerator=lambda: my_source.enumerate(),
    fetcher=lambda job: my_source.fetch(job),
    store=store,
    registry=builtin_registry(),
    diff_provider="by_id",
    assess_provider="passthrough",
)
print(report.summary())
```

### Declaring a source as a profile

`examples/demo-scraper/source.json`:

```json
{
  "name": "demo-scraper",
  "providers": {
    "diff": "by_id",
    "assess": "passthrough"
  },
  "policies": {
    "max_retries": 1
  }
}
```

```python
from datasource_kit import load_profile, validate_source, builtin_registry

profile = load_profile("examples/demo-scraper/source.json")
errors = validate_source(profile, builtin_registry())
assert not errors
```

---

## Design notes

- **Stdlib only** — no third-party runtime dependencies.
- **Structural protocols, not base classes** — existing datasource classes
  satisfy `DataSource` / `IngestActor` without inheritance.
- The heavyweight apparatus (job queue, runtime supervisor, completeness verdict)
  lives in the *consuming* project. The kit ships the generic machinery.
- `run_ingest` is one composition of the primitives, never the mandatory path.

## Development

```bash
uv run pytest
datasource-kit examples run demo-scraper
```
