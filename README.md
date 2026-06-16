# datasource-kit

A small, **dependency-free** toolkit for managing *many* datasources from one
place. It captures the plumbing that repeats across every datasource so projects
can focus on **what** each source is, not **how** the machinery runs.

It deliberately supports **two models** behind one tiny surface:

| Model | Who | Shape |
|---|---|---|
| **Batch reference-data** | MSDS Portal `datasources/` | download an official dataset → reload a local SQLite store → answer lookups |
| **Scraper / crawler** | Temida/hermes datasources | consume a queued job → fetch from an upstream surface → yield domain objects |

The kit imposes neither model on the other. A batch source implements
`DataSource`; a scraper implements `IngestActor`; both can register into one
`Registry` and describe themselves with a `Manifest`.

## What's in the box

- `journal` — `now_utc`, `ensure_update_log`, `record_update`: the `update_log`
  bookkeeping table for batch updaters (schema-compatible with existing MSDS DBs).
- `retry` — linear-backoff synchronous retry around a single flaky call.
- `rate_limit.TokenBucket` — thread-safe throttle for scraper sources.
- `registry.Registry[T]` — name-keyed registry with duplicate protection.
- `protocols.DataSource` / `protocols.IngestActor` — `runtime_checkable`
  structural protocols; existing classes satisfy them without importing the kit.
- `manifest.Manifest` / `manifest.SourceContract` — declarative source
  descriptors (data, not code).

- `scheduler.WorkerScheduler` — *optional*, behind the `scheduler` extra:
  an APScheduler-backed helper to run a poll/dispatch loop on a fixed interval.

The core package has **no third-party runtime dependencies** (Python ≥ 3.12).
Only `WorkerScheduler` needs the extra, and it is imported lazily so plain
`import datasource_kit` never pulls in APScheduler:

```bash
pip install "datasource-kit[scheduler]"
```

## Install (path dependency)

```toml
# consumer pyproject.toml
[tool.uv.sources]
datasource-kit = { path = "../datasource-kit", editable = true }
```

## Usage

### Batch reference-data source

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
registry.register(CLPDataSource())      # uses .name if present, else pass name=...
registry.register(PRTRDataSource(), name="prtr")
registry.get("prtr").lookup("7440-43-9")
```

### Describing a source declaratively

```python
from datasource_kit import Manifest, SourceContract

Manifest(name="clp", source_type="batch")  # batch needs nothing else

Manifest(
    name="saos",
    source_type="scraper",
    supports_autonomous=True,               # autonomous => contract required
    rate_limit={"rps": 1.0, "burst": 2.0},
    contract=SourceContract(
        source_truth="Official SAOS API dump/search/detail surfaces.",
        enumeration_method="API-first dump/search/coverage-window traversal.",
        evidence=("API dump pages", "detail responses"),
        identity_strategy="SAOS judgment identifier.",
        diff_target="canonical judgment corpus",
        coverage_unit="API page or date coverage window",
    ),
)
```

## Design notes

- **Stdlib only** so both a lightweight batch repo and a heavy scraper repo can
  adopt it without dependency friction.
- **Structural protocols, not base classes** — existing datasource classes
  already satisfy `DataSource` / `IngestActor` without inheritance or imports.
- The heavyweight scraper apparatus (job queue, coverage windows, runtime
  supervisor) intentionally lives in the *consuming* project, not here. The kit
  provides the shared vocabulary and primitives, not an orchestrator.

## Development

```bash
uv run pytest
```
