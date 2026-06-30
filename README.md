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

- `runtime.run_ingest` / `runtime.ProviderRegistry` / `runtime.builtin_registry` —
  the **Named Core Archetype**: opt-in `enumerate→fetch→persist→diff→assess→report`
  pipeline with provider-registry wiring, `TokenBucket` throttle, `with_retry`
  backoff, and in-memory defaults for zero-setup use.
- `report.IngestReport` / `report.CompletenessReport` — serializable, replayable
  output object with diff counts, retry/throttle telemetry, `source_digest`,
  `kit_version`, and `save_json`.

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

### Named Core Archetype — `run_ingest`

`run_ingest` is the opt-in composition over registered providers.  It is the
structural twin of `splot.run_round` and `reviewkit.review_document`: a single
named entry point that chains the primitives for you, never the only way to use
them.

```python
from datasource_kit import run_ingest, IngestReport

# Standalone demo — zero consumer code, zero network, zero third-party deps:
report: IngestReport = run_ingest("my-source")
print(report.status)        # "ok"
print(report.diff)          # {"added": ..., "updated": ..., ...}
print(report.retries_used)  # int
report.save_json("report.json")
```

#### Two diff strategies out of the box

| Provider name | Behaviour |
|---|---|
| `diff.by_id` (default) | Set-difference vs `store.existing_ids()` — upserts new/changed records |
| `diff.full_replace` | Replaces the entire store via `store.replace_all()` — never reads `existing_ids()` |

#### Custom assess provider

```python
from datasource_kit import SourceProfile, builtin_registry, run_ingest

reg = builtin_registry()

def my_assess(counts, evidence):
    return "complete" if counts["added"] > 0 else "empty"

reg.register("assess.mine", my_assess)

profile = SourceProfile(
    name="my-source",
    providers={
        "enumerate": "enumerate.passthrough",
        "records":   "records.passthrough",
        "diff":      "diff.by_id",
        "assess":    "assess.mine",
    },
    policies={"rate_per_sec": 5.0, "burst": 10.0, "retries": 3, "backoff": 1.0},
)
report = run_ingest(profile, registry=reg, windows=[{"id": 1}, {"id": 2}])
print(report.status)   # "complete"
```

The runtime emits **no grading verdict**. Whether counts mean "complete",
"partial", or "empty" is entirely the consumer's `assess` provider's call.

### `IngestReport`

```python
@dataclass
class IngestReport:
    source: str
    status: str                          # consumer's assess provider output
    windows: list[dict]                  # per-window telemetry
    completeness: CompletenessReport | None
    diff: dict[str, int]                 # added/updated/removed/unchanged
    retries_used: int
    rate_limit_waits: float              # total seconds on TokenBucket
    source_digest: str                   # SHA-256 of the resolved profile
    kit_version: str                     # from package metadata
    warnings: list[str]

    def save_json(self, path) -> None: ...
    def as_dict(self) -> dict: ...
```

`source_digest` + `kit_version` give deterministic replay and audit.  `save_json`
uses stdlib `json` only.

## Design notes

- **Stdlib only** so both a lightweight batch repo and a heavy scraper repo can
  adopt it without dependency friction.
- **Structural protocols, not base classes** — existing datasource classes
  already satisfy `DataSource` / `IngestActor` without inheritance or imports.
- **Two faces, one kit** — the primitives library face (`DataSource`/`IngestActor`
  + bare primitives) and the opt-in runtime face (`run_ingest`) coexist. Batch
  MSDS sources keep using the bare `DataSource` protocol; scraper-style sources
  can opt in to `run_ingest` for the full archetype loop.
- The heavyweight scraper apparatus (job queue, coverage windows, runtime
  supervisor) intentionally lives in the *consuming* project, not here. The kit
  provides the shared vocabulary and primitives, not an orchestrator.

## Development

```bash
uv run pytest
```
