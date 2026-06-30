# datasource-kit

A domain-blind ingest framework. It owns the generic **how**: a named runtime,
window/checkpoint loop, rate-limit and retry mechanics, provider validation,
explainable reports, errors, and a CLI. It refuses the **what**: which source,
which endpoints, parsing, identity rules, completeness layer names, and grading
verdicts.

It has two honest faces:

- a primitives library: `DataSource`, `IngestActor`, `Registry`, `Manifest`,
  `journal`, `results`, `window`, `ledger`, `ratelimit`, `retry`,
  `completeness`, and structural storage/artifact protocols;
- an opt-in `run_ingest` runtime, which is one composition of those primitives,
  never the only way to use the kit.

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

## Install

```toml
[tool.uv.sources]
datasource-kit = { path = "../datasource-kit", editable = true }
```

Core has no third-party runtime dependencies. Optional integrations are lazy:

```bash
pip install "datasource-kit[profiles]"   # YAML profile loading
pip install "datasource-kit[scheduler]"  # APScheduler helper
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
