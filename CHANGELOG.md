# Changelog

## v0.15.0 — 2026-09-30

- Add `FetchIntent`: a concrete `SourceIntent` that runs `plan → fetch → persist`
  with a consumer-supplied `Fetcher` and a consumer-named `Cursor`.
- Add `SequencePlanner` and `ResumePlanner` so consumers walk opaque units or
  resume tokens without the kit interpreting payload identity.
- `split_range_into_days` accepts `order=` so newest-to-oldest is first-class;
  oldest-to-newest remains the default.
- `Cursor.as_dict` / `Cursor.from_mapping` round-trip JSON checkpoints.
- Keep FastAPI adapter tests on HTTPX2's `AsyncClient` + `ASGITransport` only.
- Pin the optional FastAPI extra and the `[dev]` extra/group to current
  FastAPI and HTTPX2, and keep `uv.lock` in lockstep (`uv lock --check`).
- Document `spawn()` as fence-before-exec: `pid.json` is written with
  `status="launch_intent"` before `Popen`, and immediate exit leaves the
  tombstone rather than an empty unit directory (#84).
- Document `liveness()` as `"running"` / `"stale"` plus `FileNotFoundError`
  when pid.json is missing; it never returns `"stopped"` (#83).
- Document and contract `stop` / `stop_process` as live-handle TERM only: no
  timeout, no SIGKILL escalation, and no pid.json cleanup (#82).

## v0.14.6 — 2026-09-06

- Give ACK-gated wrapper startup its own positive, finite `startup_timeout`
  (default 30 seconds), separate from the short post-ACK crash probe (#78).
- Keep timed-out launch intent fenced and never acknowledge an unready wrapper.

## v0.14.5 — 2026-09-02

- Preserve public fleet facade injection and monkeypatch seams after the internal module split.


## v0.14.4 — 2026-09-02

- Align fleet documentation with cooperative, tombstone-preserving stop semantics.
- Report the installed datasource-kit version in ingest reports.
- Ship demo profiles in wheels and make `examples run` independent of a checkout.
- Use one public retry contract in both the toolkit and `run_ingest`.
- Split fleet process, desired-state, host, lock, and control-plane responsibilities into small modules behind the stable `datasource_kit.fleet` facade.
- Remove repository-local mill artifacts.

