# Changelog

## Unreleased

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

