# Changelog

## v0.14.5 — 2026-09-02

- Preserve public fleet facade injection and monkeypatch seams after the internal module split.


## v0.14.4 — 2026-09-02

- Align fleet documentation with cooperative, tombstone-preserving stop semantics.
- Report the installed datasource-kit version in ingest reports.
- Ship demo profiles in wheels and make `examples run` independent of a checkout.
- Use one public retry contract in both the toolkit and `run_ingest`.
- Split fleet process, desired-state, host, lock, and control-plane responsibilities into small modules behind the stable `datasource_kit.fleet` facade.
- Remove repository-local mill artifacts.

