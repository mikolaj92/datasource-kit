# Approach plan

<!-- lokay-approach source=deterministic repo=mikolaj92/datasource-kit issue=62 -->

Repository: `mikolaj92/datasource-kit`  
Issue: #62 — extra [fala] bez źródła git — pip nie dostanie mikolaj92/Fala

## Goal

Make `pip install "datasource-kit[fala]"` install `mikolaj92/Fala` (0.7.21
`record_in_process`), not the unrelated PyPI project named `fala`.

## Files likely touched

- `pyproject.toml` — PEP 508 git extra + `[tool.uv.sources]`
- `README.md` — document the GitHub-only extra
- `tests/adapters/test_fala_extra_source.py` — lock the pin

## Test plan

- `uv run --group dev pytest tests/adapters -q`

## Non-goals

- Do not commit `uv.lock` (gitignored on purpose for this library).
- Do not change the adapter API or import Fala from the core.

## Notes

- Trust intentional issue; this plan is evidence for later review, not a human gate.
- Coding agent may refine details but should stay on the stated goal and non-goals.
- Collector boundary: if implementation introduces unbounded collection, ship only a bounded collector patch that starts durably in the background after merge. The coding agent and mill must not populate data or wait for collection to finish.
