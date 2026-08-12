# Agent notes

Prefer small Unix-composable primitives (`fleet`, manifests, and similar). Keep
surfaces narrow and composable; do not grow a parallel process orchestrator that
competes with Fala.

Fala owns in-process journal and orchestration for workers that use it.
Datasource-kit stays at the OS-process and composition boundary (spawn, stop,
liveness, desired-state reconciliation, inventory). Multiple Fala journals per
worker are expected upstream (Hermes).
