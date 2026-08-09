"""datasource-kit: a generic, dependency-free toolkit for managing many datasources.

It supports five faces:

* **Ingest primitives** -- batch reference-data updaters and long-running
  scraper workers built on :class:`~datasource_kit.protocols.DataSource`,
  :class:`~datasource_kit.protocols.IngestActor`, journal, rate-limit, and
  retry mechanics, plus the opt-in ``run_ingest`` runtime.
* **Artifact backends** -- :class:`~datasource_kit.protocols.ArtifactStore`
  for bytes-in/ref-out payload persistence.
* **Execution backends** -- :class:`~datasource_kit.execution.ExecutionBackend`
  wraps one opaque synchronous callback without owning run or retry policy.
* **Autonomous worker hosting** -- :class:`~datasource_kit.worker.WorkerHost`
  owns checkpoint, heartbeat, backoff, and shutdown mechanics around consumer
  source intent; :class:`~datasource_kit.continuous.ContinuousWorkerHost` is a
  sibling for repeating already-persisted opaque steps from post-step decisions.
* **Fleet supervision** -- :mod:`~datasource_kit.fleet` domain-blind process
  supervision primitives (spawn, stop, liveness) for long-lived worker OS
  processes.

Both ingest models register into one :class:`~datasource_kit.registry.Registry`
and can be described declaratively with
:class:`~datasource_kit.manifest.Manifest`.
"""

from __future__ import annotations

from .completeness import CompletenessReport, LayerCoverage, layers_from_names
from .continuous import (
    BoundaryAction,
    ContinuousWorkerHost,
    LoopAction,
    LoopContext,
    LoopDirective,
    LoopRun,
)
from .errors import (
    DatasourceKitError,
    InventoryError,
    ProfileError,
    ProviderError,
    RegistryError,
    RuntimeStepError,
    SourceError,
    TransportError,
    ValidationError,
)
from .execution import ExecutionBackend, ExecutionRequest, InlineExecutionBackend
from .fleet import (
    DESIRED_DISABLED,
    DESIRED_ENABLED,
    DESIRED_PAUSED,
    GENERATION_ENV,
    DesiredStateReconciler,
    FleetHost,
    FleetPass,
    Liveness,
    ProcessSpec,
    ReconcileOutcome,
    SpawnResult,
    StopOutcome,
    StopResult,
    SupervisorLockError,
    UnitObservation,
    WorkerControlPlane,
    acquire_lock,
    honor_desired_state,
    liveness,
    lock_is_live,
    read_json,
    release_lock,
    spawn,
    spawn_process,
    stop,
    stop_process,
    write_json_atomic,
)
from .inventory import InventoryEntry, fleet_inventory
from .journal import ensure_update_log, now_utc, record_update
from .ledger import DiscoveredItem, DiscoveryLedgerStore, Evidence, LedgerSummary
from .manifest import EXECUTION_AUTONOMOUS, ExecutionModel, Manifest, SourceContract
from .profile import SourceProfile, load_profile, validate_source
from .protocols import (
    ArtifactStore,
    DataSource,
    Enumerator,
    Fetcher,
    IngestActor,
    InMemoryArtifactStore,
    InMemoryStore,
    MockEnumerator,
    MockFetcher,
    StoragePort,
    SupportsExistingIds,
)
from .providers import ProviderRegistry, builtin_registry
from .rate_limit import TokenBucket
from .registry import Registry
from .report import IngestReport
from .results import (
    Cursor,
    WorkerResult,
    blocked_result,
    completed_result,
    working_result,
)
from .retry import retry, retry_decorator
from .runtime import run_ingest
from .window import DayWindow, WindowIterator, split_range_into_days
from .worker import (
    BackoffPolicy,
    CheckpointStore,
    FileCheckpointStore,
    InMemoryCheckpointStore,
    SourceIntent,
    SourceOutput,
    StepDecision,
    WorkDirective,
    WorkerHeartbeat,
    WorkerHost,
    WorkerIntent,
    WorkerRun,
    WorkerStep,
)

__all__ = [
    "BoundaryAction",
    "ContinuousWorkerHost",
    "LoopAction",
    "LoopContext",
    "LoopDirective",
    "LoopRun",
    "ArtifactStore",
    "SourceIntent",
    "SourceOutput",
    "StepDecision",
    "WorkDirective",
    "WorkerStep",
    "WorkerRun",
    "WorkerIntent",
    "WorkerHost",
    "WorkerHeartbeat",
    "InMemoryCheckpointStore",
    "FileCheckpointStore",
    "CheckpointStore",
    "BackoffPolicy",
    "CompletenessReport",
    "Cursor",
    "DESIRED_DISABLED",
    "DESIRED_ENABLED",
    "DESIRED_PAUSED",
    "DataSource",
    "DatasourceKitError",
    "DayWindow",
    "DesiredStateReconciler",
    "DiscoveredItem",
    "DiscoveryLedgerStore",
    "EXECUTION_AUTONOMOUS",
    "Enumerator",
    "Evidence",
    "ExecutionBackend",
    "ExecutionModel",
    "ExecutionRequest",
    "Fetcher",
    "FleetHost",
    "FleetPass",
    "GENERATION_ENV",
    "IngestReport",
    "InMemoryArtifactStore",
    "InMemoryStore",
    "InlineExecutionBackend",
    "IngestActor",
    "InventoryEntry",
    "InventoryError",
    "LayerCoverage",
    "LedgerSummary",
    "Liveness",
    "Manifest",
    "MockEnumerator",
    "MockFetcher",
    "ProcessSpec",
    "ProfileError",
    "ProviderError",
    "ProviderRegistry",
    "ReconcileOutcome",
    "Registry",
    "RegistryError",
    "RuntimeStepError",
    "SourceContract",
    "SourceError",
    "SourceProfile",
    "SpawnResult",
    "StopOutcome",
    "StopResult",
    "StoragePort",
    "SupervisorLockError",
    "SupportsExistingIds",
    "TokenBucket",
    "TransportError",
    "UnitObservation",
    "ValidationError",
    "WindowIterator",
    "WorkerControlPlane",
    "WorkerResult",
    "acquire_lock",
    "blocked_result",
    "builtin_registry",
    "completed_result",
    "ensure_update_log",
    "fleet_inventory",
    "honor_desired_state",
    "layers_from_names",
    "liveness",
    "lock_is_live",
    "load_profile",
    "now_utc",
    "read_json",
    "record_update",
    "release_lock",
    "retry",
    "retry_decorator",
    "run_ingest",
    "spawn",
    "spawn_process",
    "split_range_into_days",
    "stop",
    "stop_process",
    "validate_source",
    "working_result",
    "write_json_atomic",
]
