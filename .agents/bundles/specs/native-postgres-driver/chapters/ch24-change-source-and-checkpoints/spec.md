---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch24-change-source-and-checkpoints
title: "Chapter 24: PostgreSQL Change Source, CAS Checkpoint Stores, and Replay Recovery"
description: Implement PostgresChangeSource, atomic compare-and-swap PostgresCheckpointStore, shielded acknowledgement reconciliation, and deterministic replay/shutdown semantics.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch23-replication-feedback-resume-and-bridge
replan_trigger: null
current_task: "native-postgres-driver:ch24:t01"
plan_revision: 6
plan_commit: ef86844f
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-10T14:45:00Z"
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: ef86844f
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgresCheckpointStore CAS protocol, PostgresReplicationGapError, and shielded ack rules
  - id: npd-proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Durable-before-feedback ordering, generation barriers, and int64 ChangeEvent IDs
  - id: cdc-py
    resource: src/py/vertebra/db/cdc.py::ChangeEvent
    title: Engine-neutral ChangeEvent envelope and signed-int64 Parquet schema compatibility
  - id: db-caps
    resource: src/py/vertebra/db/driver/capabilities.py::AsyncChangeSource
    title: Public AsyncChangeSource capability protocol
tags:
  - postgres
  - cdc
  - checkpoints
  - replication
  - recovery
---

# Chapter 24: PostgreSQL Change Source, CAS Checkpoint Stores, and Replay Recovery

<!-- truth: start -->
- **Atomic CAS Durable-Before-Feedback**: `PostgresCheckpointStore.save(checkpoint, expected=...)` atomically verifies `(system_identifier, database, slot, publication_fingerprint)` and monotonic `(commit_end_lsn, next_event_id)` before `PostgresChangeSource.acknowledge` sends native `StandbyStatusUpdate` feedback; CAS conflicts raise `PostgresCheckpointConflictError` without adopting another consumer's position [^npd-contracts] [^npd-proto-research].
- **Shielded Single-Flight Ack & Reconciliation**: One tracked, shielded acknowledgement task closes the yield gate, persists the checkpoint snapshot captured at `COMMIT` emission, and sends native feedback; any ambiguous store/native failure invalidates the generation, closes the handle, and reloads durable state before yielding further records [^npd-contracts].
- **Source-Owned Reconnect & Stable Replay IDs**: `PostgresChangeSource` always opens native streams with `reconnect=False`, owns `0.1s..5.0s` jittered retries (`max 8`), resets `next_event_id` (`1..2**63-1`) from the reloaded checkpoint so replayed transactions keep identical signed-int64 `ChangeEvent.event_id`s, and enforces an absolute `checkpoint_timeout` shutdown budget [^npd-contracts] [^cdc-py] [^db-caps].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Raw wire replication feedback (`AsyncPostgresReplication.acknowledge`) updates PostgreSQL slot flush positions without knowing whether downstream Python state was persisted or whether a cancelled caller committed a checkpoint write before crashing [^npd-proto-research]. Chapter 24 implements `PostgresReplicationDriver`, `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore` (`MemoryPostgresCheckpointStore`), and `PostgresChangeSource` (`AsyncChangeSource`) with shielded single-flight CAS acknowledgement, uncertain-outcome reload reconciliation, deterministic replay ID recovery, and fail-closed `aclose()` under `checkpoint_timeout` [^npd-contracts] [^cdc-py] [^db-caps].

### 1.2 Non-Goals

- Implementing `_replication_identity` and `_open_replication_stream` on `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (owned by `ch26:t04` using `PostgresReplicationDriver`).
- Mutating the engine-neutral `AsyncChangeSource` protocol in `src/py/vertebra/db/driver/capabilities.py` [^db-caps].

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R22.7 & R23.2 (`ch24:t01`)**: Add `PostgresReplicationDriver(Protocol)` to `events.py`; implement `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointConflictError`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore` (checking matching `checkpoint.identity_key` vs `expected.identity_key` before CAS and monotonic validation), and `PostgresChangeSource.stream()` (identity preflight -> `store.load` -> `reconnect=False` open -> signed-int64 `ChangeEvent` projection carrying `dict(record.payload)` + `"generation"` + `"commit_end_lsn"` on `"COMMIT"` + `1024`-entry commit snapshot tracking) in `change_stream.py` and lazy `__init__.py` exports with top-level `FrozenBaseStruct` imports and strict class member ordering.
- **R22.8 & R23.3 (`ch24:t02`)**: Add `PostgresReplicationGapError` and `PostgresChangeSource.acknowledge(commit_end_lsn)` with single-flight state-lock validation, yield-gate barrier, `asyncio.shield` tracked CAS-then-native-ack execution, and `KnownClosed` reload reconciliation on any ambiguous store/native failure or cancellation.
- **R22.9 & R23.4 (`ch24:t03`)**: Implement source-owned reconnect (`max 8` attempts, `0.1s..5.0s` jitter when `config.reconnect=True`) over `reconnect=False` native handles, exact `(commit_end_lsn, next_event_id)` replay ID restoration across post-yield ack failures, and absolute-deadline `aclose()` failing closed with `PostgresReplicationGapError(reason="checkpoint_uncertain")` if reconciliation remains unresolved.

### 2.2 Concrete Type & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/events.py`:
  - `class PostgresReplicationDriver(Protocol):`
    - `@property def config(self) -> PostgreSQLConfig: ...`
    - `async def _replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity: ...`
    - `async def _open_replication_stream(self, config: PostgresChangeConfig, *, start_lsn: int | None = None, reconnect: bool = False) -> AsyncPostgresReplication: ...`
- `src/py/vertebra/db/adapters/postgres/change_stream.py`:
  - `class PostgresChangeConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True): slot: str; publication: str; channel: str = "vertebra_events"; start_lsn: int | None = None; queue_capacity: int = 1024; reconnect: bool = True; checkpoint_timeout: float = 10.0`
  - `class PostgresCheckpoint(FrozenBaseStruct, frozen=True, gc=False, kw_only=True): system_identifier: str; database: str; slot: str; publication_fingerprint: str; commit_end_lsn: int; next_event_id: int` (`@property def identity_key(self) -> tuple[str, str, str, str]`)
  - `class PostgresCheckpointStore(Protocol): async def load(self, identity_key: tuple[str, str, str, str]) -> PostgresCheckpoint | None: ...; async def save(self, checkpoint: PostgresCheckpoint, *, expected: PostgresCheckpoint | None) -> None: ...`
  - `class MemoryPostgresCheckpointStore:` (`load`, `save` guarded by `asyncio.Lock`)
  - `class PostgresCheckpointConflictError(OperationalError): ...`
  - `class PostgresReplicationGapError(OperationalError): reason: Literal["identity_mismatch", "wal_unavailable", "checkpoint_uncertain", "retries_exhausted"]`
  - `class PostgresChangeSource:` (`__init__(self, driver: PostgresReplicationDriver, config: PostgresChangeConfig, *, checkpoint_store: PostgresCheckpointStore | None = None) -> None` defaulting `self._checkpoint_store` to `MemoryPostgresCheckpointStore()` when `None`, `stream() -> AsyncIterator[ChangeEvent]`, `async def acknowledge(self, commit_end_lsn: int) -> None`, `async def aclose(self) -> None`, `__aenter__`, `__aexit__`)

### 2.3 Caller -> Seam -> Collaborator Map

- Application CDC consumer -> `PostgresChangeSource.{stream, acknowledge, aclose}` -> `PostgresReplicationDriver.{_replication_identity, _open_replication_stream}` + `PostgresCheckpointStore.{load, save}` + `AsyncPostgresReplication.{next, acknowledge, aclose}`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| Two concurrent consumers call `store.save` from same `expected` | `MemoryPostgresCheckpointStore` / CAS contract allows one winner and raises `PostgresCheckpointConflictError` to the loser without mutating stored state; loser terminates stream ownership. |
| `store.save` commits then raises `CancelledError` or `native.acknowledge` fails | Tracked task invalidates generation, closes native handle, reloads `store.load(identity_key)`, installs durable `(commit_end_lsn, next_event_id)`, and on `reconnect=True` reopens from that exact checkpoint without skipping or relabeling events. |
| Caller cancels `await source.acknowledge(lsn)` mid-save | `asyncio.shield` surfaces `CancelledError` to caller while tracked task finishes CAS + reconciliation; event loop logs zero unhandled task errors. |
| `aclose()` exceeds `checkpoint_timeout` while store reload is hung/uncertain | Cancels tracked operation, attempts bounded reload within remaining budget, transitions to permanent fail-closed state, and raises `PostgresReplicationGapError(reason="checkpoint_uncertain")`. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Checkpoint key representation | Exact 4-tuple `(system_identifier, database, slot, publication_fingerprint)` | Delimiter-concatenated string `"sys:db:slot:pub"`, which collides when database or slot identifiers contain `:`. |
| Ack failure recovery | Close generation + reload durable checkpoint + reopen with `reconnect=False` | Continuing the same native generation after `save` succeeded and `native.acknowledge` failed, risking counter/LSN divergence. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch24]` (project documentation and OKF chapters `[0/33 files documented]` consolidate across `ch30`–`ch32` with final review gates in `ch33-final-review-and-release-gate`; Google-style module/class/method docstrings in `events.py` and `change_stream.py` explicitly document `MemoryPostgresCheckpointStore` non-durability and commit-only acknowledgement semantics).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R22, R23 (R22.7, R23.2) | [t01](tasks/t01-postgres-change-source-and-store.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_change_source.py` |
| R22, R23 (R22.8, R23.3) | [t02](tasks/t02-serialized-checkpoint-ack-reconciliation.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_change_source.py` |
| R22, R23 (R22.9, R23.4) | [t03](tasks/t03-change-source-replay-and-shutdown.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_change_source.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch24:t01 — Implement PostgreSQL change sources and checkpoint stores (tasks/t01-postgres-change-source-and-store.md)
- [ ] native-postgres-driver:ch24:t02 — Serialize checkpoint acknowledgements and uncertain outcomes (tasks/t02-serialized-checkpoint-ack-reconciliation.md)
- [ ] native-postgres-driver:ch24:t03 — Preserve change-source replay identity and shutdown semantics (tasks/t03-change-source-replay-and-shutdown.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/vertebra/db/adapters/postgres/__init__.py src/py/tests/unit/db/test_postgres_change_source.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/vertebra/db/adapters/postgres/__init__.py src/py/tests/unit/db/test_postgres_change_source.py && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `ef86844f`; chapter `state: planned`, `planning_status: complete`, `plan_revision: 6`, `plan_commit: ef86844f`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch25-dsn-bridge-lexer-and-permanent-drivers` and `ch26-compiler-pipeline-arrow-and-event-hooks`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresCheckpointStore CAS protocol, PostgresReplicationGapError, and shielded ack rules.
[^npd-proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Durable-before-feedback ordering, generation barriers, and int64 ChangeEvent IDs.
[^cdc-py]: `src/py/vertebra/db/cdc.py::ChangeEvent` — Engine-neutral ChangeEvent envelope and signed-int64 Parquet schema compatibility.
[^db-caps]: `src/py/vertebra/db/driver/capabilities.py::AsyncChangeSource` — Public AsyncChangeSource capability protocol.
