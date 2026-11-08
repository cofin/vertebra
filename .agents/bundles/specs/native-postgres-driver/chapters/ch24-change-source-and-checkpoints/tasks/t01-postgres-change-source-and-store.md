---
type: Task
id: "native-postgres-driver:ch24:t01"
chapter_id: ch24-change-source-and-checkpoints
title: Implement PostgreSQL change sources and checkpoint stores
description: Define PostgresReplicationDriver, PostgresChangeConfig, PostgresCheckpoint, atomic CAS PostgresCheckpointStore, MemoryPostgresCheckpointStore, and PostgresChangeSource.stream().
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch23:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/cdc.py
  - src/py/vertebra/db/driver/capabilities.py
files:
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/db/adapters/postgres/change_stream.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
tests:
  - src/py/tests/unit/db/test_postgres_change_source.py
estimated_loc: 240
plan_revision: 8
plan_commit: 8c540809
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgresCheckpointStore CAS invariants, identity_key tuple, and ChangeEvent counter bounds
  - id: cdc-py
    resource: src/py/vertebra/db/cdc.py::ChangeEvent
    title: Immutable ChangeEvent envelope and signed-int64 Parquet schema contract
  - id: db-caps
    resource: src/py/vertebra/db/driver/capabilities.py::AsyncChangeSource
    title: Public AsyncChangeSource protocol
  - id: pg-init
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: Lazy package exports for vertebra.db.adapters.postgres
tags:
  - postgres
  - cdc
  - checkpoints
  - protocol
---

# Task t01: Implement PostgreSQL Change Sources and Checkpoint Stores

## Objective & Scope Boundary

Extend `src/py/vertebra/db/adapters/postgres/events.py` with `PostgresReplicationDriver(Protocol)`, create `src/py/vertebra/db/adapters/postgres/change_stream.py` defining `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointConflictError`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, and `PostgresChangeSource.stream()`, and register lazy exports in `src/py/vertebra/db/adapters/postgres/__init__.py` [^npd-contracts] [^cdc-py] [^db-caps] [^pg-init]. Do NOT add a placeholder `acknowledge()` method in this task: full shielded `acknowledge()` orchestration lands in `ch24:t02`, and reconnect/shutdown loops land in `ch24:t03`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — `PostgresReplicationDriver`, `PostgresCheckpointStore` CAS rules, `identity_key` 4-tuple, and `1..2**63-1` counter invariants [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/events.py` — Existing `PostgresNotificationDriver(Protocol)` from `ch21:t03`.
- `src/py/vertebra/db/adapters/postgres/types.py` — `PostgresReplicationIdentity`, `PostgresReplicationRecord`, and `PG_UNCHANGED_TOAST` from `ch22:t02`.
- `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy `_EXPORT_MAP` and `__all__` barrel [^pg-init].
- `src/py/vertebra/db/cdc.py` — `ChangeEvent` envelope and `int64` Parquet flush requirements [^cdc-py].
- `src/py/vertebra/db/driver/capabilities.py` — `AsyncChangeSource` capability interface [^db-caps].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/events.py` (`from __future__ import annotations`, `TYPE_CHECKING` imports for `PostgreSQLConfig`, `PostgresChangeConfig`, `PostgresReplicationIdentity`, and `AsyncPostgresReplication` so optional `vertebra._postgres` is never eagerly imported and runtime import cycles cannot occur):
  - `class PostgresReplicationDriver(Protocol):`
    - `@property def config(self) -> PostgreSQLConfig: ...`
    - `async def _replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity: ...`
    - `async def _open_replication_stream(self, config: PostgresChangeConfig, *, start_lsn: int | None = None, reconnect: bool = False) -> AsyncPostgresReplication: ...`
- `src/py/vertebra/db/adapters/postgres/change_stream.py`:
  - `class PostgresCheckpointConflictError(OperationalError):` — raised on CAS `expected` mismatch without mutating stored state.
  - `class PostgresChangeConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):`
    - `slot: str`
    - `publication: str`
    - `channel: str = "vertebra_events"`
    - `start_lsn: int | None = None`
    - `queue_capacity: int = 1024`
    - `reconnect: bool = True`
    - `checkpoint_timeout: float = 10.0`
    - `__post_init__` validates identifiers (`slot`, `publication`, `channel`), rejects `bool` for integers, checks `queue_capacity >= 1`, `0 <= start_lsn < 2**64` when not `None`, and `math.isfinite(checkpoint_timeout) and checkpoint_timeout > 0.0`.
  - `class PostgresCheckpoint(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):`
    - `system_identifier: str`, `database: str`, `slot: str`, `publication_fingerprint: str`, `commit_end_lsn: int`, `next_event_id: int`
    - Validates non-empty strings, non-bool `0 <= commit_end_lsn < 2**64`, and non-bool `1 <= next_event_id <= 2**63 - 1`.
    - `@property def identity_key(self) -> tuple[str, str, str, str]` returning `(self.system_identifier, self.database, self.slot, self.publication_fingerprint)`.
  - `class PostgresCheckpointStore(Protocol):`
    - `async def load(self, identity_key: tuple[str, str, str, str]) -> PostgresCheckpoint | None: ...`
    - `async def save(self, checkpoint: PostgresCheckpoint, *, expected: PostgresCheckpoint | None) -> None: ...`
  - `class MemoryPostgresCheckpointStore:` — explicitly non-durable in-memory CAS store guarded by `asyncio.Lock`: verifies `current == expected`, checks monotonic `commit_end_lsn >= expected.commit_end_lsn` and `next_event_id >= expected.next_event_id` (requiring `next_event_id == expected.next_event_id` whenever `commit_end_lsn == expected.commit_end_lsn`), and raises `PostgresCheckpointConflictError` or `ValueError` before mutating on violation.
  - `class PostgresChangeSource:` (structurally satisfies `AsyncChangeSource`):
    - `def __init__(self, driver: PostgresReplicationDriver, config: PostgresChangeConfig, *, checkpoint_store: PostgresCheckpointStore | None = None) -> None` — sets `self._checkpoint_store: PostgresCheckpointStore = checkpoint_store if checkpoint_store is not None else MemoryPostgresCheckpointStore()`.
    - `async def stream(self) -> AsyncIterator[ChangeEvent]`:
      1. Resolves `identity = await self._driver._replication_identity(self._config.slot, self._config.publication)`.
      2. Loads `loaded = await asyncio.wait_for(self._checkpoint_store.load(identity.identity_key), timeout=self._config.checkpoint_timeout)`.
      3. Opens `handle = await self._driver._open_replication_stream(self._config, start_lsn=loaded.commit_end_lsn if loaded is not None else self._config.start_lsn, reconnect=False)` (never forwarding `checkpoint_timeout` as a native kwarg).
      4. Validates initial `kind == "control"` record matches `identity.identity_key`, seeds `_next_event_id = loaded.next_event_id if loaded is not None else 1`, yields `ChangeEvent(event_id=..., channel=self._config.channel, operation=record.kind.upper(), payload=...)` for `begin`/`insert`/`update`/`delete`/`truncate`/`message`/`commit` (checking `_next_event_id <= 2**63 - 1` before yield, never routing through `ChangeFeed` or auto-acknowledging), records bounded `commit_end_lsn -> post_commit_next_event_id` snapshots (`<= 1024`), and closes `handle.aclose()` in `finally`.
  - Document `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresChangeSource` with Google-style class/method docstrings and per-field PEP 257 trailing docstrings on `FrozenBaseStruct` fields (no duplicate `Attributes:` headers on structs, zero internal leak terms) for Griffe `mkdocstrings`.
- `src/py/vertebra/db/adapters/postgres/__init__.py`:
  - Lazily export `PG_UNCHANGED_TOAST`, `PgUnchangedToast`, `PostgresReplicationIdentity`, `PostgresReplicationRecord`, `PostgresReplicationDriver`, `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointConflictError`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, and `PostgresChangeSource` (zero inline `#` comments).

## Edge Cases & Error Semantics

- `identity_key` is strictly a 4-tuple of strings, never a delimiter-joined string; checkpoints with differing `publication_fingerprint` or `system_identifier` remain isolated in `MemoryPostgresCheckpointStore`.
- `MemoryPostgresCheckpointStore.save` rejects decreasing `commit_end_lsn`, decreasing `next_event_id`, or `commit_end_lsn == expected.commit_end_lsn` with `next_event_id != expected.next_event_id` (`ValueError`), and raises `PostgresCheckpointConflictError` without mutation when `current != expected`.
- `ChangeEvent.event_id` stays strictly in `1..2**63 - 1` so `ChangeFeed.flush_to_parquet` Arrow `int64` serialization succeeds; attempting to allocate past `2**63 - 1` raises `OverflowError` / `OperationalError` before yielding.
- Never use `getattr`, `Any` casts, or fake legacy driver hooks in tests: define a typed `NativeReplicationDriverPort` implementing `PostgresReplicationDriver` over real `AsyncPostgresClient.replication_identity` and `AsyncPostgresClient.open_replication`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_change_source.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Identity-before-load & `ChangeEvent` envelope stream | Seed `MemoryPostgresCheckpointStore` with `PostgresCheckpoint(..., commit_end_lsn=0x1_0000_0200, next_event_id=41)`; stream `Begin`, `Update` (`PG_UNCHANGED_TOAST`, `NULL`, `key`, `new`), standalone `Message`, `Commit(0x1_0000_0300)` via `NativeReplicationDriverPort` | `_replication_identity` executes before `store.load`; `_open_replication_stream` receives `start_lsn=0x1_0000_0200, reconnect=False`; yields `4` `ChangeEvent`s with `event_id = 41, 42, 43, 44`, `PG_UNCHANGED_TOAST` intact, and zero automatic wire `StandbyStatusUpdate` advancement; `handle.closed is True` on exit. |
| Default `MemoryPostgresCheckpointStore` & concurrent CAS writers | Construct `PostgresChangeSource(port, cfg)` with `checkpoint_store=None`; separately run two coroutines concurrently calling `save` from the same `expected = cp0` | `checkpoint_store=None` initializes an isolated `MemoryPostgresCheckpointStore()`; concurrent `save` allows one winner and raises `PostgresCheckpointConflictError` to the loser without mutating stored state. |
| Equal-LSN & regression validation | `save(cp(lsn=200, next_id=12), expected=cp(lsn=200, next_id=10))` or `save(cp(lsn=150, next_id=12), expected=cp(lsn=200, next_id=10))` | Both raise `ValueError` without mutating stored checkpoint; `save(cp(lsn=200, next_id=10), expected=cp(lsn=200, next_id=10))` succeeds. |
| 4-tuple `identity_key` collision resistance & signed64 Parquet compatibility | Keys `("sys:1", "db", "slot", "pub")` vs `("sys", "1:db", "slot", "pub")`; flush emitted `ChangeEvent` with `event_id = 2**62` through Arrow/Parquet | Store keeps both keys distinct; Arrow/Parquet `int64` serialization round-trips `event_id` losslessly. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_change_source.py -k 'change_source or checkpoint_' -q`
- Final Green + Docstring + Typecheck gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_change_source.py -k 'change_source or checkpoint_' -q
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_spanner_queue_and_cdc.py -q
  PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres/change_stream.py src/py/vertebra/db/adapters/postgres/events.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/change_stream.py src/py/tests/unit/db/test_postgres_change_source.py
  ```

## Acceptance Criteria

- [ ] `events.py`, `change_stream.py`, and `__init__.py` define and export `PostgresReplicationDriver`, `PostgresChangeConfig`, `PostgresCheckpoint`, `PostgresCheckpointConflictError`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, and `PostgresChangeSource` (defaulting `checkpoint_store=None` to `MemoryPostgresCheckpointStore()`), passing `tools.docs.scan_public_docstrings` and `mypy` with zero inline `#` comments (R22, R23).
- [ ] `PostgresChangeSource.stream()` resolves identity before loading checkpoints, opens native streams with `reconnect=False`, preserves `PG_UNCHANGED_TOAST` and signed-int64 `event_id`s, and never auto-acknowledges on yield (R22, R23).
- [ ] Commit message follows `feat(postgres): define atomic checkpoint stores and change envelopes` once verified.

## Notes & Discoveries

- `NativeReplicationDriverPort` proves `PostgresReplicationDriver` structural typing against real `AsyncPostgresClient` handles before `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` acquire production hooks in `ch26:t04` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresCheckpointStore CAS invariants, identity_key tuple, and ChangeEvent counter bounds.
[^cdc-py]: `src/py/vertebra/db/cdc.py::ChangeEvent` — Immutable ChangeEvent envelope and signed-int64 Parquet schema contract.
[^db-caps]: `src/py/vertebra/db/driver/capabilities.py::AsyncChangeSource` — Public AsyncChangeSource protocol.
[^pg-init]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package exports for vertebra.db.adapters.postgres.
