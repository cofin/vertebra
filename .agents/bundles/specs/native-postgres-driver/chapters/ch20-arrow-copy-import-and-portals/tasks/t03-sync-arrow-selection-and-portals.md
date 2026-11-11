---
type: Task
id: "native-postgres-driver:ch20:t03"
chapter_id: ch20-arrow-copy-import-and-portals
title: Expose synchronous select_to_arrow and pinned Arrow portal batch streams
description: Implement cold-discovery execute_select_to_arrow and single-scope generation-fenced ArrowPortalStream in portal.rs, and expose SyncPostgresClient.select_to_arrow and stream_arrow_batches with parameter_oids and timeout support.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch20:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch20-arrow-copy-import-and-portals/spec.md
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 240
plan_revision: 6
plan_commit: bf53faf3
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T18:46:12Z"
last_verified_commit: 384fc2ed
verification_evidence: "make dev, ruff, mypy, mypy.stubtest vertebra._postgres, pytest test_postgres_native_data.py (92 passed), cargo test postgres_arrow_tests (17 passed), and cargo clippy exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T18:46:12Z"
commit: 384fc2ed
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: bf53faf3
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgresArrowResult, select_to_arrow bounds, parameter_oids/timeout, and pinned portal transaction contracts
  - id: portal-rs
    resource: src/rs/vertebra-postgres/src/portal.rs::Portal
    title: Portal single-scope BEGIN/SAVEPOINT ownership, Format::Text rows, and generation-fenced portal execution
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Non-executing Parse/Describe/Sync cold OID discovery and PortalSuspended incremental streaming
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor
    title: Cross-extension vertebra._vertebra.ArrowCursor ShmCapsule construction
tags:
  - postgres
  - rust
  - pyo3
  - arrow
  - portals
---

# Task t03: Expose Synchronous select_to_arrow and Pinned Arrow Portal Batch Streams

## Objective & Scope Boundary

Implement `execute_select_to_arrow` and `ArrowPortalStream` in `src/rs/vertebra-postgres/src/portal.rs` and expose `SyncPostgresClient.select_to_arrow` and `SyncPostgresClient.stream_arrow_batches` (`SyncPostgresArrowStream`) in `src/rs/vertebra-postgres/src/sync_client.rs` and `src/py/vertebra/_postgres.pyi` [^pg-contracts] [^portal-rs] [^pg-protocol-research] [^py-async-rs]. Asynchronous counterparts and `asyncio` cancellation guards follow in `ch20:t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 cold custom OID discovery, `portal.rs` ownership, single-scope portal delegation, and synchronous Arrow contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — Existing `PostgresArrowSchema`, `PostgresArrowBatchBuilder`, `PostgresArrowIpcWriter`, `ArrowStorageMode`, `arrow_output_into_py_result`, and `shm_output_into_py_cursor` (`ch19:t01`–`t04`).
- `src/rs/vertebra-postgres/src/portal.rs` — `Portal`, `Portal::{columns, start_fetch, next_event, close}`, `PortalEvent::{Row, Suspended, Complete}`, and `acquire_portal_operation_with_generation` (`ch13:t02`, `ch16:t03`).
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle::run_cancellable_operation`, `SessionHandle::open_portal[_as]_with_generation`, `CommandBudget`, `PostgresConnection::enter_session_command_deadline`, `crate::metadata::resolve_parameter_metadata`, and `TypeCatalog::discover` (`ch13:t01`, `ch17:t01`).
- `src/rs/vertebra-postgres/src/sync_client.rs` — `SyncPostgresClient`, GIL detachment (`py.detach`), ambient `TransactionOwner`, and `Parameter::with_oid` / `timeout` parsing (`ch14:t04`, `ch17:t03`).
- [contracts.md](../../../contracts.md) — `select_to_arrow` and portal transaction/savepoint ownership rules [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/portal.rs`:
  - `Portal::type_catalog(&self) -> &TypeCatalog` exposing the portal's discovered `TypeCatalog` snapshot.
  - `pub(crate) async fn execute_select_to_arrow(session: &Arc<SessionHandle>, op_gen: OperationGeneration, sql: &str, parameters: &[crate::extended::Parameter], param_oids: Option<&[u32]>, owner: Option<TransactionOwner>, budget: CommandBudget, threshold_rows: usize, max_result_bytes: usize) -> Result<ArrowOutput, PostgresError>`:
    - Executes inside `session.run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, ...)` under `conn.enter_session_command_deadline(budget.deadline())` (using `SessionHandle::cleanup_deadline_expired_command` on timeout) and resolves `param_oids` via `crate::metadata::resolve_parameter_metadata` (`Parameter::with_oid`).
    - When `sql` has uncached result columns or any result OID missing from `conn.type_catalog`, executes a non-executing `Parse("", sql, param_oids) + Describe(b'S', "") + Sync` preflight followed by `conn.type_catalog.discover(...)` BEFORE `Bind + Execute` so volatile or `INSERT ... RETURNING` SQL NEVER runs twice and cold custom OIDs (`vector`, `halfvec`, arrays, composites, domains, enums, ranges) build exact lossless Arrow schemas, then streams `DataRow` frames directly into `PostgresArrowBatchBuilder` and `PostgresArrowIpcWriter` (`ArrowStorageMode::Adaptive { threshold_rows }`, `max_result_bytes`) without materializing `QueryResult.rows`.
  - `pub struct ArrowPortalStream`:
    - Delegates `BEGIN` / `SAVEPOINT` / `COMMIT` / `ROLLBACK` strictly to `SessionHandle::open_portal[_as]_with_generation` and `Portal`'s single `TransactionScope` (NEVER issuing a duplicate outer `BEGIN`) [^portal-rs].
    - Builds its schema via `PostgresArrowSchema::from_columns(portal.columns(), portal.type_catalog())`, cleanly supporting `Portal`'s `Format::Text` rows including `"regproc-text"` and `TemporalText` (`kind = 2`); when `columns: Option<Vec<String>>` is provided, validates `columns.len() == portal.columns().len()` (`PostgresError::Data` on mismatch while preserving duplicates) and renames schema fields.
    - `pub async fn next_batch_shm(&mut self) -> Result<Option<ArrowOutput>, PostgresError>` (plus generation-fenced `next_batch_shm_with_generation(op_gen)`): drives `start_fetch` / `next_event` under a fenced `OperationGeneration`, appending each `PortalEvent::Row(Vec<PgValue>)` directly to `PostgresArrowBatchBuilder` (never calling aggregate `Portal::fetch()`), flushing the current `AlwaysShm` `ArrowOutput::Shm` and retaining `1` decoded pending row + active `Execute` continuation when the `8 MiB` batch byte cap is reached before `PortalSuspended`, and issuing a new `Execute` ONLY after `PortalEvent::Suspended`.
    - `pub async fn close(&mut self) -> Result<(), PostgresError>` (and `Drop`): closes under a fenced `OperationGeneration`, issuing `CancelRequest` + `connect_timeout` bounded drain/socket retirement when closed or cancelled while paused mid-`Execute` at the `8 MiB` byte cap so live portals never leak back into pools.
- `src/rs/vertebra-postgres/src/sync_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - `SyncPostgresClient.select_to_arrow(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, parameter_oids: Sequence[int] | None = None, timeout: float | None = None, threshold_rows: int = 256, max_result_bytes: int = 268435456) -> PostgresArrowResult`:
    - Validates non-`bool` `threshold_rows >= 0`, `max_result_bytes > 0`, `parameter_oids` length/bounds, and `timeout` (`CommandBudget`) before any SQL or network I/O; detaches the GIL during `execute_select_to_arrow`, then converts `ArrowOutput` on the caller thread via `arrow::arrow_output_into_py_result`.
  - `SyncPostgresClient.stream_arrow_batches(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, parameter_oids: Sequence[int] | None = None, batch_size: int = 65536, columns: Sequence[str] | None = None) -> SyncPostgresArrowStream`:
    - Validates non-`bool` `batch_size > 0` and `parameter_oids` before SQL; returns `SyncPostgresArrowStream` (`__iter__`, `__next__(self) -> vertebra._vertebra.ArrowCursor`, `close(self) -> None`, `__enter__`, `__exit__`) detaching the GIL around `next_batch_shm` and `close` and constructing per-batch `vertebra._vertebra.ArrowCursor` via `arrow::shm_output_into_py_cursor`.

## Edge Cases & Error Semantics

- Invalid options (`threshold_rows < 0`, `batch_size <= 0`, `max_result_bytes <= 0`, `bool` ints, non-finite/negative `timeout`, or mismatched `parameter_oids`) raise `ValueError` / `DataError` synchronously with zero wire frames sent.
- Cold custom OIDs (`vector`, `enum`, `domain`, `range`, `array`, `composite`) and volatile `INSERT ... RETURNING` queries execute the statement body strictly once because cold discovery uses non-executing `Parse + Describe(b'S') + Sync` before `Bind + Execute`.
- `columns` width mismatch (differing `len(columns)` vs `len(result_columns)`), single row `> 8 MiB` in `stream_arrow_batches`, command `timeout` expiry, or mid-`Execute` `close()` rolls back only the stream's single `Portal` transaction/savepoint scope (issuing `CancelRequest` + bounded retirement when paused mid-`Execute`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_data.py` (`test_sync_arrow_*` suite)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `select_to_arrow` cold custom OIDs, volatile `INSERT ... RETURNING`, `parameter_oids`, `timeout`, and inline/SHM thresholds | Cold `enum`/`vector`/`composite` OIDs + `INSERT ... RETURNING` (`0`, `3`, and `4` rows at `threshold_rows=3`), explicit `parameter_oids`, `timeout=0.02` against stalled peer, and invalid options | Cold OIDs resolve via `Parse+Describe+Sync` with exact `postgres.arrow.version = "1"` metadata while `INSERT ... RETURNING` executes strictly once; `<= 3` inline, `4` SHM; stalled peer times out and cleans session; invalid options send `0` frames. |
| Incremental `PortalSuspended` barrier, `Format::Text` portal fidelity, & `max_size = 1` pool pinning | Peer opens portal (`BEGIN` issued once, never twice), yields `"regproc-text"` + `TemporalText` (`kind = 2`) rows in batch 1 + `PortalSuspended`, and blocks batch 2 behind a barrier | `next(stream)` yields batch-1 `ArrowCursor` preserving `"regproc-text"` and `kind = 2` `wire_text` before batch 2 releases; `pool.acquire(timeout=0.05)` blocks while open and succeeds after exhaustion or `stream.close()`. |
| Mid-`Execute` `8 MiB` byte-cap pause, resumption, mid-pause `close()`, & `columns` rename | Peer sends `3` rows of `3.5 MiB` inside one `Execute(batch_size=10)` with `columns=["x", "x"]` (plus early `stream.close()` right after batch 1 while paused mid-`Execute`, and mismatch `["only_one"]`) | Yields `2`-row SHM cursor without a second `Execute`, then either resumes `next_event` for row `3` with `("x", "x")` or fences `CancelRequest` + bounded drain/retirement on mid-pause `close()`; mismatch raises `DataError`. |
| Single-scope owned transaction vs ambient transaction savepoint on early `close()` | (a) Idle `stream_arrow_batches` closed after 1st batch; (b) `with client.transaction():` `INSERT` + early-closed `stream_arrow_batches` + `COMMIT` | (a) Peer observes exactly one `BEGIN` and one `ROLLBACK`; (b) peer observes `SAVEPOINT` -> `ROLLBACK TO SAVEPOINT` -> `RELEASE SAVEPOINT` while caller's `INSERT` commits cleanly. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k sync_arrow -q`
- Final Green + lint + type-check + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_data.py
  uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_data.py
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k sync_arrow -q
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
  ```
- Planned local commit: `feat(postgres): expose synchronous Arrow portal streams`

## Acceptance Criteria

- [x] `execute_select_to_arrow` in `portal.rs` and `SyncPostgresClient.select_to_arrow` support `parameter_oids` and `timeout` (`CommandBudget`), discover cold custom OIDs via non-executing `Parse + Describe + Sync` without double-executing volatile SQL, and build inline or `VTBARROW` SHM `PostgresArrowResult` without Python row conversion (R20).
- [x] `ArrowPortalStream` and `SyncPostgresClient.stream_arrow_batches(..., *, parameter_oids=None, batch_size=65536, columns=None)` build schemas from `Portal::type_catalog()` (`Format::Text` rows including `"regproc-text"` and `TemporalText` `kind = 2`), delegate strictly to `Portal`'s single transaction/savepoint scope without duplicate `BEGIN`, and pause mid-`Execute` at the `8 MiB` byte cap (R16, R20).
- [x] Exhaustion, mid-`Execute` `close()`, command timeouts, errors, and `Drop` run under fenced `OperationGeneration` (`CancelRequest` + `connect_timeout` bounded drain/retirement when mid-`Execute`), pass `ruff`/`mypy`/`stubtest`, and return clean sessions to `max_size = 1` pools while detaching the GIL (R16, R20).

## Notes & Discoveries

PyO3 `#[pyo3(signature = (...))]` declarations on `select_to_arrow` and `stream_arrow_batches` use literal integer defaults (`256`, `268435456`, `65536`) backed by compile-time `const` assertions so `__text_signature__` satisfies `mypy.stubtest` without emitting `Ellipsis`.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresArrowResult, select_to_arrow bounds, `parameter_oids`/`timeout`, and pinned portal transaction contracts.
[^portal-rs]: `src/rs/vertebra-postgres/src/portal.rs::Portal` — Portal single-scope `BEGIN`/`SAVEPOINT` ownership, `Format::Text` rows, and generation-fenced portal execution.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Non-executing `Parse`/`Describe`/`Sync` cold OID discovery and `PortalSuspended` incremental streaming.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor` — Cross-extension `vertebra._vertebra.ArrowCursor` ShmCapsule construction.
