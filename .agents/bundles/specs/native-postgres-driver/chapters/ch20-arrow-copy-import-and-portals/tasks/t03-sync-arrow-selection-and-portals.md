---
type: Task
id: "native-postgres-driver:ch20:t03"
chapter_id: ch20-arrow-copy-import-and-portals
title: Expose synchronous select_to_arrow and pinned Arrow portal batch streams
description: Add SyncPostgresClient.select_to_arrow and SyncPostgresClient.stream_arrow_batches returning SyncPostgresArrowStream over incremental Portal::start_fetch and Portal::next_event with AlwaysShm VTBARROW cursors.
status: stable
state: pending
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
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 230
plan_revision: 4
plan_commit: 45ff9563
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
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgresArrowResult, select_to_arrow bounds, and pinned portal transaction contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL named/unnamed Portal Execute and PortalSuspended incremental streaming
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

Implement `ArrowPortalStream` + native Arrow selection execution in `src/rs/vertebra-postgres/src/arrow.rs` and expose `SyncPostgresClient.select_to_arrow` and `SyncPostgresClient.stream_arrow_batches` (`SyncPostgresArrowStream`) in `src/rs/vertebra-postgres/src/sync_client.rs` and `src/py/vertebra/_postgres.pyi` [^pg-contracts] [^pg-protocol-research] [^py-async-rs]. Asynchronous counterparts and `asyncio` cancellation guards follow in `ch20:t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 synchronous Arrow selection and portal batch streaming contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — `PostgresArrowBatchBuilder`, `PostgresArrowIpcWriter`, `ArrowStorageMode`, and `arrow_output_into_py_result` (`ch19:t01`–`t04`).
- `src/rs/vertebra-postgres/src/portal.rs` — `Portal`, `Portal::start_fetch(max_rows)`, and `Portal::next_event() -> PortalEvent::{Row(Vec<PgValue>), Suspended, Complete { command_tag, affected_rows }}` (`ch13:t02`), plus command deadlines (`ch17:t01`).
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`'s `pub(crate)` operation reservation (`ch13:t01`) borrowing `&mut PostgresConnection` and `PostgresConnection::query_with_row_sink` (`ch12:t03`).
- `src/rs/vertebra-postgres/src/sync_client.rs` — `SyncPostgresClient`, GIL detachment (`py.detach`), and portal/transaction lease pinning (`ch14:t04`, `ch17:t03`).
- [contracts.md](../../../contracts.md) — `select_to_arrow` and portal transaction/savepoint ownership rules [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub async fn execute_select_to_arrow(session: &Arc<SessionHandle>, sql: &str, parameters: &[crate::extended::Parameter], param_oids: Option<&[u32]>, threshold_rows: usize, max_result_bytes: usize) -> Result<ArrowOutput, PostgresError>`:
    - Borrows `&mut PostgresConnection` via `SessionHandle`'s `pub(crate)` operation reservation (`ch13:t01`) and drives `PostgresConnection::query_with_row_sink` (`ch12:t03`) so extended-query `RowDescription` (`&[crate::result::Column]`) + `DataRow` frames stream directly into `PostgresArrowBatchBuilder` and `PostgresArrowIpcWriter` (`ArrowStorageMode::Adaptive { threshold_rows }`, `max_result_bytes`) without materializing `QueryResult.rows` or `PostgresQueryResult.rows` and without editing `session.rs` or `extended.rs`.
  - `pub struct ArrowPortalStream`:
    - Pins a `SessionHandle` / `PooledConnection` lease and opens a `ch13:t01` stream-owned transaction (when idle `I`) or stream-owned savepoint (when inside an ambient caller transaction `T`), then binds a `ch13:t02` `crate::portal::Portal` (`src/rs/vertebra-postgres/src/portal.rs`).
    - `pub async fn next_batch_shm(&mut self) -> Result<Option<ArrowOutput>, PostgresError>`:
      - Drives `Portal::start_fetch(self.batch_size)` and `Portal::next_event()` (`crate::portal::PortalEvent`); appends each `PortalEvent::Row(Vec<PgValue>)` directly to `PostgresArrowBatchBuilder` (never calling aggregate `Portal::fetch() -> QueryResult`).
      - When appending a row would breach the `8 MiB` batch byte cap, flushes the current non-empty `RecordBatch` into a single-batch `AlwaysShm` `ArrowOutput::Shm`, retains that `1` decoded pending `Vec<PgValue>` row plus the in-progress `Execute` continuation, and resumes `Portal::next_event()` on the following `next_batch_shm()` call — issuing a new `Portal::start_fetch` ONLY after observing `PortalEvent::Suspended`.
      - When `columns: Option<Vec<String>>` is provided, validates exact width match (`columns.len() == row_description.len()`, raising `PostgresError::Data` on mismatch while preserving duplicates) and renames schema fields accordingly.
      - On `PortalEvent::Complete`, flushes any final batch (or yields `None` after all rows are emitted), closes the portal, and commits ONLY a stream-owned transaction (or releases the stream-owned savepoint without committing ambient caller work).
    - `pub async fn close(&mut self) -> Result<(), PostgresError>` (closes active portal, rolls back stream-owned transaction or rolls back + releases stream savepoint inside ambient `T`, or retires socket if mid-`Execute` drain fails; `Drop` triggers bounded background cleanup without blocking the GIL).
- `src/rs/vertebra-postgres/src/sync_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - `SyncPostgresClient.select_to_arrow(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, threshold_rows: int = 256, max_result_bytes: int = 268435456) -> PostgresArrowResult`:
    - Validates non-`bool` `threshold_rows >= 0` and `max_result_bytes > 0` before any SQL or network I/O; detaches GIL during `execute_select_to_arrow`, then converts `ArrowOutput` on the caller thread via `arrow_output_into_py_result`.
  - `SyncPostgresClient.stream_arrow_batches(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, batch_size: int = 65536, columns: Sequence[str] | None = None) -> SyncPostgresArrowStream`:
    - Validates non-`bool` `batch_size > 0` before SQL; returns `SyncPostgresArrowStream` (`__iter__`, `__next__(self) -> vertebra._vertebra.ArrowCursor`, `close(self) -> None`, `__enter__`, `__exit__`) detaching the GIL around `next_batch_shm` and `close` and constructing per-batch `vertebra._vertebra.ArrowCursor` via `shm_output_into_py_cursor`.

## Edge Cases & Error Semantics

- Invalid options (`threshold_rows < 0`, `batch_size <= 0`, `max_result_bytes <= 0`, or `bool` ints) raise `ValueError` / `DataError` synchronously with zero wire frames sent.
- `columns` width mismatch (`len(columns) != len(result_columns)`) rolls back the stream transaction/savepoint and raises `DataError`.
- Single row exceeding `8 MiB` in `stream_arrow_batches` or total IPC exceeding `max_result_bytes` in `select_to_arrow` aborts/rolls back cleanly and releases or replaces the pooled connection.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_data.py` (`test_sync_arrow_*` suite)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `select_to_arrow` empty, inline (`<= threshold`), and SHM (`> threshold`) + option guards | `0` rows, `3` rows (`threshold_rows=3`), `4` rows (`threshold_rows=3`), and `threshold_rows=-1` / `max_result_bytes=0` | `0` & `3` rows return `PostgresArrowResult(used_shm=False, buffer=bytes)`; `4` rows returns `used_shm=True` with `vertebra._vertebra.ArrowCursor`; negative/zero options fail with `0` peer connections/SQL. |
| Incremental `PortalSuspended` barrier & `max_size = 1` pool pinning | Peer sends batch 1 (`2` rows + `PortalSuspended`) and blocks batch 2 behind a barrier on a `max_size = 1` pool | `next(stream)` yields batch-1 `ArrowCursor` before batch 2 is released; concurrent `pool.acquire(timeout=0.05)` times out while stream is open and succeeds immediately after exhaustion or `stream.close()`. |
| Mid-`Execute` `8 MiB` byte-cap pause & exact-width `columns` rename | Peer sends `3` rows of `3.5 MiB` inside one `Execute(batch_size=10)` with `columns=["x", "x"]` (and width mismatch `["only_one"]`) | Yields `2`-row SHM cursor first without issuing a second `Execute`, then resumes `next_event` to yield the `3`rd row with duplicate `("x", "x")` schema; width mismatch raises `DataError`. |
| Owned transaction vs ambient transaction savepoint on early `close()` | (a) Idle `stream_arrow_batches` closed after 1st batch; (b) `with client.transaction():` `INSERT` + early-closed `stream_arrow_batches` + `COMMIT` | (a) Stream rolls back its own transaction with zero further `Execute`; (b) stream rolls back/releases only its savepoint while caller's `INSERT` commits cleanly. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k sync_arrow -q`
- Final Green + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k sync_arrow -q
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
  ```
- Planned local commit: `feat(postgres): expose synchronous Arrow portal streams`

## Acceptance Criteria

- [ ] `SyncPostgresClient.select_to_arrow` builds inline or `VTBARROW` SHM `PostgresArrowResult` directly in Rust under validated `threshold_rows` and `max_result_bytes` without Python row conversion (R20).
- [ ] `SyncPostgresClient.stream_arrow_batches` yields per-batch `vertebra._vertebra.ArrowCursor` handles over `Portal::start_fetch` / `Portal::next_event`, pausing mid-`Execute` at the `8 MiB` byte cap and validating exact-width `columns` renames (R16, R20).
- [ ] Exhaustion, early `close()`, errors, and `Drop` preserve stream-owned transaction vs ambient savepoint boundaries and return clean sessions to `max_size = 1` pools while detaching the GIL (R16, R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresArrowResult, select_to_arrow bounds, and pinned portal transaction contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL named/unnamed Portal Execute and PortalSuspended incremental streaming.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor` — Cross-extension `vertebra._vertebra.ArrowCursor` ShmCapsule construction.
