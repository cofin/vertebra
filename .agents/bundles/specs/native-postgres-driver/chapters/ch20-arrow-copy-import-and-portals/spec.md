---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch20-arrow-copy-import-and-portals
title: "Chapter 20: Arrow-to-COPY Bulk Import, Streaming Input Bridges, and Sync/Async Arrow Portal Streams"
description: Convert Arrow RecordBatches into binary COPY frames, expose PyO3 COPY and Arrow input bridges without Python row conversion, and deliver synchronous and cancellation-safe asynchronous Arrow selection and portal batch streams.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch19-native-arrow-batches-and-shm
replan_trigger: null
current_task: "native-postgres-driver:ch20:t01"
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL Arrow selection, COPY, portal, and cancellation contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL PortalSuspended batching, CopyFail rollback, and CancelRequest drain rules
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Per-batch VTBARROW shared-memory writer and unregistered region handoff
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Event-loop AsyncCompletionQueue drainer and cancellation-safe PyCapsule publication
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor buffer_view and zero-copy IPC stream consumption
tags:
  - postgres
  - rust
  - python
  - arrow
  - copy
  - portals
---

# Chapter 20: Arrow-to-COPY Bulk Import, Streaming Input Bridges, and Sync/Async Arrow Portal Streams

<!-- truth: start -->
- **Typed Arrow-to-`COPY` Plan (`PostgresArrowImportPlan`)**: Validates `postgres.arrow.version = "1"` and standard Arrow schemas against bound `pg_catalog` target columns before `COPY` startup, encodes batches directly via `BinaryCopyEncoder` (`<= 16 MiB` IPC call cap, `<= 8 MiB` batch cap), rejects `wire_text` (`kind = 2`) and lossy timestamp/numeric conversions, and aborts mid-stream failures via `CopyFail` [^pg-contracts] [^pg-protocol-research].
- **Zero-Row-Materialization Input Bridge**: `vertebra.db.adapters.postgres.arrow` streams bounded self-contained Arrow IPC batches from `Table`, `RecordBatch`, `ArrowCursor`, and `__arrow_c_stream__` into `copy_from_arrow` / `write_arrow_ipc` while keeping provider C-stream callbacks on the caller thread [^pg-contracts] [^arrow-py].
- **Pinned Sync & Cancellation-Safe Async Arrow Portals**: `select_to_arrow` and `stream_arrow_batches` build adaptive inline or per-batch `VTBARROW` SHM cursors directly from `DataRow` / `PortalEvent` streams without `to_pylist()` or aggregate `Portal::fetch()`, guarding async cancellations so live portals never leak back into pools [^arrow-shm-rs] [^py-async-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapters 18 and 19 establish the pure-Rust binary `COPY` engine (`CopyOut`, `CopyFromStream`) and lossless Arrow `RecordBatch` / `VTBARROW` IPC writers (`PostgresArrowSchema`, `PostgresArrowBatchBuilder`, `PostgresArrowIpcWriter`). Chapter 20 completes the bidirectional Arrow and `COPY` surface across Rust and Python: `PostgresArrowImportPlan` + bounded Python IPC batch chunking (`ch20:t01`), PyO3 `SyncPostgresCopyOut` / `AsyncPostgresCopyOut` / `SyncPostgresCopyIn` / `AsyncPostgresCopyIn` + `copy_to` / `copy_from` / `copy_from_arrow` client methods (`ch20:t02`), synchronous `select_to_arrow` and `SyncPostgresArrowStream` over `PortalEvent` (`ch20:t03`), and cancellation-safe asynchronous `select_to_arrow` and `AsyncPostgresArrowStream` (`ch20:t04`) [^pg-contracts] [^pg-protocol-research] [^py-async-rs].

### 1.2 Non-Goals

- High-level `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` `select_to_arrow`, `stream_arrow`, and `load_arrow` facade cutover (owned by `ch26:t03`).
- `LISTEN`/`NOTIFY` and logical replication `CopyBoth` streams (owned by Chapters 21–24).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R20.5 (`ch20:t01`)**: Implement `PostgresArrowImportPlan` in `src/rs/vertebra-postgres/src/arrow.rs`, `SessionHandle::copy_from_arrow` and `CopyFromStream::write_arrow_ipc` in `src/rs/vertebra-postgres/src/copy.rs`, and bounded IPC batch iterators (`iter_arrow_ipc_batches`, `aiter_arrow_ipc_batches`, `load_arrow_sync`, `load_arrow_async`) in `src/py/vertebra/db/adapters/postgres/arrow.py` without calling `to_pylist()` or `to_pydict()`.
- **R19.4 & R20.6 (`ch20:t02`)**: Expose `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, and `AsyncPostgresCopyIn` plus `copy_to`, `copy_from`, and `copy_from_arrow` on `SyncPostgresClient` and `AsyncPostgresClient` in `sync_client.rs`, `async_client.rs`, and `src/py/vertebra/_postgres.pyi`, enforcing single in-flight writes, `16 MiB` per-call IPC caps, and `CopyFail` / `CancelRequest` cleanup.
- **R16.3 & R20.7 (`ch20:t03`)**: Expose `SyncPostgresClient.select_to_arrow` (`threshold_rows = 256`, `max_result_bytes = 268_435_456`) and `SyncPostgresClient.stream_arrow_batches` (`batch_size = 65_536`, `columns = None`) returning `SyncPostgresArrowStream` backed by `Portal::start_fetch` / `Portal::next_event` (`AlwaysShm`), pausing mid-`Execute` on the `8 MiB` byte cap while preserving owned transaction or ambient savepoint boundaries.
- **R16.4 & R20.8 (`ch20:t04`)**: Expose `AsyncPostgresClient.select_to_arrow` and synchronous-factory `AsyncPostgresClient.stream_arrow_batches` returning `AsyncPostgresArrowStream` (`Unopened` / `Fetching` / `Open` / `Closing` / `Closed`) over `_vertebra_postgres_completion_drainer`, with pending-completion cleanup guards that drop undelivered `ShmRegion` mappings and close/cancel portals on `asyncio` cancellation while keeping delivered batch mappings alive.

### 2.2 Concrete Type & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs` & `copy.rs`:
  - `pub const MAX_ARROW_COPY_CALL_BYTES: usize = 16 * 1024 * 1024;`
  - `pub struct PostgresArrowImportPlan` with `pub fn from_schema(schema: &Schema, target_columns: &[crate::result::Column], catalog: &TypeCatalog) -> Result<Self, PostgresError>` (`PostgresColumn` is Python-only in `types.py`) and `pub fn encode_batch(&self, batch: &RecordBatch, encoder: &mut BinaryCopyEncoder, ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>`.
  - `SessionHandle::copy_from_arrow(self: &Arc<Self>, table_name: &str, first_batch_ipc: &[u8], columns: Option<&[String]>) -> Result<CopyFromStream, PostgresError>`, `CopyFromStream::write_arrow_ipc(&mut self, ipc_bytes: &[u8]) -> Result<(), PostgresError>`, and `execute_select_to_arrow(session: &Arc<SessionHandle>, sql: &str, parameters: &[crate::extended::Parameter], param_oids: Option<&[u32]>, threshold_rows: usize, max_result_bytes: usize) -> Result<ArrowOutput, PostgresError>` (both `copy_from_arrow` and `execute_select_to_arrow` borrow `&mut PostgresConnection` via `SessionHandle`'s `pub(crate)` operation reservation from `ch13:t01` and drive `PostgresConnection::query_with_row_sink` from `ch12:t03` so `RowDescription` + `DataRow` frames stream directly into `PostgresArrowBatchBuilder` / `PostgresArrowIpcWriter` without materializing `QueryResult.rows` and without editing `session.rs` or `extended.rs`).
  - `pub struct ArrowPortalStream` (shared Rust owner driving `crate::portal::Portal::{start_fetch, next_event}` over `PortalEvent` in `src/rs/vertebra-postgres/src/portal.rs` into `PostgresArrowBatchBuilder` + `PostgresArrowIpcWriter(AlwaysShm)` with `1`-row pending byte-cap handoff and `crate::session::cancel` cleanup).
- `src/py/vertebra/db/adapters/postgres/arrow.py`:
  - `def iter_arrow_ipc_batches(source: Any, *, max_rows: int = 65_536, max_bytes: int = 8 * 1024 * 1024) -> Iterator[bytes]`
  - `async def aiter_arrow_ipc_batches(source: Any, *, max_rows: int = 65_536, max_bytes: int = 8 * 1024 * 1024) -> AsyncIterator[bytes]`
  - `def load_arrow_sync(client: SyncPostgresClient, table_name: str, source: Any, *, columns: Sequence[str] | None = None) -> int`
  - `async def load_arrow_async(client: AsyncPostgresClient, table_name: str, source: Any, *, columns: Sequence[str] | None = None) -> int`
- `src/py/vertebra/_postgres.pyi`:
  - `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, `AsyncPostgresCopyIn`, `SyncPostgresArrowStream`, `AsyncPostgresArrowStream`
  - `SyncPostgresClient.{copy_to, copy_from, copy_from_arrow, select_to_arrow, stream_arrow_batches}`
  - `AsyncPostgresClient.{copy_to, copy_from, copy_from_arrow, select_to_arrow, stream_arrow_batches}`

### 2.3 Caller -> Seam -> Collaborator Map

```
Arrow Bulk Import:
  Python Table / RecordBatch / ArrowCursor / __arrow_c_stream__
    -> adapters/postgres/arrow.py::{iter_arrow_ipc_batches, load_arrow_sync, load_arrow_async}
      -> Sync/AsyncPostgresClient.copy_from_arrow + CopyFromStream.write_arrow_ipc
        -> SessionHandle pub(crate) reservation (ch13:t01) + PostgresConnection::query_with_row_sink (ch12:t03)
        -> PostgresArrowImportPlan::encode_batch -> BinaryCopyEncoder + codecs::encode_value_with_catalog -> CopyFromStream duplex 64 KiB pump

Arrow Query & Portal Streaming:
  Sync/AsyncPostgresClient.{select_to_arrow, stream_arrow_batches}
    -> SessionHandle pub(crate) reservation + PostgresConnection::query_with_row_sink (ch12:t03) / portal.rs::Portal::{start_fetch, next_event} (ch13:t02)
      -> PostgresArrowBatchBuilder + PostgresArrowIpcWriter (ch19:t02..t04) + crate::session::cancel (ch16:t03)
        -> _vertebra_postgres_completion_drainer (async) -> PostgresArrowResult / vertebra._vertebra.ArrowCursor
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Failure / Edge Condition | Expected Behavior |
| --- | --- |
| Schema mismatch, unsigned/decimal overflow, tz-aware -> naive `timestamp`, sub-microsecond remainder, or `kind = 2` `wire_text` in first batch | Fails with `DataError` during `copy_from_arrow` prevalidation BEFORE `COPY ... FROM STDIN` starts on the server. |
| Value failure or Python source exception on batch `>= 2` during `load_arrow_*` | `finally` guard invokes `CopyFromStream::abort` (`CopyFail` + rollback of COPY-owned scope), closes input reader, and re-raises original error with `0` committed rows. |
| Portal row reaches `8 MiB` batch byte cap before `PortalSuspended` | `ArrowPortalStream` flushes current `VTBARROW` SHM batch, retains the `1` decoded pending row and active `Execute` state, and resumes `Portal::next_event()` on the next pull without sending a premature `Execute`. |
| `asyncio` task cancelled while `AsyncPostgresArrowStream.__anext__` completion is queued on the drainer | Pending-completion guard observes undelivered state, drops the `ShmRegion` mapping, closes/cancels the portal within recovery deadline, and never returns a live portal to the pool. |
| Retained `ArrowCursor` / `memoryview` after `SyncPostgresArrowStream.close()` or `AsyncPostgresArrowStream.aclose()` | Batch `ShmRegion` ownership belongs independently to the yielded cursor capsule and remains readable until all exported views release. |

### 2.5 Alternatives Considered

| Decision | Chosen Seam & Rejected Alternative |
| --- | --- |
| Self-contained bounded IPC stream chunks vs cross-`.so` Arrow C Data Stream pointers | Bounded IPC stream slices (`<= 8 MiB` batch / `<= 16 MiB` call) keep PyArrow C-stream callbacks strictly on the caller thread and avoid unsafe foreign `ArrowArrayStream` release callbacks on Tokio workers. |
| `Portal::start_fetch` + `Portal::next_event` vs `Portal::fetch` | Incremental `PortalEvent` consumption feeds `PgValue` directly into `PostgresArrowBatchBuilder` and pauses mid-`Execute` at byte caps; `Portal::fetch` would materialize an unbounded `QueryResult` first. |
| Synchronous `AsyncPostgresClient.stream_arrow_batches(...)` factory | Returning `AsyncPostgresArrowStream` directly lets `async for` and `async with` own the full portal open/cancel/close lifecycle without an unguarded pre-open coroutine gap. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch20]` (public Arrow `COPY` and streaming documentation in `docs/usage/databases.md` and `.agents/bundles/knowledge/domains/data/` lands in `ch30:t01` and `ch30:t02`; `.pyi` stubs and Python/Rust docstrings land inline in `ch20:t01`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R20.5 | [t01](tasks/t01-arrow-to-copy-field-import.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs` |
| R19.4, R20.6 | [t02](tasks/t02-streaming-arrow-input-bridge.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs` |
| R16.3, R20.7 | [t03](tasks/t03-sync-arrow-selection-and-portals.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |
| R16.4, R20.8 | [t04](tasks/t04-async-arrow-portal-cancellation.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch20:t01 — Convert Arrow RecordBatches into binary COPY fields with prevalidated target plans and IPC batch iterators (tasks/t01-arrow-to-copy-field-import.md)
- [ ] native-postgres-driver:ch20:t02 — Expose PyO3 COPY handles and stream Arrow IPC input buffers into native COPY (tasks/t02-streaming-arrow-input-bridge.md)
- [ ] native-postgres-driver:ch20:t03 — Expose synchronous select_to_arrow and pinned Arrow portal batch streams (tasks/t03-sync-arrow-selection-and-portals.md)
- [ ] native-postgres-driver:ch20:t04 — Expose asynchronous select_to_arrow and cancellation-safe Arrow portal streams (tasks/t04-async-arrow-portal-cancellation.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch21-live-notifications` and `ch26:t03` (`driver-arrow-and-owned-binds`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL Arrow selection, COPY, portal, and cancellation contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL PortalSuspended batching, CopyFail rollback, and CancelRequest drain rules.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Per-batch VTBARROW shared-memory writer and unregistered region handoff.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue drainer and cancellation-safe PyCapsule publication.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public ArrowCursor `buffer_view` and zero-copy IPC stream consumption.
