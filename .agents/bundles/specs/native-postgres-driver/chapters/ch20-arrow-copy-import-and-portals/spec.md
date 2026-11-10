---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch20-arrow-copy-import-and-portals
title: "Chapter 20: Arrow-to-COPY Bulk Import, Streaming Input Bridges, and Sync/Async Arrow Portal Streams"
description: Convert Arrow RecordBatches into binary COPY frames, expose PyO3 COPY and Arrow input bridges without Python row conversion, and deliver synchronous and cancellation-safe asynchronous Arrow selection and portal batch streams.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch19-native-arrow-batches-and-shm
replan_trigger: null
current_task: null
plan_revision: 6
plan_commit: ef86844f
state_revision: 2
last_verified_checkpoint: e5366213
last_reviewed_commit: e5366213
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-10T21:55:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: ef86844f
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: 73c8a6ff
  - by: agent:code-reviewer
    at: "2026-11-10T21:55:00Z"
    commit: e5366213
  - by: agent:quality-reviewer
    at: "2026-11-10T21:55:00Z"
    commit: e5366213
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
- **Typed Arrow-to-`COPY` Plan (`PostgresArrowImportPlan`)**: Parses literal or quote-aware `[schema.]table` identifiers (`parse_postgres_qualified_table`), discovers custom target OIDs and domain bases via bound `pg_catalog` joins before `COPY` startup, rejects text-only or unsupported binary `COPY` shapes (`"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `box`, `record`, and non-binary `"pg-array"`/`"pg-composite"` trees) and `wire_text` (`kind = 2`), preserves `BinaryCopyEncoder` header state across batches (`<= 16 MiB` IPC call cap, `<= 8 MiB` batch cap), and aborts mid-stream failures via `CopyFail` [^pg-contracts] [^pg-protocol-research].
- **Zero-Row-Materialization Input Bridge**: `vertebra.db.adapters.postgres.arrow` streams bounded self-contained Arrow IPC batches from `Table`, `RecordBatch`, `ArrowCursor`, and `__arrow_c_stream__` into `copy_from_arrow(..., schema_name=...)` and `write_arrow_ipc(batch_ipc)` while keeping provider C-stream callbacks on the caller thread [^pg-contracts] [^arrow-py].
- **Cold-Safe Selection & Single-Scope Generation-Fenced Portals**: `execute_select_to_arrow` and `ArrowPortalStream` in `portal.rs` discover cold custom OIDs via non-executing `Parse + Describe + Sync` preflights without double-running volatile SQL, share `Portal::type_catalog()` and `Portal`'s single transaction/savepoint scope, and fence `start_fetch` / `next_event` / mid-`Execute` `aclose()` under `OperationGeneration` + `CancelRequest` [^arrow-shm-rs] [^py-async-rs].
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

- **R20.5 (`ch20:t01`)**: Implement `parse_postgres_qualified_table` and post-`ch19` `PostgresArrowImportPlan` in `src/rs/vertebra-postgres/src/arrow.rs`, `SessionHandle::{copy_from_arrow, copy_from_arrow_as, copy_from_arrow_with_generation_as}`, an internal `CopyFromStream` constructor preserving `BinaryCopyEncoder.header_written` + `Option<PostgresArrowImportPlan>`, and `CopyFromStream::write_arrow_ipc` routing pre-encoded payloads through `CopyFromStream::flush_encoded_slice_internal(&payload, false)` in `src/rs/vertebra-postgres/src/copy.rs`, plus bounded IPC batch iterators (`iter_arrow_ipc_batches`, `aiter_arrow_ipc_batches`, `load_arrow_sync`, `load_arrow_async` with `schema_name: str | None = None`) in `src/py/vertebra/db/adapters/postgres/arrow.py` without calling `to_pylist()` or `to_pydict()`.
- **R19.4 & R20.6 (`ch20:t02`)**: Expose `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, and `AsyncPostgresCopyIn` plus `copy_to`, `copy_from`, and `copy_from_arrow(..., *, columns=None, schema_name=None)` (`write_arrow_ipc(self, batch_ipc: Buffer)`) on `SyncPostgresClient` and `AsyncPostgresClient` in `sync_client.rs`, `async_client.rs`, and `src/py/vertebra/_postgres.pyi`, enforcing single in-flight writes, `16 MiB` per-call IPC caps, ambient `TransactionOwner` integration, and generation-fenced `CopyFail` / `CancelRequest` cleanup across both `pg_catalog` resolution and `CopySessionLease` checkout.
- **R16.3 & R20.7 (`ch20:t03`)**: Implement `execute_select_to_arrow` (cold non-executing `Parse + Describe + Sync` + `TypeCatalog::discover` preflight under `run_cancellable_operation` and `CommandBudget`) and `ArrowPortalStream` (over `Portal::type_catalog()`, `Portal`'s single scope, and generation-fenced `start_fetch` / `next_event`) in `src/rs/vertebra-postgres/src/portal.rs`, and expose `SyncPostgresClient.select_to_arrow` (`parameter_oids=None`, `timeout=None`, `threshold_rows=256`, `max_result_bytes=268_435_456`) and `SyncPostgresClient.stream_arrow_batches` (`parameter_oids=None`, `batch_size=65_536`, `columns=None`) returning `SyncPostgresArrowStream` in `sync_client.rs` and `src/py/vertebra/_postgres.pyi`.
- **R16.4 & R20.8 (`ch20:t04`)**: Expose `AsyncPostgresClient.select_to_arrow` (`parameter_oids=None`, `timeout=None`, `threshold_rows=256`, `max_result_bytes=268_435_456`) and synchronous-factory `AsyncPostgresClient.stream_arrow_batches` (`parameter_oids=None`, `batch_size=65_536`, `columns=None`) returning `AsyncPostgresArrowStream` (`Unopened` / `Fetching` / `Open` / `Closing` / `Closed`) in `portal.rs`, `async_client.rs`, and `src/py/vertebra/_postgres.pyi`, fencing `open_portal[_as]_with_generation`, `start_fetch`, `next_event`, and mid-`Execute` `aclose()` under `OperationGeneration` + `CancelRequest` + `connect_timeout` bounded drain/socket retirement while dropping undelivered `ShmRegion` mappings on `asyncio` cancellation.

### 2.2 Concrete Type & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs` & `src/rs/vertebra-postgres/src/copy.rs`:
  - `pub const MAX_ARROW_COPY_CALL_BYTES: usize = 16 * 1024 * 1024;` and `pub const MAX_ARROW_COPY_BATCH_BYTES: usize = 8 * 1024 * 1024;`
  - `pub fn parse_postgres_qualified_table(table_name: &str, schema_name: Option<&str>) -> Result<(Option<String>, String), PostgresError>`: validates literal `(schema_name, table_name)` without splitting dots when `schema_name` is `Some(s)`, or parses double-quote-aware `table` / `schema.table` when `schema_name` is `None` (rejecting empty segments, unclosed `"`, embedded `\0`, or `> 2` segments).
  - `pub struct PostgresArrowImportPlan` with `pub fn from_schema(schema: &Schema, target_columns: &[crate::result::Column], catalog: &TypeCatalog) -> Result<Self, PostgresError>` (resolving domain chains via `catalog.resolve_domain_base(col.type_oid)`, accepting top-level `"pg-enum"`, `UInt32` `"scalar"` for `oid`/`regproc`/`xid`/`cid`, `"scalar"` `xml`, and `"unknown"` `Utf8` targeting text-family columns while rejecting target `705`, `"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `BOX_OID`, `RECORD_OID`, and any `"pg-array"`/`"pg-composite"` unsupported by `codecs::supports_binary_with_catalog` at schema-plan time) and `pub fn encode_batch(&self, batch: &RecordBatch, encoder: &mut BinaryCopyEncoder, ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>` (rejecting temporal/`"pg-timestamptz"` `kind == 2` `wire_text` before wire writes).
  - `SessionHandle::copy_from_arrow`, `SessionHandle::copy_from_arrow_as`, and `pub(crate) async fn copy_from_arrow_with_generation_as(self: &Arc<Self>, op_gen: OperationGeneration, table_name: &str, schema_name: Option<&str>, first_batch_ipc: &[u8], columns: Option<&[String]>, owner: Option<TransactionOwner>) -> Result<CopyFromStream, PostgresError>`, an internal `CopyFromStream` constructor accepting the pre-initialized `BinaryCopyEncoder` (`header_written` preserved from `first_batch_ipc`) + `Option<PostgresArrowImportPlan>`, and `CopyFromStream::write_arrow_ipc(&mut self, batch_ipc: &[u8]) -> Result<(), PostgresError>` flushing pre-encoded bytes via `CopyFromStream::flush_encoded_slice_internal(&payload, false)`.
- `src/rs/vertebra-postgres/src/portal.rs`:
  - `pub(crate) async fn execute_select_to_arrow(session: &Arc<SessionHandle>, op_gen: OperationGeneration, sql: &str, parameters: &[crate::extended::Parameter], param_oids: Option<&[u32]>, owner: Option<TransactionOwner>, budget: CommandBudget, threshold_rows: usize, max_result_bytes: usize) -> Result<ArrowOutput, PostgresError>` running inside `session.run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, ...)` with `conn.enter_session_command_deadline(budget.deadline())`, `crate::metadata::resolve_parameter_metadata`, and non-executing `Parse("", sql, param_oids) + Describe(b'S', "") + Sync` + `conn.type_catalog.discover(...)` preflight when result columns/OIDs are cold before streaming `DataRow` frames into `PostgresArrowBatchBuilder` + `PostgresArrowIpcWriter`.
  - `Portal::type_catalog(&self) -> &TypeCatalog` and `pub struct ArrowPortalStream` delegating `BEGIN`/`SAVEPOINT`/`COMMIT`/`ROLLBACK` strictly to `SessionHandle::open_portal[_as]_with_generation` and driving generation-fenced `start_fetch`, `next_event`, and mid-`Execute` cancellation/close over `Portal`'s `Format::Text` rows (including `"regproc-text"` and `TemporalText` `kind = 2`).
- `src/py/vertebra/db/adapters/postgres/arrow.py`:
  - `def iter_arrow_ipc_batches(source: Any, *, max_rows: int = 65_536, max_bytes: int = 8 * 1024 * 1024) -> Iterator[bytes]`
  - `async def aiter_arrow_ipc_batches(source: Any, *, max_rows: int = 65_536, max_bytes: int = 8 * 1024 * 1024) -> AsyncIterator[bytes]`
  - `def load_arrow_sync(client: SyncPostgresClient, table_name: str, source: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> int`
  - `async def load_arrow_async(client: AsyncPostgresClient, table_name: str, source: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> int`
- `src/py/vertebra/_postgres.pyi` (matching `contracts.md` and `ch26:t03` 1:1):
  - `SyncPostgresCopyIn.write_arrow_ipc(self, batch_ipc: Buffer) -> None`, `finish(self) -> int`, `close(self) -> None`
  - `AsyncPostgresCopyIn.write_arrow_ipc(self, batch_ipc: Buffer) -> Awaitable[None]`, `finish(self) -> Awaitable[int]`, `aclose(self) -> Awaitable[None]`
  - `SyncPostgresClient.copy_from_arrow(self, table_name: str, first_batch_ipc: Buffer, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> SyncPostgresCopyIn`
  - `AsyncPostgresClient.copy_from_arrow(self, table_name: str, first_batch_ipc: Buffer, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> Awaitable[AsyncPostgresCopyIn]`
  - `SyncPostgresClient.{copy_to, copy_from, select_to_arrow, stream_arrow_batches}`, `AsyncPostgresClient.{copy_to, copy_from, select_to_arrow, stream_arrow_batches}`, `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresArrowStream`, and `AsyncPostgresArrowStream`

### 2.3 Caller -> Seam -> Collaborator Map

```
Arrow Bulk Import:
  Python Table / RecordBatch / ArrowCursor / __arrow_c_stream__
    -> adapters/postgres/arrow.py::{iter_arrow_ipc_batches, load_arrow_sync, load_arrow_async}
      -> Sync/AsyncPostgresClient.copy_from_arrow(..., schema_name=...) + CopyIn.write_arrow_ipc(batch_ipc)
        -> SessionHandle::copy_from_arrow_with_generation_as (bound pg_catalog JOIN + TypeCatalog::discover + CopySessionLease)
        -> PostgresArrowImportPlan::encode_batch -> pre-initialized BinaryCopyEncoder -> CopyFromStream::flush_encoded_slice_internal(&payload, false)

Arrow Query & Portal Streaming:
  Sync/AsyncPostgresClient.{select_to_arrow, stream_arrow_batches}
    -> portal.rs::execute_select_to_arrow (run_cancellable_operation + CommandBudget + cold Parse/Describe/Sync + TypeCatalog::discover)
    -> portal.rs::ArrowPortalStream (SessionHandle::open_portal[_as]_with_generation + Portal::type_catalog + fenced start_fetch/next_event/close)
      -> arrow.rs::{PostgresArrowSchema, PostgresArrowBatchBuilder, PostgresArrowIpcWriter, arrow_output_into_py_result, shm_output_into_py_cursor}
        -> _vertebra_postgres_completion_drainer (async) -> PostgresArrowResult / vertebra._vertebra.ArrowCursor
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Failure / Edge Condition | Expected Behavior |
| --- | --- |
| Schema mismatch, text-only `"regproc-text"`/`"pg-range"`/`"record-text"`/`"unknown-text"`, non-binary `"pg-array"`/`"pg-composite"`, unsigned/decimal overflow, tz-aware -> naive `timestamp`, sub-microsecond remainder, or `"pg-timestamptz"` / temporal `kind = 2` `wire_text` in first batch (including `0`-row schema-only checks) | Fails with `DataError` during `copy_from_arrow` prevalidation BEFORE `COPY ... FROM STDIN` starts on the server. |
| Value failure or Python source exception on batch `>= 2` during `load_arrow_*`, or normal batch-1 -> batch-2 -> `finish()` transition | Preserved `BinaryCopyEncoder.header_written` emits the 19-byte `PGCOPY` header once across the entire stream; mid-stream failures invoke `CopyFromStream::abort` (`CopyFail` + rollback of COPY-owned scope), close input readers, and re-raise with `0` committed rows. |
| Cold custom OIDs or volatile `INSERT ... RETURNING` SQL passed to `select_to_arrow` | Non-executing `Parse + Describe + Sync` + `TypeCatalog::discover` resolves result columns and custom OIDs before `Bind + Execute`, building lossless Arrow metadata while executing volatile SQL exactly once. |
| Portal paused mid-`Execute` at the `8 MiB` batch byte cap, followed by resumption or `asyncio` cancellation / `aclose()` | Resumption drains `Portal::next_event()` before issuing a new `Execute` without duplicate outer `BEGIN`; cancellation or mid-`Execute` `aclose()` triggers generation-fenced `CancelRequest` + `connect_timeout` bounded drain/socket retirement, drops undelivered `ShmRegion` mappings, and never returns a live portal to the pool. |
| Retained `ArrowCursor` / `memoryview` after `SyncPostgresArrowStream.close()` or `AsyncPostgresArrowStream.aclose()` | Batch `ShmRegion` ownership belongs independently to the yielded cursor capsule and remains readable until all exported views release. |

### 2.5 Alternatives Considered

| Decision | Chosen Seam & Rejected Alternative |
| --- | --- |
| Self-contained bounded IPC stream chunks vs cross-`.so` Arrow C Data Stream pointers | Bounded IPC stream slices (`<= 8 MiB` batch / `<= 16 MiB` call) keep PyArrow C-stream callbacks strictly on the caller thread and avoid unsafe foreign `ArrowArrayStream` release callbacks on Tokio workers. |
| `Portal::start_fetch` + `Portal::next_event` vs `Portal::fetch` | Incremental `PortalEvent` consumption feeds `PgValue` directly into `PostgresArrowBatchBuilder` and pauses mid-`Execute` at byte caps; `Portal::fetch` would materialize an unbounded `QueryResult` first. |
| Synchronous `AsyncPostgresClient.stream_arrow_batches(...)` factory | Returning `AsyncPostgresArrowStream` directly lets `async for` and `async with` own the full portal open/cancel/close lifecycle without an unguarded pre-open coroutine gap. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch20]` (repository-wide public docs and OKF knowledge chapters (`[0/33 files documented]`) are owned by `ch30`–`ch32`, with final reviews in `ch33-final-review-and-release-gate`; `.pyi` stubs and Python/Rust docstrings land inline in `ch20:t01`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R20.5 | [t01](tasks/t01-arrow-to-copy-field-import.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs` |
| R19.4, R20.6 | [t02](tasks/t02-streaming-arrow-input-bridge.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs` |
| R16.3, R20.7 | [t03](tasks/t03-sync-arrow-selection-and-portals.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |
| R16.4, R20.8 | [t04](tasks/t04-async-arrow-portal-cancellation.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_data.py`, `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |

## 5. Implementation Plan

- [x] [34a428d7] native-postgres-driver:ch20:t01 — Convert Arrow RecordBatches into binary COPY fields with prevalidated target plans and IPC batch iterators (tasks/t01-arrow-to-copy-field-import.md)
- [x] [1fb9b77f] native-postgres-driver:ch20:t02 — Expose PyO3 COPY handles and stream Arrow IPC input buffers into native COPY (tasks/t02-streaming-arrow-input-bridge.md)
- [x] [e5da5da5] native-postgres-driver:ch20:t03 — Expose synchronous select_to_arrow and pinned Arrow portal batch streams (tasks/t03-sync-arrow-selection-and-portals.md)
- [x] [5cbf832b] native-postgres-driver:ch20:t04 — Expose asynchronous select_to_arrow and cancellation-safe Arrow portal streams (tasks/t04-async-arrow-portal-cancellation.md)

**Chapter Recovery Checkpoint:** `make dev && PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests && PYTHONPATH="$(pwd)/src/py:$(pwd)/.venv/lib/python3.12/site-packages" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests && uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/vertebra/db/adapters/postgres/arrow.py src/py/tests/unit/db/test_postgres_native_data.py && uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/vertebra/db/adapters/postgres/arrow.py src/py/tests/unit/db/test_postgres_native_data.py && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_revision: 6`, `plan_commit: ef86844f`; chapter `state: completed` at `e5366213` (`last_reviewed_commit: e5366213`, `last_verified_checkpoint: e5366213`; `make lint-all`, `cargo test -p vertebra-postgres` `186 passed`, `test_postgres_native_data.py` `162 passed` -> exit `0`).
- **Remaining Tasks in Chapter**: None (`4/4` completed: `t01` `34a428d7`, `t02` `1fb9b77f`, `t03` `e5da5da5`, `t04` `5cbf832b`).
- **Downstream Dependents**: Unblocks `ch21-live-notifications` and `ch26:t03` (`driver-arrow-and-owned-binds`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL Arrow selection, COPY, portal, and cancellation contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL PortalSuspended batching, CopyFail rollback, and CancelRequest drain rules.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Per-batch VTBARROW shared-memory writer and unregistered region handoff.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue drainer and cancellation-safe PyCapsule publication.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public ArrowCursor `buffer_view` and zero-copy IPC stream consumption.
