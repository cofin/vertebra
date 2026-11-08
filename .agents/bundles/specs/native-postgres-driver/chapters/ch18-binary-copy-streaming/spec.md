---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch18-binary-copy-streaming
title: "Chapter 18: Incremental Binary COPY Codec, COPY TO Streaming, and COPY FROM Backpressure Recovery"
description: Implement incremental PostgreSQL binary COPY framing, exclusive COPY TO STDOUT row streaming with CancelRequest drain recovery, and duplex COPY FROM STDIN chunked transmission with CopyFail abort semantics in vertebra-postgres.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch17-command-deadlines-portals-and-tz-fallback
replan_trigger: null
current_task: "native-postgres-driver:ch18:t01"
plan_revision: 8
plan_commit: 8c540809
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
    title: Native PostgreSQL public value, COPY, and error contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 binary COPY framing, CopyFail, and CancelRequest recovery rules
  - id: pg-core-copy
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_pgcopy_binary_rows
    title: Existing whole-buffer PGCOPY wire helper in vertebra-db
  - id: pg-py-core
    resource: src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore
    title: Existing Python PGCOPY binary signature and helper baseline
tags:
  - postgres
  - rust
  - copy
  - streaming
  - backpressure
---

# Chapter 18: Incremental Binary COPY Codec, COPY TO Streaming, and COPY FROM Backpressure Recovery

<!-- truth: start -->
- **Incremental `PGCOPY` Codec**: `BinaryCopyDecoder` and `BinaryCopyEncoder` frame `PGCOPY\n\xff\r\n\0` headers, nullable `i32` fields, and the single `-1i16` trailer incrementally across arbitrary `CopyData` splits with `16 MiB` row/field caps, `64 KiB` extension caps, and critical-flag (`bits 16..31`, including OID bit `16`) rejection [^pg-contracts] [^pg-protocol-research].
- **Exclusive `COPY TO` Ownership & Recovery**: `CopyOut` pins the physical `SessionHandle` and active transaction scope, validates binary `CopyOutResponse` formats, yields rows before stream completion, verifies the `COPY <n>` tag against decoded row count, and aborts early exits via `CancelRequest` + bounded drain or socket retirement [^pg-protocol-research].
- **Duplex `COPY FROM` Backpressure & `CopyFail` Abort**: `CopyFromStream` emits `<= 64 KiB` `CopyData` frames while polling backend `ErrorResponse` frames concurrently to prevent tiny-window deadlocks, commits COPY-owned scopes only on `finish()`, and aborts failures via sanitized `frontend::copy_fail` while leaving ambient caller transactions in failed (`E`) state [^pg-contracts] [^pg-protocol-research].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/rs/vertebra-db/src/postgres/mod.rs` and `src/py/vertebra/db/adapters/postgres/core.py` provide whole-buffer `PGCOPY` helpers that materialize complete payloads in memory and ignore critical header flags, fragmented `CopyData` boundaries, and live session recovery [^pg-core-copy] [^pg-py-core]. Chapter 18 delivers the native Rust binary `COPY` engine in `src/rs/vertebra-postgres/src/copy.rs` and `session.rs`: incremental `BinaryCopyDecoder` / `BinaryCopyEncoder` framing, streaming `CopyOut` (`COPY ... TO STDOUT (FORMAT binary)`) with `CancelRequest` drain/retirement on early exit, and duplex `CopyFromStream` (`COPY ... FROM STDIN (FORMAT binary)`) with concurrent `ErrorResponse` polling and `CopyFail` recovery [^pg-contracts] [^pg-protocol-research].

### 1.2 Non-Goals

- Arrow `Schema` / `RecordBatch` builders and `VTBARROW` shared-memory IPC serialization (owned by Chapter 19).
- Arrow-to-`COPY` batch plans (`PostgresArrowImportPlan`) and the PyO3 `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, and `AsyncPostgresCopyIn` client bindings in `sync_client.rs`, `async_client.rs`, and `src/py/vertebra/_postgres.pyi` (owned by `ch20:t01` and `ch20:t02` so every task stays within `<= 3` production files).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R19.1 (`ch18:t01`)**: Implement `BinaryCopyDecoder` and `BinaryCopyEncoder` in `src/rs/vertebra-postgres/src/copy.rs` over exact `PGCOPY\n\xff\r\n\0` (`11`-byte) binary framing, checked `16 MiB` frame/row/field and `64 KiB` header-extension limits, critical-flag (`0xFFFF_0000`) and OID-flag (`0x0001_0000`) rejection, single `-1i16` trailer enforcement, and shared `codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)` field encoding.
- **R19.2 (`ch18:t02`)**: Implement `PostgresConnection::start_copy_out`, `SessionHandle::copy_to`, and `CopyOut` owning an exclusive `SessionHandle` / `PooledConnection` and transaction pin, validating binary `CopyOutResponse` overall and per-column formats, streaming incremental `Option<Vec<Option<Bytes>>>` rows across `CopyData` frames, verifying `CommandComplete("COPY <n>")` count parity, and recovering early closes via `CancelRequest` + bounded drain or physical session retirement.
- **R19.3 (`ch18:t03`)**: Implement `PostgresConnection::start_copy_in`, `SessionHandle::copy_from`, and `CopyFromStream` validating binary `CopyInResponse`, encoding `PgValue` rows into `<= 64 KiB` `CopyData` chunks with bounded `1`-row + `2`-chunk buffering, polling socket reads concurrently while writes block, finishing via trailer + `CopyDone` -> `u64` row count, and aborting via sanitized `frontend::copy_fail` + drain without committing partial rows.

### 2.2 Concrete Type & Signature Contracts

- `src/rs/vertebra-postgres/src/copy.rs`:
  - `pub const PGCOPY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";`
  - `pub const DEFAULT_MAX_COPY_ROW_BYTES: usize = 16 * 1024 * 1024;`
  - `pub const DEFAULT_MAX_COPY_FIELD_BYTES: usize = 16 * 1024 * 1024;`
  - `pub const DEFAULT_MAX_COPY_EXTENSION_BYTES: usize = 64 * 1024;`
  - `pub const COPY_DATA_CHUNK_BYTES: usize = 64 * 1024;`
  - `pub struct BinaryCopyDecoder` with `pub fn new(expected_columns: usize) -> Self`, `pub fn with_limits(expected_columns: usize, max_row_bytes: usize, max_field_bytes: usize, max_ext_bytes: usize) -> Self`, `pub fn push(&mut self, chunk: &[u8]) -> Result<(), PostgresError>`, `pub fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, PostgresError>`, and `pub fn finish(self) -> Result<u64, PostgresError>`.
  - `pub struct BinaryCopyEncoder` with `pub fn new(expected_columns: usize) -> Result<Self, PostgresError>`, `pub fn encode_row_values(&mut self, column_oids: &[u32], row: &[PgValue], ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>`, `pub fn encode_raw_row(&mut self, fields: &[Option<&[u8]>], out: &mut BytesMut) -> Result<(), PostgresError>`, and `pub fn finish_trailer(&mut self, out: &mut BytesMut) -> Result<(), PostgresError>`.
  - `pub struct CopyOut` with `pub fn column_count(&self) -> usize`, `pub async fn next_row(&mut self) -> Result<Option<Vec<Option<Bytes>>>, PostgresError>`, `pub fn rows_delivered(&self) -> u64`, and `pub async fn close(&mut self) -> Result<(), PostgresError>`.
  - `pub struct CopyFromStream` with `pub fn column_count(&self) -> usize`, `pub async fn write_rows(&mut self, column_oids: &[u32], rows: &[Vec<PgValue>]) -> Result<(), PostgresError>`, `pub async fn finish(&mut self) -> Result<u64, PostgresError>`, and `pub async fn abort(&mut self, reason: &str) -> Result<(), PostgresError>`.
- `src/rs/vertebra-postgres/src/session.rs`:
  - `PostgresConnection::start_copy_out(&mut self, sql: &str) -> Result<usize, PostgresError>`
  - `PostgresConnection::start_copy_in(&mut self, sql: &str) -> Result<usize, PostgresError>`
  - `SessionHandle::copy_to(self: &Arc<Self>, sql: &str) -> Result<CopyOut, PostgresError>`
  - `SessionHandle::copy_from(self: &Arc<Self>, sql: &str) -> Result<CopyFromStream, PostgresError>`

### 2.3 Caller -> Seam -> Collaborator Map

```
Caller (Rust wire tests / ch20 PyO3 COPY & Arrow bridges)
  -> SessionHandle::{copy_to, copy_from} (src/rs/vertebra-postgres/src/session.rs)
    -> PostgresConnection::{start_copy_out, start_copy_in} (exclusive lease + pipeline guard)
      -> CopyOut / CopyFromStream (src/rs/vertebra-postgres/src/copy.rs)
        -> BinaryCopyDecoder / BinaryCopyEncoder + codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)
        -> crate::session::cancel (PostgresCancelHandle CancelRequest on early CopyOut close) / frontend::copy_fail (CopyFromStream abort)
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Failure / Edge Condition | Expected Behavior |
| --- | --- |
| Critical `PGCOPY` header bits (`flags & 0xFFFF_0000 != 0`, including OID bit `16`) | `BinaryCopyDecoder` fails closed with `PostgresError::Data` before allocating row storage; reserved low bits `0..15` are ignored. |
| Header extension `> 64 KiB` or field/row `> 16 MiB` | Checked length validation rejects before `BytesMut::reserve` / `Vec::with_capacity`. |
| Row split byte-by-byte across multiple `CopyData` frames, or trailing bytes after `-1i16` trailer | Incremental state machine yields exact row once complete; bytes after `-1i16` or missing `-1i16` at `CopyDone` fail `finish()`. |
| Text `CopyOutResponse` / `CopyInResponse` (`overall_format == 0` or any column format `== 0`) | Rejected immediately as `PostgresError::Data` / `Protocol`, draining or retiring the session before returning. |
| Consumer drops or closes `CopyOut` midstream | Sends `CancelRequest` (never `CopyFail`), drains until `ReadyForQuery` within `connect_timeout`, and retires the physical socket so pooled callers receive a fresh backend. |
| Tiny-window peer emits `ErrorResponse` during `CopyFromStream::write_rows` before draining client socket | Duplex `tokio::select!` read/write loop observes `ErrorResponse`, halts writes without deadlocking, drains `ReadyForQuery`, and surfaces the server `SQLSTATE` error. |
| Unresolved `PgValue::TemporalText` (`special="wire_text"`) passed to `CopyFromStream::write_rows` | Rejected with `PostgresError::Data` before emitting `CopyData` bytes; active `CopyFromStream` aborts via `CopyFail` without committing partial input. |

### 2.5 Alternatives Considered

| Decision | Chosen Seam & Rejected Alternative |
| --- | --- |
| Streaming parser vs whole-buffer `vertebra_db::postgres::decode_pgcopy_binary_rows` | Dedicated incremental `BinaryCopyDecoder` retains at most one incomplete row (`<= 16 MiB`). Reusing whole-buffer helpers would buffer entire `COPY` exports in RAM and miss critical-flag validation. |
| Duplex `CopyFromStream` polling vs sequential write-all-then-read | Concurrent read/write polling in `CopyFromStream` detects early server constraint `ErrorResponse` frames while TCP send buffers are full. Write-all-then-read deadlocks when both peers block on full send windows. |
| `CopyOut` early-close recovery | `CancelRequest` + bounded drain + physical session retirement (since PostgreSQL forbids `CopyFail` during `COPY TO STDOUT`). Returning an undrained or cancelled socket to the pool would leak delayed cancels or corrupt subsequent borrowers. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch18]` (user-facing `COPY` documentation in `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and OKF chapters in `.agents/bundles/knowledge/domains/data/` are consolidated in `ch30:t01`–`ch30:t04`; Rust doc comments on public `copy.rs` and `session.rs` APIs land inline in `ch18:t01`–`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R19.1 | [t01](tasks/t01-incremental-binary-copy-codec.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_copy_codec_tests.rs` |
| R19.2 | [t02](tasks/t02-copy-to-streaming.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs` |
| R19.3 | [t03](tasks/t03-copy-from-backpressure-and-abort.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch18:t01 — Frame binary COPY rows incrementally with bounded decoder and encoder state machines (tasks/t01-incremental-binary-copy-codec.md)
- [ ] native-postgres-driver:ch18:t02 — Implement exclusive COPY TO STDOUT streaming and CancelRequest recovery (tasks/t02-copy-to-streaming.md)
- [ ] native-postgres-driver:ch18:t03 — Implement duplex COPY FROM STDIN chunked backpressure and CopyFail abort recovery (tasks/t03-copy-from-backpressure-and-abort.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_codec_tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch19-native-arrow-batches-and-shm` and `ch20-arrow-copy-import-and-portals` (which wires Arrow `COPY` plans and PyO3 `copy_to` / `copy_from` / `copy_from_arrow` handles).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL public value, COPY, and error contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 binary COPY framing, CopyFail, and CancelRequest recovery rules.
[^pg-core-copy]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_pgcopy_binary_rows` — Existing whole-buffer PGCOPY wire helper in `vertebra-db`.
[^pg-py-core]: `src/py/vertebra/db/adapters/postgres/core.py::PostgreSQLCore` — Existing Python PGCOPY binary signature and helper baseline.
