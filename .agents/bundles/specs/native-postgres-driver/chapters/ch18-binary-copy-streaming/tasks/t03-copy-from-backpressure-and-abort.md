---
type: Task
id: "native-postgres-driver:ch18:t03"
chapter_id: ch18-binary-copy-streaming
title: Implement duplex COPY FROM STDIN chunked backpressure and CopyFail abort recovery
description: Add PostgresConnection::start_copy_in, SessionHandle::copy_from, and CopyFromStream in vertebra-postgres with <= 64 KiB CopyData chunking, concurrent ErrorResponse read polling, finish row counts, and sanitized CopyFail abort recovery.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch18:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch18-binary-copy-streaming/spec.md
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs
estimated_loc: 230
plan_revision: 7
plan_commit: 1c75af65
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
    title: PostgreSQL COPY FROM transaction ownership and value encoding contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL COPY FROM STDIN duplex backpressure and CopyFail recovery rules
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: AsyncRead and AsyncWrite TransportStream duplex polling seam
tags:
  - postgres
  - rust
  - copy
  - backpressure
  - transactions
---

# Task t03: Implement Duplex COPY FROM STDIN Chunked Backpressure and CopyFail Abort Recovery

## Objective & Scope Boundary

Implement `PostgresConnection::start_copy_in`, `SessionHandle::copy_from`, and `CopyFromStream` in `src/rs/vertebra-postgres/src/{copy,session}.rs` to stream `COPY ... FROM STDIN (FORMAT binary)` rows in `<= 64 KiB` `CopyData` frames with concurrent socket read/write polling, exact `CommandComplete` counts on `finish()`, and sanitized `frontend::copy_fail` abort recovery without partial commits [^pg-contracts] [^pg-protocol-research] [^pg-conn-rs]. Arrow batch import plans and PyO3 `SyncPostgresCopyIn` / `AsyncPostgresCopyIn` wrappers belong to `ch20:t01` and `ch20:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 18 duplex `COPY FROM STDIN` chunking and `CopyFail` abort contracts.
- `src/rs/vertebra-postgres/src/copy.rs` — `BinaryCopyEncoder` (`ch18:t01`) and `CopyOut` ownership patterns (`ch18:t02`).
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection`, `SessionHandle`, transaction state (`I`/`T`/`E`), and exclusive operation guards.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — Shared `codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)` and `TypeCatalog` lookup.
- [contracts.md](../../../contracts.md) — Transaction pinning, `TemporalText` refusal, and pool reset contracts [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `COPY FROM` `CopyFail`, fresh `Sync` recovery, and tiny-window deadlock avoidance [^pg-protocol-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/session.rs`:
  - `pub async fn start_copy_in(&mut self, sql: &str) -> Result<usize, PostgresError>` on `PostgresConnection`:
    - Rejects active pipeline scope and multi-command SQL before emitting frames.
    - Begins an owned transaction when session is idle (`I`) so `CopyFromStream` commits only on explicit `finish()`, or retains caller ownership when already inside an ambient transaction (`T`).
    - Validates `CopyInResponse` (`overall_format == 1` and all per-column formats `== 1`), returning `column_count`.
  - `pub async fn copy_from(self: &Arc<Self>, sql: &str) -> Result<CopyFromStream, PostgresError>` on `SessionHandle`.
- `src/rs/vertebra-postgres/src/copy.rs`:
  - `pub const COPY_DATA_CHUNK_BYTES: usize = 64 * 1024;`
  - `pub const MAX_COPY_FAIL_REASON_BYTES: usize = 256;`
  - `pub struct CopyFromStream`:
    - `pub fn column_count(&self) -> usize`
    - `pub async fn write_rows(&mut self, column_oids: &[u32], rows: &[Vec<PgValue>]) -> Result<(), PostgresError>` (validates `column_oids.len() == column_count` and non-overlapping state before emitting bytes; encodes one row at a time via `BinaryCopyEncoder::encode_row_values`, slices encoded bytes into `<= 64 KiB` `frontend::copy_data` frames retaining at most 1 row buffer + 2 outbound chunks, and drives duplex `tokio::io::split` / `poll_read` + `poll_write` so incoming `ErrorResponse` / `NoticeResponse` / `ParameterStatus` frames are consumed while socket writes block)
    - `pub(crate) async fn write_encoded_payload(&mut self, encoded_rows: &[u8]) -> Result<(), PostgresError>` (reusable duplex `<= 64 KiB` `CopyData` pump consumed by `write_rows` and `ch20:t01` Arrow `COPY` import)
    - `pub async fn finish(&mut self) -> Result<u64, PostgresError>` (writes `PGCOPY` header if `0` rows were written, appends single `-1i16` trailer + `frontend::copy_done`, sends fresh `Sync` if extended mode requires it, reads `CommandComplete("COPY <n>")` + `ReadyForQuery`, commits COPY-owned transaction only on success, releases lease, and returns `u64` count)
    - `pub async fn abort(&mut self, reason: &str) -> Result<(), PostgresError>` (idempotent once closed; truncates/sanitizes `reason` to valid UTF-8 `<= 256` bytes replacing `\0` with `'?'`, sends `frontend::copy_fail`, drains expected `ErrorResponse` + `ReadyForQuery` within `connect_timeout`, rolls back COPY-owned transaction or preserves ambient `E` status for caller `rollback()`, and retires socket if drain fails)
    - `impl Drop for CopyFromStream` (if neither `finish()` nor `abort()` completed, spawns bounded background `abort("COPY FROM dropped before finish")` / retirement on `db_runtime_handle()` and never commits partial input)

## Edge Cases & Error Semantics

- Overlapping `write_rows`, `finish`, or `abort` calls fail immediately with `PostgresError::Interface` before writing wire bytes.
- Encoding error mid-batch (wrong column count, incompatible `PgValue` / OID, oversize `> 16 MiB` row/field, or unresolved `TemporalText`) aborts the active `CopyFromStream` via `CopyFail` + rollback (when COPY-owned) and returns the original `PostgresError::Data` rather than replacing it with the server's `CopyFail` `57014` error.
- Tiny-window server constraint error (`23505` unique violation) emitted before the peer reads remaining `CopyData`: duplex read polling stops outbound `CopyData` transmission immediately, drains `ReadyForQuery`, rolls back COPY-owned transaction (or leaves ambient transaction in `E`), and returns canonical `IntegrityError` (`23505`) without deadlocking.
- Repeated `abort()` after `finish()` or prior `abort()` is a clean idempotent no-op.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_copy_wire_tests.rs` (`copy_in_*` suite over real loopback TCP peers with tiny `SO_RCVBUF` / paused-read barriers and `PhysicalPool` `max_size = 1`)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Multi-chunk binary `COPY FROM` & `0`-row `finish()` | (a) Write `500` rows (`> 128 KiB`) of `(i32, Option<String>, Decimal)` then `finish()`; (b) `copy_from` followed immediately by `finish()` | (a) Peer observes exact `PGCOPY` header once, `CopyData` frames each `<= 65_536` bytes, single `0xFFFF` trailer, `CopyDone`, and `COMMIT` (when idle-started), returning `Ok(500)`; (b) emits header + `0xFFFF` + `CopyDone`, returning `Ok(0)`. |
| Tiny-window duplex `ErrorResponse` without deadlock | Peer stops reading socket after `CopyInResponse` and immediately sends `ErrorResponse(23505)` + `ReadyForQuery(E)` while client calls `write_rows` with `2 MiB` of rows (`32` chunks) | `write_rows` terminates promptly within `1s` (zero write-all deadlock), rolls back COPY-owned transaction, returns `SQLSTATE 23505`, and leaves `max_size = 1` pool usable. |
| Explicit `abort(reason)` with embedded NUL & overlong text | Write 2 rows, call `abort("producer\0failed_" + &"x".repeat(400))`, then call `abort("again")` | Peer receives valid `CopyFail` with NUL-free `<= 256`-byte UTF-8 message, drains `ErrorResponse(57014)` + `ReadyForQuery`, rolls back COPY-owned scope so `0` rows commit; second `abort` is a no-op. |
| Ambient transaction preservation on `abort` & server error | `session.begin().await`, `copy_from` + `abort("bad batch")` (and in second test server `23505`) | No `COMMIT` or implicit `ROLLBACK` is sent by `CopyFromStream`; ambient transaction stays pinned in server `E` (`Failed`) status until caller invokes `session.rollback().await`. |
| Drop without `finish()` & pipeline `COPY` refusal | (a) Write 3 rows then `drop(copy_in)` on `max_size = 1` pool; (b) call `copy_from` inside active pipeline | (a) Background cleanup sends `CopyFail` + `ROLLBACK` (or retires socket) and next `pool.acquire()` sees `0` committed rows; (b) fails before sending `COPY` SQL. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests copy_in`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_copy_wire_tests copy_in
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): stream COPY FROM with abort recovery`

## Acceptance Criteria

- [ ] `CopyFromStream` validates binary `CopyInResponse`, splits outbound `CopyData` frames to `<= 64 KiB`, retains at most 1 row buffer + 2 chunks, and polls backend `ErrorResponse` frames concurrently while writes block (R19).
- [ ] `finish()` emits exactly one `-1i16` trailer + `CopyDone`, returns the server `COPY <n>` count, and commits only COPY-owned transactions (R19).
- [ ] `abort(reason)`, encoder/constraint errors, and `Drop` send sanitized `frontend::copy_fail`, drain or retire boundedly, preserve original producer/server errors, keep ambient transactions caller-owned, and never commit partial input (R13, R19).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgreSQL COPY FROM transaction ownership and value encoding contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL COPY FROM STDIN duplex backpressure and CopyFail recovery rules.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — AsyncRead and AsyncWrite TransportStream duplex polling seam.
