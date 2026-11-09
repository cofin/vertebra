---
type: Task
flow_id: native-mysql-driver
task_id: t03
title: "Implement MySQL VTBARROW ShmCapsule ArrowCursor, load_from_arrow, and BrokerEngineBackend"
description: "Implement binary-protocol row to arrow 60.1.0 RecordBatch to VTBARROW POSIX SHM ShmCapsule (select_to_arrow, stream_arrow_batches), batched load_from_arrow bulk ingestion, and BrokerEngineBackend registration in vertebra-db and vertebra-mysql."
status: stable
state: in_progress
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies:
  - t02
files_to_modify:
  - src/rs/vertebra-db/src/lib.rs
  - src/rs/vertebra-mysql/src/arrow_ipc.rs
  - src/rs/vertebra-mysql/src/client.rs
  - src/rs/vertebra-mysql/src/lib.rs
  - src/py/vertebra/_mysql.pyi
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-spanner/src/arrow_ipc.rs
  - src/py/vertebra/db/arrow.py
commands_to_run:
  - "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql -p vertebra-db --locked"
  - "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql -p vertebra-db --all-targets --locked -- -D warnings"
  - "uv run python -m mypy.stubtest vertebra._mysql"
  - "uv run python -m tools.scripts.review_setup validate"
commit_sha: null
created_at: "2026-11-09T03:25:00Z"
updated_at: "2026-11-09T03:25:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: Shared VTBARROW POSIX SHM zero-copy frame writer and reader
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py
    title: Python ArrowCursor zero-copy VTBARROW SHM consumer
---

# Task t03: Implement MySQL `VTBARROW` `ShmCapsule` `ArrowCursor`, `load_from_arrow`, and `BrokerEngineBackend`

<!-- truth: start -->
- `select_to_arrow` and `stream_arrow_batches` on `SyncMySQLClient` and `AsyncMySQLClient` decode MySQL rows directly into `arrow 60.1.0` `RecordBatch`es, write `VTBARROW` POSIX shared-memory frames via `write_shm_arrow_region_unregistered` (`/vtb-my-{pid:x}-{seq:x}` `<= 31` bytes), and return `ArrowCursor` instances backed by `PyCapsule` (`"vertebra.ShmRegion"`) [^mysql-spec] [^arrow-shm-rs] [^arrow-py].
- `load_from_arrow` ingests `ArrowCursor`, `VTBARROW` SHM capsules, Arrow IPC streams, or column/row collections via chunked multi-row `INSERT` batches, and `vertebra-mysql` registers its `BrokerEngineBackend` for `DIALECT_TAG_MYSQL` (`2`) [^mysql-spec].
<!-- truth: end -->

## Context

Task `t03` connects MySQL query execution to Vertebra's zero-copy `VTBARROW` POSIX shared-memory Arrow IPC pipeline (`select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow`) and registers `vertebra-mysql` as the native MySQL `BrokerEngineBackend` [^mysql-spec] [^arrow-shm-rs].

## Public Seam & TDD Plan

1. **Red**:
   - Write Rust and Python tests verifying:
     - `select_to_arrow` and `stream_arrow_batches` produce `ArrowCursor` with `has_shm_capsule == True` (`capsule_name() == "vertebra.ShmRegion"`), SHM segment names matching `/vtb-my-...` (`<= 31` bytes), and typed columns (`Int64`, `UInt64`, `Float64`, `Boolean`, `Utf8`, `Binary`, `Date32`, `Timestamp(Microsecond)`, `Decimal128(38, 9)`).
     - `load_from_arrow` reads an `ArrowCursor` (or `VTBARROW` SHM capsule / Arrow IPC bytes / tabular records), executes batched multi-row `INSERT` statements, and round-trips rows back through `select_to_arrow`.
     - `BrokerEngineBackend` registration in `vertebra-db` / `vertebra-mysql` resolves dialect tag `2` (`DIALECT_TAG_MYSQL`) to the registered MySQL backend.
2. **Green**:
   - Implement `src/rs/vertebra-mysql/src/arrow_ipc.rs` using `arrow-{array,buffer,ipc,schema} 60.1.0` and `vertebra_db::arrow_shm::write_shm_arrow_region_unregistered` + `vertebra_db::py_async::construct_py_arrow_cursor`.
   - Expose `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` on `SyncMySQLClient` and `AsyncMySQLClient`.
   - Add `BrokerEngineBackend` trait/registry in `src/rs/vertebra-db/src/lib.rs` and register the MySQL backend in `src/rs/vertebra-mysql/src/lib.rs`.
3. **Refactor & Verify**:
   - Run `cargo test`, `cargo clippy`, `mypy.stubtest vertebra._mysql`, and `review_setup validate`.

## Acceptance Criteria

- [ ] `select_to_arrow` and `stream_arrow_batches` on both `SyncMySQLClient` and `AsyncMySQLClient` return `ArrowCursor` instances backed by `VTBARROW` POSIX SHM `PyCapsule`s (`has_shm_capsule is True`).
- [ ] `load_from_arrow` ingests `ArrowCursor` / `VTBARROW` SHM data in configurable batches and returns accurate inserted row counts.
- [ ] `BrokerEngineBackend` is registered for `DIALECT_TAG_MYSQL` (`2`) when `vertebra._mysql` is initialized.
- [ ] `cargo test`, `cargo clippy`, `mypy.stubtest vertebra._mysql`, and `review_setup validate` pass.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs` — Shared VTBARROW POSIX SHM zero-copy frame writer and reader.
[^arrow-py]: `src/py/vertebra/db/arrow.py` — Python ArrowCursor zero-copy VTBARROW SHM consumer.
