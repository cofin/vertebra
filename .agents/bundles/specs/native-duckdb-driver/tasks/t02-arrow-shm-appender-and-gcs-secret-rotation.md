---
type: Task
id: "native-duckdb-driver:t02"
title: "Implement Arrow 60 VTBARROW ShmCapsule streaming, Appender bulk load, GCS secret rotation, and DuckdbBrokerPool cleanup"
description: "Implement zero-copy arrow 60 RecordBatch -> VTBARROW ShmCapsule (ArrowCursor / select_to_arrow / stream_arrow_batches), DuckDB Appender bulk ingestion (load_from_arrow / appender_load_arrow), DuckDBCore.build_gcs_secret_sql relocation with per-session applied_token_generation rotation, and DuckdbBrokerPool removal from vertebra-db."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-duckdb-driver:t01"
read_set:
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
files:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/vertebra/_duckdb.pyi
tests:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 450
plan_revision: 4
plan_commit: 88c30004
state_revision: 3
claimed_by: agent
claimed_at: "2026-11-09T03:38:00Z"
blocked_reason: null
unblock_condition: null
next_step: "Completed"
last_verified_at: "2026-11-09T14:40:00Z"
last_verified_commit: 3855b8f7
verification_evidence: "cargo test -p vertebra-db -p vertebra-duckdb --locked, cargo clippy -p vertebra-db -p vertebra-duckdb --all-targets --locked -- -D warnings, pytest test_data_storage_integrations.py -k duckdb, review_setup validate"
created_at: "2026-11-09T02:52:00Z"
updated_at: "2026-11-09T15:45:00Z"
commit: 3855b8f7
sources:
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Shared-memory Arrow IPC stream writer for VTBARROW capsules
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig
    title: DuckDB configuration and bootstrap pragma primitives in vertebra-db
  - id: duckdb-core-py
    resource: src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore
    title: DuckDBCore URI, pragma, and GCS secret SQL builder helpers
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql
    title: GCS secret SQL builder relocated to DuckDBCore with delegating wrapper retained
tags:
  - rust
  - duckdb
  - arrow
  - shm
  - gcs
---

# Task t02: Implement Arrow 60 `VTBARROW` `ShmCapsule` Streaming, `Appender` Bulk Load, GCS Secret Rotation, and `DuckdbBrokerPool` Cleanup

## Objective & Scope Boundary

Implement zero-copy `arrow 60` `RecordBatch` -> `VTBARROW` `ShmCapsule` (`ArrowCursor` via `construct_py_arrow_cursor` for `select_to_arrow` and `stream_arrow_batches`), DuckDB `Appender` bulk ingestion (`load_from_arrow` / `appender_load_arrow` from Arrow IPC streams / `ArrowCursor` / PyArrow tables or row sequences via `Appender::append_record_batch` and `Appender::append_rows` with explicit `flush()`), and GCS ADC secret rotation (`DuckDBCore.build_gcs_secret_sql` relocated from `src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` with a delegating wrapper retained in `extensions/sqlspec/storage.py`, plus per-session `applied_token_generation` rotation via `GcpTokenCache` or explicit token provider) [^arrow-shm-rs] [^duckdb-core-py] [^sqlspec-storage]. Clean up `DuckdbBrokerPool` from `src/rs/vertebra-db/src/duckdb/mod.rs` and `src/rs/vertebra-db/src/lib.rs` while retaining `DuckdbConfig` and `DuckdbConfig::bootstrap_statements` [^duckdb-rs].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/duckdb/core.py`:
  - `DuckDBCore.build_gcs_secret_sql(secret_name: str = "vertebra_gcs_adc", scope: Sequence[str] = ("gs://", "gcs://")) -> tuple[str, tuple[str, ...]]`
- `src/py/vertebra/extensions/sqlspec/storage.py`:
  - `build_duckdb_gcs_secret_sql(secret_name: str = "vertebra_gcs_adc", scope: Sequence[str] = ("gs://", "gcs://")) -> tuple[str, tuple[str, ...]]` (delegates to `DuckDBCore.build_gcs_secret_sql`)
- `src/rs/vertebra-duckdb/src/lib.rs`:
  - `select_to_arrow(sql, parameters=None, threshold_rows=1000) -> ArrowCursor`
  - `stream_arrow_batches(sql, parameters=None, batch_size=65536) -> list[ArrowCursor]`
  - `appender_load_arrow(table_name, ipc_bytes, columns=None) -> int` / `load_from_arrow(table_name, data, columns=None) -> int`
  - `apply_gcs_secret(secret_name="vertebra_gcs_adc", token=None, generation=None, scope=None, force=False) -> bool` and `applied_token_generation` property
- `src/rs/vertebra-db/src/duckdb/mod.rs`:
  - Retain `DuckdbConfig` and `DuckdbConfig::bootstrap_statements`; remove `DuckdbBrokerPool`.

## Acceptance Criteria

- [x] `select_to_arrow` and `stream_arrow_batches` serialize `arrow 60` `RecordBatch`es into `VTBARROW` `ShmCapsule` `ArrowCursor` objects (`used_shm` respected based on `threshold_rows`).
- [x] `Appender` bulk ingestion (`appender_load_arrow` and row batch append) writes Arrow IPC batches and row sequences with explicit `flush()` and maps constraint failures to `IntegrityError`.
- [x] `DuckDBCore.build_gcs_secret_sql` is canonical in `src/py/vertebra/db/adapters/duckdb/core.py`, `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` delegates to it, and per-session `applied_token_generation` rotation updates only when generation advances or `force=True`.
- [x] `DuckdbBrokerPool` is removed from `src/rs/vertebra-db/src/duckdb/mod.rs` and `src/rs/vertebra-db/src/lib.rs`.

[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Shared-memory Arrow IPC stream writer for `VTBARROW` capsules.
[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig` — DuckDB configuration and bootstrap pragma primitives in `vertebra-db`.
[^duckdb-core-py]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` URI, pragma, and GCS secret SQL builder helpers.
[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` — GCS secret SQL builder relocated to `DuckDBCore` with delegating wrapper retained.
