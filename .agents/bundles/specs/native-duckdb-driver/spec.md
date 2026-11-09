---
type: Spec
flow_id: native-duckdb-driver
title: "Standalone Rust DuckDB Wheel (vertebra-duckdb)"
description: "Standalone PyO3 crate src/rs/vertebra-duckdb (vertebra._duckdb + vertebra-core[duckdb]) built on bundled duckdb with arrow 60 -> VTBARROW ShmCapsule streaming, Appender bulk load, GCS ADC secret rotation, lossless column metadata, savepoint isolation, delayed-commit pipelines, and interrupt cancellation, replacing the PyPI duckdb package with vertebra._duckdb."
status: stable
state: completed
has_chapters: false
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 4
plan_commit: 88c30004
state_revision: 6
current_task: null
last_verified_checkpoint: "native-duckdb-driver:t06"
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-09T21:28:00Z"
generated:
  by: flow-plan
  at: "2026-11-09T02:52:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T21:15:00Z"
    commit: 88c30004
sources:
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig
    title: DuckDB configuration and bootstrap pragma primitives in vertebra-db
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud baseline removing the DuckDB sqlite3 fallback and DuckdbBrokerPool
  - id: duckdb-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: Python SyncDuckDBDriver and AsyncDuckDBDriver target cutover module
  - id: duckdb-pipeline-py
    resource: src/py/vertebra/db/adapters/duckdb/pipeline.py
    title: SyncDuckDBPipeline and AsyncDuckDBPipeline delayed-commit batch pipeline module
  - id: duckdb-core-py
    resource: src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore
    title: DuckDBCore URI, pragma, and GCS secret SQL builder helpers
  - id: duckdb-config-py
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig
    title: DuckDBConfig immutable configuration and cgroup-clamped memory settings
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql
    title: GCS secret SQL builder relocated to DuckDBCore with delegating wrapper retained
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue, construct_py_arrow_cursor, and independent event-loop drainer
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Shared-memory Arrow IPC stream writer for VTBARROW capsules
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix
tags:
  - rust
  - duckdb
  - arrow
  - driver
  - wheel
---

# Specification: Standalone Rust DuckDB Wheel (`vertebra-duckdb`)

<!-- truth: start -->
- **Isolated Amalgamation & `arrow 60` Wheel**: `src/rs/vertebra-duckdb` (`vertebra._duckdb`, `vertebra-core[duckdb]`) isolates bundled `duckdb = "=1.20000.1"` (`bundled`, `appender-arrow`) and `arrow-* = "=60.1.0"` from `vertebra-core` and `vertebra-oracle` (`arrow 59`) [^duckdb-rs].
- **Zero-Copy `VTBARROW`, `Appender` & Lossless Metadata**: `SyncDuckDBClient` (`SyncCompletionQueue`) and `AsyncDuckDBClient` (`AsyncCompletionQueue` + `Notifier` with `"_vertebra_duckdb_completion_drainer"`) stream `arrow 60` `RecordBatch` frames into `VTBARROW` `ShmCapsule` `ArrowCursor`, preserve column metadata on empty result sets via `execute_with_description`, support transactional savepoint isolation (`savepoint`, `rollback_to_savepoint`, `release_savepoint`), ingest via `Appender`, and rotate GCS ADC secrets via `DuckDBCore.build_gcs_secret_sql` and per-session `applied_token_generation` [^py-async-rs] [^arrow-shm-rs] [^sqlspec-storage].
- **Native Python Driver Cutover & Pipeline Parity**: `SyncDuckDBDriver` and `AsyncDuckDBDriver` inherit directly from `SyncDatabaseDriver[DuckDBConfig]` and `AsyncDatabaseDriver[DuckDBConfig]`, loading `vertebra._duckdb` via `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")` without `ThreadOffloadAsyncDatabaseDriver`, `asyncio.to_thread`, `sqlite3`, or PyPI `duckdb`, providing `SyncDuckDBPipeline` and `AsyncDuckDBPipeline`, and activating `duckdb` in `test_driver_conformance.py` [^duckdb-py] [^duckdb-pipeline-py] [^conformance-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Architecture

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Fail-loud baseline.** The completed [`driver-fail-loud` baseline](../../knowledge/domains/data/database-drivers-and-dialers.md) removes the DuckDB `sqlite3` fallback and `DuckdbBrokerPool`, keeping `DuckdbConfig::bootstrap_statements` as the shared pragma source [^fail-loud-baseline] [^duckdb-rs].
- **Isolated `vertebra-duckdb` crate (`src/rs/vertebra-duckdb/` -> `vertebra._duckdb` + `vertebra-core[duckdb]`).** Isolate `duckdb` `1.20000.1` (`bundled`, `appender-arrow`) and `arrow-{array,buffer,ipc,schema}` `=60.1.0` inside a standalone workspace PyO3 `cdylib` + `rlib` crate (`#[pymodule(gil_used = false)] fn _duckdb`, `src/py/vertebra/_duckdb.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].duckdb`, sibling wheel isolation `exclude` globs, `Makefile` `duckdb-test` / `duckdb-wheel-check`, and `areas.toml` registration) so `vertebra-core` stays free of the C++ DuckDB amalgamation and `arrow 60` never collides with `vertebra-oracle` (`arrow 59`) [^duckdb-rs].
- **Dedicated Worker-Thread Session Pool & Completion Queues.** Expose `SyncDuckDBClient` (`SyncCompletionQueue`) and `AsyncDuckDBClient` (`AsyncCompletionQueue` + `Notifier` via `vertebra_db::py_async::spawn_rust_db_task_with_attr` with `"_vertebra_duckdb_completion_drainer"`) backed by a single-writer / multi-connection session pool over dedicated Rust worker threads (`Connection::try_clone()` for shared in-memory or file-backed instances), applying DuckDB 2.0-safe pragmas via `DuckdbConfig::bootstrap_statements` (`parquet_metadata_cache`, `httpfs_connection_caching`, `enable_http_metadata_cache`, cgroup-clamped `max_memory`/`threads`), out-of-band `duckdb::InterruptHandle::interrupt()` query cancellation, and `DatabaseError` hierarchy mapping (`OperationalError`, `IntegrityError`, `ProgrammingError`, `DataError`, `QueryCancelledError`, `NotFoundError`, `InterfaceError`, `SerializationFailure` with `.sqlstate`, `.vendor_code`, and `.is_transient`) [^duckdb-rs] [^py-async-rs].
- **Zero-Copy Arrow SHM, Appender Bulk Load, & GCS Secret Rotation.** Serialize `arrow 60` `RecordBatch` streams directly into `VTBARROW` `ShmCapsule` `ArrowCursor` frames via `construct_py_arrow_cursor` (`select_to_arrow`, `stream_arrow_batches`), bulk-load Arrow IPC streams and row batches through DuckDB `Appender` (`append_record_batch` / `append_rows` + explicit `flush`), relocate `DuckDBCore.build_gcs_secret_sql` from `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` (keeping a delegating wrapper in `extensions/sqlspec/storage.py`), and track per-session `applied_token_generation` rotation via `GcpTokenCache` (`SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenCacheCapsule`) or explicit token providers [^arrow-shm-rs] [^duckdb-core-py] [^sqlspec-storage].
- **Lossless Metadata, Savepoint Isolation, Config Hardening & Secret Redaction.** Expose `execute_with_description(sql, parameters=None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]` (preserving column names from the statement/Arrow schema even when `0` rows are returned) and `execute_tuples(sql, parameters=None) -> list[tuple[Any, ...]]`, implement transactional savepoints (`savepoint`, `rollback_to_savepoint`, `release_savepoint`), enforce NUL-byte rejection (`'\0'`), `max_connections >= 1`, fail-loud `ExtensionSpec` (`INSTALL <ext> [FROM <repo>]; LOAD <ext>`), and redact secrets/bearer tokens in `Debug`, `Display`, and `__repr__` [^duckdb-rs] [^duckdb-config-py].
- **Delayed-Commit Pipelines, Python Driver Cutover & Conformance.** Implement `SyncDuckDBPipeline` and `AsyncDuckDBPipeline` in `src/py/vertebra/db/adapters/duckdb/pipeline.py` (with delayed commit on `__exit__` / `__aexit__` and savepoint nesting when `driver.in_transaction` is `True`), cut `src/py/vertebra/db/adapters/duckdb/driver.py` (`SyncDuckDBDriver`, `AsyncDuckDBDriver`) over from PyPI `duckdb` and `sqlite3` fallback to `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")` delegating to `SyncDuckDBClient` and `AsyncDuckDBClient` with zero `ThreadOffloadAsyncDatabaseDriver` or `asyncio.to_thread`, keep PyPI `duckdb` isolated exclusively to benchmark storage (`tools/benchmarks`), and activate `duckdb` (`pending_flow_id=None`, `supports_pipeline=True`) in `src/py/tests/integration/db/test_driver_conformance.py` across standard and free-threaded interpreters [^duckdb-py] [^duckdb-pipeline-py] [^conformance-py].

## 2. Requirements & Traceability

- **R1 (Standalone Crate, Session Pool, Pragmas, Cancellation & Error Hierarchy)**: Owned by [t01](tasks/t01-vertebra-duckdb-crate-and-pool.md).
- **R2 (Arrow 60 -> `VTBARROW` SHM Streaming, Appender Load, GCS Secret Rotation & `DuckdbBrokerPool` Cleanup)**: Owned by [t02](tasks/t02-arrow-shm-appender-and-gcs-secret-rotation.md).
- **R3 (Python `SyncDuckDBDriver` / `AsyncDuckDBDriver` Direct Native Cutover)**: Owned by [t03](tasks/t03-python-duckdb-driver-cutover-and-docs.md).
- **R4 (Lossless 0-Row Column Metadata, Savepoints, Config Hardening & Secret Redaction)**: Owned by [t04](tasks/t04-lossless-metadata-savepoints-and-config-hardening.md).
- **R5 (Delayed-Commit `SyncDuckDBPipeline` & `AsyncDuckDBPipeline` with Savepoint Nesting)**: Owned by [t05](tasks/t05-delayed-commit-sync-and-async-pipelines.md).
- **R6 (Wheel Isolation, Conformance Matrix Activation, Unit Tests & Documentation Sync)**: Owned by [t06](tasks/t06-wheel-isolation-conformance-and-docs-parity.md).

## 3. Implementation Plan

- [x] [f5df6f98] native-duckdb-driver:t01 — Scaffold vertebra-duckdb crate, session pool, pragmas, interrupt cancellation, and DatabaseError mapping (tasks/t01-vertebra-duckdb-crate-and-pool.md)
- [x] [3855b8f7] native-duckdb-driver:t02 — Implement Arrow 60 VTBARROW ShmCapsule streaming, Appender bulk load, GCS secret rotation, and DuckdbBrokerPool cleanup (tasks/t02-arrow-shm-appender-and-gcs-secret-rotation.md)
- [x] [9c549ed1] native-duckdb-driver:t03 — Cut over Python SyncDuckDBDriver and AsyncDuckDBDriver to vertebra._duckdb without ThreadOffloadAsyncDatabaseDriver (tasks/t03-python-duckdb-driver-cutover-and-docs.md)
- [x] [8869f7c1] native-duckdb-driver:t04 — Add lossless 0-row column metadata, savepoints, config validation hardening, and secret redaction (tasks/t04-lossless-metadata-savepoints-and-config-hardening.md)
- [x] [bb4d2b59] native-duckdb-driver:t05 — Implement SyncDuckDBPipeline and AsyncDuckDBPipeline with delayed commit and savepoint nesting (tasks/t05-delayed-commit-sync-and-async-pipelines.md)
- [x] [89de2b8c] native-duckdb-driver:t06 — Activate DuckDB conformance matrix, wheel isolation targets, unit tests, and documentation sync (tasks/t06-wheel-isolation-conformance-and-docs-parity.md)

## 4. Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively for DuckDB via `vertebra._duckdb` with `pending_flow_id=None` and `supports_pipeline=True`.
- `AsyncDuckDBDriver` inherits directly from `AsyncDatabaseDriver[DuckDBConfig]` with zero `ThreadOffloadAsyncDatabaseDriver`, `asyncio.to_thread`, `sqlite3`, or PyPI `duckdb` usage.
- Arrow SHM streaming, `Appender` bulk load, lossless 0-row `execute_with_description`, savepoint partial rollback, `SyncDuckDBPipeline` / `AsyncDuckDBPipeline`, `interrupt()` cancellation, and GCS `token_generation` secret rotation pass unit and integration tests.
- Wheel isolation check (`duckdb-wheel-check` and `test_optional_driver_wheels.py`) confirms `vertebra-duckdb` builds cleanly without leaking `vertebra/*.py` or sibling `.pyi` files.
- `mypy.stubtest vertebra._duckdb`, `cargo test -p vertebra-duckdb --locked`, `cargo clippy -p vertebra-duckdb --all-targets --locked -- -D warnings`, Ruff, Mypy, pytest, and `tools.scripts.review_setup validate` pass.

[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig` — DuckDB configuration and bootstrap pragma primitives in `vertebra-db`.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud baseline removing the DuckDB `sqlite3` fallback and `DuckdbBrokerPool`.
[^duckdb-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Python `SyncDuckDBDriver` and `AsyncDuckDBDriver` target cutover module.
[^duckdb-pipeline-py]: `src/py/vertebra/db/adapters/duckdb/pipeline.py` — `SyncDuckDBPipeline` and `AsyncDuckDBPipeline` delayed-commit batch pipeline module.
[^duckdb-core-py]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` URI, pragma, and GCS secret SQL builder helpers.
[^duckdb-config-py]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig` — `DuckDBConfig` immutable configuration and cgroup-clamped memory settings.
[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` — GCS secret SQL builder relocated to `DuckDBCore` with delegating wrapper retained.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue`, `construct_py_arrow_cursor`, and independent event-loop drainer.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Shared-memory Arrow IPC stream writer for `VTBARROW` capsules.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix.
