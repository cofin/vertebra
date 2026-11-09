---
type: Task
id: "native-duckdb-driver:t01"
title: "Scaffold vertebra-duckdb crate, session pool, pragmas, interrupt cancellation, and DatabaseError mapping"
description: "Create src/rs/vertebra-duckdb (vertebra._duckdb) with bundled duckdb =1.20000.1 and arrow 60.1.0, SyncDuckDBClient (SyncCompletionQueue) and AsyncDuckDBClient (AsyncCompletionQueue + Notifier), worker-thread session pool, DuckDB 2.0-safe pragmas, InterruptHandle cancellation, DatabaseError mapping, _duckdb.pyi stub, and workspace packaging."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-db/src/duckdb/mod.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-spanner/Cargo.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - .agents/skills/pyo3-bridge/SKILL.md
files:
  - src/rs/vertebra-duckdb/Cargo.toml
  - src/rs/vertebra-duckdb/pyproject.toml
  - src/rs/vertebra-duckdb/build.rs
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-duckdb/src/errors.rs
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/py/vertebra/_duckdb.pyi
  - src/rs/vertebra-bigquery/pyproject.toml
  - src/rs/vertebra-bundler/pyproject.toml
  - src/rs/vertebra-spanner/pyproject.toml
  - src/rs/vertebra-storage/pyproject.toml
  - pyproject.toml
  - Makefile
  - .agents/skills/review-core/references/areas.toml
tests:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/tests/unit/build/test_packaging_and_pgo.py
  - src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 750
plan_revision: 3
plan_commit: d7a629b1
state_revision: 2
claimed_by: agent
claimed_at: "2026-11-09T02:52:00Z"
blocked_reason: null
unblock_condition: null
next_step: "Completed"
last_verified_at: "2026-11-09T03:36:00Z"
last_verified_commit: 7c9d8e1c
verification_evidence: "cargo test -p vertebra-duckdb --locked, cargo clippy -p vertebra-duckdb --all-targets --locked -- -D warnings, mypy.stubtest vertebra._duckdb, pytest test_freethreaded_gil.py test_packaging_and_pgo.py, review_setup validate"
created_at: "2026-11-09T02:52:00Z"
updated_at: "2026-11-09T03:36:00Z"
commit: 088c2d30
sources:
  - id: duckdb-rs
    resource: src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig
    title: DuckDB configuration and bootstrap pragma primitives in vertebra-db
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue, construct_py_arrow_cursor, and independent event-loop drainer
  - id: spanner-cargo
    resource: src/rs/vertebra-spanner/Cargo.toml
    title: Reference optional PyO3 extension Cargo manifest
  - id: spanner-wheel
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Reference optional PyO3 Maturin wheel layout in vertebra namespace
tags:
  - rust
  - duckdb
  - pyo3
  - wheel
---

# Task t01: Scaffold `vertebra-duckdb` Crate, Session Pool, Pragmas, Interrupt Cancellation, and `DatabaseError` Mapping

## Objective & Scope Boundary

Create `src/rs/vertebra-duckdb/` (`Cargo.toml` with `duckdb = { version = "=1.20000.1", features = ["bundled", "appender-arrow"] }`, `arrow-{array,buffer,ipc,schema} = "=60.1.0"`, `vertebra-core`, `vertebra-db = { path = "../vertebra-db", features = ["gcp", "pyo3"] }`, `pyo3`, `pyproject.toml`, `build.rs`, `src/errors.rs`, `src/lib.rs` with `#[pymodule(gil_used = false)] fn _duckdb`) [^duckdb-rs] [^spanner-cargo] [^spanner-wheel]. Register `"vertebra-duckdb"` in `src/rs/Cargo.toml`, update `src/rs/Cargo.lock`, add `src/py/vertebra/_duckdb.pyi` (and exclude `_duckdb.pyi` from sibling optional wheels' `pyproject.toml`), add `[project.optional-dependencies].duckdb` (`vertebra-duckdb==0.3.0`) in root `pyproject.toml`, and register `vertebra-duckdb` in `.agents/skills/review-core/references/areas.toml` and `Makefile`.

Implement `SyncDuckDBClient` (`SyncCompletionQueue` with `py.detach`) and `AsyncDuckDBClient` (`AsyncCompletionQueue` + `Notifier` with `"_vertebra_duckdb_completion_drainer"` via `vertebra_db::py_async::spawn_rust_db_task_with_attr`), single-writer / multi-connection session pool management over per-connection Rust worker threads (`Connection::try_clone()`), DuckDB 2.0-safe pragmas via `DuckdbConfig::bootstrap_statements` (`parquet_metadata_cache`, `httpfs_connection_caching`, `enable_http_metadata_cache`, cgroup-clamped `max_memory`/`threads`), `execute`, `execute_many`, `execute_with_description`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value`, `begin`/`commit`/`rollback`, `cursor()`, `interrupt()`/`cancel()` via `duckdb::InterruptHandle`, and `DatabaseError` hierarchy mapping (`OperationalError`, `IntegrityError`, `ProgrammingError`, `DataError`, `QueryCancelledError`, `NotFoundError`, `InterfaceError`) [^duckdb-rs] [^py-async-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-duckdb/Cargo.toml`:
  - `[lib]`: `name = "_duckdb"`, `crate-type = ["cdylib", "rlib"]`
  - `[features]`: `default = []`, `extension-module = ["pyo3/extension-module"]`
  - `[dependencies]`: `duckdb = { version = "=1.20000.1", features = ["bundled", "appender-arrow"] }`, `arrow-array = "=60.1.0"`, `arrow-buffer = "=60.1.0"`, `arrow-ipc = "=60.1.0"`, `arrow-schema = "=60.1.0"`, `pyo3 = { workspace = true }`, `tokio = { workspace = true }`, `vertebra-core = { path = "../vertebra-core" }`, `vertebra-db = { path = "../vertebra-db", features = ["gcp", "pyo3"] }`
- `vertebra._duckdb` (`src/rs/vertebra-duckdb/src/lib.rs` & `src/py/vertebra/_duckdb.pyi`):
  - `FREE_THREADED: bool`, `GIL_USED: bool`, `__version__: str`
  - `SyncDuckDBClient`: synchronous worker-backed DuckDB client and session pool (`connect`, `close`, `cursor`, `begin`, `commit`, `rollback`, `interrupt`, `cancel`, `execute`, `execute_many`, `execute_with_description`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `apply_gcs_secret`)
  - `AsyncDuckDBClient`: asynchronous completion-queue-backed DuckDB client and session pool returning `Awaitable[...]` via `spawn_rust_db_task_with_attr(..., "_vertebra_duckdb_completion_drainer")`

## Acceptance Criteria

- [x] `src/rs/vertebra-duckdb` compiles cleanly under `cargo test -p vertebra-duckdb --locked` and `cargo clippy -p vertebra-duckdb --all-targets --locked -- -D warnings`.
- [x] `mypy.stubtest vertebra._duckdb` passes with zero discrepancies against `src/py/vertebra/_duckdb.pyi`.
- [x] `SyncDuckDBClient` and `AsyncDuckDBClient` execute queries, positional/named binds, transactions, `interrupt()`/`cancel()`, and `DatabaseError` hierarchy mappings without holding the GIL.
- [x] `test_packaging_and_pgo.py`, `test_freethreaded_gil.py`, and `tools.scripts.review_setup validate` pass.

[^duckdb-rs]: `src/rs/vertebra-db/src/duckdb/mod.rs::DuckdbConfig` — DuckDB configuration and bootstrap pragma primitives in `vertebra-db`.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue`, `construct_py_arrow_cursor`, and independent event-loop drainer.
[^spanner-cargo]: `src/rs/vertebra-spanner/Cargo.toml` — Reference optional PyO3 extension Cargo manifest.
[^spanner-wheel]: `src/rs/vertebra-spanner/pyproject.toml` — Reference optional PyO3 Maturin wheel layout in `vertebra` namespace.
